package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"

	"orchestrator/node"
	"orchestrator/scheduler"
	"orchestrator/task"
	"orchestrator/worker"
)

type Manager struct {
	Pending       queue.Queue
	TaskDb        map[uuid.UUID]*task.Task
	EventDb       map[uuid.UUID]*task.TaskEvent
	Workers       []string
	WorkerNodes   []*node.Node
	WorkerTaskMap map[string][]uuid.UUID
	TaskWorkerMap map[uuid.UUID]string
	Scheduler     scheduler.Scheduler
}

func New(workers []string, schedulerType string) (*Manager, error) {
	workerTaskMap := make(map[string][]uuid.UUID)
	nodes := make([]*node.Node, len(workers))
	for _, worker := range workers {
		workerTaskMap[worker] = []uuid.UUID{}

		nodeApi := fmt.Sprintf("http://%s", worker)
		newNode := node.NewNode(worker, nodeApi, "worker")
		nodes = append(nodes, &newNode)
	}

	var sched scheduler.Scheduler
	switch schedulerType {
	case "roundrobin":
		sched = &scheduler.RoundRobin{}
	case "epvm":
		sched = &scheduler.Epvm{}
	default:
		return nil, fmt.Errorf("unsupported scheduler type: %s", schedulerType)
	}

	return &Manager{
		Pending:       *queue.New(),
		Workers:       workers,
		WorkerNodes:   nodes,
		TaskDb:        make(map[uuid.UUID]*task.Task),
		EventDb:       make(map[uuid.UUID]*task.TaskEvent),
		WorkerTaskMap: workerTaskMap,
		TaskWorkerMap: make(map[uuid.UUID]string),
		Scheduler:     sched,
	}, nil
}

func (m *Manager) GetTasks() []*task.Task {
	result := make([]*task.Task, 0, len(m.TaskDb))
	for _, t := range m.TaskDb {
		result = append(result, t)
	}
	return result
}

func (m *Manager) AddTask(tEvent task.TaskEvent) {
	m.Pending.Enqueue(tEvent)
}

func (m *Manager) SendWork() {
	if m.Pending.Len() == 0 {
		return
	}

	dequeued := m.Pending.Dequeue()
	tEvent := dequeued.(task.TaskEvent)
	m.EventDb[tEvent.Id] = &tEvent
	log.Printf("starting task processing: %v", tEvent.Task)

	taskWorker, found := m.TaskWorkerMap[tEvent.Id]
	if found {
		persistedTask := m.TaskDb[tEvent.Task.Id]
		if tEvent.State != task.Completed {
			log.Printf("invalid request: existing task %v cannot transition to state %v", persistedTask.Id, tEvent.State)
			return
		}
		if task.ValidStateTransition(persistedTask.State, tEvent.State) {
			m.stopTask(tEvent.Task.Id, taskWorker)
		} else {
			log.Printf("invalid request: existing task %v in state %v cannot transition to the completed state", persistedTask.Id, persistedTask.State)
		}
		return
	}

	wNode, err := m.selectWorker(tEvent.Task)
	if err != nil {
		log.Printf("failed to select a worker to execute task %v", tEvent.Task.Id)
		return
	}

	m.WorkerTaskMap[wNode.Name] = append(m.WorkerTaskMap[wNode.Name], tEvent.Task.Id)
	m.TaskWorkerMap[tEvent.Task.Id] = wNode.Name
	m.TaskDb[tEvent.Task.Id] = &tEvent.Task

	jsonTaskEvent, err := json.Marshal(tEvent)
	if err != nil {
		log.Printf("failed to marshal task event %v", tEvent)
		return
	}

	url := fmt.Sprintf("http://%s/tasks", wNode.Name)
	response, err := http.Post(url, "application/json", bytes.NewBuffer(jsonTaskEvent))
	if err != nil {
		log.Printf("failed to send post request to %s", wNode.Name)
		m.Pending.Enqueue(tEvent) // Try again
		return
	}
	defer response.Body.Close()

	decoder := json.NewDecoder(response.Body)
	if response.StatusCode != http.StatusCreated {
		e := worker.ErrResponse{}
		err = decoder.Decode(&e)
		if err != nil {
			log.Print("failed to decode error message")
			return
		}
		log.Printf("received an unexpected response code (%d) from worker %s: %v", response.StatusCode, wNode.Name, e.Message)
		return
	}

	t := task.Task{}
	err = decoder.Decode(&t)
	if err != nil {
		log.Printf("error decoding task reponse: %v", err)
	} else {
		wNode.TaskCount++
		log.Printf("%#v", t)
	}
}

func (m *Manager) ProcessTasks() {
	for {
		log.Print("starting queued tasks processing")
		m.SendWork()
		log.Print("queued tasks processing completed")
		time.Sleep(10 * time.Second)
	}
}

func (m *Manager) CheckTasksHealth() {
	for {
		log.Print("checking tasks health")
		m.checkTasksHealth()
		log.Print("tasks health check completed")
		time.Sleep(10 * time.Second)
	}
}

func (m *Manager) UpdateTasks() {
	for {
		log.Print("checking for workers' tasks update")
		m.updateTasks()
		log.Print("tasks update completed")
		time.Sleep(10 * time.Second)
	}
}

func (m *Manager) updateTasks() {
	for _, worker := range m.Workers {
		log.Printf("checking worker %s for task updates", worker)
		url := fmt.Sprintf("http://%s/tasks", worker)
		response, err := http.Get(url)
		if err != nil {
			log.Printf("failed to send get request to %s: %v", worker, err)
			continue
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			log.Printf("received an unexpected response code (%d) from worker %s", response.StatusCode, worker)
			continue
		}

		decoder := json.NewDecoder(response.Body)
		var tasks []*task.Task
		err = decoder.Decode(&tasks)
		if err != nil {
			log.Printf("error decoding tasks reponse: %v", err)
			continue
		}

		for _, t := range tasks {
			m.updateTask(t)
		}
	}
}

func (m *Manager) stopTask(taskId uuid.UUID, worker string) {
	url := fmt.Sprintf("http://%s/tasks/%v", worker, taskId)
	request, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		log.Printf("error creating task deletion request: %v", err)
		return
	}

	client := http.Client{}
	response, err := client.Do(request)
	if err != nil {
		log.Printf("task deletion request sending failed: %v", err)
		return
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		log.Printf("received an unexpected response code (%d) from worker %s", response.StatusCode, worker)
		return
	}

	log.Printf("task %s has been scheduled to stop", taskId)
}

func (m *Manager) updateTask(t *task.Task) {
	dbTask, found := m.TaskDb[t.Id]
	if !found {
		log.Printf("task %s not found in local database", t.Id)
		return
	}

	dbTask.State = t.State
	dbTask.StartTime = t.StartTime
	dbTask.FinishTime = t.FinishTime
	dbTask.ContainerId = t.ContainerId
	log.Printf("task %s updated in local database", t.Id)
}

func (m *Manager) checkTasksHealth() {
	for _, t := range m.TaskDb {
		if t.RestartCount >= 3 {
			continue
		}

		if t.State == task.Running {
			err := m.checkTaskHealth(*t)
			if err != nil {
				log.Printf("health check failed for %v: %v", t.Id, err)
				m.restartTask(t)
			}
		} else if t.State == task.Failed {
			m.restartTask(t)
		}
	}
}

func (m *Manager) checkTaskHealth(t task.Task) error {
	workerAddr := m.TaskWorkerMap[t.Id]
	workerHost := strings.Split(workerAddr, ":")
	hostPort, err := getHostPort(t.PortBindings)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s:%s%s", workerHost[0], hostPort, t.HealthCheck)
	response, err := http.Get(url)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("health check call returned unexpected status code: %d (%s)", response.StatusCode, response.Status)
	}

	log.Printf("task %v is healthy", t.Id)
	return nil
}

func (m *Manager) restartTask(t *task.Task) {
	workerAddr := m.TaskWorkerMap[t.Id]
	t.State = task.Scheduled
	t.RestartCount++
	m.TaskDb[t.Id] = t // Ensure the task state is updated

	tEvent := task.TaskEvent{
		Id:        uuid.New(),
		State:     task.Running,
		Timestamp: time.Now(),
		Task:      *t,
	}
	data, err := json.Marshal(tEvent)
	if err != nil {
		log.Printf("unable to marshal task object: %v", tEvent)
		return
	}

	url := fmt.Sprintf("http://%s/tasks", workerAddr)
	response, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("error connecting to %s: %v", workerAddr, err)
		return
	}
	defer response.Body.Close()

	d := json.NewDecoder(response.Body)
	if response.StatusCode != http.StatusCreated {
		e := worker.ErrResponse{}
		err := d.Decode(&e)
		if err != nil {
			log.Printf("error decoding error response: %v", err)
			return
		}
		log.Printf("response error [%d]: %s", e.HTTPStatusCode, e.Message)
		return
	}

	newTask := task.Task{}
	err = d.Decode(&newTask)
	if err != nil {
		log.Printf("error decoding response: %v", err)
		return
	}
	log.Printf("%#v", newTask)
}

func getHostPort(pb map[string]string) (string, error) {
	for _, port := range pb {
		return port, nil
	}
	return "", errors.New("port map is empty")
}

func (m *Manager) selectWorker(t task.Task) (*node.Node, error) {
	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no available candidates match resource request for task %v", t.Id)
	}
	scores := m.Scheduler.Score(t, candidates)
	return m.Scheduler.Pick(scores, candidates), nil
}
