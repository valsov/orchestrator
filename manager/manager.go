package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"

	"orchestrator/node"
	"orchestrator/scheduler"
	"orchestrator/store"
	"orchestrator/task"
	"orchestrator/worker"
)

type Manager struct {
	Pending       *queue.Queue
	TaskDb        store.Store[uuid.UUID, task.Task]
	EventDb       store.Store[uuid.UUID, task.TaskEvent]
	Workers       []string
	WorkerNodes   []*node.Node
	WorkerTaskMap map[string][]uuid.UUID
	TaskWorkerMap map[uuid.UUID]string
	Scheduler     scheduler.Scheduler
}

func New(workers []string, schedulerType string, storeType string) (*Manager, error) {
	workerTaskMap := make(map[string][]uuid.UUID)
	nodes := make([]*node.Node, len(workers))
	for i, worker := range workers {
		workerTaskMap[worker] = []uuid.UUID{}

		nodeApi := fmt.Sprintf("http://%s", worker)
		newNode := node.NewNode(worker, nodeApi, "worker")
		nodes[i] = &newNode
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

	var taskDb store.Store[uuid.UUID, task.Task]
	var taskEventDb store.Store[uuid.UUID, task.TaskEvent]
	switch storeType {
	case "memory":
		taskDb = store.NewMemoryStore[uuid.UUID, task.Task]()
		taskEventDb = store.NewMemoryStore[uuid.UUID, task.TaskEvent]()
	case "persisted":
		var err error
		taskDb, err = store.NewPersistedStore[uuid.UUID, task.Task]("manager_tasks.db", 0600, "tasks")
		if err != nil {
			return nil, err
		}
		taskEventDb, err = store.NewPersistedStore[uuid.UUID, task.TaskEvent]("manager_task_events.db", 0600, "taskEvents")
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported store type: %s", storeType)
	}

	return &Manager{
		Pending:       queue.New(),
		Workers:       workers,
		WorkerNodes:   nodes,
		TaskDb:        taskDb,
		EventDb:       taskEventDb,
		WorkerTaskMap: workerTaskMap,
		TaskWorkerMap: make(map[uuid.UUID]string),
		Scheduler:     sched,
	}, nil
}

func (m *Manager) Close() error {
	err1 := m.TaskDb.Close()
	err2 := m.EventDb.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (m *Manager) GetTasks() []task.Task {
	tasks, err := m.TaskDb.List()
	if err != nil {
		log.Printf("failed to get tasks from store: %v", err)
		return nil
	}
	return tasks
}

func (m *Manager) AddTask(tEvent task.TaskEvent) {
	m.Pending.Enqueue(tEvent)
}

func (m *Manager) ProcessTasks() {
	for {
		log.Print("starting queued tasks processing")
		m.sendWork()
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

func (m *Manager) CheckNodesStats() {
	for {
		log.Print("checking nodes stats")
		m.updateNodesStats()
		log.Print("nodes stats retrieval completed")
		time.Sleep(10 * time.Second)
	}
}

func (m *Manager) sendWork() {
	if m.Pending.Len() == 0 {
		return
	}

	dequeued := m.Pending.Dequeue()
	tEvent := dequeued.(task.TaskEvent)
	if err := m.EventDb.Put(tEvent.Id, tEvent); err != nil {
		log.Printf("failed to store dequeued task event, err: %v", err)
	}
	log.Printf("starting task processing: %v", tEvent.Task)

	taskWorker, found := m.TaskWorkerMap[tEvent.Id]
	if found {
		persistedTask, err := m.TaskDb.Get(tEvent.Task.Id)
		if err != nil {
			log.Printf("failed to retrieve task with Id %v from store, err: %v", tEvent.Task.Id, err)
			return
		}
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
	if err = m.TaskDb.Put(tEvent.Task.Id, tEvent.Task); err != nil {
		log.Printf("failed to store task, err: %v", err)
		return
	}

	jsonTaskEvent, err := json.Marshal(tEvent)
	if err != nil {
		log.Printf("failed to marshal task event %v, err: %v", tEvent, err)
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

func (m *Manager) updateNodesStats() {
	for _, node := range m.WorkerNodes {
		err := node.UpdateStats()
		if err != nil {
			log.Printf("failed to update node '%s' stats, err: %v", node.Name, err)
		}
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
	var wNode *node.Node
	for _, n := range m.WorkerNodes {
		if n.Name == worker {
			wNode = n
			break
		}
	}
	if wNode == nil {
		log.Printf("couldn't find worker node with name: %s", worker)
		return
	}

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

	wNode.TaskCount--
	log.Printf("task %s has been scheduled to stop", taskId)
}

func (m *Manager) updateTask(t *task.Task) {
	dbTask, err := m.TaskDb.Get(t.Id)
	if err != nil {
		log.Printf("failed to retrieve task %s from store, err: %v", t.Id, err)
		return
	}

	dbTask.State = t.State
	dbTask.StartTime = t.StartTime
	dbTask.FinishTime = t.FinishTime
	dbTask.ContainerId = t.ContainerId

	if err := m.TaskDb.Put(t.Id, dbTask); err != nil {
		log.Printf("failed to update task %s, err: %v", t.Id, err)
		return
	}

	log.Printf("task %s updated in local database", t.Id)
}

func (m *Manager) checkTasksHealth() {
	tasks := m.GetTasks()
	for _, t := range tasks {
		if t.RestartCount >= 3 {
			continue
		}

		if t.State == task.Failed {
			m.restartTask(t)
		}
	}
}

func (m *Manager) restartTask(t task.Task) {
	// Update task in store
	t.State = task.Scheduled
	t.RestartCount++
	if err := m.TaskDb.Put(t.Id, t); err != nil {
		log.Printf("failed to update task %s, err: %v", t.Id, err)
		return
	}

	tEvent := task.TaskEvent{
		Id:        uuid.New(),
		State:     task.Running,
		Timestamp: time.Now(),
		Task:      t,
	}
	data, err := json.Marshal(tEvent)
	if err != nil {
		log.Printf("unable to marshal task object: %v", tEvent)
		return
	}

	workerAddr := m.TaskWorkerMap[t.Id]
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

func (m *Manager) selectWorker(t task.Task) (*node.Node, error) {
	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no available candidates match resource request for task %v", t.Id)
	}
	scores := m.Scheduler.Score(t, candidates)
	return m.Scheduler.Pick(scores, candidates), nil
}
