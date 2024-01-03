package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"orchestrator/node"
	"orchestrator/scheduler"
	"orchestrator/store"
	"orchestrator/task"
	"orchestrator/worker"
)

type Manager struct {
	Pending       chan task.TaskEvent
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
		Pending:       make(chan task.TaskEvent, 10),
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
		log.Err(err).Msg("failed to get tasks from store")
		return nil
	}
	return tasks
}

func (m *Manager) AddTask(tEvent task.TaskEvent) {
	// Run inside a goroutine to avoid blocking API call if chan is full
	go func() {
		m.Pending <- tEvent
	}()
}

func (m *Manager) ProcessTasks() {
	log.Debug().Msg("starting queued tasks processing")
	for {
		t, ok := <-m.Pending
		if !ok {
			log.Debug().Msg("tasks channel closed, stop processing")
			return
		}

		m.sendWork(t)
	}
}

func (m *Manager) CheckTasksHealth() {
	for {
		log.Debug().Msg("checking tasks health")
		m.checkTasksHealth()
		log.Debug().Msg("tasks health check completed")
		time.Sleep(10 * time.Second)
	}
}

func (m *Manager) UpdateTasks() {
	for {
		log.Debug().Msg("checking for workers' tasks update")
		m.updateTasks()
		log.Debug().Msg("tasks update completed")
		time.Sleep(10 * time.Second)
	}
}

func (m *Manager) CheckNodesStats() {
	for {
		log.Debug().Msg("checking nodes stats")
		m.updateNodesStats()
		log.Debug().Msg("nodes stats retrieval completed")
		time.Sleep(10 * time.Second)
	}
}

func (m *Manager) sendWork(tEvent task.TaskEvent) {
	if err := m.EventDb.Put(tEvent.Id, tEvent); err != nil {
		log.Err(err).Msg("failed to store dequeued task event")
	}

	taskLogger := log.With().
		Str("task-id", tEvent.Task.Id.String()).
		Logger()
	taskLogger.Debug().Msg("starting task processing")

	taskWorker, found := m.TaskWorkerMap[tEvent.Id]
	if found {
		persistedTask, err := m.TaskDb.Get(tEvent.Task.Id)
		if err != nil {
			taskLogger.Err(err).Msg("failed to retrieve task from store")
			return
		}
		if tEvent.State != task.Completed {
			taskLogger.Error().
				Str("target-state", fmt.Sprintf("%v", tEvent.State)).
				Msg("invalid request: can't request other state transition than 'completed' for an existing task")
			return
		}
		if task.ValidStateTransition(persistedTask.State, tEvent.State) {
			m.stopTask(tEvent.Task.Id, taskWorker)
		} else {
			taskLogger.Error().
				Str("initial-state", fmt.Sprintf("%v", persistedTask.State)).
				Msg("invalid request: forbidden state transition to 'completed'")
		}
		return
	}

	wNode, err := m.selectWorker(tEvent.Task)
	if err != nil {
		taskLogger.Err(err).Msg("failed to select a worker to execute task")
		return
	}

	m.WorkerTaskMap[wNode.Name] = append(m.WorkerTaskMap[wNode.Name], tEvent.Task.Id)
	m.TaskWorkerMap[tEvent.Task.Id] = wNode.Name
	if err = m.TaskDb.Put(tEvent.Task.Id, tEvent.Task); err != nil {
		taskLogger.Err(err).Msg("failed to store task")
		return
	}

	jsonTaskEvent, err := json.Marshal(tEvent)
	if err != nil {
		taskLogger.Err(err).
			Interface("task-event", tEvent).
			Msg("failed to marshal task event")
		return
	}

	url := fmt.Sprintf("%s/tasks", wNode.Api)
	response, err := http.Post(url, "application/json", bytes.NewBuffer(jsonTaskEvent))
	if err != nil {
		taskLogger.Err(err).
			Str("node", wNode.Name).
			Str("url", url).
			Msg("failed to send post request")
		m.AddTask(tEvent) // Try again
		return
	}
	defer response.Body.Close()

	decoder := json.NewDecoder(response.Body)
	if response.StatusCode != http.StatusCreated {
		e := worker.ErrResponse{}
		err = decoder.Decode(&e)
		if err != nil {
			taskLogger.Err(err).Msg("failed to decode error message")
			return
		}
		taskLogger.Error().
			Int("status-code", response.StatusCode).
			Str("node", wNode.Name).
			Str("url", url).
			Str("message", e.Message).
			Msg("received an unexpected response code from worker")
		return
	}

	t := task.Task{}
	err = decoder.Decode(&t)
	if err != nil {
		taskLogger.Err(err).Msg("error decoding task reponse")
	} else {
		wNode.TaskCount++
	}
}

func (m *Manager) updateNodesStats() {
	for _, node := range m.WorkerNodes {
		err := node.UpdateStats()
		if err != nil {
			log.Err(err).Str("node", node.Name).Msg("failed to update node stats")
		}
	}
}

func (m *Manager) updateTasks() {
	for _, worker := range m.Workers {
		workerLogger := log.Logger.
			With().
			Str("worker", worker).
			Logger()
		workerLogger.Debug().Msg("checking worker for task updates")
		url := fmt.Sprintf("http://%s/tasks", worker)
		response, err := http.Get(url)
		if err != nil {
			workerLogger.Err(err).Msg("failed to send get request")
			continue
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			workerLogger.Error().
				Int("status-code", response.StatusCode).
				Msg("received an unexpected response code from worker")
			continue
		}

		decoder := json.NewDecoder(response.Body)
		var tasks []*task.Task
		err = decoder.Decode(&tasks)
		if err != nil {
			workerLogger.Err(err).Msg("error decoding tasks reponse")
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

	taskLogger := log.Logger.
		With().
		Str("task-id", taskId.String()).
		Str("worker", worker).
		Logger()
	if wNode == nil {
		taskLogger.Error().Msg("couldn't find worker node")
		return
	}

	url := fmt.Sprintf("http://%s/tasks/%v", worker, taskId)
	request, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		taskLogger.Err(err).Msg("error creating task deletion request")
		return
	}

	client := http.Client{}
	response, err := client.Do(request)
	if err != nil {
		taskLogger.Err(err).Msg("task deletion request sending failed")
		return
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		taskLogger.Error().
			Int("status-code", response.StatusCode).
			Msg("received an unexpected response code from worker")
		return
	}

	wNode.TaskCount--
	taskLogger.Info().Msg("task has been scheduled to stop")
}

func (m *Manager) updateTask(t *task.Task) {
	taskLogger := log.Logger.
		With().
		Str("task-id", t.Id.String()).
		Logger()

	dbTask, err := m.TaskDb.Get(t.Id)
	if err != nil {
		taskLogger.Err(err).Msg("failed to retrieve task from store")
		return
	}

	dbTask.State = t.State
	dbTask.StartTime = t.StartTime
	dbTask.FinishTime = t.FinishTime
	dbTask.ContainerId = t.ContainerId

	if err := m.TaskDb.Put(t.Id, dbTask); err != nil {
		taskLogger.Err(err).Msg("failed to update task")
		return
	}

	taskLogger.Debug().Msg("task updated in local database")
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
	taskLogger := log.Logger.
		With().
		Str("task-id", t.Id.String()).
		Logger()

	// Update task in store
	t.State = task.Scheduled
	t.RestartCount++
	if err := m.TaskDb.Put(t.Id, t); err != nil {
		taskLogger.Err(err).Msg("failed to update task")
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
		taskLogger.Err(err).
			Interface("task-event", tEvent).
			Msg("unable to marshal task object")
		return
	}

	workerAddr := m.TaskWorkerMap[t.Id]
	url := fmt.Sprintf("http://%s/tasks", workerAddr)
	response, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		taskLogger.Err(err).
			Str("worker", workerAddr).
			Msg("error sending task creation request to worker")
		return
	}
	defer response.Body.Close()

	d := json.NewDecoder(response.Body)
	if response.StatusCode != http.StatusCreated {
		e := worker.ErrResponse{}
		err := d.Decode(&e)
		if err != nil {
			taskLogger.Err(err).Msg("error decoding error response")
			return
		}
		taskLogger.Error().
			Int("status-code", response.StatusCode).
			Str("message", e.Message).
			Msg("received error response from worker")
		return
	}

	newTask := task.Task{}
	err = d.Decode(&newTask)
	if err != nil {
		taskLogger.Err(err).Msg("error decoding worker response")
		return
	}
}

func (m *Manager) selectWorker(t task.Task) (*node.Node, error) {
	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no available candidates match resource request for task %v", t.Id)
	}
	scores := m.Scheduler.Score(t, candidates)
	return m.Scheduler.Pick(scores, candidates), nil
}
