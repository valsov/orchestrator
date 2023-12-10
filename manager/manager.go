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

	"orchestrator/task"
	"orchestrator/worker"
)

type Manager struct {
	Pending       queue.Queue
	TaskDb        map[uuid.UUID]*task.Task
	EventDb       map[uuid.UUID]*task.TaskEvent
	Workers       []string
	WorkerTaskMap map[string][]uuid.UUID
	TaskWorkerMap map[uuid.UUID]string
	LastWorker    int // Temporary solution to the scheduling need
}

func New(workers []string) *Manager {
	workerTaskMap := make(map[string][]uuid.UUID)
	for worker := range workers {
		workerTaskMap[workers[worker]] = []uuid.UUID{}
	}

	return &Manager{
		Pending:       *queue.New(),
		Workers:       workers,
		TaskDb:        make(map[uuid.UUID]*task.Task),
		EventDb:       make(map[uuid.UUID]*task.TaskEvent),
		WorkerTaskMap: workerTaskMap,
		TaskWorkerMap: make(map[uuid.UUID]string),
	}
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

	w := m.SelectWorker()
	dequeued := m.Pending.Dequeue()
	tEvent := dequeued.(task.TaskEvent)
	log.Printf("starting task processing: %v", tEvent.Task)

	m.EventDb[tEvent.Id] = &tEvent
	m.WorkerTaskMap[w] = append(m.WorkerTaskMap[w], tEvent.Task.Id)
	m.TaskWorkerMap[tEvent.Task.Id] = w
	m.TaskDb[tEvent.Task.Id] = &tEvent.Task

	jsonTaskEvent, err := json.Marshal(tEvent)
	if err != nil {
		log.Printf("failed to marshal task event %v", tEvent)
		return
	}

	url := fmt.Sprintf("http://%s/tasks", w)
	response, err := http.Post(url, "application/json", bytes.NewBuffer(jsonTaskEvent))
	if err != nil {
		log.Printf("failed to send post request to %s", w)
		m.Pending.Enqueue(tEvent) // Try again
		return
	}
	defer response.Body.Close()

	decoder := json.NewDecoder(response.Body)
	if response.StatusCode != http.StatusCreated {
		e := worker.ErrResponse{}
		err = decoder.Decode(&e)
		if err != nil {
			log.Println("failed to decode error message")
			return
		}
		log.Printf("received an unexpected response code (%d) from worker %s: %v", response.StatusCode, w, e.Message)
		return
	}

	t := task.Task{}
	err = decoder.Decode(&t)
	if err != nil {
		log.Printf("error decoding task reponse: %v", err)
	} else {
		log.Printf("%#v", t)
	}
}

func (m *Manager) SelectWorker() string {
	var newWorker int
	if m.LastWorker == len(m.Workers)-1 {
		newWorker = 0
	} else {
		newWorker = m.LastWorker + 1
	}
	m.LastWorker = newWorker

	return m.Workers[newWorker]
}

func (m *Manager) ProcessTasks() {
	for {
		log.Print("starting queued tasks processing")
		m.SendWork()
		log.Print("queued tasks processing completed")
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
