package manager

import (
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"

	"orchestrator/task"
)

type Manager struct {
	Pending       queue.Queue
	TaskDb        map[string][]task.Task
	EventDb       map[string][]task.TaskEvent
	Workers       []string
	WorkerTaskMap map[string][]uuid.UUID
	TaskWorkerMap map[uuid.UUID]string
}

func (m *Manager) SelectWorker() {
	panic("TODO")
}

func (m *Manager) UpdateTasks() {
	panic("TODO")
}

func (m *Manager) SendWork() {
	panic("TODO")
}
