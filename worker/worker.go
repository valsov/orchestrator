package worker

import (
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"

	"orchestrator/task"
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        map[uuid.UUID]task.Task
	TaskCount int
}

func (w *Worker) CollectStats() {
	panic("TODO")
}

func (w *Worker) RunTask() {
	panic("TODO")
}

func (w *Worker) StartTask() {
	panic("TODO")
}

func (w *Worker) StopTask() {
	panic("TODO")
}
