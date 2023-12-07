package worker

import (
	"fmt"
	"log"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"

	"orchestrator/task"
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        map[uuid.UUID]*task.Task
	TaskCount int
}

func (w *Worker) CollectStats() {
	panic("TODO")
}

func (w *Worker) RunTask() task.DockerResult {
	t := w.Queue.Dequeue()
	if t == nil {
		log.Println("No task in queue")
		return task.DockerResult{}
	}

	queuedTask := t.(task.Task)

	storedTask := w.Db[queuedTask.Id]
	if storedTask == nil {
		storedTask = &queuedTask
		w.Db[storedTask.Id] = storedTask
	}

	var result task.DockerResult
	if task.ValidStateTransition(storedTask.State, queuedTask.State) {
		switch queuedTask.State {
		case task.Scheduled:
			result = w.StartTask(queuedTask)
		case task.Completed:
			result = w.StopTask(queuedTask)
		default:
			result.Error = fmt.Errorf("running a task shouldn't be represented with a %v state", queuedTask.State)
		}
	} else {
		result.Error = fmt.Errorf("invalid state transition from %v to %v", storedTask.State, queuedTask.State)
	}

	return result
}

func (w *Worker) StartTask(t task.Task) task.DockerResult {
	panic("TODO")
}

func (w *Worker) StopTask(t task.Task) task.DockerResult {
	panic("TODO")
}
