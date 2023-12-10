package worker

import (
	"fmt"
	"log"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"

	"orchestrator/task"
)

type Worker struct {
	Name  string
	Queue queue.Queue
	Db    map[uuid.UUID]*task.Task
	//TaskCount int
	Stats *Stats
}

func (w *Worker) GetTasks() []*task.Task {
	result := make([]*task.Task, 0, len(w.Db))
	for _, t := range w.Db {
		result = append(result, t)
	}
	return result
}

func (w *Worker) AddTask(t *task.Task) {
	w.Queue.Enqueue(t)
}

func (w *Worker) RunTasks() {
	for {
		if w.Queue.Len() == 0 {
			log.Print("no tasks to run")
		} else {
			result := w.runNextTask()
			if result.Error != nil {
				log.Printf("error processing task: %v", result.Error)
			}
		}
		time.Sleep(10 * time.Second)
	}
}

func (w *Worker) StartTask(t *task.Task) task.DockerResult {
	t.StartTime = time.Now().UTC()
	config := task.NewConfig(t)
	d := task.NewDocker(config)

	result := d.Run()
	if result.Error != nil {
		log.Printf("Error running task %v: %v\n", t.Id, result.Error)
		t.State = task.Failed
		w.Db[t.Id] = t
		return result
	}

	t.ContainerId = result.ContainerId
	t.State = task.Running
	w.Db[t.Id] = t

	return result
}

func (w *Worker) StopTask(t *task.Task) task.DockerResult {
	config := task.NewConfig(t)
	d := task.NewDocker(config)

	result := d.Stop(t.ContainerId)
	if result.Error != nil {
		log.Printf("Error stopping container %s: %v", t.ContainerId, result.Error)
		return result
	}

	t.FinishTime = time.Now().UTC()
	t.State = task.Completed
	w.Db[t.Id] = t
	log.Printf("Stopped and removed container %s for task %v\n", t.ContainerId, t.Id)
	return result
}

func (w *Worker) CollectStats() {
	for {
		w.Stats = GetStats()
		time.Sleep(10 * time.Second)
	}
}

func (w *Worker) runNextTask() task.DockerResult {
	t := w.Queue.Dequeue()
	if t == nil {
		log.Println("No task in queue")
		return task.DockerResult{}
	}

	queuedTask := t.(*task.Task)

	storedTask := w.Db[queuedTask.Id]
	if storedTask == nil {
		storedTask = queuedTask
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
