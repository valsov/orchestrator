package worker

import (
	"fmt"
	"log"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"

	"orchestrator/stats"
	"orchestrator/store"
	"orchestrator/task"
)

type Worker struct {
	Name  string
	Queue *queue.Queue
	Db    store.Store[uuid.UUID, task.Task]
	Stats *stats.Stats
}

func New(name string, storeType string) (*Worker, error) {
	var db store.Store[uuid.UUID, task.Task]
	switch storeType {
	case "memory":
		db = store.NewMemoryStore[uuid.UUID, task.Task]()
	case "persisted":
		var err error
		dbFileName := fmt.Sprintf("%s.db", name)
		db, err = store.NewPersistedStore[uuid.UUID, task.Task](dbFileName, 0600, "tasks")
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported store type: %s", storeType)
	}

	return &Worker{
		Name:  name,
		Queue: queue.New(),
		Db:    db,
	}, nil
}

func (w *Worker) Close() error {
	return w.Db.Close()
}

func (w *Worker) GetTasks() []task.Task {
	taskList, err := w.Db.List()
	if err != nil {
		log.Printf("error retrieving tasks from store: %v", err)
		return nil
	}

	return taskList
}

func (w *Worker) AddTask(t task.Task) {
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

func (w *Worker) UpdateTasks() {
	for {
		log.Print("checking tasks status")
		w.updateTasks()
		log.Print("tasks status check completed")
		time.Sleep(10 * time.Second)
	}
}

func (w *Worker) StartTask(t task.Task) task.DockerResult {
	t.StartTime = time.Now().UTC()
	config := task.NewConfig(t)
	d := task.NewDocker(config)

	result := d.Run()
	if result.Error != nil {
		log.Printf("error running task %v: %v", t.Id, result.Error)
		t.State = task.Failed
		if err := w.Db.Put(t.Id, t); err != nil {
			log.Printf("failed to store task %v, err: %v", t.Id, err)
		}
		return result
	}

	t.ContainerId = result.ContainerId
	t.State = task.Running
	if err := w.Db.Put(t.Id, t); err != nil {
		log.Printf("failed to store task %v, err: %v", t.Id, err)
	}

	return result
}

func (w *Worker) StopTask(t task.Task) task.DockerResult {
	config := task.NewConfig(t)
	d := task.NewDocker(config)

	result := d.Stop(t.ContainerId)
	if result.Error != nil {
		log.Printf("error stopping container %s: %v", t.ContainerId, result.Error)
		return result
	}

	t.FinishTime = time.Now().UTC()
	t.State = task.Completed
	if err := w.Db.Put(t.Id, t); err != nil {
		log.Printf("failed to store task %v, err: %v", t.Id, err)
	}
	log.Printf("stopped and removed container %s for task %v", t.ContainerId, t.Id)
	return result
}

func (w *Worker) CollectStats() {
	for {
		w.Stats = stats.GetStats()
		time.Sleep(10 * time.Second)
	}
}

func (w *Worker) InspectTask(t task.Task) task.DockerInspectReponse {
	config := task.NewConfig(t)
	d := task.NewDocker(config)
	return d.Inspect(t.ContainerId)
}

func (w *Worker) runNextTask() task.DockerResult {
	t := w.Queue.Dequeue()
	if t == nil {
		log.Print("no task in queue")
		return task.DockerResult{}
	}

	queuedTask := t.(task.Task)

	storedTask, err := w.Db.Get(queuedTask.Id)
	if err != nil {
		storedTask = queuedTask
		if err := w.Db.Put(storedTask.Id, storedTask); err != nil {
			log.Printf("failed to store task %v, err: %v", storedTask.Id, err)
		}
	}

	var result task.DockerResult
	if task.ValidStateTransition(storedTask.State, queuedTask.State) {
		switch queuedTask.State {
		case task.Scheduled:
			if queuedTask.ContainerId != "" {
				// Case of a restart when the container is still running
				result = w.StopTask(queuedTask)
				if result.Error != nil {
					log.Printf("failed to stop task %v: %v", queuedTask.Id, result.Error)
					return result
				}
			}
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

func (w *Worker) updateTasks() {
	tasks, err := w.Db.List()
	if err != nil {
		log.Printf("failed to retrieve task list from store, err: %v", err)
		return
	}
	for _, t := range tasks {
		if t.State != task.Running {
			continue
		}

		inspectRes := w.InspectTask(t)
		update := false
		if inspectRes.Error != nil {
			log.Printf("task inspection error [%v]: %v", t.Id, inspectRes.Error)
		} else if inspectRes.Container == nil {
			log.Printf("no container for running task [%v]", t.Id)
			t.State = task.Failed
			update = true
		} else if inspectRes.Container.State.Status == "exited" {
			log.Printf("container exited for running task [%v]", t.Id)
			t.State = task.Failed
			update = true
		} else {
			for port, binds := range inspectRes.Container.NetworkSettings.NetworkSettingsBase.Ports {
				if len(binds) != 0 {
					t.PortBindings[string(port)] = binds[0].HostPort
					update = true
				}
			}
		}
		if !update {
			continue
		}

		if err := w.Db.Put(t.Id, t); err != nil {
			log.Printf("failed to store task %v, err: %v", t.Id, err)
		}
	}
}
