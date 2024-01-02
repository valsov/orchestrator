package worker

import (
	"fmt"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

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
		log.Err(err).Msg("error retrieving tasks from store")
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
			log.Debug().Msg("no tasks to run")
		} else {
			err := w.runNextTask()
			if err != nil {
				log.Err(err).Msg("error processing task")
			}
		}
		time.Sleep(10 * time.Second)
	}
}

func (w *Worker) UpdateTasks() {
	for {
		log.Debug().Msg("checking tasks status")
		w.updateTasks()
		log.Debug().Msg("tasks status check completed")
		time.Sleep(10 * time.Second)
	}
}

func (w *Worker) StartTask(t task.Task) error {
	t.StartTime = time.Now().UTC()
	config := task.NewConfig(t)
	d := task.NewDocker(config)

	containerId, err := d.Run()
	taskLogger := log.With().
		Str("task-id", t.Id.String()).
		Logger()
	if err != nil {
		taskLogger.Err(err).Msg("error running task")
		t.State = task.Failed
		if err := w.Db.Put(t.Id, t); err != nil {
			taskLogger.Err(err).Msg("failed to store task")
		}
		return err
	}

	t.ContainerId = containerId
	t.State = task.Running
	if err := w.Db.Put(t.Id, t); err != nil {
		taskLogger.Err(err).Msg("failed to store task")
	}

	taskLogger.Info().Str("container-id", t.ContainerId).Msg("created and started container")
	return err
}

func (w *Worker) StopTask(t task.Task) error {
	config := task.NewConfig(t)
	d := task.NewDocker(config)

	err := d.Stop(t.ContainerId)
	taskLogger := log.With().
		Str("task-id", t.Id.String()).
		Str("container-id", t.ContainerId).
		Logger()
	if err != nil {
		taskLogger.Err(err).Msg("error stopping container")
		return err
	}

	t.FinishTime = time.Now().UTC()
	t.State = task.Completed
	if err := w.Db.Put(t.Id, t); err != nil {
		taskLogger.Err(err).Msg("failed to store task")
	}
	taskLogger.Info().Msg("stopped and removed container")
	return nil
}

func (w *Worker) CollectStats() {
	for {
		w.Stats = stats.GetStats()
		time.Sleep(10 * time.Second)
	}
}

func (w *Worker) InspectTask(t task.Task) (types.ContainerJSON, error) {
	config := task.NewConfig(t)
	d := task.NewDocker(config)
	return d.Inspect(t.ContainerId)
}

func (w *Worker) runNextTask() error {
	t := w.Queue.Dequeue()
	if t == nil {
		log.Debug().Msg("no task in queue")
		return nil
	}

	queuedTask := t.(task.Task)
	storedTask, err := w.Db.Get(queuedTask.Id)
	if err != nil {
		storedTask = queuedTask
		if err := w.Db.Put(storedTask.Id, storedTask); err != nil {
			log.Err(err).Str("task-id", storedTask.Id.String()).Msg("failed to store task")
		}
	}

	if !task.ValidStateTransition(storedTask.State, queuedTask.State) {
		return fmt.Errorf("invalid state transition from %v to %v", storedTask.State, queuedTask.State)
	}

	switch queuedTask.State {
	case task.Scheduled:
		if queuedTask.ContainerId != "" {
			// Case of a restart when the container is still running
			err = w.StopTask(queuedTask)
			if err != nil {
				log.Err(err).Str("task-id", storedTask.Id.String()).Msg("failed to stop task")
				return err
			}
		}
		return w.StartTask(queuedTask)
	case task.Completed:
		return w.StopTask(queuedTask)
	default:
		return fmt.Errorf("running a task shouldn't be represented with a %v state", queuedTask.State)
	}
}

func (w *Worker) updateTasks() {
	tasks, err := w.Db.List()
	if err != nil {
		log.Err(err).Msg("failed to retrieve task list from store")
		return
	}
	for _, t := range tasks {
		if t.State != task.Running {
			continue
		}

		taskLogger := log.With().
			Str("task-id", t.Id.String()).
			Logger()
		container, err := w.InspectTask(t)
		update := false
		if err != nil {
			taskLogger.Err(err).Msg("task inspection error")
		} else if container.State.Status == "exited" {
			taskLogger.Error().Msg("container exited for task in running state")
			t.State = task.Failed
			update = true
		} else {
			for port, binds := range container.NetworkSettings.NetworkSettingsBase.Ports {
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
			taskLogger.Err(err).Msg("failed to store task")
		}
	}
}
