package worker

import (
	"fmt"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"orchestrator/stats"
	"orchestrator/store"
	"orchestrator/task"
)

// Worker manages the execution of tasks
type Worker struct {
	// Name of the worker
	Name string
	// Pending tasks to be executed
	Pending chan task.Task
	// Tasks store
	Db store.Store[uuid.UUID, task.Task]
	// Stats of the worker
	Stats *stats.Stats
}

// Create a new worker with the given name and store type
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
		Name:    name,
		Pending: make(chan task.Task, 10),
		Db:      db,
	}, nil
}

// Cleanup the worker's resources
func (w *Worker) Close() error {
	return w.Db.Close()
}

// Retrieve all tasks from the data store
func (w *Worker) GetTasks() []task.Task {
	taskList, err := w.Db.List()
	if err != nil {
		log.Err(err).Msg("error retrieving tasks from store")
		return nil
	}

	return taskList
}

// Add a task to the pending queue
func (w *Worker) AddTask(t task.Task) {
	// Run inside a goroutine to avoid blocking API call if chan is full
	go func() {
		w.Pending <- t
	}()
}

// Start the pending tasks execution loop
func (w *Worker) RunTasks() {
	log.Debug().Msg("starting queued tasks processing")
	for {
		t, ok := <-w.Pending
		if !ok {
			log.Debug().Msg("tasks channel closed, stop processing")
			return
		}

		err := w.runTask(t)
		if err != nil {
			log.Err(err).Msg("error processing task")
		}
	}
}

// Start the tasks update loop, it updates the status and informations of registered tasks
func (w *Worker) UpdateTasks() {
	for {
		log.Debug().Msg("checking tasks status")
		w.updateTasks()
		log.Debug().Msg("tasks status check completed")
		time.Sleep(10 * time.Second)
	}
}

// Start the stats collection loop
func (w *Worker) CollectStats() {
	for {
		w.Stats = stats.GetStats()
		time.Sleep(10 * time.Second)
	}
}

// Decide if the given task should be started or stopped and execute the corresponding action
func (w *Worker) runTask(queuedTask task.Task) error {
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
			err = w.stopTask(queuedTask)
			if err != nil {
				log.Err(err).Str("task-id", storedTask.Id.String()).Msg("failed to stop task")
				return err
			}
		}
		return w.startTask(queuedTask)
	case task.Completed:
		return w.stopTask(queuedTask)
	default:
		return fmt.Errorf("running a task shouldn't be represented with a %v state", queuedTask.State)
	}
}

// Start a task by creating and starting a container for it
func (w *Worker) startTask(t task.Task) error {
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

// Stop a task by stopping and removing the linked container
func (w *Worker) stopTask(t task.Task) error {
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

// Inspect the container related to the given task
func (w *Worker) inspectTask(t task.Task) (types.ContainerJSON, error) {
	config := task.NewConfig(t)
	d := task.NewDocker(config)
	return d.Inspect(t.ContainerId)
}

// Update the status and other informations of all registered tasks
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
		container, err := w.inspectTask(t)
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
