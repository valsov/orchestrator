package manager

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"orchestrator/store"
	"orchestrator/task"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type ErrResponse struct {
	HTTPStatusCode int
	Message        string
}

func (a *Api) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	data := json.NewDecoder(r.Body)

	tEvent := task.TaskEvent{}
	err := data.Decode(&tEvent)
	if err != nil {
		log.Err(err).Msg("start task handler error: failed to unmarshall request body") // todo: add body to log props
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrResponse{
			Message:        fmt.Sprintf("error unmarshalling request body: %v", err),
			HTTPStatusCode: http.StatusBadRequest,
		})
		return
	}

	a.Manager.AddTask(tEvent)
	log.Info().Str("task-id", tEvent.Task.Id.String()).Msg("task queued for creation")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(tEvent.Task)
}

func (a *Api) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskId := chi.URLParam(r, "taskId")
	if taskId == "" {
		log.Debug().Msg("taskId parameter is missing")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	taskUuid, err := uuid.Parse(taskId)
	if err != nil {
		log.Debug().Msg("taskId parameter isn't a valid uuid")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	t, err := a.Manager.TaskDb.Get(taskUuid)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			log.Debug().Str("task-id", taskUuid.String()).Msg("task not found in store")
			w.WriteHeader(http.StatusNotFound)
		} else {
			log.Err(err).Str("task-id", taskUuid.String()).Msg("failed to retrieve task from store")
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}

	t.State = task.Completed
	tEvent := task.TaskEvent{
		Id:        uuid.New(),
		State:     task.Completed,
		Timestamp: time.Now().UTC(),
		Task:      t,
	}
	a.Manager.AddTask(tEvent)

	log.Info().Str("task-id", tEvent.Task.Id.String()).Msg("task stop request queued")
	w.WriteHeader(http.StatusNoContent)
}

func (a *Api) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(a.Manager.GetTasks())
}

func (a *Api) GetNodesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(a.Manager.WorkerNodes)
}
