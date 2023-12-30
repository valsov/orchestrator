package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"orchestrator/task"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
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
		errMessage := fmt.Sprintf("error unmarshalling request body: %v", err)
		log.Print(errMessage)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrResponse{
			Message:        errMessage,
			HTTPStatusCode: http.StatusBadRequest,
		})
		return
	}

	a.Worker.AddTask(tEvent.Task)
	log.Printf("[w] added task %v", tEvent.Task.Id)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(tEvent.Task)
}

func (a *Api) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskId := chi.URLParam(r, "taskId")
	if taskId == "" {
		log.Print("taskId parameter is missing")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	taskUuid, err := uuid.Parse(taskId)
	if err != nil {
		log.Printf("taskId parameter isn't a valid uuid: %s", taskId)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	t, err := a.Worker.Db.Get(taskUuid)
	if err != nil {
		log.Printf("failed to retrieve task with id %v from store, err: %v", taskUuid, err)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	t.State = task.Completed
	a.Worker.AddTask(t) // Submit deletion request

	log.Printf("task %v submitted for deletion, stopping container %s", t.Id, t.ContainerId)
	w.WriteHeader(http.StatusNoContent)
}

func (a *Api) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(a.Worker.GetTasks())
}

func (a *Api) GetMetricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(a.Worker.Stats)
}
