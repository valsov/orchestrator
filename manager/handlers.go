package manager

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"orchestrator/task"
	"time"

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

	a.Manager.AddTask(tEvent)
	log.Printf("[m] added task %v", tEvent.Task.Id)
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
		log.Print("taskId parameter isn't a valid uuid")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	t, err := a.Manager.TaskDb.Get(taskUuid)
	if err != nil {
		log.Printf("failed to retrieve task with id %v, err: %v", taskUuid, err)
		w.WriteHeader(http.StatusNotFound)
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

	log.Printf("task event %v submitted to stop task %v", tEvent.Id, tEvent.Task.Id)
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
