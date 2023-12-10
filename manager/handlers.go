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

	t, found := a.Manager.TaskDb[taskUuid]
	if !found {
		log.Printf("couldn't find a task with id %v", taskUuid)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	tCopy := *t // Dereference task to change state without impacting the task stored in DB
	tCopy.State = task.Completed
	tEvent := task.TaskEvent{
		Id:        uuid.New(),
		State:     task.Completed,
		Timestamp: time.Now().UTC(),
		Task:      tCopy,
	}
	a.Manager.AddTask(tEvent)

	log.Printf("task event %v submitted to stop task %v", tEvent.Id, tEvent.Task.Id)
	w.WriteHeader(http.StatusNoContent)
}

func (a *Api) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(a.Manager.GetTasks())
}
