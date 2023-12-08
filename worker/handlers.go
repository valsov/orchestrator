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
		log.Println(errMessage)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrResponse{
			Message:        errMessage,
			HTTPStatusCode: http.StatusBadRequest,
		})
		return
	}

	a.Worker.AddTask(&tEvent.Task)
	log.Printf("added task %v\n", tEvent.Task.Id)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(tEvent.Task)
}

func (a *Api) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskId := chi.URLParam(r, "taskId")
	if taskId == "" {
		log.Println("taskId parameter is missing")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	taskUuid, err := uuid.Parse(taskId)
	if err != nil {
		log.Println("taskId parameter isn't a valid uuid")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	t, found := a.Worker.Db[taskUuid]
	if !found {
		log.Printf("couldn't find a task with id %v\n", taskUuid)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	tCopy := *t // Dereference task to change state without impacting the task stored in DB
	tCopy.State = task.Completed
	a.Worker.AddTask(&tCopy) // Submit deletion request

	log.Printf("task %v submitted for deletion, stopping container %s", t.Id, t.ContainerId)
	w.WriteHeader(http.StatusNoContent)
}

func (a *Api) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(a.Worker.GetTasks())
}
