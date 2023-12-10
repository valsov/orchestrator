package main

import (
	"fmt"
	"orchestrator/manager"
	"orchestrator/task"
	"orchestrator/worker"
	"os"
	"strconv"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

func main() {
	workerApi, workerApiAddr := startWorker()
	managerApi := startManager(workerApiAddr)

	go workerApi.StartRouter()
	managerApi.StartRouter()
}

func startWorker() (*worker.Api, string) {
	host := os.Getenv("WORKER_HOST")
	port, _ := strconv.Atoi(os.Getenv("WORKER_PORT"))

	w := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}
	api := worker.Api{Address: host, Port: port, Worker: &w}
	go w.RunTasks()
	go w.CollectStats()
	go w.UpdateTasks()

	return &api, fmt.Sprintf("%s:%d", host, port)
}

func startManager(workerApiAddr string) *manager.Api {
	host := os.Getenv("MANAGER_HOST")
	port, _ := strconv.Atoi(os.Getenv("MANAGER_PORT"))

	workers := []string{workerApiAddr}
	m := manager.New(workers)
	go m.ProcessTasks()
	go m.UpdateTasks()
	go m.CheckTasksHealth()

	return &manager.Api{Address: host, Port: port, Manager: m}
}
