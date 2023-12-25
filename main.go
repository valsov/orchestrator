package main

import (
	"fmt"
	"log"
	"orchestrator/manager"
	"orchestrator/worker"
	"os"
	"strconv"
)

func main() {
	workerApi, workerApiAddr := startWorker()
	managerApi := startManager(workerApiAddr)
	defer func() {
		if err := managerApi.Manager.Close(); err != nil {
			log.Printf("failed to stop manager, err: %v", err)
		}
		if err := workerApi.Worker.Close(); err != nil {
			log.Printf("failed to stop worker, err: %v", err)
		}
	}()

	go workerApi.StartRouter()
	managerApi.StartRouter()
}

func startWorker() (*worker.Api, string) {
	host := os.Getenv("WORKER_HOST")
	port, _ := strconv.Atoi(os.Getenv("WORKER_PORT"))

	w, _ := worker.New("w1", "memory")
	api := worker.Api{Address: host, Port: port, Worker: w}
	go w.RunTasks()
	go w.CollectStats()
	go w.UpdateTasks()

	return &api, fmt.Sprintf("%s:%d", host, port)
}

func startManager(workerApiAddr string) *manager.Api {
	host := os.Getenv("MANAGER_HOST")
	port, _ := strconv.Atoi(os.Getenv("MANAGER_PORT"))

	workers := []string{workerApiAddr}
	m, _ := manager.New(workers, "roundrobin", "memory")
	go m.ProcessTasks()
	go m.UpdateTasks()
	go m.CheckTasksHealth()

	return &manager.Api{Address: host, Port: port, Manager: m}
}
