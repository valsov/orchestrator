package main

import (
	"fmt"
	"orchestrator/task"
	"orchestrator/worker"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

func main() {
	db := make(map[uuid.UUID]*task.Task)
	w := worker.Worker{
		Db:    db,
		Queue: *queue.New(),
	}

	t := task.Task{
		Id:    uuid.New(),
		Name:  "test-container",
		State: task.Scheduled,
		Image: "strm/helloworld-http",
	}

	fmt.Println("starting task")
	w.AddTask(&t)
	result := w.RunNextTask()
	if result.Error != nil {
		panic(result.Error)
	}

	fmt.Printf("task %v is running in container %s\n", t.Id, result.ContainerId)
	time.Sleep(time.Second * 10)

	fmt.Print("stopping task")
	t.State = task.Completed
	w.AddTask(&t)
	result = w.RunNextTask()
	if result.Error != nil {
		panic(result.Error)
	}
}
