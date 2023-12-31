package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"orchestrator/node"
	"orchestrator/task"
	"os"
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
)

type taskInput struct {
	Name          string
	Image         string
	Cpu           float64
	Memory        int64
	Disk          int64
	ExposedPorts  []string
	PortBindings  map[string]string
	RestartPolicy string
}

func main() {
	app := &cli.App{
		Name:  "containers orchestration client",
		Usage: "query orchestration manager and submit commands",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "host",
				Usage:    "manager API host",
				Required: true,
			},
			&cli.IntFlag{
				Name:     "port",
				Aliases:  []string{"p"},
				Usage:    "manager API port",
				Required: true,
			},
		},
		Commands: []*cli.Command{
			{
				Name:      "start",
				Usage:     "submit a start task request",
				ArgsUsage: "path to the file containing the yaml representation of the task to start",
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() != 1 {
						return fmt.Errorf("wrong arguments count, expected=1, got=%d", ctx.Args().Len())
					}
					url := getUrl(ctx.String("host"), ctx.Int("port"))
					return startTask(url, ctx.Args().First())
				},
			},
			{
				Name:      "stop",
				Usage:     "submit a stop task request",
				ArgsUsage: "id of the task to stop",
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() != 1 {
						return fmt.Errorf("wrong arguments count, expected=1, got=%d", ctx.Args().Len())
					}
					url := getUrl(ctx.String("host"), ctx.Int("port"))
					id, err := uuid.Parse(ctx.Args().First())
					if err != nil {
						return err
					}
					return stopTask(url, id)
				},
			},
			{
				Name:  "list",
				Usage: "get all tasks from the manager",
				Action: func(ctx *cli.Context) error {
					url := getUrl(ctx.String("host"), ctx.Int("port"))
					return listTasks(url)
				},
			},
			{
				Name:      "get",
				Usage:     "get a specific task from the manager",
				ArgsUsage: "id of the task to query",
				Action: func(ctx *cli.Context) error {
					if ctx.Args().Len() != 1 {
						return fmt.Errorf("wrong arguments count, expected=1, got=%d", ctx.Args().Len())
					}
					url := getUrl(ctx.String("host"), ctx.Int("port"))
					id, err := uuid.Parse(ctx.Args().First())
					if err != nil {
						return err
					}
					return getTask(url, id)
				},
			},
			{
				Name:  "list-nodes",
				Usage: "get registered nodes from the manager",
				Action: func(ctx *cli.Context) error {
					url := getUrl(ctx.String("host"), ctx.Int("port"))
					return listNodes(url)
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Printf("[ERROR] %v", err)
	}
}

func startTask(baseUrl string, filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open task file, err: %v", err)
	}
	defer f.Close()

	buffer, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("failed to read task file, err: %v", err)
	}

	var tInput taskInput
	err = json.Unmarshal(buffer, &tInput)
	if err != nil {
		return fmt.Errorf("invalid json representation of task in file, err: %v", err)
	}

	exposedPorts, err := portSliceToPortSet(tInput.ExposedPorts)
	if err != nil {
		return fmt.Errorf("failed to parse exposed ports, err: %v", err)
	}
	tEvent := task.TaskEvent{
		Id:        uuid.New(),
		State:     task.Scheduled,
		Timestamp: time.Now(),
		Task: task.Task{
			Id:            uuid.New(),
			State:         task.Scheduled,
			Name:          tInput.Name,
			Image:         tInput.Image,
			Cpu:           tInput.Cpu,
			Memory:        tInput.Memory,
			Disk:          tInput.Disk,
			ExposedPorts:  exposedPorts,
			PortBindings:  tInput.PortBindings,
			RestartPolicy: tInput.RestartPolicy,
		},
	}
	jsonTaskEvent, err := json.Marshal(tEvent)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/tasks", baseUrl)
	response, err := http.Post(url, "application/json", bytes.NewBuffer(jsonTaskEvent))
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusCreated {
		return fmt.Errorf("received invalid http status code: %d", response.StatusCode)
	}

	fmt.Println("[OK] task creation request successfully submitted")
	return nil
}

func stopTask(baseUrl string, taskId uuid.UUID) error {
	url := fmt.Sprintf("%s/tasks/%v", baseUrl, taskId)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return err
	}

	client := http.Client{}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("received invalid http status code: %d", response.StatusCode)
	}

	fmt.Println("[OK] task deletion request successfully submitted")
	return nil
}

func listTasks(baseUrl string) error {
	tasks, err := getTasksFromManager(baseUrl)
	if err != nil {
		return err
	}

	if len(tasks) == 0 {
		fmt.Println("No task found")
		return nil
	}

	fmt.Printf("[OK] found %d task(s):\n", len(tasks))
	for _, t := range tasks {
		fmt.Printf("- %#v\n", t)
	}
	return nil
}

func getTask(baseUrl string, taskId uuid.UUID) error {
	tasks, err := getTasksFromManager(baseUrl)
	if err != nil {
		return err
	}

	var foundTask *task.Task
	for _, t := range tasks {
		if t.Id == taskId {
			foundTask = &t
			break
		}
	}
	if foundTask == nil {
		return fmt.Errorf("task with id %v not found", taskId)
	}

	fmt.Printf("%#v\n", *foundTask)
	return nil
}

func getTasksFromManager(baseUrl string) ([]task.Task, error) {
	url := fmt.Sprintf("%s/tasks", baseUrl)
	response, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	var tasks []task.Task
	data := json.NewDecoder(response.Body)
	err = data.Decode(&tasks)
	return tasks, err
}

func listNodes(baseUrl string) error {
	url := fmt.Sprintf("%s/nodes", baseUrl)
	response, err := http.Get(url)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	var nodes []node.Node
	data := json.NewDecoder(response.Body)
	err = data.Decode(&nodes)
	if err != nil {
		return err
	}

	if len(nodes) == 0 {
		fmt.Println("[INFO] no managed node found")
		return nil
	}

	fmt.Printf("[OK] found %d node(s):\n", len(nodes))
	for _, n := range nodes {
		fmt.Printf("- %#v\n", n)
	}
	return nil
}

func getUrl(host string, port int) string {
	if !strings.HasPrefix(host, "http") {
		host = fmt.Sprintf("http://%s:%d", host, port)
	}
	return host
}

func portSliceToPortSet(ports []string) (nat.PortSet, error) {
	pSet, _, err := nat.ParsePortSpecs(ports)
	if err != nil {
		return nil, err
	}
	return pSet, nil
}
