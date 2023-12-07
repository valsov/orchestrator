package task

import (
	"context"
	"io"
	"log"
	"os"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
)

type Task struct {
	Id            uuid.UUID
	Name          string
	ContainerId   string
	State         State
	Image         string
	Cpu           float64
	Memory        int64
	Disk          int64
	ExposedPorts  nat.PortSet
	PortBindings  map[string]string
	RestartPolicy string
	StartTime     time.Time
	FinishTime    time.Time
}

type TaskEvent struct {
	Id        uuid.UUID
	State     State
	Timestamp time.Time
	Task      Task
}

type Config struct {
	Name          string
	ContainerId   string
	AttachStdin   bool
	AttachStdout  bool
	AttachStderr  bool
	Cmd           []string
	Image         string
	Cpu           float64
	Memory        int64
	Disk          int64
	Env           []string
	RestartPolicy string
	ExposedPorts  nat.PortSet
}

func NewConfig(t *Task) *Config {
	return &Config{
		Name:          t.Name,
		ExposedPorts:  t.ExposedPorts,
		Image:         t.Image,
		Cpu:           t.Cpu,
		Memory:        t.Memory,
		Disk:          t.Disk,
		RestartPolicy: t.RestartPolicy,
	}
}

type Docker struct {
	Client      *client.Client
	Config      Config
	ContainerId string
}

func NewDocker(c *Config) *Docker {
	dClient, _ := client.NewClientWithOpts(client.FromEnv)
	return &Docker{
		Client: dClient,
		Config: *c,
	}
}

type DockerResult struct {
	Error       error
	Action      string
	ContainerId string
	Result      string
}

func (d *Docker) Run() DockerResult {
	ctx := context.Background()
	reader, err := d.Client.ImagePull(ctx, d.Config.Image, types.ImagePullOptions{})
	if err != nil {
		log.Printf("Error pulling image %s: %v", d.Config.Image, err)
		return DockerResult{Error: err}
	}
	io.Copy(os.Stdout, reader) // Display pull result

	containerConfig := container.Config{
		Image: d.Config.Image,
		Env:   d.Config.Env,
	}
	hostConfig := container.HostConfig{
		RestartPolicy:   container.RestartPolicy{Name: d.Config.RestartPolicy},
		Resources:       container.Resources{Memory: d.Config.Memory},
		PublishAllPorts: true,
	}
	response, err := d.Client.ContainerCreate(ctx, &containerConfig, &hostConfig, nil, nil, d.Config.Name)
	if err != nil {
		log.Printf("Error creating container with image %s: %v", d.Config.Image, err)
		return DockerResult{Error: err}
	}

	err = d.Client.ContainerStart(ctx, response.ID, types.ContainerStartOptions{})
	if err != nil {
		log.Printf("Error starting container %s: %v", response.ID, err)
		return DockerResult{Error: err}
	}

	d.Config.ContainerId = response.ID
	out, err := d.Client.ContainerLogs(ctx, response.ID, types.ContainerLogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		log.Printf("Error getting logs for container %s: %v", response.ID, err)
	}
	stdcopy.StdCopy(os.Stdout, os.Stderr, out)

	return DockerResult{
		ContainerId: response.ID,
		Action:      "start",
		Result:      "success",
	}
}

func (d *Docker) Stop(containerId string) DockerResult {
	log.Printf("attempting to stop container %s", containerId)
	ctx := context.Background()
	if err := d.Client.ContainerStop(ctx, containerId, container.StopOptions{}); err != nil {
		log.Println(err)
		panic(err)
	}

	if err := d.Client.ContainerRemove(ctx, containerId, types.ContainerRemoveOptions{}); err != nil {
		log.Println(err)
		panic(err)
	}

	return DockerResult{Action: "stop", Result: "success", ContainerId: containerId}
}
