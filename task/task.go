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
	State         State
	Image         string
	Memory        int
	Disk          int
	ExposedPorts  nat.PortSet
	PortBindings  map[string]string
	RestartPolicy string
	StartTime     time.Time
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
	Memory        int64
	Disk          int64
	Env           []string
	RestartPolicy string
}

type Docker struct {
	Client      *client.Client
	Config      Config
	ContainerId string
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
