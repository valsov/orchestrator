package task

import (
	"context"
	"io"
	"math"
	"os"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// Container specification with desired state
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
	RestartCount  int
}

// Task Submission event
type TaskEvent struct {
	Id        uuid.UUID
	State     State
	Timestamp time.Time
	Task      Task
}

// Container configuration
type Config struct {
	Name          string
	ContainerId   string
	Cmd           []string
	Image         string
	Cpu           float64
	Memory        int64
	Disk          int64
	Env           []string
	RestartPolicy string
	ExposedPorts  nat.PortSet
	PortBindings  map[string]string
}

// Create a Config object from a Task object
func NewConfig(t Task) Config {
	return Config{
		Name:          t.Name,
		ExposedPorts:  t.ExposedPorts,
		PortBindings:  t.PortBindings,
		Image:         t.Image,
		Cpu:           t.Cpu,
		Memory:        t.Memory,
		Disk:          t.Disk,
		RestartPolicy: t.RestartPolicy,
	}
}

// Docker container client
type ContainerClient struct {
	*client.Client
}

// Get a ready to use container client
func NewContainerClient() *ContainerClient {
	client, _ := client.NewClientWithOpts(client.FromEnv)
	return &ContainerClient{client}
}

// Start a new docker container with the given configuration
func (c *ContainerClient) Run(conf Config) (string, error) {
	ctx := context.Background()
	reader, err := c.ImagePull(ctx, conf.Image, types.ImagePullOptions{})
	if err != nil {
		log.Err(err).Str("image", conf.Image).Msg("error pulling image")
		return "", err
	}
	io.Copy(os.Stdout, reader) // Display pull result

	containerConfig := container.Config{
		Image:        conf.Image,
		Env:          conf.Env,
		ExposedPorts: conf.ExposedPorts,
	}
	hostConfig := container.HostConfig{
		RestartPolicy: container.RestartPolicy{Name: conf.RestartPolicy},
		Resources: container.Resources{
			Memory:   conf.Memory,
			NanoCPUs: int64(conf.Cpu * math.Pow(10, 9)),
		},
		PortBindings: createPortMap(conf.PortBindings, "127.0.0.1"),
	}
	response, err := c.ContainerCreate(ctx, &containerConfig, &hostConfig, nil, nil, conf.Name)
	if err != nil {
		log.Err(err).Str("image", conf.Image).Msg("error creating container")
		return "", err
	}

	err = c.ContainerStart(ctx, response.ID, types.ContainerStartOptions{})
	if err != nil {
		log.Err(err).Str("image", conf.Image).Str("container-id", response.ID).Msg("error starting container")
		return "", err
	}

	conf.ContainerId = response.ID
	out, err := c.ContainerLogs(ctx, response.ID, types.ContainerLogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		log.Err(err).Str("image", conf.Image).Str("container-id", response.ID).Msg("error getting logs for container")
	}
	stdcopy.StdCopy(os.Stdout, os.Stderr, out)

	return response.ID, nil
}

// Stop the container with the given id
func (c *ContainerClient) Stop(containerId string) error {
	log.Debug().Str("container-id", containerId).Msg("attempting to stop container")
	ctx := context.Background()
	if err := c.ContainerStop(ctx, containerId, container.StopOptions{}); err != nil {
		log.Err(err).Str("container-id", containerId).Msg("failed to stop container")
		return err
	}
	if err := c.ContainerRemove(ctx, containerId, types.ContainerRemoveOptions{}); err != nil {
		log.Err(err).Str("container-id", containerId).Msg("failed to remove container")
		return err
	}

	return nil
}

// Retrieve informations about the container with the given id
func (c *ContainerClient) Inspect(containerId string) (types.ContainerJSON, error) {
	ctx := context.Background()
	response, err := c.ContainerInspect(ctx, containerId)
	if err != nil {
		log.Err(err).Str("container-id", containerId).Msg("error inspecting container")
		return types.ContainerJSON{}, err
	}
	return response, nil
}

// Generate a PortMap based on the given map and host IP address
func createPortMap(m map[string]string, hostIp string) nat.PortMap {
	pm := make(nat.PortMap, len(m))
	for portStr, boundPort := range m {
		port := nat.Port(portStr)
		pBinding := nat.PortBinding{HostIP: hostIp, HostPort: boundPort}
		pm[port] = []nat.PortBinding{pBinding}
	}
	return pm
}
