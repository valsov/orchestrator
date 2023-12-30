package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/urfave/cli/v2"

	"orchestrator/manager"
)

func main() {
	app := &cli.App{
		Name:  "containers orchestration manager",
		Usage: "start the manager process and API",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "host",
				Aliases: []string{"h"},
				Usage:   "host to serve the API on",
				Value:   "127.0.0.1",
			},
			&cli.IntFlag{
				Name:    "port",
				Aliases: []string{"p"},
				Usage:   "port to serve the API on",
				Value:   8080,
			},
			&cli.StringFlag{
				Name:     "storeType",
				Aliases:  []string{"st"},
				Usage:    `store type to use for tasks, allowed values: "memory", "persisted"`,
				Required: true,
				Action: func(ctx *cli.Context, v string) error {
					if v != "memory" && v != "persisted" {
						return errors.New(`invalid storeType, allowed values: "memory", "persisted"`)
					}
					return nil
				},
			},
			&cli.StringFlag{
				Name:     "schedulerType",
				Aliases:  []string{"sct"},
				Usage:    `scheduler type to select a worker for new tasks, allowed values: "roundrobin", "epvm"`,
				Required: true,
				Action: func(ctx *cli.Context, v string) error {
					if v != "roundrobin" && v != "epvm" {
						return errors.New(`invalid schedulerType, allowed values: "roundrobin", "epvm"`)
					}
					return nil
				},
			},
			&cli.StringSliceFlag{
				Name:     "worker",
				Aliases:  []string{"w"},
				Usage:    "address of container orchestration worker(s) API to manage",
				Required: true,
			},
		},
		Action: func(ctx *cli.Context) error {
			startManager(ctx.String("host"), ctx.Int("port"), ctx.String("storeType"), ctx.String("schedulerType"), ctx.StringSlice("worker"))
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func startManager(host string, port int, storeType string, schedulerType string, workers []string) {
	for i := 0; i < len(workers); i++ {
		if !strings.HasPrefix(workers[i], "http") {
			workers[i] = fmt.Sprintf("http://%s", workers[i])
		}
	}
	m, err := manager.New(workers, schedulerType, storeType)
	if err != nil {
		log.Printf("manager creation failed: %v", err)
		return
	}

	defer func() {
		if err := m.Close(); err != nil {
			log.Printf("failed to stop manager, err: %v", err)
		}
	}()

	// Launch backgound routines
	go m.ProcessTasks()
	go m.UpdateTasks()
	go m.CheckTasksHealth()

	// Run API
	if !strings.HasPrefix(host, "http") {
		host = fmt.Sprintf("http://%s", host)
	}
	log.Printf("Manager API listening on %s:%d", host, port)
	api := manager.Api{Address: host, Port: port, Manager: m}
	api.StartRouter()
}
