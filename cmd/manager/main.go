package main

import (
	"errors"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"

	"orchestrator/logger"
	"orchestrator/manager"
)

func main() {
	app := &cli.App{
		Name:  "containers orchestration manager",
		Usage: "start the manager process and API",
		Flags: []cli.Flag{
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
			&cli.StringFlag{
				Name:  "logLevel",
				Usage: `log level to use, allowed values: "debug", "info", "error"`,
				Value: "info",
				Action: func(ctx *cli.Context, v string) error {
					if v != "debug" && v != "info" && v != "error" {
						return errors.New(`invalid logLevel, allowed values: "debug", "info", "error"`)
					}
					return nil
				},
			},
		},
		Action: func(ctx *cli.Context) error {
			logger.Setup(ctx.String("logLevel"), "manager")
			startManager(ctx.Int("port"), ctx.String("storeType"), ctx.String("schedulerType"), ctx.StringSlice("worker"))
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal().Err(err).Msg("")
	}
}

func startManager(port int, storeType string, schedulerType string, workers []string) {
	m, err := manager.New(workers, schedulerType, storeType)
	if err != nil {
		log.Err(err).Msg("manager creation failed")
		return
	}

	defer func() {
		if err := m.Close(); err != nil {
			log.Err(err).Msg("failed to stop manager")
		}
	}()

	// Launch backgound routines
	go m.ProcessTasks()
	go m.UpdateTasks()
	go m.CheckTasksHealth()
	go m.CheckNodesStats()

	// Run API
	host := "127.0.0.1"
	log.Info().Msgf("Manager API listening on %s:%d", host, port)
	api := manager.Api{Address: host, Port: port, Manager: m}
	api.StartRouter()
}
