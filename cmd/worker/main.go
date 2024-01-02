package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"

	"orchestrator/logger"
	"orchestrator/worker"
)

func main() {
	app := &cli.App{
		Name:  "containers orchestration worker",
		Usage: "start the worker process and API",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "name",
				Aliases:  []string{"n"},
				Usage:    "name of the worker",
				Required: true,
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
			name := ctx.String("name")
			logger.Setup(ctx.String("logLevel"), fmt.Sprintf("worker-%s", name))
			startWorker(name, ctx.Int("port"), ctx.String("storeType"))
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal().Err(err).Msg("")
	}
}

func startWorker(name string, port int, storeType string) {
	w, err := worker.New(name, storeType)
	if err != nil {
		log.Err(err).Msg("worker creation failed")
		return
	}

	defer func() {
		if err := w.Close(); err != nil {
			log.Err(err).Msg("failed to stop worker")
		}
	}()

	// Launch backgound routines
	go w.RunTasks()
	go w.CollectStats()
	go w.UpdateTasks()

	// Run API
	host := "127.0.0.1"
	log.Info().Msgf("Worker %s API listening on %s:%d", name, host, port)
	api := worker.Api{Address: host, Port: port, Worker: w}
	api.StartRouter()
}
