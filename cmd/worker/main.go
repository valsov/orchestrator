package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/urfave/cli/v2"

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
		},
		Action: func(ctx *cli.Context) error {
			startWorker(ctx.String("name"), ctx.String("host"), ctx.Int("port"), ctx.String("storeType"))
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func startWorker(name string, host string, port int, storeType string) {
	w, err := worker.New(name, storeType)
	if err != nil {
		log.Printf("worker creation failed: %v", err)
		return
	}

	defer func() {
		if err := w.Close(); err != nil {
			log.Printf("failed to stop worker, err: %v", err)
		}
	}()

	// Launch backgound routines
	go w.RunTasks()
	go w.CollectStats()
	go w.UpdateTasks()

	// Run API
	if !strings.HasPrefix(host, "http") {
		host = fmt.Sprintf("http://%s", host)
	}
	log.Printf("Worker %s API listening on %s:%d", name, host, port)
	api := worker.Api{Address: host, Port: port, Worker: w}
	api.StartRouter()
}
