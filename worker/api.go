package worker

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
)

// Worker API for tasks management and data retrieval
type Api struct {
	Address string
	Port    int
	Worker  *Worker
	Router  *chi.Mux
}

// Start the worker API server
func (a *Api) StartRouter() {
	a.initRouter()
	if err := http.ListenAndServe(fmt.Sprintf("%s:%d", a.Address, a.Port), a.Router); err != nil {
		log.Err(err).Msg("api server error")
	}
}

func (a *Api) initRouter() {
	a.Router = chi.NewRouter()
	a.Router.Route("/tasks", func(r chi.Router) {
		r.Post("/", a.startTaskHandler)
		r.Delete("/{taskId}", a.stopTaskHandler)
		r.Get("/", a.getTasksHandler)
	})
	a.Router.Route("/metrics", func(r chi.Router) {
		r.Get("/", a.getMetricsHandler)
	})
}
