package manager

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
)

// Manager API for tasks management, data and worker nodes retrieval
type Api struct {
	Address string
	Port    int
	Manager *Manager
	Router  *chi.Mux
}

// Start the manager API server
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
	a.Router.Route("/nodes", func(r chi.Router) {
		r.Get("/", a.getNodesHandler)
	})
}
