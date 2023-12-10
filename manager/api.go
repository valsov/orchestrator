package manager

import (
	"fmt"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Api struct {
	Address string
	Port    int
	Manager *Manager
	Router  *chi.Mux
}

func (a *Api) StartRouter() {
	a.initRouter()
	if err := http.ListenAndServe(fmt.Sprintf("%s:%d", a.Address, a.Port), a.Router); err != nil {
		log.Print(err)
	}
}

func (a *Api) initRouter() {
	a.Router = chi.NewRouter()
	a.Router.Route("/tasks", func(r chi.Router) {
		r.Post("/", a.StartTaskHandler)
		r.Delete("/{taskId}", a.StopTaskHandler)
		r.Get("/", a.GetTasksHandler)
	})
}
