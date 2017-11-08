package apiserver

import (
	"fmt"
	"net/http"

	"github.com/mijia/sweb/form"
	"github.com/mijia/sweb/log"
	"github.com/mijia/sweb/server"
	"golang.org/x/net/context"
)

type RestfulCntStatusHstry struct {
	server.BaseResource
}

func (rpg RestfulCntStatusHstry) Get(ctx context.Context, r *http.Request) (int, interface{}) {
	pgName := form.ParamString(r, "name", "")
	if pgName == "" {
		return http.StatusBadRequest, fmt.Sprintf("No pod group name provided.")
	}
	instance := form.ParamInt(r, "instance", -1)
	if instance == -1 {
		return http.StatusBadRequest, fmt.Sprintf("No pod instance provided.")
	}
	orcEngine := getEngine(ctx)
	podStatusHstries := orcEngine.FetchPodStaHstry(pgName, instance)
	log.Infof("podStatusHstry:%v", podStatusHstries)
	return http.StatusOK, podStatusHstries
}
