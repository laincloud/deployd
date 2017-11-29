package apiserver

import (
	"fmt"
	"net/http"

	"github.com/mijia/sweb/form"
	"github.com/mijia/sweb/log"
	"github.com/mijia/sweb/server"
	"golang.org/x/net/context"
)

type EngineConfigApi struct {
	server.BaseResource
}

func (eca EngineConfigApi) Get(ctx context.Context, r *http.Request) (int, interface{}) {
	e := getEngine(ctx)
	return http.StatusOK, e.Config()
}

func (eca EngineConfigApi) Patch(ctx context.Context, r *http.Request) (int, interface{}) {
	e := getEngine(ctx)
	config := e.Config()
	if err := form.ParamBodyJson(r, &config); err != nil {
		log.Warnf("Failed to decode Engine Config, %s", err)
		return http.StatusBadRequest, fmt.Sprintf("Invalid Engine Config params format: %s", err)
	}
	e.SetConfig(config)
	return http.StatusOK, e.Config()
}

type EngineMaintenanceApi struct {
	server.BaseResource
}

func (ema EngineMaintenanceApi) Patch(ctx context.Context, r *http.Request) (int, interface{}) {
	e := getEngine(ctx)
	status := form.ParamBoolean(r, "on", false)
	e.Maintaince(status)
	return http.StatusOK, e.Config()
}
