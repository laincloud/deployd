package apiserver

import (
	"fmt"
	"github.com/mijia/sweb/form"
	"github.com/mijia/sweb/log"
	"github.com/mijia/sweb/server"
	"golang.org/x/net/context"
	"net/http"
)

type RestfulStatus struct {
	server.BaseResource
}

func (rs RestfulStatus) Patch(ctx context.Context, r *http.Request) (int, interface{}) {
	var status struct {
		Status string `json:"status"`
	}
	if err := form.ParamBodyJson(r, &status); err != nil {
		log.Warnf("Failed to decode engine status, %s", err)
		return http.StatusBadRequest, fmt.Sprintf("Invalid Status params format: %s", err)
	}

	switch status.Status {
	case "start":
		getEngine(ctx).Start()
	case "stop":
		getEngine(ctx).Stop()
	default:
		return http.StatusBadRequest, fmt.Sprintf("Invalid Status, it should be start or stop")
	}
	return http.StatusAccepted, "Accept"
}

func (rs RestfulStatus) Get(ctx context.Context, r *http.Request) (int, interface{}) {
	status := "started"
	if !getEngine(ctx).Started() {
		status = "stopped"
	}
	return http.StatusOK, map[string]string{
		"status": status,
	}
}
