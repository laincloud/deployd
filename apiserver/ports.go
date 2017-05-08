package apiserver

import (
	"fmt"
	"net/http"

	"github.com/laincloud/deployd/engine"
	"github.com/mijia/sweb/form"
	"github.com/mijia/sweb/log"
	"github.com/mijia/sweb/server"
	"golang.org/x/net/context"
)

type RestfulPorts struct {
	server.BaseResource
}

type Ports struct {
	Ports []int
}

func (rn RestfulPorts) Get(ctx context.Context, r *http.Request) (int, interface{}) {
	return http.StatusOK, engine.FetchAllPortsInfo()
}

func (rn RestfulPorts) Post(ctx context.Context, r *http.Request) (int, interface{}) {
	options := []string{"validate"}
	cmd := form.ParamStringOptions(r, "cmd", options, "noop")
	switch cmd {
	case "validate":
		var ports Ports
		if err := form.ParamBodyJson(r, &ports); err != nil {
			log.Warnf("Failed to decode valiad ports, %s", err)
			return http.StatusBadRequest, fmt.Sprintf("Invalid ports params format: %s", err)
		}
		occs := engine.OccupiedPorts(ports.Ports...)
		if len(occs) == 0 {
			return http.StatusOK, nil
		}
		return http.StatusBadRequest, fmt.Sprintf("Conflicted ports: %v", occs)
	}
	return http.StatusBadRequest, fmt.Sprintf("Unknown request!")
}
