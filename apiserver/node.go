package apiserver

import (
	"fmt"
	"net/http"

	"github.com/mijia/sweb/form"
	"github.com/mijia/sweb/server"
	"golang.org/x/net/context"
)

type RestfulNodes struct {
	server.BaseResource
}

func (rn RestfulNodes) Get(ctx context.Context, r *http.Request) (int, interface{}) {
	nodes, err := getEngine(ctx).GetNodes()
	if err != nil {
		return http.StatusInternalServerError, err.Error()
	}
	return http.StatusAccepted, nodes
}

func (rn RestfulNodes) Patch(ctx context.Context, r *http.Request) (int, interface{}) {
	fromNode := form.ParamString(r, "from", "")
	targetNode := form.ParamString(r, "to", "")
	forceDrift := form.ParamBoolean(r, "force", false)
	pgName := form.ParamString(r, "pg", "")
	pgInstance := form.ParamInt(r, "pg_instance", -1)

	if fromNode == "" {
		return http.StatusBadRequest, "from node name required"
	}
	if fromNode == targetNode {
		return http.StatusBadRequest, "from node equals to target node"
	}

	cmd := form.ParamString(r, "cmd", "")
	switch cmd {
	case "drift":
		engine := getEngine(ctx)
		engine.DriftNode(fromNode, targetNode, pgName, pgInstance, forceDrift)
		return http.StatusAccepted, map[string]interface{}{
			"message":    "PodGroups will be drifting",
			"from":       fromNode,
			"to":         targetNode,
			"pgName":     pgName,
			"pgInstance": pgInstance,
			"forceDrift": forceDrift,
		}
	default:
		return http.StatusBadRequest, fmt.Sprintf("Unkown command %s", cmd)
	}
}

func (rn RestfulNodes) Delete(ctx context.Context, r *http.Request) (int, interface{}) {
	node := form.ParamString(r, "node", "")

	if node == "" {
		return http.StatusBadRequest, "from node name required"
	}
	engine := getEngine(ctx)
	engine.RemoveNode(node)
	return http.StatusAccepted, map[string]interface{}{
		"message": "containers in node will be drift",
		"node":    node,
	}
}
