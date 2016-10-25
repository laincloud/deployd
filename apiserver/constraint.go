package apiserver

import (
	"fmt"

	"github.com/laincloud/deployd/engine"
	"github.com/mijia/sweb/form"
	"github.com/mijia/sweb/server"
	"golang.org/x/net/context"
	"net/http"
)

type RestfulConstraints struct {
	server.BaseResource
}

func (rc RestfulConstraints) Get(ctx context.Context, r *http.Request) (int, interface{}) {
	cstType := form.ParamString(r, "type", "node")

	if constraint, ok := getEngine(ctx).GetConstraints(cstType); !ok {
		return http.StatusNotFound, fmt.Sprintf("No constraint found")
	} else {
		return http.StatusOK, constraint
	}
}

func (rc RestfulConstraints) Patch(ctx context.Context, r *http.Request) (int, interface{}) {

	cstType := form.ParamString(r, "type", "")
	cstValue := form.ParamString(r, "value", "")
	equal := form.ParamBoolean(r, "equal", false)
	soft := form.ParamBoolean(r, "soft", true)

	if cstType == "" {
		return http.StatusBadRequest, "constaint type required"
	}
	if cstValue == "" {
		return http.StatusBadRequest, "constraint value required"
	}

	constraint := engine.ConstraintSpec{cstType, equal, cstValue, soft}

	if err := getEngine(ctx).UpdateConstraints(constraint); err != nil {
		return http.StatusInternalServerError, err.Error()
	}

	urlReverser := getUrlReverser(ctx)
	return http.StatusAccepted, map[string]string{
		"message":   "Constaints will be patched",
		"check_url": urlReverser.Reverse("Get_RestfulConstraints") + "?type=" + cstType,
	}
}

func (rc RestfulConstraints) Delete(ctx context.Context, r *http.Request) (int, interface{}) {
	cstType := form.ParamString(r, "type", "node")

	if err := getEngine(ctx).DeleteConstraints(cstType); err != nil {
		if err == engine.ErrConstraintNotExists {
			return http.StatusNotFound, err.Error()
		}
		return http.StatusInternalServerError, err.Error()
	}

	urlReverser := getUrlReverser(ctx)
	return http.StatusAccepted, map[string]string{
		"message":   "Constraint will be deleted from the orc engine.",
		"check_url": urlReverser.Reverse("Get_RestfulConstraints") + "?type=" + cstType,
	}
}
