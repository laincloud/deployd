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

type RestfulDependPods struct {
	server.BaseResource
}

func (rdp RestfulDependPods) Get(ctx context.Context, r *http.Request) (int, interface{}) {
	dpName := form.ParamString(r, "name", "")
	if dpName == "" {
		return http.StatusBadRequest, fmt.Sprintf("Missing dependency pod name for the request")
	}
	orcEngine := getEngine(ctx)
	if podsSpec, err := orcEngine.GetDependencyPod(dpName); err != nil {
		if err == engine.ErrDependencyPodNotExists {
			return http.StatusNotFound, err.Error()
		}
		return http.StatusInternalServerError, err.Error()
	} else {
		return http.StatusOK, podsSpec
	}
}

func (rdp RestfulDependPods) Delete(ctx context.Context, r *http.Request) (int, interface{}) {
	dpName := form.ParamString(r, "name", "")
	if dpName == "" {
		return http.StatusBadRequest, fmt.Sprintf("Missing dependency pod name for the request")
	}
	force := form.ParamBoolean(r, "force", false)
	orcEngine := getEngine(ctx)
	if err := orcEngine.RemoveDependencyPod(dpName, force); err != nil {
		if err == engine.ErrDependencyPodNotExists {
			return http.StatusNotFound, err.Error()
		}
		return http.StatusInternalServerError, err.Error()
	}
	urlReverser := getUrlReverser(ctx)
	return http.StatusAccepted, map[string]string{
		"message":   "Dependency pod will be removed from the orc engine.",
		"check_url": urlReverser.Reverse("Get_RestfulDependPods") + "?name=" + dpName,
	}
}

func (rdp RestfulDependPods) Put(ctx context.Context, r *http.Request) (int, interface{}) {
	var podSpec engine.PodSpec
	if err := form.ParamBodyJson(r, &podSpec); err != nil {
		return http.StatusBadRequest, fmt.Sprintf("Bad parameter format for PodSpec, %s", err)
	}
	if !podSpec.VerifyParams() {
		return http.StatusBadRequest, fmt.Sprintf("Missing parameters for PodSpec")
	}

	orcEngine := getEngine(ctx)
	if err := orcEngine.UpdateDependencyPod(podSpec); err != nil {
		if err == engine.ErrDependencyPodNotExists {
			return http.StatusNotFound, err.Error()
		}
		return http.StatusInternalServerError, err.Error()
	}
	urlReverser := getUrlReverser(ctx)
	return http.StatusAccepted, map[string]string{
		"message":   "Dependency PodSpec would be updated in orc engine.",
		"check_url": urlReverser.Reverse("Get_RestfulDependPods") + "?name=" + podSpec.Name,
	}
}

func (rdp RestfulDependPods) Post(ctx context.Context, r *http.Request) (int, interface{}) {
	var podSpec engine.PodSpec
	if err := form.ParamBodyJson(r, &podSpec); err != nil {
		log.Warnf("Failed to decode PodSpec, %s", err)
		return http.StatusBadRequest, fmt.Sprintf("Bad parameter format for PodSpec, %s", err)
	}
	if ok := podSpec.VerifyParams(); !ok {
		return http.StatusBadRequest, fmt.Sprintf("Missing paremeters for PodSpec")
	}

	orcEngine := getEngine(ctx)
	if err := orcEngine.NewDependencyPod(podSpec); err != nil {
		if err == engine.ErrDependencyPodExists {
			return http.StatusMethodNotAllowed, err.Error()
		}
		return http.StatusInternalServerError, err.Error()
	}
	urlReverser := getUrlReverser(ctx)
	return http.StatusAccepted, map[string]string{
		"message":   "Dependency pod will be added into orc engine.",
		"check_url": urlReverser.Reverse("Get_RestfulDependPods") + "?name=" + podSpec.Name,
	}
}
