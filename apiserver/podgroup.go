package apiserver

import (
	"fmt"
	"net/http"

	"github.com/mijia/sweb/form"
	"github.com/mijia/sweb/log"
	"github.com/mijia/sweb/server"
	"golang.org/x/net/context"
	"github.com/laincloud/deployd/engine"
)

type RestfulPodGroups struct {
	server.BaseResource
}

func (rpg RestfulPodGroups) Post(ctx context.Context, r *http.Request) (int, interface{}) {
	var pgSpec engine.PodGroupSpec
	if err := form.ParamBodyJson(r, &pgSpec); err != nil {
		log.Warnf("Failed to decode PodGroupSpec, %s", err)
		return http.StatusBadRequest, fmt.Sprintf("Invalid PodGroupSpec params format: %s", err)
	}
	if ok := pgSpec.VerifyParams(); !ok {
		return http.StatusBadRequest, fmt.Sprintf("Missing paremeters for PodGroupSpec")
	}

	orcEngine := getEngine(ctx)
	if err := orcEngine.NewPodGroup(pgSpec); err != nil {
		switch err {
		case engine.ErrNotEnoughResources, engine.ErrPodGroupExists, engine.ErrDependencyPodNotExists:
			return http.StatusMethodNotAllowed, err.Error()
		default:
			return http.StatusInternalServerError, err.Error()
		}
	}

	urlReverser := getUrlReverser(ctx)
	return http.StatusAccepted, map[string]string{
		"message":   "PodGroupSpec added into the orc engine.",
		"check_url": urlReverser.Reverse("Get_RestfulPodGroups") + "?name=" + pgSpec.Name,
	}
}

func (rpg RestfulPodGroups) Delete(ctx context.Context, r *http.Request) (int, interface{}) {
	pgName := form.ParamString(r, "name", "")
	if pgName == "" {
		return http.StatusBadRequest, fmt.Sprintf("No pod group name provided.")
	}
	orcEngine := getEngine(ctx)
	if err := orcEngine.RemovePodGroup(pgName); err != nil {
		if err == engine.ErrPodGroupNotExists {
			return http.StatusNotFound, err.Error()
		}
		return http.StatusInternalServerError, err.Error()
	}

	urlReverser := getUrlReverser(ctx)
	return http.StatusAccepted, map[string]string{
		"message":   "PodGroupSpec will be deleted from the orc engine.",
		"check_url": urlReverser.Reverse("Get_RestfulPodGroups") + "?name=" + pgName,
	}
}

func (rpg RestfulPodGroups) Get(ctx context.Context, r *http.Request) (int, interface{}) {
	pgName := form.ParamString(r, "name", "")
	if pgName == "" {
		return http.StatusBadRequest, fmt.Sprintf("No pod group name provided.")
	}
	forceUpdate := form.ParamBoolean(r, "force_update", false)

	orcEngine := getEngine(ctx)
	if forceUpdate {
		if err := orcEngine.RefreshPodGroup(pgName, forceUpdate); err != nil {
			if err == engine.ErrPodGroupNotExists {
				return http.StatusNotFound, err.Error()
			}
			return http.StatusInternalServerError, err.Error()
		}
	}
	podGroup, ok := orcEngine.InspectPodGroup(pgName)
	if !ok {
		return http.StatusNotFound, fmt.Sprintf("No such pod group name=%s", pgName)
	}
	return http.StatusOK, podGroup
}

func (rpg RestfulPodGroups) Patch(ctx context.Context, r *http.Request) (int, interface{}) {
	pgName := form.ParamString(r, "name", "")
	if pgName == "" {
		return http.StatusBadRequest, fmt.Sprintf("No pod group name provided.")
	}

	orcEngine := getEngine(ctx)
	options := []string{"replica", "spec"}
	cmd := form.ParamStringOptions(r, "cmd", options, "noop")
	var err error
	switch cmd {
	case "replica":
		numInstance := form.ParamInt(r, "num_instances", -1)
		restartOption := form.ParamStringOptions(r, "restart_policy", []string{"never, always, onfail"}, "na")
		restartPolicy := -1
		switch restartOption {
		case "never":
			restartPolicy = engine.RestartPolicyNever
		case "always":
			restartPolicy = engine.RestartPolicyAlways
		case "onfail":
			restartPolicy = engine.RestartPolicyOnFail
		}
		if numInstance < 0 {
			return http.StatusBadRequest, fmt.Sprintf("Bad parameter for num_instances, should be > 0 but %d", numInstance)
		}
		if restartPolicy != -1 {
			err = orcEngine.RescheduleInstance(pgName, numInstance, engine.RestartPolicy(restartPolicy))
		} else {
			err = orcEngine.RescheduleInstance(pgName, numInstance)
		}
	case "spec":
		var podSpec engine.PodSpec
		if bodyErr := form.ParamBodyJson(r, &podSpec); bodyErr != nil {
			return http.StatusBadRequest, fmt.Sprintf("Bad parameter format for PodSpec, %s", bodyErr)
		}
		if !podSpec.VerifyParams() {
			return http.StatusBadRequest, fmt.Sprintf("Missing parameter for PodSpec")
		}
		err = orcEngine.RescheduleSpec(pgName, podSpec)
	}

	if err != nil {
		switch err {
		case engine.ErrPodGroupNotExists:
			return http.StatusNotFound, err.Error()
		case engine.ErrNotEnoughResources, engine.ErrDependencyPodNotExists:
			return http.StatusMethodNotAllowed, err.Error()
		default:
			return http.StatusInternalServerError, err.Error()
		}
	}

	urlReverser := getUrlReverser(ctx)
	return http.StatusAccepted, map[string]string{
		"message":   "PodGroupSpec will be patched and rescheduled.",
		"check_url": urlReverser.Reverse("Get_RestfulPodGroups") + "?name=" + pgName,
	}
}
