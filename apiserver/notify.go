package apiserver

import (
	"fmt"

	"github.com/laincloud/deployd/engine"
	"github.com/mijia/sweb/form"
	"github.com/mijia/sweb/server"
	"golang.org/x/net/context"
	"net/http"
	"net/url"
)

type RestfulNotifies struct {
	server.BaseResource
}

func (rc RestfulNotifies) Get(ctx context.Context, r *http.Request) (int, interface{}) {
	notifies := getEngine(ctx).GetNotifies()
	if len(notifies) == 0 {
		return http.StatusNotFound, fmt.Sprintf("No notify found")
	} else {
		return http.StatusOK, notifies
	}
}

func (rc RestfulNotifies) Post(ctx context.Context, r *http.Request) (int, interface{}) {

	callback := form.ParamString(r, "callback", "")

	if callback == "" {
		return http.StatusBadRequest, "constaint type required"
	}

	if _, err := url.ParseRequestURI(callback); err != nil {
		return http.StatusBadRequest, fmt.Sprintf("callback url not valid: %s", err)
	}

	if err := getEngine(ctx).AddNotify(callback); err != nil {
		return http.StatusInternalServerError, err.Error()
	}

	urlReverser := getUrlReverser(ctx)
	return http.StatusAccepted, map[string]string{
		"message":   "notify will be added",
		"check_url": urlReverser.Reverse("Get_RestfulNotifies"),
	}
}

func (rc RestfulNotifies) Delete(ctx context.Context, r *http.Request) (int, interface{}) {
	callback := form.ParamString(r, "callback", "")

	if callback == "" {
		return http.StatusBadRequest, "callback value requird"
	}

	if err := getEngine(ctx).DeleteNotify(callback); err != nil {
		if err == engine.ErrNotifyNotExists {
			return http.StatusNotFound, err.Error()
		}
		return http.StatusInternalServerError, err.Error()
	}

	urlReverser := getUrlReverser(ctx)
	return http.StatusAccepted, map[string]string{
		"message":   "notify uri will be deleted from the orc engine.",
		"check_url": urlReverser.Reverse("Get_RestfulNotifies"),
	}
}
