package apiserver

import (
	"net/http"
	"strings"
	"time"

	"github.com/mijia/sweb/log"
	"github.com/mijia/sweb/server"
	"golang.org/x/net/context"
)

// StatWare is the statistics middleware which would log all the access and performation information.
type ReadOnlySwitch struct {
}

// ServeHTTP implements the Middleware interface. Would log all the access, status and performance information.
func (m *ReadOnlySwitch) ServeHTTP(ctx context.Context, w http.ResponseWriter, r *http.Request, next server.Handler) context.Context {
	start := time.Now()
	e := getEngine(ctx)

	if e.ReadOnly() && strings.ToUpper(r.Method) != "GET" &&
		!strings.HasPrefix(r.URL.Path, "/api/engine/") {
		log.Warnf("Do ReadOnly Request!, %q %q, duration=%v",
			r.Method, r.URL.Path, time.Since(start))
		w.WriteHeader(403)
		w.Write([]byte("Read Only Permitted.\n"))
		return ctx
	}
	return next(ctx, w, r)
}

// NewStatWare returns a new StatWare, some ignored urls can be specified with prefixes which would not be logged.
func NewReadOnlySwitch() server.Middleware {
	return &ReadOnlySwitch{}
}
