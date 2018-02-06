package apiserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/laincloud/deployd/cluster/swarm"
	"github.com/laincloud/deployd/engine"
	"github.com/laincloud/deployd/network"
	setcd "github.com/laincloud/deployd/storage/etcd"
	"github.com/mijia/sweb/log"
	"github.com/mijia/sweb/server"
	"golang.org/x/net/context"
)

type UrlReverser interface {
	Reverse(name string, params ...interface{}) string
	Assets(path string) string
}

type Server struct {
	*server.Server

	swarmAddress string
	etcdAddress  string
	isDebug      bool
	started      bool
	engine       *engine.OrcEngine
	runtime      *server.RuntimeWare
}

func (s *Server) ListenAndServe(addr string) error {
	orcEngine, err := initOrcEngine(s.swarmAddress, s.etcdAddress, s.isDebug)
	if err != nil {
		return err
	}
	s.engine = orcEngine

	// init network manager for net recover
	initNetwWorkMgr(s.etcdAddress)

	ctx := context.Background()
	ctx = context.WithValue(ctx, "engine", orcEngine)
	ctx = context.WithValue(ctx, "urlReverser", s)
	s.Server = server.New(ctx, s.isDebug)

	ignoredUrls := []string{"/debug/vars"}
	s.Middleware(server.NewRecoveryWare(s.isDebug))
	s.Middleware(server.NewStatWare(ignoredUrls...))
	if s.runtime == nil {
		s.runtime = server.NewRuntimeWare(ignoredUrls, true, 15*time.Minute).(*server.RuntimeWare)
	}
	s.Middleware(s.runtime)
	s.Middleware(NewReadOnlySwitch())

	s.RestfulHandlerAdapter(s.adaptResourceHandler)
	s.AddRestfulResource("/api/podgroups", "RestfulPodGroups", RestfulPodGroups{})
	s.AddRestfulResource("/api/depends", "RestfulDependPods", RestfulDependPods{})
	s.AddRestfulResource("/api/nodes", "RestfulNodes", RestfulNodes{})
	s.AddRestfulResource("/api/engine/config", "EngineConfig", EngineConfigApi{})
	s.AddRestfulResource("/api/engine/maintenance", "EngineMaintenance", EngineMaintenanceApi{})
	s.AddRestfulResource("/api/status", "RestfulStatus", RestfulStatus{})
	s.AddRestfulResource("/api/constraints", "RestfulConstraints", RestfulConstraints{})
	s.AddRestfulResource("/api/notifies", "RestfulNotifies", RestfulNotifies{})
	s.AddRestfulResource("/api/ports", "RestfulPorts", RestfulPorts{})
	s.AddRestfulResource("/api/guard", "RestfulGuard", RestfulGuard{})
	s.AddRestfulResource("/api/cntstatushistory", "RestfulCntStatusHstry", RestfulCntStatusHstry{})

	s.Get("/debug/vars", "RuntimeStat", s.getRuntimeStat)
	s.NotFound(func(ctx context.Context, w http.ResponseWriter, r *http.Request) context.Context {
		s.renderError(w, http.StatusNotFound, "Page not found", "")
		return ctx
	})
	s.MethodNotAllowed(func(ctx context.Context, w http.ResponseWriter, r *http.Request) context.Context {
		s.renderError(w, http.StatusMethodNotAllowed, "Method is not allowed", "")
		return ctx
	})

	s.started = true
	defer func() { s.started = false }()

	return s.Run(addr)
}

func (s *Server) getRuntimeStat(ctx context.Context, w http.ResponseWriter, r *http.Request) context.Context {
	http.DefaultServeMux.ServeHTTP(w, r)
	return ctx
}

func (s *Server) adaptResourceHandler(handler server.ResourceHandler) server.Handler {
	return func(ctx context.Context, w http.ResponseWriter, r *http.Request) context.Context {
		code, data := handler(ctx, r)
		if code < 400 {
			s.renderJsonOr500(w, code, data)
		} else {
			errMessage := ""
			if msg, ok := data.(string); ok {
				errMessage = msg
			}
			switch code {
			case http.StatusMethodNotAllowed:
				if errMessage == "" {
					errMessage = fmt.Sprintf("Method %q is not allowed", r.Method)
				}
				s.renderError(w, code, errMessage, data)
			case http.StatusNotFound:
				if errMessage == "" {
					errMessage = "Cannot find the resource"
				}
				s.renderError(w, code, errMessage, data)
			case http.StatusBadRequest:
				if errMessage == "" {
					errMessage = "Invalid request get or post params"
				}
				s.renderError(w, code, errMessage, data)
			default:
				if errMessage == "" {
					errMessage = fmt.Sprintf("HTTP Error Code: %d", code)
				}
				s.renderError(w, code, errMessage, data)
			}
		}
		return ctx
	}
}

const (
	kContentCharset = "; charset=UTF-8"
	kContentJson    = "application/json"
)

func (s *Server) renderJson(w http.ResponseWriter, status int, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	data = append(data, '\n')
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", kContentJson+kContentCharset)
	w.WriteHeader(status)
	if status != http.StatusNoContent {
		_, err = w.Write(data)
	}
	return err
}

func (s *Server) renderJsonOr500(w http.ResponseWriter, status int, v interface{}) {
	if err := s.renderJson(w, status, v); err != nil {
		s.renderError(w, http.StatusInternalServerError, err.Error(), "")
	}
}

func (s *Server) renderError(w http.ResponseWriter, status int, msg string, data interface{}) {
	apiError := ApiError{msg, data}
	if err := s.renderJson(w, status, apiError); err != nil {
		log.Errorf("Server got a json rendering error, %s", err)
		// we fallback to the http.Error instead return a json formatted error
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) Shutdown() {
	if s.started {
		s.Stop(time.Second)
	}
	if s.engine != nil {
		s.engine.Stop()
		s.engine = nil
	}
}

type ApiError struct {
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

func initOrcEngine(swarmAddr string, etcdAddr string, isDebug bool) (*engine.OrcEngine, error) {
	store, err := setcd.NewStore(etcdAddr, isDebug)
	if err != nil {
		return nil, err
	}

	cluster, err := swarm.NewCluster(swarmAddr, 10*time.Second, 20*time.Second)
	if err != nil {
		return nil, err
	}

	return engine.New(cluster, store)
}

func initNetwWorkMgr(endpoint string) {
	network.InitNetWorkManager("calico", endpoint)
}

func New(swarmAddr, etcdAddr string, isDebug bool) *Server {
	srv := &Server{
		swarmAddress: swarmAddr,
		etcdAddress:  etcdAddr,
		isDebug:      isDebug,
		started:      false,
		engine:       nil,
		runtime:      nil,
	}
	return srv
}

func getEngine(ctx context.Context) *engine.OrcEngine {
	return ctx.Value("engine").(*engine.OrcEngine)
}

func getUrlReverser(ctx context.Context) UrlReverser {
	return ctx.Value("urlReverser").(UrlReverser)
}
