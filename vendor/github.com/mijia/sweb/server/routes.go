package server

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/mijia/sweb/log"
)

const (
	kAssetsReverseKey = "_!#assets_"
)

// EnableExtraAssetsMapping can be used to set some extra assets mapping data for server,
// server would look up this mapping for the frontend assets first when reverse an assets url.
func (s *Server) EnableExtraAssetsMapping(assetsMapping map[string]string) {
	s.extraAssetsMapping = assetsMapping
}

// Reverse would reverse the named routes with params supported. E.g. we have a routes "/hello/:name" named "Hello",
// then we can call s.Reverse("Hello", "world") gives us "/hello/world"
func (s *Server) Reverse(name string, params ...interface{}) string {
	path, ok := s.namedRoutes[name]
	if !ok {
		log.Warnf("Server routes reverse failed, cannot find named routes %q", name)
		return "/no_such_named_routes_defined"
	}
	if len(params) == 0 || path == "/" {
		return path
	}
	strParams := make([]string, len(params))
	for i, param := range params {
		strParams[i] = fmt.Sprint(param)
	}
	parts := strings.Split(path, "/")[1:]
	paramIndex := 0
	for i, part := range parts {
		if part[0] == ':' || part[0] == '*' {
			if paramIndex < len(strParams) {
				parts[i] = strParams[paramIndex]
				paramIndex++
			}
		}
	}
	return httprouter.CleanPath("/" + strings.Join(parts, "/"))
}

// Assets would reverse the assets url, e.g. s.Assets("images/test.png") gives us "/assets/images/test.png"
func (s *Server) Assets(path string) string {
	if asset, ok := s.extraAssetsMapping[path]; ok {
		path = asset
	}
	return s.Reverse(kAssetsReverseKey, path)
}

// Files register the static file system or assets to the router
func (s *Server) Files(path string, root http.FileSystem) {
	s.router.ServeFiles(path, root)
	s.namedRoutes[kAssetsReverseKey] = path
}
