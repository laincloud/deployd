package proxy

import (
	"github.com/mijia/sweb/log"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
)

type Proxy struct {
	s       *http.Server
	addr    string
	dest    string
	lock    *sync.RWMutex
	stop    chan struct{}
	started bool
}

func New(addr string, dest string) *Proxy {
	p := &Proxy{
		addr:    addr,
		dest:    dest,
		lock:    &sync.RWMutex{},
		started: false,
		stop:    make(chan struct{}),
	}
	p.s = &http.Server{Addr: p.addr, Handler: p}
	return p
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if p.dest == "" {
		log.Warnf("Proxy's destination is empty")
		return
	}
	if err := hijack(p.dest, w, r); err != nil {
		log.Errorf("Hijack return error: %v", err)
	}
}

func (p *Proxy) Run() error {
	log.Infof("Proxy running, working on [%s] => [%s]", p.addr, p.dest)

	p.started = true
	defer func() { p.started = false }()

	l, err := net.Listen("tcp", p.addr)
	if err != nil {
		return err
	}

	go func() {
		<-p.stop
		l.Close()
	}()

	return p.s.Serve(l)
}

func (p *Proxy) Shutdown() {
	if p.started {
		p.stop <- struct{}{}
	}
}

func (p *Proxy) SetDest(dest string) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.dest = dest
	log.Infof("Proxy's destination changed, now working on [%s] => [%s]", p.addr, p.dest)
}

func (p *Proxy) Dest() string {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.dest
}

func hijack(addr string, w http.ResponseWriter, r *http.Request) error {
	var (
		conn net.Conn
		err  error
	)

	if parts := strings.SplitN(addr, "://", 2); len(parts) == 2 {
		addr = parts[1]
	}

	conn, err = net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	hj, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return err
	}
	defer hj.Close()
	defer conn.Close()

	// write request to <addr>
	if err := r.Write(conn); err != nil {
		return err
	}

	// transfer data for this two connection
	errc := make(chan error, 2)
	cp := func(dst io.Writer, src io.Reader) {
		_, err := io.Copy(dst, src)
		// io.Writer do not having CloseWrite(), convert.
		if conn, ok := dst.(interface {
			CloseWrite() error
		}); ok {
			conn.CloseWrite()
		}
		errc <- err
	}
	go cp(conn, hj)
	go cp(hj, conn)

	// wait copy finished
	if err := <-errc; err != nil {
		return err
	}
	if err := <-errc; err != nil {
		return err
	}
	return nil
}
