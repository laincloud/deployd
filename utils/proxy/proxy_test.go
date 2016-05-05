package proxy

import (
	"fmt"
	"os/exec"
	"testing"
	"time"
)

func TestRun(t *testing.T) {
	from, to := "127.0.0.1:1234", "docker.io"
	t.Logf("testing proxy %s => %s", from, to)
	p := New(from, to)
	defer p.Stop()
	go p.Run()
	time.Sleep(time.Second)
	t.Logf("send GET request to http://%s/v1/search", from)
	output, err := exec.Command("curl", fmt.Sprintf("http://%s/v1/search", from)).Output()
	if err != nil {
		t.Error(err)
	}
	t.Log(string(output))
}

func TestStop(t *testing.T) {
	from, to := "127.0.0.1:1234", "docker.io"
	p := New(from, to)

	go func() {
		time.Sleep(time.Second)
		p.Stop()
	}()

	if err := p.Run(); err != nil {
		t.Log(err)
	}
}
