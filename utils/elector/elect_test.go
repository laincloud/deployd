package elector

import (
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"os"
	"strings"
	"testing"
)

var (
	e     *Elector
	etcds []string
	err   error
)

func init() {
	etcds = strings.Split(os.Getenv("ETCD_TEST"), ",")
	e, err = New(etcds, LeaderKey, "127.0.0.1:2378")
	if err != nil {
		panic(err)
	}
}

func TestRunElection(t *testing.T) {

	stop := make(chan struct{})
	defer close(stop)

	ch := e.Run(stop)

	st, err := libkv.NewStore(store.ETCD, etcds, nil)
	if err != nil {
		t.Error(err)
	}

	t.Log("leader changed to ", <-ch)
	if err := st.Put(LeaderKey, []byte("hello"), nil); err != nil {
		t.Error(err)
	}
	t.Log("leader changed to ", <-ch)
	if err := st.Put(LeaderKey, []byte("world"), nil); err != nil {
		t.Error(err)
	}
	t.Log("leader changed to ", <-ch)
	if err := st.Put(LeaderKey, []byte(""), nil); err != nil {
		t.Error(err)
	}
	t.Log("leader changed to ", <-ch)
	if err := st.Delete(LeaderKey); err != nil {
		t.Error(err)
	}
	t.Log("leader changed to ", <-ch)
	if err := st.Put(LeaderKey, []byte("192.168.77.22:1234"), nil); err != nil {
		t.Error(err)
	}
	t.Log("leader changed to ", <-ch)
}

func TestIsLeader(t *testing.T) {
	t.Log("isleader:", e.IsLeader())
}
