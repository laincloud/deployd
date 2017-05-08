package engine

import (
	"fmt"
	"testing"
	"time"

	"github.com/laincloud/deployd/cluster/swarm"
	"github.com/laincloud/deployd/storage/etcd"
	"github.com/mijia/sweb/log"
)

func TestEagleViewRefresh(t *testing.T) {
	etcdAddr := "http://192.168.77.21:4001"
	swarmAddr := "tcp://192.168.77.21:2376"
	isDebug := true

	log.EnableDebug()
	_, err := etcd.NewStore(etcdAddr, isDebug)
	if err != nil {
		t.Errorf("Cannot init the etcd storage")
	}

	kluster, err := swarm.NewCluster(swarmAddr, 30*time.Second, 10*time.Minute, isDebug)
	if err != nil {
		t.Errorf("Cannot init the swarm cluster manager")
	}

	ev := NewRuntimeEagleView()
	if err := ev.Refresh(kluster); err != nil {
		t.Errorf("Cannot refresh eagle view, %s", err)
	}

	if pods, ok := ev.GetRuntimeEaglePods("console.web.web"); !ok {
		t.Errorf("Didn't get back runtime pods")
	} else {
		fmt.Printf("%+v\n", pods)
	}

	if _, err := ev.RefreshPodGroup(kluster, "console.web.web"); err != nil {
		t.Errorf("Cannot refresh the pod group in eagle view")
	}

	if pods, ok := ev.GetRuntimeEaglePods("console.web.web"); !ok {
		t.Errorf("Didn't get back runtime pods")
	} else {
		fmt.Printf("%+v\n", pods)
	}
}
