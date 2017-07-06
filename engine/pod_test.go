package engine

import (
	"testing"
	"time"

	"github.com/laincloud/deployd/cluster/swarm"
	"github.com/laincloud/deployd/storage/etcd"
	"github.com/mijia/sweb/log"
)

func TestPodController(t *testing.T) {
	etcdAddr := "http://127.0.0.1:2379"
	swarmAddr := "tcp://127.0.0.1:2376"
	isDebug := false

	_, err := etcd.NewStore(etcdAddr, isDebug)
	if err != nil {
		t.Errorf("Cannot init the etcd storage")
	}

	c, err := swarm.NewCluster(swarmAddr, 30*time.Second, 10*time.Minute)
	if err != nil {
		t.Errorf("Cannot init the swarm cluster manager")
	}

	cstController = NewConstraintController()

	cSpec := NewContainerSpec("training/webapp")
	cSpec.Command = []string{"python", "app.py"}
	cSpec.MemoryLimit = 15 * 1024 * 1024
	cSpec.Expose = 5000
	podSpec := NewPodSpec(cSpec)
	podSpec.Name = "hello.proc.web.foo"
	podSpec.Namespace = "hello"

	pc := &podController{
		spec: podSpec,
		pod: Pod{
			InstanceNo: 1,
		},
	}
	pc.pod.State = RunStatePending

	pc.Deploy(c)
	if pc.pod.State != RunStateSuccess {
		t.Fatal("Pod should be deployed")
	}

	cId := pc.pod.Containers[0].Id

	ev := NewRuntimeEagleView()
	if _, err := ev.RefreshPodGroup(c, podSpec.Name); err != nil {
		t.Fatal("Failed to refresh the pod group")
	}
	podContainers, ok := ev.GetRuntimeEaglePods(podSpec.Name)
	if !ok || len(podContainers) == 0 {
		t.Fatal("Failed to get the runtime eagle pods from swarm")
	}
	log.Infof("podContainers:%v len:%v", podContainers, len(podContainers))
	if podContainers[0].Container.Id != cId {
		t.Fatal("Should have the same container id as we deployed")
	}

	pc.Refresh(c)
	if pc.pod.State != RunStateSuccess {
		t.Fatal("The pod should be in success run state")
	}

	pc.Stop(c)
	if pc.pod.State != RunStateFail {
		t.Fatal("The pod should be stopped and exited")
	}

	pc.Start(c)
	if pc.pod.State != RunStateSuccess {
		t.Fatal("The pod should be restarted and in success run state")
	}

	pc.Remove(c)
	if err := ev.Refresh(c); err != nil {
		t.Fatal("Failed to refresh the pod group")
	}
	podContainers, ok = ev.GetRuntimeEaglePods(podSpec.Name)
	if ok {
		t.Fatal("Should not get data for the pods")
	}
}
