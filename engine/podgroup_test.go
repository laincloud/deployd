package engine

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/laincloud/deployd/cluster"
	"github.com/laincloud/deployd/cluster/swarm"
	"github.com/laincloud/deployd/storage"
	"github.com/laincloud/deployd/storage/etcd"
)

func TestContainerSorter(t *testing.T) {
	containers := []*container{
		&container{version: 1, instance: 1, driftCount: 1},
		&container{version: 2, instance: 1, driftCount: 1},
		&container{version: 1, instance: 1, driftCount: 2},
		&container{version: 3, instance: 1, driftCount: 1}}
	By(ByVersionAndDriftCounter).Sort(containers)
	for _, container := range containers {
		fmt.Println(container)
	}
}

func TestPodGroupRefresh(t *testing.T) {
	etcdAddr := "http://127.0.0.1:2379"
	ConfigPortsManager(etcdAddr)
	c, store, err := initClusterAndStore()
	if err != nil {
		t.Fatalf("Cannot create the cluster and storage, %s", err)
	}

	engine, err := New(c, store)
	if err != nil {
		t.Fatalf("Cannot create the orc engine, %s", err)
	}

	namespace := "hello"
	name := "hello.proc.web.web"
	pgSpec := createPodGroupSpec(namespace, name, 2)
	pgSpec.RestartPolicy = RestartPolicyAlways
	if err := engine.NewPodGroup(pgSpec); err != nil {
		t.Fatalf("Should not return error, %s", err)
	}

	time.Sleep(20 * time.Second)
	if pg, ok := engine.InspectPodGroup(name); !ok {
		t.Errorf("We should have the pod group, but we don't get it")
	} else if pg.State != RunStateSuccess {
		t.Errorf("We should have the pod deployed and running, %#v", pg.State)
	} else {
		containerIds := pg.Pods[0].ContainerIds()
		for _, cId := range containerIds {
			c.RemoveContainer(cId, true, false)
		}
	}

	time.Sleep(20 * time.Second)

	if pg, ok := engine.InspectPodGroup(name); !ok {
		t.Errorf("We should have the pod group, but we don't get it")
	} else if pg.State != RunStateSuccess {
		t.Errorf("We should have the pod deployed and running, %#v", pg.State)
	}

	if err := engine.RemovePodGroup(name); err != nil {
		t.Errorf("We should be able to remove the pod group, %s", err)
	}

	time.Sleep(20 * time.Second)
}

func TestEnginePodGroup(t *testing.T) {
	etcdAddr := "http://127.0.0.1:2379"
	ConfigPortsManager(etcdAddr)
	c, store, err := initClusterAndStore()
	if err != nil {
		t.Fatalf("Cannot create the cluster and storage, %s", err)
	}

	engine, err := New(c, store)
	if err != nil {
		t.Fatalf("Cannot create the orc engine, %s", err)
	}

	namespace := "hello"
	name := "hello.proc.web.web"
	pgSpec := createPodGroupSpec(namespace, name, 1)
	if err := engine.NewPodGroup(pgSpec); err != nil {
		t.Fatalf("Should not return error, %s", err)
	}
	if err := engine.NewPodGroup(pgSpec); err == nil {
		t.Errorf("Should return exists error, but we got no problem")
	}

	time.Sleep(20 * time.Second)
	if pg, ok := engine.InspectPodGroup(name); !ok {
		t.Errorf("We should have the pod group, but we don't get it")
	} else if pg.State != RunStateSuccess {
		t.Errorf("We should have the pod deployed and running")
	}

	engine.RescheduleInstance(name, 3)
	time.Sleep(20 * time.Second)
	if pg, ok := engine.InspectPodGroup(name); !ok {
		t.Errorf("We should have the pod group, but we don't get it")
	} else if len(pg.Pods) != 3 {
		t.Errorf("We should have 3 instance of the pods")
	}

	engine.RescheduleInstance(name, 1)
	time.Sleep(30 * time.Second)
	if pg, ok := engine.InspectPodGroup(name); !ok {
		t.Errorf("We should have the pod group, but we don't get it")
	} else if len(pg.Pods) != 1 {
		bytes, err := json.Marshal(pg.Pods)
		pods := ""
		if err == nil {
			pods = string(bytes)
		}
		t.Errorf("We should have 1 instance of the pods : %v", pods)
	}

	podSpec := createPodSpec(namespace, name)
	podSpec.Containers[0].MemoryLimit = 24 * 1024 * 1024
	engine.RescheduleSpec(name, podSpec)
	time.Sleep(40 * time.Second)
	if pg, ok := engine.InspectPodGroup(name); !ok {
		t.Errorf("We should have the pod group, but we don't get it")
	} else if pg.Spec.Version != 1 {
		t.Errorf("We should have version 1 of the pods")
	}

	if err := engine.RemovePodGroup(name); err != nil {
		t.Errorf("We should be able to remove the pod group, %s", err)
	} else if err := engine.NewPodGroup(pgSpec); err == nil {
		t.Errorf("We should not be able to deploy pod group again in short time we remove it")
	}
	time.Sleep(20 * time.Second)
}

func initClusterAndStore() (cluster.Cluster, storage.Store, error) {
	etcdAddr := "http://127.0.0.1:2379"
	swarmAddr := "tcp://127.0.0.1:2376"

	store, err := etcd.NewStore(etcdAddr, false)
	if err != nil {
		return nil, nil, err
	}

	c, err := swarm.NewCluster(swarmAddr, 30*time.Second, 10*time.Minute)
	if err != nil {
		return nil, nil, err
	}

	return c, store, nil
}

func createPodGroupSpec(namespace, name string, numInstance int) PodGroupSpec {
	podSpec := createPodSpec(namespace, name)
	return NewPodGroupSpec(name, namespace, podSpec, numInstance)
}

func createPodSpec(namespace, name string) PodSpec {
	cSpec := NewContainerSpec("busybox")
	cSpec.Command = []string{"/bin/sh", "-c", "while true; do echo Hello world; sleep 1; done"}
	cSpec.MemoryLimit = 15 * 1024 * 1024
	cSpec.Expose = 5000
	podSpec := NewPodSpec(cSpec)
	podSpec.Name = name
	podSpec.Namespace = namespace
	podSpec.Annotation = fmt.Sprintf("{\"test\":\"Unit test for %s\"}", name)
	return podSpec
}
