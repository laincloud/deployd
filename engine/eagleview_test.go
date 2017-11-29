package engine

import (
	"fmt"
	"testing"
	"time"
)

func TestEagleViewRefresh(t *testing.T) {
	etcdAddr := "http://127.0.0.1:2379"
	ConfigPortsManager(etcdAddr)

	kluster, store, err := initClusterAndStore()
	if err != nil {
		t.Fatalf("Cannot create the cluster and storage, %s", err)
	}

	engine, err := New(kluster, store)
	if err != nil {
		t.Fatalf("Cannot create the orc engine, %s", err)
	}

	namespace := "hello"
	name := "hello.web.web"
	pgSpec := createPodGroupSpec(namespace, name, 1)
	pgSpec.RestartPolicy = RestartPolicyAlways
	if err := engine.NewPodGroup(pgSpec); err != nil {
		t.Fatalf("Should not return error, %s", err)
	}

	time.Sleep(20 * time.Second)
	ev := NewRuntimeEagleView()
	if err := ev.Refresh(kluster); err != nil {
		t.Errorf("Cannot refresh eagle view, %s", err)
	}

	if pods, ok := ev.GetRuntimeEaglePods("hello.web.web"); !ok {
		t.Errorf("Didn't get back runtime pods")
	} else {
		fmt.Printf("%+v\n", pods)
	}

	if _, err := ev.RefreshPodGroup(kluster, "hello.web.web"); err != nil {
		t.Errorf("Cannot refresh the pod group in eagle view")
	}

	if pods, ok := ev.GetRuntimeEaglePods("hello.web.web"); !ok {
		t.Errorf("Didn't get back runtime pods")
	} else {
		fmt.Printf("%+v\n", pods)
	}

	if err := engine.RemovePodGroup(name); err != nil {
		t.Errorf("We should be able to remove the pod group, %s", err)
	}

	time.Sleep(20 * time.Second)
}
