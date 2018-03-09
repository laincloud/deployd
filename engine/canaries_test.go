package engine

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEngineCanaries(t *testing.T) {
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
	name := "hello.canary.web"
	pgSpec := createPodGroupSpec(namespace, name, 1)
	data := map[string]interface{}{"suffix": 1, "upstream": "beta1"}
	strategies := []*Strategy{&Strategy{"uidsuffix", []interface{}{data}}}
	canary := &Canary{strategies}
	canarySpec := CanaryPodsWithSpec{pgSpec, canary}
	if err := engine.NewCanary(canarySpec); err != nil {
		t.Fatalf("Should not return error, %s", err)
	}
	if err := engine.NewCanary(canarySpec); err == nil {
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

	if nCanary := engine.FetchCanaryInfos(name); nCanary == nil {
		t.Fatalf("Should return canary!")
	} else {
		if !nCanary.Equal(canary) {
			t.Fatalf("Canary info should not changed")
		}
	}

	if err := engine.RemovePodGroup(name); err != nil {
		t.Errorf("We should be able to remove the pod group, %s", err)
	} else if err := engine.NewPodGroup(pgSpec); err == nil {
		t.Errorf("We should not be able to deploy pod group again in short time we remove it")
	}
	time.Sleep(20 * time.Second)

	if nCanary := engine.FetchCanaryInfos(name); nCanary != nil {
		t.Fatalf("Should not return nCanary")
	}
}
