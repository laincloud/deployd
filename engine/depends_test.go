package engine

import (
	"fmt"
	"testing"
	"time"
)

func TestDependsPodCtrl(t *testing.T) {
	c, store, err := initClusterAndStore()
	if err != nil {
		t.Fatalf("Cannot create the cluster and storage, %s", err)
	}

	engine, err := New(c, store)
	if err != nil {
		t.Fatalf("Cannot create the orc engine, %s", err)
	}

	publisher := NewPublisher(true)
	publisher.AddListener(engine)

	podSpec := createPodSpec("hello", "hello.portal")
	if err := engine.NewDependencyPod(podSpec); err != nil {
		t.Errorf("Cannot create dependency pod, %s", err)
	}

	event := DependencyEvent{
		Type:      "add",
		Name:      "hello.portal",
		NodeName:  "node1",
		Namespace: "client.proc.foo",
	}
	publisher.EmitEvent(event)
	publisher.EmitEvent(event)
	event.NodeName = "node2"
	publisher.EmitEvent(event)

	time.Sleep(15 * time.Second)
	if pods, err := engine.GetDependencyPod("hello.portal"); err != nil {
		t.Errorf("Cannot get the depends pods back, %s", err)
	} else {
		if nsPods, ok := pods.Pods["client.proc.foo"]; !ok {
			t.Errorf("We should get the namespace back")
		} else if len(nsPods) != 2 {
			t.Errorf("We should have 2 portal pods running on each node")
		}
	}

	fmt.Println("==========================\n\n")

	podSpec = podSpec.Clone()
	podSpec.Containers[0].MemoryLimit = 20 * 1024 * 1024
	if err := engine.UpdateDependencyPod(podSpec); err != nil {
		t.Errorf("Cannot update the depends pod, %s", err)
	}
	time.Sleep(30 * time.Second)
	if pods, err := engine.GetDependencyPod("hello.portal"); err != nil {
		t.Errorf("Cannot get the depends pods back, %s", err)
	} else {
		if nsPods, ok := pods.Pods["client.proc.foo"]; !ok {
			t.Errorf("We should get the namespace back")
		} else if len(nsPods) != 2 {
			t.Errorf("We should have 2 portal pods running on each node")
		}
	}

	time.Sleep(10 * time.Minute)

	fmt.Println("==========================\n\n")
	if err := engine.RemoveDependencyPod("hello.portal", true); err != nil {
		t.Errorf("Cannot remove the depends pods, %s", err)
	}
	time.Sleep(30 * time.Second)
	if _, err := engine.GetDependencyPod("hello.portal"); err == nil {
		t.Errorf("We should not get the depends pods back, %s", err)
	}
}
