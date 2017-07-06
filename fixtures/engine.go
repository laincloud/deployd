package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/laincloud/deployd/cluster/swarm"
	"github.com/laincloud/deployd/engine"
	"github.com/laincloud/deployd/storage/etcd"
	"github.com/mijia/sweb/log"
)

func main() {
	timeout := time.Duration(30 * time.Second)
	cluster, err := swarm.NewCluster("tcp://192.168.51.21:8178", timeout, 30*time.Minute)
	if err != nil {
		panic(err)
	}
	store, err := etcd.NewStore("http://192.168.51.21:4001", debug)
	if err != nil {
		panic(err)
	}

	orcEngine, err := engine.New(cluster, store)
	if err != nil {
		panic(err)
	}

	tRschSpec := false

	containerSpec := engine.NewContainerSpec("training/webapp")
	containerSpec.Name = "c1"
	containerSpec.Command = []string{"python", "app.py"}
	if tRschSpec {
		containerSpec.Volumes = []string{"/tmp"}
	}
	containerSpec.MemoryLimit = 15 * 1024 * 1024
	containerSpec.Expose = 5000

	pgSpec := engine.NewPodGroupSpec("hello.proc.web.foo", "hello", engine.NewPodSpec(containerSpec), 1)
	pgSpec.RestartPolicy = engine.RestartPolicyAlways

	err = orcEngine.NewPodGroup(pgSpec)
	if err != nil {
		panic(fmt.Sprintf("Fail to create new pod group, %s", err))
	}

	time.Sleep(3 * time.Second)
	orcEngine.RescheduleInstance(pgSpec.Name, 2)

	if tRschSpec {
		time.Sleep(3 * time.Second)
		containerSpec.MemoryLimit = 32 * 1024 * 1024
		cSpec2 := containerSpec
		cSpec2.Name = "c2"
		orcEngine.RescheduleSpec(pgSpec.Name, engine.NewPodSpec(containerSpec, cSpec2))
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, os.Kill)
	go func() {
		<-sigs
		orcEngine.RemovePodGroup(pgSpec.Name)
		time.Sleep(5 * time.Second)
		os.Exit(1)
	}()

	time.Sleep(20 * time.Minute)
}

func dddd(title string, v interface{}) {
	fmt.Println(title)
	data, _ := json.MarshalIndent(v, "", "  ")
	fmt.Printf("%s\n\n", string(data))
}
