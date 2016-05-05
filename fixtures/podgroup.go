package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"runtime"

	"github.com/laincloud/deployd/engine"
)

var apiServer string

func main() {
	var command string
	flag.StringVar(&apiServer, "api", "http://localhost:9000", "Deployd api server address")
	flag.StringVar(&command, "cmd", "create", "Command to test the server")
	flag.Parse()

	pgName := "hello.proc.web.foo"
	depName := "rpcserver.proc.portal"

	switch command {
	case "create":
		create(pgName)
	case "get":
		inspect(pgName)
	case "rm":
		remove(pgName)
	case "ri":
		patchInstance(pgName)
	case "rs":
		patchSpec(pgName)
	case "dcreate":
		createDependency(depName)
	case "dget":
		getDependency(depName)
	case "drm":
		removeDependency(depName)
	case "dup":
		updateDependency(depName)
	default:
		fmt.Println("Unknown command", command)
	}
}

func removeDependency(depName string) {
	data, err := sendRequest("DELETE", "api/depends?name="+depName, nil, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
}

func getDependency(depName string) {
	data, err := sendRequest("GET", "api/depends?name="+depName, nil, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
}

func updateDependency(depName string) {
	containerSpec := getContainerSpec()
	podSpec := engine.NewPodSpec(containerSpec)
	podSpec.Name = "rpcserver.proc.portal"
	podSpec.Namespace = "rpcserver"
	body, err := json.Marshal(podSpec)
	if err != nil {
		panic(err)
	}
	data, err := sendRequest("PUT", "api/depends", body, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
}

func createDependency(depName string) {
	containerSpec := getContainerSpec()
	podSpec := engine.NewPodSpec(containerSpec)
	podSpec.Name = "rpcserver.proc.portal"
	podSpec.Namespace = "rpcserver"
	body, err := json.Marshal(podSpec)
	if err != nil {
		panic(err)
	}
	data, err := sendRequest("POST", "api/depends", body, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
}

func remove(pgName string) {
	data, err := sendRequest("DELETE", "api/podgroups?name="+pgName, nil, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
}

func inspect(pgName string) {
	data, err := sendRequest("GET", "api/podgroups?name="+pgName+"&force_update=false", nil, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
}

func patchInstance(pgName string) {
	v := url.Values{}
	v.Set("name", pgName)
	v.Set("cmd", "replica")
	v.Set("num_instances", "2")
	data, err := sendRequest("PATCH", "api/podgroups?"+v.Encode(), nil, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
}

func patchSpec(pgName string) {
	containerSpec := getContainerSpec()
	containerSpec.MemoryLimit = 25 * 1024 * 1024
	podSpec := engine.NewPodSpec(containerSpec)
	podSpec.Name = pgName
	podSpec.Namespace = "hello"

	body, err := json.Marshal(podSpec)
	if err != nil {
		panic(err)
	}
	v := url.Values{}
	v.Set("name", pgName)
	v.Set("cmd", "spec")
	data, err := sendRequest("PATCH", "api/podgroups?"+v.Encode(), body, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
}

func create(pgName string) {
	containerSpec := getContainerSpec()
	podSpec := engine.NewPodSpec(containerSpec)
	//podSpec.Dependencies = []engine.Dependency{
	//engine.Dependency{
	//PodName: "rpcserver.proc.portal",
	//},
	//}
	pgSpec := engine.NewPodGroupSpec(pgName, "hello", podSpec, 2)

	body, err := json.Marshal(pgSpec)
	if err != nil {
		panic(err)
	}
	data, err := sendRequest("POST", "api/podgroups", body, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
}

func getContainerSpec() engine.ContainerSpec {
	containerSpec := engine.NewContainerSpec("busybox")
	containerSpec.Entrypoint = []string{"/bin/sh", "-c", "while true; do echo Hello world; sleep 1; done"}
	containerSpec.MemoryLimit = 15 * 1024 * 1024
	containerSpec.Expose = 5000
	return containerSpec
}

func sendRequest(method string, path string, body []byte, headers map[string]string) ([]byte, error) {
	b := bytes.NewBuffer(body)
	urlPath := fmt.Sprintf("%s/%s", apiServer, path)
	fmt.Printf("SendRequest %q, [%s]\n", method, urlPath)
	req, err := http.NewRequest(method, urlPath, b)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	if headers != nil {
		for key, value := range headers {
			req.Header.Add(key, value)
		}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		fmt.Printf("Response error: %d - %s\n", resp.StatusCode, resp.Status)
	}
	return data, err
}

var httpClient *http.Client

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	httpClient = &http.Client{}
}
