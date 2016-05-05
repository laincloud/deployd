package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime"
)

var apiServer string

func main() {
	var command string
	flag.StringVar(&apiServer, "api", "http://localhost:9000", "Deployd api server address")
	flag.StringVar(&command, "cmd", "create", "Command to test the server")
	flag.Parse()

	procName := "hello/procs/web/foo"

	switch command {
	case "create":
		createProc(procName)
	case "rm":
		removeProc(procName)
	case "get":
		getProc(procName)
	case "ctrl":
		controlProc(procName)
	case "r1":
		rescheduleProc(procName)
	case "r2":
		rescheduleProc2(procName)
	case "clean":
		cleanProc(procName)
	default:
		fmt.Println("Unknown command", command)
	}
}

func rescheduleProc2(procName string) {
	p := map[string]interface{}{
		"num_instances": 1,
	}
	body, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	data, err := sendRequest("PATCH", "api/apps/"+procName+"?command=reschedule", body, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println("Instance Schedule Example")
	fmt.Println(string(data))
}

func rescheduleProc(procName string) {
	p := map[string]interface{}{
		"cpu":           0,
		"num_instances": 2,
		"memory":        "25m",
		"env":           []string{"DEBUG=true"},
	}
	body, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	data, err := sendRequest("PATCH", "api/apps/"+procName+"?command=reschedule", body, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println("Full Schedule Example")
	fmt.Println(string(data))
}

func cleanProc(procName string) {
	data, err := sendRequest("PATCH", "api/apps/"+procName+"?command=clean", nil, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println("clean")
	fmt.Println(string(data))
}

func controlProc(procName string) {
	ctrlRequest := func(command string) {
		fmt.Println("Running patch command", command)
		data, err := sendRequest("PATCH", "api/apps/"+procName+"?command="+command, nil, nil)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(data))
	}
	ctrlRequest("stop")
	ctrlRequest("start")
	ctrlRequest("restart")
}

func getProc(procName string) {
	data, err := sendRequest("GET", "api/apps/"+procName+"?force_update=true", nil, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
}

func removeProc(procName string) {
	_, err := sendRequest("DELETE", "api/apps/"+procName+"?force=true", nil, nil)
	if err != nil {
		panic(err)
	}
}

func createProc(procName string) {
	var p struct {
		Image        string   `json:"image"`
		Env          []string `json:"env"`
		User         string   `json:"user"`
		WorkingDir   string   `json:"working_dir"`
		Volumes      []string `jsone"volumes"`
		Command      []string `json:"command"`
		NumInstances int      `json:"num_instances"`
		CpuLimit     int      `json:"cpu"`
		MemoryLimit  string   `json:"memory"`
		Expose       int      `json:"expose"`
	}
	p.Image = "training/webapp"
	p.Command = []string{"python", "app.py"}
	p.Expose = 5000
	p.NumInstances = 1
	p.CpuLimit = 1
	p.MemoryLimit = "10m"
	p.Volumes = []string{"tmp"}

	body, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	data, err := sendRequest("POST", "api/apps/"+procName, body, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
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
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("Response error: %d - %s", resp.StatusCode, resp.Status)
	}

	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	return data, err
}

var httpClient *http.Client

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	httpClient = &http.Client{}
}
