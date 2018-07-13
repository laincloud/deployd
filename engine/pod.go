package engine

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/laincloud/deployd/cluster"
	"github.com/laincloud/deployd/network"
	"github.com/laincloud/deployd/utils/units"
	"github.com/laincloud/deployd/utils/util"
	"github.com/mijia/adoc"
	"github.com/mijia/go-generics"
	"github.com/mijia/sweb/log"
)

// some instances written with JAVA like language may suffer from OOM,
// if they are restarted before (now - RestartInfoClearInterval), clear the restart info
var RestartInfoClearInterval time.Duration

const (
	DefaultHealthInterval = 10
	DefaultHealthTimeout  = 3
	DefaultHealthRetries  = 3

	DefaultSetUpTime = 20

	CPUQuota        = int64(1000000)
	CPUMaxPctg      = 50 // max percentage of total cpu
	CPUMaxLevel     = 8
	CPUDeafultLevel = 2

	CURL_TMPLT = `echo $(curl -m %v -s -o /dev/null -w '%%{http_code}\n' %s) | grep -Eq "^[2-3]..$"`
)

// podController is controlled by the podGroupController
type podController struct {
	spec  PodSpec
	pod   Pod
	event chan interface{}
}

func (pc *podController) String() string {
	return fmt.Sprintf("PodCtrl %s", pc.spec)
}

func (pc *podController) launchEvent(v interface{}) {
	select {
	case pc.event <- v:
	default:
	}
}

func (pc *podController) Deploy(cluster cluster.Cluster, extraFilters []string) {
	if pc.pod.State != RunStatePending {
		return
	}

	log.Infof("%s deploying", pc)
	start := time.Now()
	defer func() {
		pc.pod.UpdatedAt = time.Now()
		log.Infof("%s deployed, state=%+v, duration=%s", pc, pc.pod.ImRuntime, time.Now().Sub(start))
	}()

	pc.pod.Containers = make([]Container, len(pc.spec.Containers))
	pc.pod.LastError = ""
	filters := make([]string, 0, len(pc.spec.Filters)+1)
	filters = append(filters, pc.spec.Filters...)
	containerLabel := ContainerLabel{
		Name: pc.spec.Name,
	}

	filters = append(filters, containerLabel.NameAffinity())

	constraints := cstController.GetAllConstraints()
	for _, cstSpec := range constraints {
		filter := cstController.LoadFilterFromConstrain(cstSpec)
		filters = append(filters, filter)
	}

	filters = append(filters, extraFilters...)
	for i, cSpec := range pc.spec.Containers {
		log.Infof("%s create container, filter is %v", pc, filters)
		id, err := pc.createContainer(cluster, filters, i)
		if err != nil {
			log.Warnf("%s Cannot create container, error=%q, spec=%+v", pc, err, cSpec)
			pc.pod.State = RunStateError
			pc.pod.LastError = fmt.Sprintf("Cannot create container, %s", err)
			return
		}
		pc.startContainer(cluster, id)
		pc.pod.Containers[i].Id = id
		pc.refreshContainer(cluster, i)
		pc.spec.PrevState.IPs[i] = pc.pod.Containers[i].ContainerIp
	}

	if pc.pod.State == RunStatePending {
		pc.pod.State = RunStateSuccess
		pc.pod.TargetState = ExpectStateRun
	}
}

func (pc *podController) Drift(cluster cluster.Cluster, fromNode, toNode string, force bool) bool {
	if pc.pod.State == RunStatePending {
		return false
	}
	if fromNode == toNode {
		return false
	}

	pod := pc.pod
	if len(pod.Containers) == 0 {
		return false
	}
	if pod.NodeName() != fromNode {
		return false
	}
	// if we have stateful pod, the containers cannot be drifted so far
	if pc.spec.IsStateful() && !force {
		log.Warnf("%s cannot be drifted since it is a stateful pod", pc)
		return false
	}

	log.Infof("%s drifting", pc)
	start := time.Now()
	defer func() {
		log.Infof("%s drifted, state=%+v, duration=%s", pc, pc.pod.ImRuntime, time.Now().Sub(start))
	}()

	pc.Remove(cluster)
	time.Sleep(10 * time.Second)
	pc.pod.State = RunStatePending
	pc.pod.DriftCount += 1
	extraFilters := make([]string, 0, 1)
	if toNode == "" {
		extraFilters = append(extraFilters, fmt.Sprintf("constraint:node!=%s", fromNode))
	} else {
		extraFilters = append(extraFilters, fmt.Sprintf("constraint:node==%s", toNode))
	}
	pc.Deploy(cluster, extraFilters)
	return true
}

func (pc *podController) Remove(cluster cluster.Cluster) {
	log.Infof("%s removing", pc)
	start := time.Now()
	defer func() {
		log.Warnf("%s removed, duration=%s", pc, time.Now().Sub(start))
	}()

	pc.pod.LastError = ""
	for _, container := range pc.pod.Containers {
		if container.Id == "" {
			continue
		}
		// try to stop first, then remove it
		if err := cluster.StopContainer(container.Id, pc.spec.GetKillTimeout()); err != nil {
			log.Warnf("%s cannot stop the container %s, %s, remove it directly", pc, container.Id, err.Error())
		}
		if err := cluster.RemoveContainer(container.Id, true, false); err != nil {
			log.Warnf("%s Cannot remove the container %s, %s", pc, container.Id, err)
			pc.pod.LastError = fmt.Sprintf("Fail to remove container, %s", err)
		}
	}
	pc.pod.Containers = nil
	pc.pod.State = RunStateExit
	pc.pod.UpdatedAt = time.Now()
}

func (pc *podController) Stop(cluster cluster.Cluster) {
	if pc.pod.State != RunStateSuccess {
		return
	}
	log.Infof("%s stopping", pc)
	start := time.Now()
	defer func() {
		log.Infof("%s stopped, state=%+v, duration=%s", pc, pc.pod.ImRuntime, time.Now().Sub(start))
	}()
	pc.pod.LastError = ""
	for i, container := range pc.pod.Containers {
		if err := cluster.StopContainer(container.Id, pc.spec.GetKillTimeout()); err != nil {
			log.Warnf("%s Cannot stop the container %s, %s", pc, container.Id, err)
			pc.pod.State = RunStateError
			pc.pod.LastError = fmt.Sprintf("Cannot stop container, %s", err)
		} else {
			pc.refreshContainer(cluster, i)
		}
	}
	pc.pod.UpdatedAt = time.Now()
}

func (pc *podController) Start(cluster cluster.Cluster) {
	if pc.pod.State == RunStatePending || pc.pod.State == RunStateSuccess {
		return
	}
	log.Infof("%s starting", pc)
	start := time.Now()
	defer func() {
		log.Infof("%s started, state=%+v, duration=%s", pc, pc.pod.ImRuntime, time.Now().Sub(start))
	}()
	pc.pod.State = RunStateSuccess
	pc.pod.LastError = ""
	for i, container := range pc.pod.Containers {
		if err := pc.startContainer(cluster, container.Id); err == nil {
			pc.refreshContainer(cluster, i)
		} else {
			log.Warnf("%s Cannot start the container %s, %s", pc, container.Id, err)
			pc.pod.State = RunStateError
			pc.pod.LastError = fmt.Sprintf("Cannot start container, %s", err)
		}
	}
	pc.UpdateRestartInfo()
	pc.pod.UpdatedAt = time.Now()
}

func (pc *podController) Restart(cluster cluster.Cluster) {
	log.Infof("%s restarting", pc)
	start := time.Now()
	defer func() {
		log.Infof("%s restarted, state=%+v, duration=%s", pc, pc.pod.ImRuntime, time.Now().Sub(start))
	}()
	pc.pod.State = RunStateSuccess
	pc.pod.LastError = ""
	for i, container := range pc.pod.Containers {
		if err := cluster.RestartContainer(container.Id, pc.spec.GetKillTimeout()); err != nil {
			log.Warnf("%s Cannot restart the container %s, %s", pc, container.Id, err)
			pc.pod.State = RunStateError
			pc.pod.LastError = fmt.Sprintf("Cannot restart container, %s", err)
		} else {
			pc.refreshContainer(cluster, i)
		}
	}
	pc.UpdateRestartInfo()
	pc.pod.UpdatedAt = time.Now()
}

func (pc *podController) UpdateRestartInfo() {
	now := time.Now()
	if pc.pod.RestartCount == 0 || pc.pod.RestartAt.Add(RestartInfoClearInterval).Before(now) {
		pc.pod.RestartCount = 1
	} else {
		pc.pod.RestartCount += 1
	}
	pc.pod.RestartAt = now
}

func (pc *podController) Refresh(cluster cluster.Cluster) {
	log.Infof("%s refreshing", pc)
	start := time.Now()
	defer func() {
		log.Infof("%s refreshed, state=%+v, duration=%s", pc, pc.pod.ImRuntime, time.Now().Sub(start))
	}()

	pc.pod.State = RunStateSuccess
	pc.pod.LastError = ""

	for i := 0; i < len(pc.spec.Containers); i += 1 {
		pc.refreshContainer(cluster, i)
	}
	pc.pod.UpdatedAt = time.Now()
}

func (pc *podController) startContainer(cluster cluster.Cluster, id string) error {
	// start a new instance, ip conflict may happend
	// (when 1. node down abnormally and did not reclaim resource as usually
	//       2. swarm agent just down, we thought the node is just down and recreate )
	// so we should reclaim ip resource when find ip conflict so that we can start new instance correctly
	if err := cluster.StartContainer(id); err != nil {
		log.Warnf("%s Cannot start container %s, %s", pc, id, err)
		if corruptedIp := util.IpConflictErrorMatch(err.Error()); corruptedIp != "" {
			// release ip
			if e := network.ReleaseIp(corruptedIp); e == nil {
				// if released successed restart container again
				if err = cluster.StartContainer(id); err == nil {
					return nil
				}
			} else {
				log.Warnf("%s Cannot release IP %s, %s", pc, id, err)
			}
		}
		pc.pod.State = RunStateError
		pc.pod.LastError = fmt.Sprintf("Cannot start container, %s", err)
		return err
	}
	return nil
}

// tryCorrectIPAddress try to correct container's ip address to given ip. return true if successed, otherwise return false.
func (pc *podController) tryCorrectIPAddress(c cluster.Cluster, id, fromIP, toIP string) bool {
	if err := c.DisconnectContainer(pc.spec.Namespace, id, true); err != nil {
		log.Errorf("%s fail to disconnect network %s to container %s, %s", pc, pc.spec.Namespace, id, err.Error())
		// do not return false, try to connect.
	}
	if err := c.ConnectContainer(pc.spec.Namespace, id, toIP); err != nil {
		log.Errorf("%s fail to connect network %s to container %s by using IP %s, %s", pc, pc.spec.Namespace, id, toIP, err.Error())
		log.Infof("%s try to recover network using old ip %s", pc, fromIP)
		if err := c.ConnectContainer(pc.spec.Namespace, id, fromIP); err != nil {
			log.Errorf("%s fail to recover network %s to container %s by using oldIP %s, %s, now container ip lost, give up!", pc, pc.spec.Namespace, id, fromIP, err.Error())
			log.Warnf("%s can not set any ip for container %s, give ip!!!")
		}
		return false
	}
	return true
}

func (pc *podController) refreshContainer(kluster cluster.Cluster, index int) {
	if index < 0 || index >= len(pc.pod.Containers) {
		return
	}
	id := pc.pod.Containers[index].Id
	if id == "" {
		pc.pod.State = RunStateMissing
		pc.pod.LastError = fmt.Sprintf("Missing container, without the container id.")
		return
	}

	spec := pc.spec.Containers[index]
	if info, err := kluster.InspectContainer(id); err != nil {
		if adoc.IsNotFound(err) {
			log.Warnf("%s We found some missing container %s, %s", pc, id, err)
			pc.pod.State = RunStateMissing
			pc.pod.LastError = fmt.Sprintf("Missing container %q, %s", id, err)
		} else {
			log.Warnf("%s Failed to inspect container %s, %s", pc, id, err)
			pc.pod.State = RunStateError
			pc.pod.LastError = fmt.Sprintf("Cannot inspect the container, %s", err)
		}
	} else {
		// inspect pod successed
		network := pc.spec.Network
		if network == "" {
			network = pc.spec.Namespace
		}
		prevIP, nowIP := pc.spec.PrevState.IPs[index], info.NetworkSettings.Networks[network].IPAddress
		// NOTE: if the container's ip is not equal to prev ip, try to correct it; if failed, accpet new ip
		if prevIP != "" && prevIP != nowIP {
			log.Warnf("%s find the IP changed, prev is %s, but now is %s, try to correct it", pc, prevIP, nowIP)
			if !pc.tryCorrectIPAddress(kluster, id, nowIP, prevIP) {
				log.Warnf("%s fail to correct container ip to %s, accpet new ip %s.", pc, prevIP, nowIP)
			} else {
				nowIP = prevIP
			}
		}

		container := Container{
			Id:            id,
			Runtime:       info,
			NodeName:      info.Node.Name,
			NodeIp:        info.Node.IP,
			Protocol:      "tcp",
			ContainerIp:   nowIP,
			ContainerPort: spec.Expose,
		}
		// FIXME: until we start working on the multiple ports
		if ports, ok := info.NetworkSettings.Ports[fmt.Sprintf("%d/tcp", spec.Expose)]; ok && len(ports) > 0 {
			if port, err := strconv.Atoi(ports[0].HostPort); err == nil {
				container.NodePort = port
			}
		}

		pc.spec.PrevState.NodeName = info.Node.Name
		pc.spec.PrevState.IPs[index] = container.ContainerIp
		pc.pod.Containers[index] = container
		state := info.State
		pc.pod.OOMkilled = state.OOMKilled
		if !state.Running {
			if state.Error != "" {
				pc.pod.State = RunStateFail
				pc.pod.LastError = state.Error
			} else {
				pc.pod.State = RunStateExit
			}
		}
		health := state.Health
		if health != nil {
			// created container extends its last running health status when restart the container
			options := pc.spec.HealthConfig.FetchOption()
			checkedPoint := options.Interval*options.Retries + options.Timeout + 1
			if health.Status == HealthState(HealthStateStarting).String() {
				pc.pod.Healthst = HealthState(HealthStateStarting)
			} else if health.Status == HealthState(HealthStateHealthy).String() &&
				time.Now().After(state.StartedAt.Add(time.Second*time.Duration(checkedPoint))) {
				pc.pod.Healthst = HealthState(HealthStateHealthy)
			} else {
				// Make sure checked enough times
				pc.pod.Healthst = HealthState(HealthStateUnHealthy)
			}
		} else {
			pc.pod.Healthst = HealthState(HealthStateNone)
		}
	}
}

func (pc *podController) createContainer(cluster cluster.Cluster, filters []string, index int) (string, error) {
	cc := pc.createContainerConfig(filters, index)
	hc := pc.createHostConfig(index)
	nc := pc.createNetworkingConfig(index)
	name := pc.createContainerName(index)
	return cluster.CreateContainer(cc, hc, nc, name)
}

func (pc *podController) createContainerConfig(filters []string, index int) adoc.ContainerConfig {
	podSpec := pc.spec
	spec := podSpec.Containers[index]

	volumes := make(map[string]struct{})
	for _, v := range spec.Volumes {
		volumes[v] = struct{}{}
	}
	for _, sv := range spec.SystemVolumes {
		parts := strings.Split(sv, ":")
		if len(parts) > 1 {
			volumes[parts[1]] = struct{}{}
		}
	}

	injectEnvs := append(spec.Env, []string{
		fmt.Sprintf("DEPLOYD_POD_INSTANCE_NO=%d", pc.pod.InstanceNo),
		fmt.Sprintf("DEPLOYD_POD_NAME=%s", pc.spec.Name),
		fmt.Sprintf("DEPLOYD_POD_NAMESPACE=%s", pc.spec.Namespace),
	}...)
	injectEnvs = append(injectEnvs, filters...)

	containerLabel := ContainerLabel{
		Name:           podSpec.Name,
		Namespace:      podSpec.Namespace,
		InstanceNo:     pc.pod.InstanceNo,
		Version:        podSpec.Version,
		DriftCount:     pc.pod.DriftCount,
		ContainerIndex: index,
		Annotation:     podSpec.Annotation,
	}

	labelsMap := containerLabel.Label2Maps()
	for key, value := range podSpec.Labels {
		labelsMap[key] = value
	}

	cc := adoc.ContainerConfig{
		Image:      spec.Image,
		Cmd:        spec.Command,
		Env:        injectEnvs,
		User:       spec.User,
		WorkingDir: spec.WorkingDir,
		Volumes:    volumes,
		Entrypoint: spec.Entrypoint,
		Labels:     labelsMap,
	}
	if podSpec.HealthConfig.Cmd == "none" {
		cc.Healthcheck = &adoc.HealthConfig{
			Test: []string{"NONE"},
		}
	} else {
		options := podSpec.HealthConfig.FetchOption()
		cmd := podSpec.HealthConfig.Cmd
		if cmd == "" {
			var annotions map[string]interface{}
			if err := json.Unmarshal([]byte(podSpec.Annotation), &annotions); err == nil {
				if healthcheck, ok := annotions["healthcheck"]; ok {
					port := spec.Expose
					healthcheckUrl, ok := healthcheck.(string)
					if ok {
						url := "http://localhost:" + strconv.Itoa(port) + healthcheckUrl
						cmd = fmt.Sprintf(CURL_TMPLT, strconv.Itoa(options.Timeout), url)
					}
				} else {
					log.Info("annotation without healthcheck")
				}
			} else {
				log.Errorf("unmarsha podSpec.Annotation %v err:%v", podSpec.Annotation, err)
			}
		}
		if cmd != "" {
			cc.Healthcheck = &adoc.HealthConfig{
				Test:     []string{"CMD-SHELL", cmd + " || exit 1"},
				Interval: time.Duration(options.Interval) * time.Second,
				Timeout:  time.Duration(options.Timeout) * time.Second,
				Retries:  options.Retries,
			}
		}
	}

	if spec.Expose > 0 {
		cc.ExposedPorts = map[string]struct{}{
			fmt.Sprintf("%d/tcp", spec.Expose): struct{}{},
		}
	}
	return cc
}

func (pc *podController) createHostConfig(index int) adoc.HostConfig {
	podSpec := pc.spec
	spec := podSpec.Containers[index]
	if spec.CpuLimit > CPUMaxLevel {
		spec.CpuLimit = CPUMaxLevel
	} else if spec.CpuLimit < 1 {
		spec.CpuLimit = CPUDeafultLevel
	}
	resource := FetchResource()
	BlkioDeviceReadBps := make([]*adoc.ThrottleDevice, 0)
	BlkioDeviceWriteBps := make([]*adoc.ThrottleDevice, 0)
	BlkioDeviceReadIOps := make([]*adoc.ThrottleDevice, 0)
	BlkioDeviceWriteIOps := make([]*adoc.ThrottleDevice, 0)
	for _, device := range resource.Devices {
		ratio := DefautDeviceRatio
		if device.Ratio > 0 && device.Ratio < 1 {
			ratio = device.Ratio
		}
		if rate, err := units.FromHumanSize(device.MaxRate); err == nil {
			ratebps := &adoc.ThrottleDevice{Path: device.Path, Rate: ratio * uint64(rate) / 100}
			iops := &adoc.ThrottleDevice{Path: device.Path, Rate: ratio * device.MaxIops / 100}
			BlkioDeviceReadBps = append(BlkioDeviceReadBps, ratebps)
			BlkioDeviceWriteBps = append(BlkioDeviceWriteBps, ratebps)
			BlkioDeviceReadIOps = append(BlkioDeviceReadIOps, iops)
			BlkioDeviceWriteIOps = append(BlkioDeviceWriteIOps, iops)
		}
	}
	swappiness := int64(0)
	hc := adoc.HostConfig{
		Resources: adoc.Resources{
			Memory:               spec.MemoryLimit,
			MemorySwap:           spec.MemoryLimit, // Memory == MemorySwap means disable swap
			MemorySwappiness:     &swappiness,
			CPUPeriod:            CPUQuota,
			CPUQuota:             int64(spec.CpuLimit*resource.Cpu*CPUMaxPctg) * CPUQuota / int64(CPUMaxLevel*100),
			BlkioDeviceReadBps:   BlkioDeviceReadBps,
			BlkioDeviceWriteBps:  BlkioDeviceWriteBps,
			BlkioDeviceReadIOps:  BlkioDeviceReadIOps,
			BlkioDeviceWriteIOps: BlkioDeviceWriteIOps,
		},
	}
	if spec.Expose > 0 {
		hc.PortBindings = map[string][]adoc.PortBinding{
			fmt.Sprintf("%d/tcp", spec.Expose): []adoc.PortBinding{
				adoc.PortBinding{},
			},
		}
	}
	if len(spec.Volumes) > 0 {
		binds := make([]string, len(spec.Volumes))
		for i, v := range spec.Volumes {
			// /data/lain/volumes/hello/hello.proc.web.foo/1/{c0}/{v:v}
			if len(podSpec.Containers) > 1 {
				binds[i] = fmt.Sprintf("%s/%s/%s/%d/c%d/%s:%s", kLainVolumeRoot, podSpec.Namespace, podSpec.Name, pc.pod.InstanceNo, index, v, v)
			} else {
				binds[i] = fmt.Sprintf("%s/%s/%s/%d/%s:%s", kLainVolumeRoot, podSpec.Namespace, podSpec.Name, pc.pod.InstanceNo, v, v)
			}
		}
		hc.Binds = binds
	}
	hc.Binds = append(hc.Binds, spec.SystemVolumes...)

	if len(spec.CloudVolumes) > 0 {
		var binds []string
		for _, cv := range spec.CloudVolumes {
			if cv.Type == CloudVolumeMultiMode {
				for _, dir := range cv.Dirs {
					binds = append(binds, fmt.Sprintf("%s/%s/%s/%d/%s:%s", kLainCloudVolumeRoot, podSpec.Namespace, podSpec.Name, pc.pod.InstanceNo, dir, dir))
				}
			} else {
				for _, dir := range cv.Dirs {
					binds = append(binds, fmt.Sprintf("%s/%s/%s/%s:%s", kLainCloudVolumeRoot, podSpec.Namespace, podSpec.Name, dir, dir))
				}
			}
		}
		hc.Binds = append(hc.Binds, binds...)
	}

	hc.NetworkMode = podSpec.Network
	if hc.NetworkMode == "" {
		hc.NetworkMode = podSpec.Namespace
	}
	if len(spec.DnsSearch) > 0 {
		hc.DnsSearch = generics.Clone_StringSlice(spec.DnsSearch)
	}
	if spec.LogConfig.Type != "" {
		hc.LogConfig.Type = spec.LogConfig.Type
		hc.LogConfig.Config = generics.Clone_StringStringMap(spec.LogConfig.Config)
	}
	return hc
}

func (pc *podController) createContainerName(index int) string {
	segs := make([]string, 0, 2)
	segs = append(segs, fmt.Sprintf("%s.v%d-i%d-d%d", pc.spec.Name, pc.spec.Version, pc.pod.InstanceNo, pc.pod.DriftCount))
	if len(pc.spec.Containers) > 1 {
		segs = append(segs, fmt.Sprintf("c%d", index))
	}
	return strings.Join(segs, "-")
}

func (pc *podController) createNetworkingConfig(index int) adoc.NetworkingConfig {
	podSpec := pc.spec
	net := podSpec.Network
	if net == "" {
		net = podSpec.Namespace
	}
	nc := adoc.NetworkingConfig{}
	// ipamc := adoc.IPAMConfig{}
	// ipamc.IPv4Address = pc.spec.PrevState.IPs[index]
	// nc.EndpointsConfig = map[string]adoc.EndpointConfig{
	// 	net: adoc.EndpointConfig{
	// 		ipamc,
	// 	},
	// }
	return nc
}
