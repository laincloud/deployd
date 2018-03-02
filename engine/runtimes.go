package engine

import (
	"time"

	"github.com/mijia/adoc"
)

type RunState int
type HealthState int
type ExpectState int
type PGOpState int32

var RestartMaxCount int

const (
	RunStatePending      = iota // waiting for operation
	RunStateDrift               // drifting from one node to another
	RunStateSuccess             // ok
	RunStateExit                // exited
	RunStateFail                // start failed with error
	RunStateInconsistent        // container's state is different between deployd and swarm
	RunStateMissing             // container is missing and need create it. happened when node down .etc
	RunStateRemoved             // removed
	RunStatePaused              // paused
	RunStateError               // call docker interface with error
)

const (
	HealthStateNone = iota
	HealthStateStarting
	HealthStateHealthy
	HealthStateUnHealthy
)

const (
	ExpectStateRun = iota
	ExpectStateStop
)

const (
	PGOpStateIdle = iota
	PGOpStateUpgrading
	PGOpStateScheduling
	PGOpStateDrifting
	PGOpStateRemoving
	PGOpStateStarting
	PGOpStateStoping
	PGOpStateRestarting
)

func (rs RunState) String() string {
	switch rs {
	case RunStatePending:
		return "RunStatePending"
	case RunStateDrift:
		return "RunStateDrift"
	case RunStateSuccess:
		return "RunStateSuccess"
	case RunStateExit:
		return "RunStateExit"
	case RunStateFail:
		return "RunStateFail"
	case RunStateMissing:
		return "RunStateMissing"
	case RunStateInconsistent:
		return "RunStateInconsistent"
	case RunStateRemoved:
		return "RunStateRemoved"
	case RunStatePaused:
		return "RunStatePaused"
	case RunStateError:
		return "RunStateError"
	default:
		return "Unknown RunState"
	}
}

func (hs HealthState) String() string {
	switch hs {
	case HealthStateNone:
		return "none"
	case HealthStateStarting:
		return "starting"
	case HealthStateHealthy:
		return "healthy"
	case HealthStateUnHealthy:
		return "unhealthy"
	default:
		return "none"
	}
}

func (es ExpectState) String() string {
	switch es {
	case ExpectStateRun:
		return "Run"
	case ExpectStateStop:
		return "Stop"
	default:
		return "error"
	}
}

func (pgos PGOpState) String() string {
	switch pgos {
	case PGOpStateIdle:
		return "Idle"
	case PGOpStateUpgrading:
		return "Upgrading"
	case PGOpStateScheduling:
		return "Scheduling"
	case PGOpStateDrifting:
		return "Drifting"
	case PGOpStateRemoving:
		return "Removing"
	case PGOpStateStarting:
		return "Starting"
	case PGOpStateStoping:
		return "Stoping"
	case PGOpStateRestarting:
		return "Restarting"
	default:
		return "error"
	}
}

type ImRuntime struct {
	BaseRuntime
	TargetState  ExpectState
	DriftCount   int
	RestartCount int
	RestartAt    time.Time
}

type BaseRuntime struct {
	Healthst  HealthState
	State     RunState
	OOMkilled bool
	LastError string
	UpdatedAt time.Time
}

type Container struct {
	// FIXME(mijia): multiple ports supporing, will have multiple entries of <NodePort, ContainerPort, Protocol>
	Id            string
	Runtime       adoc.ContainerDetail
	NodeName      string
	NodeIp        string
	ContainerIp   string
	NodePort      int
	ContainerPort int
	Protocol      string
}

func (c Container) Clone() Container {
	// So far we maybe only care about the basic information like in the Equals
	return c
}

func (c Container) Equals(o Container) bool {
	// The ContainerDetail from adoc change would reflect to the Pod runtime changes
	return c.Id == o.Id &&
		c.NodeName == o.NodeName &&
		c.NodeIp == o.NodeIp &&
		c.ContainerIp == o.ContainerIp &&
		c.NodePort == o.NodePort &&
		c.ContainerPort == o.ContainerPort &&
		c.Protocol == o.Protocol
}

type Pod struct {
	InstanceNo int
	Containers []Container
	ImRuntime
}

func (p Pod) Clone() Pod {
	n := p
	n.Containers = make([]Container, len(p.Containers))
	for i := range p.Containers {
		n.Containers[i] = p.Containers[i].Clone()
	}
	return n
}

func (p Pod) Equals(o Pod) bool {
	if len(p.Containers) != len(o.Containers) {
		return false
	}
	for i := range p.Containers {
		if !p.Containers[i].Equals(o.Containers[i]) {
			return false
		}
	}
	return p.InstanceNo == o.InstanceNo &&
		p.State == o.State &&
		p.LastError == o.LastError &&
		p.DriftCount == o.DriftCount
}

func (pod Pod) ContainerIds() []string {
	ids := make([]string, len(pod.Containers))
	for i, container := range pod.Containers {
		ids[i] = container.Id
	}
	return ids
}

func (pod Pod) NeedRestart(policy RestartPolicy) bool {
	if pod.TargetState == ExpectStateStop {
		return false
	}
	state := pod.State
	if policy == RestartPolicyAlways {
		return state == RunStateExit || state == RunStateFail
	}
	if policy == RestartPolicyOnFail {
		return state == RunStateFail
	}
	return false
}

func (pod Pod) RestartEnoughTimes() bool {
	if len(pod.Containers) > 0 && pod.RestartAt.Add(2*RestartInfoClearInterval).Before(pod.Containers[0].Runtime.State.FinishedAt) {
		return false
	}
	return pod.RestartCount >= RestartMaxCount
}

func (pod Pod) NodeName() string {
	if len(pod.Containers) > 0 {
		return pod.Containers[0].NodeName
	}
	return ""
}

func (pod Pod) NodeIp() string {
	if len(pod.Containers) > 0 {
		return pod.Containers[0].NodeIp
	}
	return ""
}

func (pod Pod) PodIp() string {
	if len(pod.Containers) > 0 {
		return pod.Containers[0].ContainerIp
	}
	return ""
}

func (pod *Pod) ChangeTargetState(state ExpectState) {
	pod.TargetState = state
}

type PodGroup struct {
	Pods []Pod
	BaseRuntime
}

func (pg PodGroup) Clone() PodGroup {
	n := pg
	n.Pods = make([]Pod, len(pg.Pods))
	for i := range pg.Pods {
		n.Pods[i] = pg.Pods[i].Clone()
	}
	return n
}

func (pg PodGroup) Equals(o PodGroup) bool {
	if len(pg.Pods) != len(o.Pods) {
		return false
	}
	for i := range pg.Pods {
		if !pg.Pods[i].Equals(o.Pods[i]) {
			return false
		}
	}
	return pg.State == o.State &&
		pg.LastError == o.LastError
}

func (group PodGroup) collectNodes() map[string]string {
	nodes := make(map[string]string)
	for _, pod := range group.Pods {
		name := pod.NodeName()
		ip := pod.NodeIp()
		if name != "" && ip != "" {
			nodes[name] = ip
		}
	}
	return nodes
}

type DependencyEvent struct {
	Type      string // add, remove, verify
	Name      string
	NodeName  string
	Namespace string
}

type OperationEvent struct {
	Type   string // start, over
	PgName string
}
