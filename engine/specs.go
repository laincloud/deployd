package engine

import (
	"fmt"
	"strconv"
	"time"

	"github.com/laincloud/deployd/storage"
	"github.com/mijia/adoc"
	"github.com/mijia/go-generics"
	"github.com/mijia/sweb/log"
)

const (
	kLainDeploydRootKey = "/lain/deployd"
	kLainConstraintKey  = "constraints"
	kLainNotifyKey      = "notifies"
	kLainPodGroupKey    = "pod_groups"
	kLainDependencyKey  = "depends"
	kLainSpecKey        = "specs"
	kLainPodKey         = "pods"
	kLainNodesKey       = "nodes"
	kLainLastPodSpecKey = "last_spec"
	kLainPgOpingKey     = "operating"
	kLainCanaryKey      = "canaries"

	kLainLabelPrefix   = "cc.bdp.lain.deployd"
	kLainLogVolumePath = "/lain/logs"

	MinPodSetupTime = 0
	MaxPodSetupTime = 300

	MinPodKillTimeout = 10
	MaxPodKillTimeout = 120

	PGCanaryType = "canary"
)

var (
	kLainVolumeRoot      = "/data/lain/volumes"
	kLainCloudVolumeRoot = "/data/lain/cloud-volumes"
)

type ImSpec struct {
	Name      string
	Namespace string
	Version   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

type ContainerLabel struct {
	Name           string
	Namespace      string
	InstanceNo     int
	Version        int
	DriftCount     int
	ContainerIndex int
	Annotation     string
}

func configSpecsVars(store storage.Store) error {
	if v, err := store.GetRaw(EtcdCloudVolumeRootKey); err == nil {
		kLainCloudVolumeRoot = v
	}

	if v, err := store.GetRaw(EtcdVolumeRootKey); err == nil {
		kLainVolumeRoot = v
	}
	log.Debugf("cloud_volume_root: %s, volumes_root: %s", kLainCloudVolumeRoot, kLainVolumeRoot)
	return nil
}

func (label ContainerLabel) NameAffinity() string {
	return fmt.Sprintf("affinity:%s.pg_name!=~%s", kLainLabelPrefix, label.Name)
}

func (label ContainerLabel) Label2Maps() map[string]string {
	labelMaps := make(map[string]string)
	labelMaps[kLainLabelPrefix+".pg_name"] = label.Name
	labelMaps[kLainLabelPrefix+".pg_namespace"] = label.Namespace
	labelMaps[kLainLabelPrefix+".instance_no"] = fmt.Sprintf("%d", label.InstanceNo)
	labelMaps[kLainLabelPrefix+".version"] = fmt.Sprintf("%d", label.Version)
	labelMaps[kLainLabelPrefix+".drift_count"] = fmt.Sprintf("%d", label.DriftCount)
	labelMaps[kLainLabelPrefix+".container_index"] = fmt.Sprintf("%d", label.ContainerIndex)
	labelMaps[kLainLabelPrefix+".annotation"] = label.Annotation
	return labelMaps
}

func (label *ContainerLabel) FromMaps(m map[string]string) bool {
	var err error
	hasError := false
	label.Name = m[kLainLabelPrefix+".pg_name"]
	hasError = hasError || label.Name == ""
	label.Namespace = m[kLainLabelPrefix+".pg_namespace"]
	label.InstanceNo, err = strconv.Atoi(m[kLainLabelPrefix+".instance_no"])
	hasError = hasError || err != nil
	label.Version, err = strconv.Atoi(m[kLainLabelPrefix+".version"])
	hasError = hasError || err != nil
	label.DriftCount, err = strconv.Atoi(m[kLainLabelPrefix+".drift_count"])
	hasError = hasError || err != nil
	label.ContainerIndex, err = strconv.Atoi(m[kLainLabelPrefix+".container_index"])
	hasError = hasError || err != nil
	label.Annotation = m[kLainLabelPrefix+".annotation"]
	return !hasError
}

const (
	CloudVolumeSingleMode = "single"
	CloudVolumeMultiMode  = "multi"
)

type CloudVolumeSpec struct {
	Type string
	Dirs []string
}

func (s CloudVolumeSpec) VerifyParams() bool {
	verify := s.Type == CloudVolumeMultiMode ||
		s.Type == CloudVolumeSingleMode

	return verify
}

func (s CloudVolumeSpec) Clone() CloudVolumeSpec {
	newSpec := s
	newSpec.Type = s.Type
	newSpec.Dirs = generics.Clone_StringSlice(s.Dirs)
	return newSpec
}

func (s CloudVolumeSpec) Equals(o CloudVolumeSpec) bool {
	return s.Type == o.Type &&
		generics.Equal_StringSlice(s.Dirs, o.Dirs)
}

type HealthCnfOptions struct {
	Interval int `json:"interval"`
	Timeout  int `json:"timeout"`
	Retries  int `json:"retries"`
}

func (hco HealthCnfOptions) Equals(cp HealthCnfOptions) bool {
	return hco.Interval == cp.Interval &&
		hco.Timeout == cp.Timeout &&
		hco.Retries == cp.Retries
}

type HealthConfig struct {
	Cmd     string           `json:"cmd"`
	Options HealthCnfOptions `json:"options"`
}

func (hc HealthConfig) Equals(cp HealthConfig) bool {
	return hc.Cmd == cp.Cmd &&
		hc.Options.Equals(cp.Options)
}

func (hc HealthConfig) FetchOption() HealthCnfOptions {
	interval := DefaultHealthInterval
	timeout := DefaultHealthTimeout
	retries := DefaultHealthRetries
	options := hc.Options
	if options.Interval > interval {
		interval = options.Interval
	}
	if options.Timeout > timeout {
		timeout = options.Timeout
	}
	if options.Retries > retries {
		retries = options.Retries
	}
	return HealthCnfOptions{
		Interval: interval,
		Timeout:  timeout,
		Retries:  retries,
	}
}

type ContainerSpec struct {
	ImSpec
	Image         string
	Env           []string
	User          string
	WorkingDir    string
	DnsSearch     []string
	Volumes       []string // a stateful flag
	SystemVolumes []string // not a stateful flag, every node has system volumes
	CloudVolumes  []CloudVolumeSpec
	Command       []string
	Entrypoint    []string
	CpuLimit      int
	MemoryLimit   int64
	Expose        int
	LogConfig     adoc.LogConfig
}

func (s ContainerSpec) Clone() ContainerSpec {
	newSpec := s
	newSpec.Env = generics.Clone_StringSlice(s.Env)
	newSpec.Volumes = generics.Clone_StringSlice(s.Volumes)
	newSpec.SystemVolumes = generics.Clone_StringSlice(s.SystemVolumes)
	newSpec.Command = generics.Clone_StringSlice(s.Command)
	newSpec.DnsSearch = generics.Clone_StringSlice(s.DnsSearch)
	if s.Entrypoint == nil {
		newSpec.Entrypoint = nil
	} else {
		newSpec.Entrypoint = generics.Clone_StringSlice(s.Entrypoint)
	}
	newSpec.LogConfig.Type = s.LogConfig.Type
	newSpec.LogConfig.Config = generics.Clone_StringStringMap(s.LogConfig.Config)

	for i := range s.CloudVolumes {
		newSpec.CloudVolumes[i] = s.CloudVolumes[i].Clone()
	}
	return newSpec
}

func (s ContainerSpec) VerifyParams() bool {
	verify := s.Image != "" &&
		s.CpuLimit >= 0 &&
		s.MemoryLimit >= 0 &&
		s.Expose >= 0
	if !verify {
		return false
	}
	for _, cvSpec := range s.CloudVolumes {
		if !cvSpec.VerifyParams() {
			return false
		}
	}
	return true
}

func (s ContainerSpec) Equals(o ContainerSpec) bool {
	if (s.Entrypoint == nil && o.Entrypoint != nil) || (s.Entrypoint != nil && o.Entrypoint == nil) {
		return false
	}

	return s.Name == o.Name &&
		s.Image == o.Image &&
		generics.Equal_StringSlice(s.Env, o.Env) &&
		generics.Equal_StringSlice(s.Command, o.Command) &&
		generics.Equal_StringSlice(s.DnsSearch, o.DnsSearch) &&
		s.CpuLimit == o.CpuLimit &&
		s.MemoryLimit == o.MemoryLimit &&
		s.Expose == o.Expose &&
		s.User == o.User &&
		s.WorkingDir == o.WorkingDir &&
		generics.Equal_StringSlice(s.Volumes, o.Volumes) &&
		generics.Equal_StringSlice(s.SystemVolumes, o.SystemVolumes) &&
		generics.Equal_StringSlice(s.Entrypoint, o.Entrypoint) &&
		s.LogConfig.Type == o.LogConfig.Type &&
		generics.Equal_StringStringMap(s.LogConfig.Config, o.LogConfig.Config)
}

func NewContainerSpec(image string) ContainerSpec {
	spec := ContainerSpec{
		Image: image,
	}
	spec.Version = 1
	spec.CreatedAt = time.Now()
	spec.UpdatedAt = spec.CreatedAt
	return spec
}

type DependencyPolicy int

const (
	DependencyNamespaceLevel = iota
	DependencyNodeLevel
)

type Dependency struct {
	PodName string
	Policy  DependencyPolicy
}

func (d Dependency) Clone() Dependency {
	return d
}

type PodPrevState struct {
	NodeName string
	IPs      []string
}

func NewPodPrevState(length int) PodPrevState {
	return PodPrevState{
		NodeName: "",
		IPs:      make([]string, length),
	}
}

func (pps PodPrevState) Clone() PodPrevState {
	newState := pps
	newState.IPs = make([]string, len(pps.IPs))
	copy(newState.IPs, pps.IPs)
	return newState
}

type PodSpec struct {
	ImSpec
	Network      string
	Containers   []ContainerSpec
	Filters      []string // for cluster scheduling
	Labels       map[string]string
	Dependencies []Dependency
	Annotation   string
	Stateful     bool
	SetupTime    int
	KillTimeout  int
	PrevState    PodPrevState
	HealthConfig HealthConfig
}

func (s PodSpec) GetSetupTime() int {
	if s.SetupTime < MinPodSetupTime {
		return MinPodSetupTime
	} else if s.SetupTime > MaxPodSetupTime {
		return MaxPodSetupTime
	}
	return s.SetupTime
}

func (s PodSpec) GetKillTimeout() int {
	if s.KillTimeout < MinPodKillTimeout {
		return MinPodKillTimeout
	} else if s.KillTimeout > MaxPodKillTimeout {
		return MaxPodKillTimeout
	}
	return s.KillTimeout
}

func (s PodSpec) String() string {
	return fmt.Sprintf("Pod[name=%s, version=%d, depends=%+v, stateful=%v, #containers=%d]",
		s.Name, s.Version, s.Dependencies, s.Stateful, len(s.Containers))
}

func (s PodSpec) Clone() PodSpec {
	newSpec := s
	newSpec.Filters = generics.Clone_StringSlice(s.Filters)
	newSpec.Labels = generics.Clone_StringStringMap(s.Labels)
	newSpec.Containers = make([]ContainerSpec, len(s.Containers))
	newSpec.PrevState = s.PrevState.Clone()
	for i := range s.Containers {
		newSpec.Containers[i] = s.Containers[i].Clone()
	}
	newSpec.Dependencies = make([]Dependency, len(s.Dependencies))
	for i := range s.Dependencies {
		newSpec.Dependencies[i] = s.Dependencies[i].Clone()
	}
	newSpec.HealthConfig = s.HealthConfig
	return newSpec
}

func (s PodSpec) VerifyParams() bool {
	verify := s.Name != "" && s.Namespace != "" &&
		len(s.Containers) > 0
	if !verify {
		return false
	}
	for _, cSpec := range s.Containers {
		if !cSpec.VerifyParams() {
			return false
		}
	}
	return true
}

func (s PodSpec) IsHardStateful() bool {
	return s.Stateful
}

func (s PodSpec) IsStateful() bool {
	return s.HasVolumes() || s.Stateful
}

func (s PodSpec) HasVolumes() bool {
	for _, container := range s.Containers {
		if len(container.Volumes) == 1 && container.Volumes[0] == kLainLogVolumePath {
			continue
		}
		if len(container.Volumes) > 0 {
			return true
		}
	}
	return false
}

func (s PodSpec) Equals(o PodSpec) bool {
	if len(s.Containers) != len(o.Containers) {
		return false
	}
	for i := range s.Containers {
		if !s.Containers[i].Equals(o.Containers[i]) {
			return false
		}
	}
	if len(s.Dependencies) != len(o.Dependencies) {
		return false
	}
	for i := range s.Dependencies {
		if s.Dependencies[i] != o.Dependencies[i] {
			return false
		}
	}
	return s.Name == o.Name &&
		s.Namespace == o.Namespace &&
		s.Version == o.Version &&
		s.Annotation == o.Annotation &&
		s.Stateful == o.Stateful &&
		generics.Equal_StringSlice(s.Filters, o.Filters) &&
		generics.Equal_StringStringMap(s.Labels, o.Labels) &&
		s.KillTimeout == o.KillTimeout &&
		s.SetupTime == o.SetupTime &&
		s.HealthConfig.Equals(o.HealthConfig)
}

func (s PodSpec) Merge(o PodSpec) PodSpec {
	// deal with params keeping original
	if len(s.Containers) > 0 {
		sc := s.Containers[0]
		for i, _ := range o.Containers {
			if i >= len(s.Containers) {
				sc = s.Containers[0]
			} else {
				sc = s.Containers[i]
			}
			if o.Containers[i].CpuLimit == 0 {
				o.Containers[i].CpuLimit = sc.CpuLimit
			}
			if o.Containers[i].MemoryLimit == 0 {
				o.Containers[i].MemoryLimit = sc.MemoryLimit
			}
		}
	}
	s.Containers = o.Containers
	s.Dependencies = o.Dependencies
	s.Filters = o.Filters
	s.Labels = o.Labels
	s.Annotation = o.Annotation
	s.Stateful = o.Stateful
	s.Version += 1
	s.UpdatedAt = time.Now()
	s.SetupTime = o.SetupTime
	s.KillTimeout = o.KillTimeout
	s.HealthConfig = o.HealthConfig
	return s
}

func NewPodSpec(containerSpec ContainerSpec, otherSpecs ...ContainerSpec) PodSpec {
	cSpecs := make([]ContainerSpec, 1+len(otherSpecs))
	cSpecs[0] = containerSpec
	for i, cs := range otherSpecs {
		cSpecs[i+1] = cs
	}
	spec := PodSpec{
		Containers: cSpecs,
		PrevState:  NewPodPrevState(len(otherSpecs) + 1),
	}
	spec.Version = 1
	spec.CreatedAt = time.Now()
	spec.UpdatedAt = spec.CreatedAt
	return spec
}

type RestartPolicy int

const (
	RestartPolicyNever = iota
	RestartPolicyAlways
	RestartPolicyOnFail
)

func (rp RestartPolicy) String() string {
	switch rp {
	case RestartPolicyNever:
		return "RestartPolicyNever"
	case RestartPolicyAlways:
		return "RestartPolicyAlways"
	case RestartPolicyOnFail:
		return "RestartPolicyOnFail"
	default:
		return "Unknown RestartPolicy"
	}
}

type PodGroupPrevState struct {
	Nodes []string
	// we think a instance only have one ip, as now a instance only have one container.
	IPs []string
}

func (pgps PodGroupPrevState) Clone() PodGroupPrevState {
	newState := PodGroupPrevState{
		Nodes: make([]string, len(pgps.Nodes)),
		IPs:   make([]string, len(pgps.Nodes)),
	}
	copy(newState.Nodes, pgps.Nodes)
	copy(newState.IPs, pgps.IPs)
	return newState
}

func (pgps PodGroupPrevState) Reset(instanceNo int) PodGroupPrevState {
	newState := PodGroupPrevState{
		Nodes: make([]string, instanceNo),
		IPs:   make([]string, instanceNo),
	}
	copy(newState.Nodes, pgps.Nodes)
	copy(newState.IPs, pgps.IPs)
	return newState
}

func (pgps PodGroupPrevState) Length() int {
	if pgps.Nodes == nil {
		return 0
	}
	return len(pgps.Nodes)
}

type PodGroupSpec struct {
	ImSpec
	Pod           PodSpec
	NumInstances  int
	RestartPolicy RestartPolicy
}

func (spec PodGroupSpec) String() string {
	return fmt.Sprintf("PodGroup[name=%s, version=%d, #instances=%d, restart=%s]",
		spec.Name, spec.Version, spec.NumInstances, spec.RestartPolicy)
}

func (spec PodGroupSpec) Clone() PodGroupSpec {
	newSpec := spec
	newSpec.Pod = spec.Pod.Clone()
	return newSpec
}

func (spec PodGroupSpec) Equals(o PodGroupSpec) bool {
	return spec.Name == o.Name &&
		spec.Namespace == o.Namespace &&
		spec.Version == o.Version &&
		spec.Pod.Equals(o.Pod) &&
		spec.NumInstances == o.NumInstances &&
		spec.RestartPolicy == o.RestartPolicy
}

func (spec PodGroupSpec) VerifyParams() bool {
	verify := spec.Name != "" &&
		spec.Namespace != "" &&
		spec.NumInstances >= 0
	if !verify {
		return false
	}
	return spec.Pod.VerifyParams()
}

func NewPodGroupSpec(name string, namespace string, podSpec PodSpec, numInstances int) PodGroupSpec {
	spec := PodGroupSpec{
		Pod:          podSpec,
		NumInstances: numInstances,
	}
	spec.Name = name
	spec.Namespace = namespace
	spec.Version = 1
	spec.CreatedAt = time.Now()
	spec.UpdatedAt = spec.CreatedAt
	spec.Pod.ImSpec = spec.ImSpec
	return spec
}
