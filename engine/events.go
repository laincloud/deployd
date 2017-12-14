package engine

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/laincloud/deployd/storage"
	"github.com/laincloud/deployd/utils/util"
	"github.com/mijia/adoc"
	"github.com/mijia/sweb/log"
)

type Listener interface {
	ListenerId() string
	HandleEvent(payload interface{})
}

type Publisher interface {
	EmitEvent(payload interface{})
	AddListener(subscriber Listener)
	RemoveListener(subscriber Listener)
}

type _BasePublisher struct {
	sync.RWMutex
	goRoutine bool
	listeners map[string]Listener
}

func NewPublisher(goRoutine bool) Publisher {
	return &_BasePublisher{
		goRoutine: goRoutine,
		listeners: make(map[string]Listener),
	}
}

func (pub *_BasePublisher) EmitEvent(payload interface{}) {
	pub.RLock()
	listeners := make([]Listener, 0, len(pub.listeners))
	for _, listener := range pub.listeners {
		listeners = append(listeners, listener)
	}
	pub.RUnlock()

	emitFn := func() {
		for _, listener := range listeners {
			listener.HandleEvent(payload)
		}
	}
	if pub.goRoutine {
		go emitFn()
	} else {
		emitFn()
	}
}

func (pub *_BasePublisher) AddListener(listener Listener) {
	pub.Lock()
	defer pub.Unlock()
	pub.listeners[listener.ListenerId()] = listener
}

func (pub *_BasePublisher) RemoveListener(listener Listener) {
	pub.Lock()
	defer pub.Unlock()
	delete(pub.listeners, listener.ListenerId())
}

func handleContainerEvent(engine *OrcEngine, event *adoc.Event) {
	if strings.HasPrefix(event.Status, "health_status") {
		id := event.ID
		if cont, err := engine.cluster.InspectContainer(id); err == nil {
			status := HealthState(HealthStateNone)
			switch event.Status {
			case "health_status: starting":
				status = HealthStateStarting
				break
			case "health_status: healthy":
				status = HealthStateHealthy
				break
			case "health_status: unhealthy":
				status = HealthStateUnHealthy
				break
			}
			containerName := strings.TrimLeft(cont.Name, "/")
			if podName, instance, err := util.ParseNameInstanceNo(containerName); err == nil {
				pgCtrl, ok := engine.pgCtrls[podName]
				if ok {
					if len(pgCtrl.podCtrls) >= instance {
						pgCtrl.Lock()
						pgCtrl.podCtrls[instance-1].pod.Healthst = status
						pgCtrl.Unlock()
						pgCtrl.opsChan <- pgOperSnapshotGroup{true}
						pgCtrl.opsChan <- pgOperSaveStore{true}
					}
				}
			}
		} else {
			log.Errorf("ParseNameInstanceNo error:%v", err)
		}
	} else {
		switch event.Status {
		case adoc.DockerEventStop:
			savePodStaHstry(engine, event)
		case adoc.DockerEventStart:
			savePodStaHstry(engine, event)
		}
	}
}

func HandleDockerEvent(engine *OrcEngine, event *adoc.Event) {
	switch event.Type {
	case adoc.ContainerEventType:
		handleContainerEvent(engine, event)
		break
	}
}

// cnt means container

const (
	KCntStatus          = "/lain/deployd/histroy"
	FmtKCntStatusLstPos = "/lain/deployd/histroy/%s/%d/lastpos"
	FmtKCntStatusInfo   = "/lain/deployd/histroy/%s/%d/%d"

	DefaultStatusSize = 20
)

type StatusLastPos struct {
	Pos  int
	Size int
}

type StatusMessage struct {
	Status string `json:"status,omitempty"`
	From   string `json:"from,omitempty"`
	Time   int64  `json:"time,omitempty"`
	Action string `json:"action,omitempty"`
}

type podStatusHistory struct {
	pos      *StatusLastPos
	podname  string
	instance int
	dirty    bool
	lock     *sync.Mutex
	dirtys   []bool
	locks    []*sync.Mutex
	statuses []*StatusMessage
}

func (podSta *podStatusHistory) Save(engine *OrcEngine) {
	pos := podSta.pos.Pos
	podSta.dirtys[pos] = true
	podSta.dirty = true

	if err := podSta.saveStatus(engine, pos); err == nil {
		podSta.dirtys[pos] = false
		if err := podSta.saveLastPos(engine); err == nil {
			podSta.dirty = false
		}
	}
}

func (podSta *podStatusHistory) Check(engine *OrcEngine) {
	statusClean := true
	for pos, dirty := range podSta.dirtys {
		if dirty {
			if err := podSta.saveStatus(engine, pos); err == nil {
				podSta.dirtys[pos] = false
			} else {
				statusClean = false
			}
		}
	}
	if statusClean && podSta.dirty {
		if err := podSta.saveLastPos(engine); err == nil {
			podSta.dirty = false
		}
	}
}

func (podSta *podStatusHistory) saveStatus(engine *OrcEngine, pos int) error {
	lock := podSta.locks[pos]
	lock.Lock()
	defer lock.Unlock()
	if err := engine.store.Set(fmt.Sprintf(FmtKCntStatusInfo, podSta.podname, podSta.instance, pos),
		podSta.statuses[pos], true); err != nil {
		log.Error("save container %s status info failed by error %s ", podSta.podname, err)
		return err
	}
	return nil
}

func (podSta *podStatusHistory) saveLastPos(engine *OrcEngine) error {
	lock := podSta.lock
	lock.Lock()
	defer lock.Unlock()
	return engine.store.Set(fmt.Sprintf(FmtKCntStatusLstPos, podSta.podname, podSta.instance), podSta.pos)
}

type PodGroupStatusHistory struct {
	podStatuses map[int]*podStatusHistory // instance: podStatusHistory
}

type engineStatusHistory struct {
	pgStatuses map[string]*PodGroupStatusHistory // podname: PodGroupStatusHistory
}

func (esh *engineStatusHistory) checkStatus(engine *OrcEngine) {
	for _, pgsh := range esh.pgStatuses {
		for _, podsh := range pgsh.podStatuses {
			podsh.Check(engine)
		}
	}
}

// egStatuses:map[]
//	pgStatuses: map[]
//    podStatusHistory:map[]
//		DETAILDATA
var (
	egStatuses *engineStatusHistory
	egLock     *sync.Mutex
)

func init() {
	egStatuses = &engineStatusHistory{
		pgStatuses: make(map[string]*PodGroupStatusHistory),
	}
	egLock = &sync.Mutex{}
}

func MaintainEngineStatusHistory(engine *OrcEngine) {
	tick := time.Tick(1 * time.Hour)
	for {
		select {
		case <-tick:
			egStatuses.checkStatus(engine)
		case <-engine.stop:
			return
		}
	}
}

// Sync with etcd data when start deployd
// ugly finished! should change with allKeysByPrefix(return all non-dir node)
func SyncEventsDataFromStorage(engine *OrcEngine) bool {
	egStatuses.pgStatuses = make(map[string]*PodGroupStatusHistory)
	store := engine.store
	podgroups, err := store.KeysByPrefix(KCntStatus)
	log.Debugf("events:%v, %v ", podgroups, err)
	if err != nil {
		return err == storage.KMissingError
	}
	for _, podgroup := range podgroups { //   /lain/deployd/histroy
		pgsh := &PodGroupStatusHistory{
			podStatuses: make(map[int]*podStatusHistory),
		}
		podname := strings.TrimPrefix(podgroup, KCntStatus+"/")
		egStatuses.pgStatuses[podname] = pgsh
		instances, err := store.KeysByPrefix(podgroup)
		if err != nil {
			return false
		}
		for _, instanceKey := range instances { //   /lain/deployd/histroy/$podgroup
			instanceK := strings.TrimPrefix(instanceKey, podgroup+"/")
			if instance, err := strconv.Atoi(instanceK); err == nil {
				podSH := newPodStatusHistory(podname, instance)
				pgsh.podStatuses[instance] = podSH
				var pos StatusLastPos
				if err := store.Get(fmt.Sprintf(FmtKCntStatusLstPos, podname, instance), &pos); err != nil {
					if err != storage.KMissingError {
						return false
					}
				}
				podSH.pos = &pos
				statusKeys, err := store.KeysByPrefix(instanceKey)
				if err != nil {
					return false
				}
				for _, statusKey := range statusKeys { //  /lain/deployd/histroy/$podgroup/$instance
					indexKey := strings.TrimPrefix(statusKey, instanceKey+"/")
					if index, err := strconv.Atoi(indexKey); err == nil {
						var status StatusMessage
						if err := store.Get(statusKey, &status); err == nil {
							podSH.statuses[index] = &status
						} else {
							return false
						}
					}
				}
			}
		}
	}
	return true
}

func newPodStatusHistory(podname string, instance int) *podStatusHistory {
	dirtys := make([]bool, DefaultStatusSize)
	locks := make([]*sync.Mutex, DefaultStatusSize)
	statuses := make([]*StatusMessage, DefaultStatusSize)
	for i := 0; i < DefaultStatusSize; i++ {
		dirtys[i] = false
		locks[i] = &sync.Mutex{}
	}
	psh := &podStatusHistory{
		pos:      &StatusLastPos{0, DefaultStatusSize},
		podname:  podname,
		instance: instance,
		dirty:    true,
		lock:     &sync.Mutex{},
		dirtys:   dirtys,
		locks:    locks,
		statuses: statuses,
	}
	return psh
}

func NewPodStatusHistory(podname string, instance int, status *StatusMessage) *podStatusHistory {
	psh := newPodStatusHistory(podname, instance)
	psh.statuses[0] = status
	return psh
}

func savePodStaHstry(engine *OrcEngine, event *adoc.Event) {
	actor := event.Actor
	if name, ok := actor.Attributes["name"]; ok {
		if podname, instance, err := util.ParseNameInstanceNo(name); err == nil {
			status := &StatusMessage{
				Status: event.Status,
				From:   event.From,
				Time:   event.Time,
				Action: event.Action,
			}
			nextPos := 0
			egLock.Lock()
			defer egLock.Unlock()
			if pgStatus, ok := egStatuses.pgStatuses[podname]; ok {
				if podStatus, ok := pgStatus.podStatuses[instance]; ok {
					nextPos = (podStatus.pos.Pos + 1) % podStatus.pos.Size
					podStatus.statuses[nextPos] = status
					podStatus.pos.Pos = nextPos
				} else {
					pgStatus.podStatuses[instance] = NewPodStatusHistory(podname, instance, status)
				}
			} else {
				podStatuses := make(map[int]*podStatusHistory)
				psh := NewPodStatusHistory(podname, instance, status)
				podStatuses[instance] = psh
				egStatuses.pgStatuses[podname] = &PodGroupStatusHistory{podStatuses}
			}
			egStatuses.pgStatuses[podname].podStatuses[instance].Save(engine)
		}
	}
}

func FetchPodStaHstry(engine *OrcEngine, podname string, instance int) []*StatusMessage {
	stmsgs := make([]*StatusMessage, 0)
	if pgSH, ok := egStatuses.pgStatuses[podname]; ok {
		if podSH, ok := pgSH.podStatuses[instance]; ok {
			pos := podSH.pos
			start := (pos.Pos + 1) % pos.Size
			if podSH.statuses[start] == nil {
				for i := 0; i <= pos.Pos; i++ {
					stmsgs = append(stmsgs, podSH.statuses[i])
				}
			} else {
				for i := start; i < pos.Size; i++ {
					stmsgs = append(stmsgs, podSH.statuses[i])
				}
				for i := 0; i < start; i++ {
					stmsgs = append(stmsgs, podSH.statuses[i])
				}
			}
			return stmsgs
		}
	}
	return stmsgs
}
