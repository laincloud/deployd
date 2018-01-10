package engine

import (
	"strings"
	"sync"

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

//*************************container events ****************************//
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
					pgCtrl.Lock()
					if len(pgCtrl.podCtrls) >= instance {
						podCtrl := pgCtrl.podCtrls[instance-1]
						podCtrl.pod.Healthst = status
						if status == HealthStateHealthy {
							podCtrl.launchEvent(struct{}{})
						}
						pgCtrl.opsChan <- pgOperSnapshotGroup{true}
						pgCtrl.opsChan <- pgOperSaveStore{true}
					}
					pgCtrl.Unlock()
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
