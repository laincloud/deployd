package engine

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/laincloud/deployd/cluster"
	"github.com/laincloud/deployd/storage"
	"github.com/mijia/adoc"
	"github.com/mijia/sweb/log"
)

var RefreshInterval int

var cstController *constraintController

var ntfController *notifyController

var (
	ErrPodGroupExists         = errors.New("PodGroup has already existed")
	ErrPodGroupNotExists      = errors.New("PodGroup not existed")
	ErrPodGroupCleaning       = errors.New("PodGroup is removing, need to wait for that")
	ErrNotEnoughResources     = errors.New("Not enough CPUs and Memory to use")
	ErrDependencyPodExists    = errors.New("DependencyPod has already existed")
	ErrDependencyPodNotExists = errors.New("DependencyPod not existed")
	ErrConstraintNotExists    = errors.New("Constraint not existed")
	ErrNotifyNotExists        = errors.New("Notify uri not existed")
)

const (
	ClusterFailedThreadSold = 20
)

type EngineConfig struct {
	ReadOnly    bool `json:"readonly"`
	Maintenance bool `json:"maintenance"`
}

type OrcEngine struct {
	sync.RWMutex

	config       EngineConfig
	cluster      cluster.Cluster
	store        storage.Store
	eagleView    *RuntimeEagleView
	pgCtrls      map[string]*podGroupController
	rmPgCtrls    map[string]*podGroupController
	dependsCtrls map[string]*dependsController
	rmDepCtrls   map[string]*dependsController
	opsChan      chan orcOperation
	refreshAllChan chan bool
	stop         chan struct{}
	clstrFailCnt int
}

const (
	maxDownNode         = 3
	downNodeResetPeriod = 3 * time.Minute
)

func (engine *OrcEngine) ReadOnly() bool {
	return engine.config.ReadOnly || engine.config.Maintenance
}

func (engine *OrcEngine) Config() EngineConfig {
	return engine.config
}

func (engine *OrcEngine) Maintaince(maintaince bool) {
	engine.RLock()
	defer engine.RUnlock()
	engine.config.Maintenance = maintaince
}

func (engine *OrcEngine) SetConfig(config EngineConfig) {
	engine.RLock()
	defer engine.RUnlock()
	engine.config = config
	ConfigEngine(engine)
}

func (engine *OrcEngine) PgOpStart(pgName string) {
	pgOpStart(engine.store, pgName)
}

func (engine *OrcEngine) PgOpOver(pgName string) {
	pgOpOver(engine.store, pgName)
}

func (engine *OrcEngine) ListenerId() string {
	return "deployd.orc_engine"
}

func (engine *OrcEngine) HandleEvent(payload interface{}) {
	// Handle the dependency events
	switch event := payload.(type) {
	case DependencyEvent:
		engine.RLock()
		defer engine.RUnlock()
		if depCtrl, ok := engine.dependsCtrls[event.Name]; ok {
			log.Debugf("Engine handle event %#v, dispatch it to dependsCtrl %s", event, depCtrl)
			engine.opsChan <- orcOperDependsDispatch{depCtrl, event}
		} else {
			// FIXME we are missing some dependency, should create the alarm
			log.Warnf("Engine found some missing dependency pod, %s", event.Name)
		}
	case OperationEvent:
		log.Infof("OperationEvent is %v\n", event)
		engine.opsChan <- orcOperEventHandler{event}
	default:
		log.Debugf("I don't know about type %T!\n", event)
	}
}

func (engine *OrcEngine) NewDependencyPod(spec PodSpec) error {
	engine.Lock()
	defer engine.Unlock()

	if _, ok := engine.dependsCtrls[spec.Name]; ok {
		return ErrDependencyPodExists
	}
	if _, ok := engine.rmDepCtrls[spec.Name]; ok {
		return ErrDependencyPodExists
	}

	depCtrl := engine.initDependsCtrl(spec, nil)
	engine.dependsCtrls[spec.Name] = depCtrl
	engine.opsChan <- orcOperDependsAddSpec{depCtrl}
	return nil
}

func (engine *OrcEngine) GetDependencyPod(name string) (NamespacePodsWithSpec, error) {
	engine.RLock()
	defer engine.RUnlock()

	if depCtrl, ok := engine.dependsCtrls[name]; !ok {
		return NamespacePodsWithSpec{}, ErrDependencyPodNotExists
	} else {
		return depCtrl.Inspect(), nil
	}
}

func (engine *OrcEngine) UpdateDependencyPod(spec PodSpec) error {
	engine.RLock()
	defer engine.RUnlock()
	if depCtrl, ok := engine.dependsCtrls[spec.Name]; !ok {
		return ErrDependencyPodNotExists
	} else {
		engine.opsChan <- orcOperDependsUpdateSpec{depCtrl, spec}
		return nil
	}
}

func (engine *OrcEngine) RemoveDependencyPod(name string, force bool) error {
	engine.Lock()
	defer engine.Unlock()
	if depCtrl, ok := engine.dependsCtrls[name]; !ok {
		return ErrDependencyPodNotExists
	} else {
		engine.opsChan <- orcOperDependsRemoveSpec{depCtrl, force}
		delete(engine.dependsCtrls, name)
		engine.rmDepCtrls[name] = depCtrl
		go engine.checkDependsRemoveResult(name, depCtrl)
		return nil
	}
	return nil
}

func (engine *OrcEngine) GetNodes() ([]cluster.Node, error) {
	return engine.cluster.GetResources()
}

func (engine *OrcEngine) NewPodGroup(spec PodGroupSpec) error {
	engine.Lock()
	defer engine.Unlock()
	if _, ok := engine.pgCtrls[spec.Name]; ok {
		return ErrPodGroupExists
	}
	if _, ok := engine.rmPgCtrls[spec.Name]; ok {
		return ErrPodGroupCleaning
	}
	spec.CreatedAt = time.Now()
	spec.Pod.CreatedAt = spec.CreatedAt
	for _, depends := range spec.Pod.Dependencies {
		if _, ok := engine.dependsCtrls[depends.PodName]; !ok {
			//We will allow the weak reference to the dependency pods and won't return an error
			//FIXME: generate the alarm message or alarm data
			log.Warnf("Engine found some missing dependency pod, %s", depends.PodName)
		}
	}

	var pg PodGroup
	pg.State = RunStatePending
	pgCtrl := engine.initPodGroupCtrl(spec, nil, pg)
	engine.pgCtrls[spec.Name] = pgCtrl
	engine.opsChan <- orcOperDeploy{pgCtrl}
	return nil
}

func (engine *OrcEngine) InspectPodGroup(name string) (PodGroupWithSpec, bool) {
	engine.RLock()
	defer engine.RUnlock()
	if pgCtrl, ok := engine.pgCtrls[name]; !ok {
		return PodGroupWithSpec{}, false
	} else {
		return pgCtrl.Inspect(), true
	}
}

func (engine *OrcEngine) FetchPodStaHstry(name string, instance int) []*StatusMessage {
	return FetchPodStaHstry(engine, name, instance)
}

func (engine *OrcEngine) RefreshPodGroup(name string, forceUpdate bool) error {
	engine.RLock()
	defer engine.RUnlock()
	if pgCtrl, ok := engine.pgCtrls[name]; !ok {
		return ErrPodGroupNotExists
	} else {
		engine.opsChan <- orcOperRefresh{pgCtrl, forceUpdate}
		return nil
	}
}

func (engine *OrcEngine) RemovePodGroup(name string) error {
	engine.Lock()
	defer engine.Unlock()
	if pgCtrl, ok := engine.pgCtrls[name]; !ok {
		return ErrPodGroupNotExists
	} else {
		if err := canOperation(pgCtrl, PGOpStateRemoving); err != nil {
			return err
		}
		log.Infof("start delete %v\n", name)
		engine.opsChan <- orcOperRemove{pgCtrl}
		delete(engine.pgCtrls, name)
		engine.rmPgCtrls[name] = pgCtrl
		go engine.checkPodGroupRemoveResult(name, pgCtrl)
		return nil
	}
}

func (engine *OrcEngine) RescheduleInstance(name string, numInstances int, restartPolicy ...RestartPolicy) error {
	engine.RLock()
	defer engine.RUnlock()
	if pgCtrl, ok := engine.pgCtrls[name]; !ok {
		return ErrPodGroupNotExists
	} else {
		if err := canOperation(pgCtrl, PGOpStateScheduling); err != nil {
			return err
		}
		engine.opsChan <- orcOperRescheduleInstance{pgCtrl, numInstances, restartPolicy}
		return nil
	}
}

func (engine *OrcEngine) RescheduleSpec(name string, podSpec PodSpec) error {
	engine.RLock()
	defer engine.RUnlock()
	if pgCtrl, ok := engine.pgCtrls[name]; !ok {
		return ErrPodGroupNotExists
	} else {
		if err := canOperation(pgCtrl, PGOpStateUpgrading); err != nil {
			return err
		}
		for _, depends := range podSpec.Dependencies {
			if _, ok := engine.dependsCtrls[depends.PodName]; !ok {
				// We will allow the weak reference to the dependency pods and won't return an error
				// FIXME: generate the alarm message or alarm data
				log.Warnf("Engine found some missing dependency pod, %s", depends.PodName)
			}
		}
		if engine.hasEnoughResource(pgCtrl, podSpec) {
			engine.opsChan <- orcOperRescheduleSpec{pgCtrl, podSpec}
		} else {
			pgCtrl.Lock()
			pgCtrl.group.LastError = "No resources available to scheduler container"
			pgCtrl.Unlock()
			log.Info("No resources available to scheduler container")
			pgCtrl.opsChan <- pgOperSaveStore{true}
			pgCtrl.opsChan <- pgOperOver{}
		}

		return nil
	}
}

func (engine *OrcEngine) DriftNode(fromNode, toNode string, pgName string, pgInstance int, force bool) {
	engine.RLock()
	defer engine.RUnlock()
	if pgName == "" {
		for _, pgCtrl := range engine.pgCtrls {
			_pgCtrl := pgCtrl
			engine.opsChan <- orcOperScheduleDrift{_pgCtrl, fromNode, toNode, pgInstance, force}
		}
	} else {
		if pgCtrl, ok := engine.pgCtrls[pgName]; ok {
			engine.opsChan <- orcOperScheduleDrift{pgCtrl, fromNode, toNode, pgInstance, force}
		}
	}
	// FIXME: do we need to tell dependsCtrl to drift?
	// so far we just wait for the dependsCtrl to react to the events
}

func (engine *OrcEngine) ChangeState(pgName, op string, instance int) error {
	engine.RLock()
	defer engine.RUnlock()
	if pgCtrl, ok := engine.pgCtrls[pgName]; ok {
		targetState := PGOpStateIdle
		switch op {
		case "stop":
			targetState = PGOpStateStoping
		case "start":
			targetState = PGOpStateStarting
		case "restart":
			targetState = PGOpStateRestarting
		}
		if err := canOperation(pgCtrl, (PGOpState)(targetState)); err != nil {
			return err
		}
		engine.opsChan <- orcOperChangeState{pgCtrl, op, instance}
	} else {
		return ErrPodGroupNotExists
	}
	return nil
}

func (engine *OrcEngine) hasEnoughResource(pgCtrl *podGroupController, podSpec PodSpec) bool {
	if resources, err := engine.cluster.GetResources(); err != nil {
		return false
	} else {
		podsLen := pgCtrl.spec.NumInstances
		singleMemory := podSpec.Containers[0].MemoryLimit
		availbleNums := 0
		for _, resource := range resources {
			nodeAvailMem := (resource.Memory - resource.UsedMemory)
			for _, pod := range pgCtrl.group.Pods {
				if pod.NodeName() == resource.Name {
					nodeAvailMem += pgCtrl.spec.Pod.Containers[0].MemoryLimit
				}
			}
			availbleNums += int(nodeAvailMem / singleMemory)
			if availbleNums >= podsLen {
				return true
			}
		}
	}
	return false
}

func (engine *OrcEngine) Start() {
	engine.Lock()
	defer engine.Unlock()
	log.Infof("Start engine...")
	if engine.stop != nil { // stop is not nil, means it having been started
		log.Debugf("Engine having been started, ignore.")
		return
	}
	engine.stop = make(chan struct{})
	go engine.initOperationWorker()
	go engine.startClusterMonitor()
	go engine.checkOperatingPgs()
}

func (engine *OrcEngine) Stop() {
	engine.Lock()
	defer engine.Unlock()
	log.Infof("Stop engine...")
	if engine.stop == nil {
		log.Debugf("Engine having been stop, ignore.")
		return
	}
	select {
	case _, ok := <-engine.stop:
		if !ok {
			return // channel was closed
		}
	default:
	}
	close(engine.stop)
	engine.stop = nil
}

func (engine *OrcEngine) GuardGotoSleep() bool {
	engine.Lock()
	defer engine.Unlock()
	return GuardGotoSleep(engine.store)
}

func (engine *OrcEngine) GuardGotoWork() bool {
	engine.Lock()
	defer engine.Unlock()
	return GuardGotoWork(engine.store)
}

func (engine *OrcEngine) Started() bool {
	return engine.stop != nil
}

func (engine *OrcEngine) LoadDependsPods() error {
	depCtrls := make(map[string]*dependsController)
	specDirKey := strings.Join([]string{kLainDeploydRootKey, kLainDependencyKey, kLainSpecKey}, "/")
	if specNames, err := engine.store.KeysByPrefix(specDirKey); err != nil {
		if err != storage.KMissingError {
			return err
		}
	} else {
		for _, name := range specNames {
			var spec PodSpec
			if err := engine.store.Get(name, &spec); err != nil {
				log.Errorf("Failed to load dependency pod spec %q from storage, %s", name, err)
				return err
			}

			var pods map[string]map[string]SharedPodWithSpec
			podsKey := strings.Join([]string{kLainDeploydRootKey, kLainDependencyKey, kLainPodKey, spec.Name}, "/")
			if err := engine.store.Get(podsKey, &pods); err != nil {
				if err != storage.KMissingError {
					log.Errorf("Failed to load dependency pods runtime %q from storage, %s", podsKey, err)
					return err
				} else {
					// we should allow only have the spec but no pods
					log.Warnf("Found empty dependency pods runtime %q from storage, %s", podsKey, err)
				}
			}
			depCtrls[spec.Name] = engine.initDependsCtrl(spec, pods)
			log.Infof("Loaded DependsController, %s", depCtrls[spec.Name])
		}
	}
	engine.dependsCtrls = depCtrls
	return nil
}

func (engine *OrcEngine) LoadPodGroups() error {
	pgCtrls := make(map[string]*podGroupController)
	pgKey := fmt.Sprintf("%s/%s", kLainDeploydRootKey, kLainPodGroupKey)
	if pgNamespaces, err := engine.store.KeysByPrefix(pgKey); err != nil {
		if err != storage.KMissingError {
			return err
		}
	} else {
		for _, pgNamespace := range pgNamespaces {
			pgNames, err := engine.store.KeysByPrefix(pgNamespace)
			if err != nil {
				if err != storage.KMissingError {
					return err
				}
			}
			for _, pgName := range pgNames {
				var pgWithSpec PodGroupWithSpec
				if err := engine.store.Get(pgName, &pgWithSpec); err != nil {
					log.Errorf("Failed to load pod group with spec %q from storage, %s", pgName, err)
					return err
				}
				spec, states, pg := pgWithSpec.Spec, pgWithSpec.PrevState, pgWithSpec.PodGroup
				pgCtrls[spec.Name] = engine.initPodGroupCtrl(spec, states, pg)
				log.Infof("Loaded PodGroupController, %s", pgCtrls[spec.Name])
			}
		}
	}
	engine.pgCtrls = pgCtrls
	return nil
}

func (engine *OrcEngine) GetConstraints(cstType string) (ConstraintSpec, bool) {
	return cstController.GetConstraint(cstType)
}

func (engine *OrcEngine) UpdateConstraints(spec ConstraintSpec) error {
	return cstController.SetConstraint(spec, engine.store)
}

func (engine *OrcEngine) DeleteConstraints(cstType string) error {
	if _, ok := cstController.GetConstraint(cstType); !ok {
		return ErrConstraintNotExists
	} else {
		return cstController.RemoveConstraint(cstType, engine.store)
	}
}

func (engine *OrcEngine) GetNotifies() []string {
	notifies := ntfController.GetAllNotifies()
	return ntfController.CallbackList(notifies)
}

func (engine *OrcEngine) AddNotify(callback string) error {
	return ntfController.AddNotify(callback, engine.store)
}

func (engine *OrcEngine) DeleteNotify(callback string) error {
	if _, ok := ntfController.GetAllNotifies()[callback]; !ok {
		return ErrNotifyNotExists
	} else {
		return ntfController.RemoveNotify(callback, engine.store)
	}
}

func (engine *OrcEngine) initDependsCtrl(spec PodSpec, pods map[string]map[string]SharedPodWithSpec) *dependsController {
	depCtrl := newDependsController(spec, pods)
	depCtrl.Activate(engine.cluster, engine.store, engine.eagleView, engine.stop)
	return depCtrl
}

func (engine *OrcEngine) initPodGroupCtrl(spec PodGroupSpec, states []PodPrevState, pg PodGroup) *podGroupController {
	pgCtrl := newPodGroupController(spec, states, pg, engine)
	pgCtrl.AddListener(engine)
	pgCtrl.Activate(engine.cluster, engine.store, engine.eagleView, engine.stop)
	return pgCtrl
}

func (engine *OrcEngine) clusterRequestFailed() {
	engine.clstrFailCnt++
	if engine.clstrFailCnt > ClusterFailedThreadSold && engine.clstrFailCnt%ClusterFailedThreadSold == 0 {
		ntfController.Send(NewNotifySpec("Cluster", "Cluster-Manager",
			1, time.Now(), NotifyClusterUnHealthy))
	}
}

func (engine *OrcEngine) checkOperatingPgs() {
	engine.RLock()
	defer engine.RUnlock()
	unFinishedPgs := operatings(engine.store)
	log.Infof("Unfinished works:%v \n", unFinishedPgs)
	for _, pgName := range unFinishedPgs {
		if pgCtrl, ok := engine.pgCtrls[pgName]; ok {
			engine.opsChan <- orcOperRefresh{pgCtrl, false}
		} else {
			engine.cleanCorruptedPodGroup(pgName)
		}
	}
}

func (engine *OrcEngine) cleanCorruptedPodGroup(pgName string) {
	c := engine.cluster
	if pods, err := engine.eagleView.RefreshPodGroup(c, pgName); err != nil {
		log.Warn("Refresh corrupted PodGroup Failed with Error:%v\n", err)
	} else {
		for _, pod := range pods {
			container := pod.Container
			if err := c.StopContainer(container.Id, 10); err != nil {
				log.Warnf("cannot stop the container %s, %s, remove it directly", container.Id, err.Error())
			}
			if err := c.RemoveContainer(container.Id, true, false); err != nil {
				log.Warnf("Cannot remove the container %s, %s", container.Id, err)
			}
		}
	}
}

func (engine *OrcEngine) clusterRequestSucceed() {
	engine.clstrFailCnt = 0
}

func (engine *OrcEngine) refreshAllPodGroups() {
	engine.RLock()
	if len(engine.pgCtrls) > 0 {
		rInterval := RefreshInterval / 2 * 1000 / len(engine.pgCtrls)
		index := 0
		for _, pgCtrl := range engine.pgCtrls {
			if atomic.LoadInt32(&(pgCtrl.refreshable)) == 1 {
				interval := index * rInterval
				_pgCtrl := pgCtrl
				index++
				go func() {
					log.Infof("%s will be refreshed after %d seconds", _pgCtrl, interval/1000)
					time.Sleep(time.Duration(interval) * time.Millisecond)
					engine.opsChan <- orcOperRefresh{_pgCtrl, false}
				}()
			}
		}
	}
	if len(engine.dependsCtrls) > 0 {
		rInterval := RefreshInterval / 2 * 1000 / len(engine.dependsCtrls)
		index := 0
		for _, depCtrl := range engine.dependsCtrls {
			interval := index * rInterval
			_depCtrl := depCtrl
			index++
			go func() {
				log.Infof("%s will be refreshed after %d seconds", _depCtrl, interval/1000)
				time.Sleep(time.Duration(RefreshInterval/2*1000+interval) * time.Millisecond)
				engine.opsChan <- orcOperDependsRefresh{_depCtrl}
			}()
		}
	}
	engine.RUnlock()
}

// This will be running inside the go routine
func (engine *OrcEngine) initOperationWorker() {
	tick := time.Tick(time.Duration(RefreshInterval) * time.Second)
	portsTick := time.Tick(5 * time.Minute)
	for {
		select {
		case op := <-engine.opsChan:
			op.Do(engine)
		case <-engine.refreshAllChan:
			engine.refreshAllPodGroups()
		case <-tick:
			engine.refreshAllPodGroups()
		case <-portsTick:
			RefreshPorts(engine.pgCtrls)
		case <-engine.stop:
			return
		}
	}
}

// This will be running inside the go routine
func (engine *OrcEngine) checkDependsRemoveResult(name string, depCtrl *dependsController) {
	tick := time.Tick(5 * time.Second)
	for _ = range tick {
		switch depCtrl.RemoveStatus() {
		case 1:
			log.Infof("<OrcEngine> DependsCtrl %s is safely removed", name)
			engine.Lock()
			delete(engine.rmDepCtrls, name)
			engine.Unlock()
			return
		case 2:
			log.Infof("<OrcEngine> DependsCtrl %s cannot be removed, someone maybe using it", name)
			engine.Lock()
			engine.dependsCtrls[name] = depCtrl
			delete(engine.rmDepCtrls, name)
			engine.Unlock()
			return
		}
	}
}

// This will be running inside the go routine
func (engine *OrcEngine) checkPodGroupRemoveResult(name string, pgCtrl *podGroupController) {
	timeout := time.After(60 * time.Second)
	tick := time.Tick(5 * time.Second)
	for {
		select {
		case <-tick:
			if pgCtrl.IsRemoved() {
				log.Infof("<OrcEngine> PodGroup %s is safely removed", name)
				engine.Lock()
				delete(engine.rmPgCtrls, name)
				engine.Unlock()
				return
			}
		case <-timeout:
			log.Errorf("!!!<OrcEngine> timeout when checking pod group results, pg %s need to be checked and removed annually.", name)
			engine.Lock()
			delete(engine.rmPgCtrls, name)
			engine.Unlock()
			return
		}
	}
}

func canOperation(pgCtrl *podGroupController, target PGOpState) error {
	if canOp := pgCtrl.CanOperate(target); !canOp {
		return OperLockedError{info: "Scheduling"}
	}
	return nil
}

// fetch all operating pgs' name
func operatings(store storage.Store) []string {
	operatingPrefixKey := kLainDeploydRootKey + "/" + kLainPgOpingKey
	results := make([]string, 0)
	if keys, err := store.KeysByPrefix(operatingPrefixKey); err != nil {
		log.Warnf("[Store] Failed to fetch operating pod groups %s, %s", operatingPrefixKey, err)
	} else {
		for _, key := range keys {
			results = append(results, strings.TrimPrefix(key, operatingPrefixKey+"/"))
		}
	}
	return results
}

func pgOpStart(store storage.Store, pgname string) {
	operatingKey := kLainDeploydRootKey + "/" + kLainPgOpingKey + "/" + pgname
	if err := store.SetWithTTL(operatingKey, struct{}{}, DefaultLastSpecCacheTTL, true); err != nil {
		log.Warnf("[Store] Failed to save operating pod group %s, %s", operatingKey, err)
	}
}

func pgOpOver(store storage.Store, pgname string) {
	operatingKey := kLainDeploydRootKey + "/" + kLainPgOpingKey + "/" + pgname
	if err := store.Remove(operatingKey); err != nil {
		log.Warnf("[Store] Failed to remove operating pod group %s, %s", operatingKey, err)
	}
}

// kLainPgOpingKey

func (engine *OrcEngine) onClusterNodeLost(nodeName string, downCount int) {
	log.Warnf("Cluster node is down, [%q], %d nodes down in all, will check if need stop the engine", nodeName, downCount)
	if downCount >= maxDownNode {
		log.Warnf("Too many cluster nodes stoped in a short period, need stop the engine")
		engine.Stop()
	}
}

func (engine *OrcEngine) startClusterMonitor() {
	restart := make(chan bool)
	downNodes := make(map[string]time.Time)
	for {
		succeed := SyncEventsDataFromStorage(engine)
		if !succeed {
			time.Sleep(1 * time.Hour)
		} else {
			break
		}
	}
	go MaintainEngineStatusHistory(engine) //
	eventMonitorId := engine.cluster.MonitorEvents("", func(event adoc.Event, err error) {
		if err != nil {
			// log.Warnf("Error during the cluster event monitor, will try to restart the monitor, %s", err)
			engine.clusterRequestFailed()
			restart <- true
		} else {
			engine.clusterRequestSucceed()
			if strings.HasPrefix(event.From, "swarm") {
				switch event.Status {
				case "engine_disconnect":
					now := time.Now()
					log.Warnf("got engine disconnect event from %s, downTime: %v", event.Node.Name, now)
					downNodes[event.Node.Name] = now
					downCount := 0
					for _, v := range downNodes {
						if v.Add(downNodeResetPeriod).After(now) {
							downCount++
						}
					}
					engine.onClusterNodeLost(event.Node.Name, downCount)
				case "engine_connect":
					log.Infof("got engine connect event from %s", event.Node.Name)
					if _, ok := downNodes[event.Node.Name]; ok {
						delete(downNodes, event.Node.Name)
						engine.refreshAllChan <- true
					}
				}
			} else {
				HandleDockerEvent(engine, &event)
			}
		}
	})
	shouldRestart := false
	select {
	case <-engine.stop:
		engine.cluster.StopMonitor(eventMonitorId)
	case <-restart:
		engine.cluster.StopMonitor(eventMonitorId)
		close(restart)
		time.Sleep(200 * time.Millisecond)
		shouldRestart = true
	}
	if shouldRestart {
		engine.startClusterMonitor()
	}
}

func New(cluster cluster.Cluster, store storage.Store) (*OrcEngine, error) {
	engine := &OrcEngine{
		cluster:      cluster,
		config:       EngineConfig{ReadOnly: false},
		store:        store,
		pgCtrls:      make(map[string]*podGroupController),
		rmPgCtrls:    make(map[string]*podGroupController),
		dependsCtrls: make(map[string]*dependsController),
		rmDepCtrls:   make(map[string]*dependsController),
		opsChan:      make(chan orcOperation, 500),
		stop:         nil,
		clstrFailCnt: 0,
	}
	configSpecsVars(store)
	watchResource(store)
	WatchEngineConfig(engine)

	eagleView := NewRuntimeEagleView()
	//if err := eagleView.Refresh(cluster); err != nil {
	//log.Warnf("<OrcEngine> Cannot refresh all the runtime data for bootstraping, %s", err)
	//return nil, err
	//}
	engine.eagleView = eagleView

	cstController = NewConstraintController()
	if err := cstController.LoadConstraints(engine.store); err != nil {
		return nil, err
	}

	ntfController = NewNotifyController(engine.stop)
	if err := ntfController.LoadNotifies(engine.store); err != nil {
		return nil, err
	}

	if err := engine.LoadDependsPods(); err != nil {
		return nil, err
	}

	if err := engine.LoadPodGroups(); err != nil {
		return nil, err
	}

	engine.Start()

	return engine, nil
}
