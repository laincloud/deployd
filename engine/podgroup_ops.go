package engine

import (
	"fmt"
	"time"

	"github.com/laincloud/deployd/cluster"
	"github.com/laincloud/deployd/storage"
	"github.com/mijia/adoc"
	"github.com/mijia/go-generics"
	"github.com/mijia/sweb/log"
)

type pgOperation interface {
	Do(pgCtrl *podGroupController, c cluster.Cluster, store storage.Store, ev *RuntimeEagleView) bool
}

type pgOperSaveStore struct {
	force bool
}

type pgOperCacheLastSpec struct {
	spec PodGroupSpec
}

func (op pgOperCacheLastSpec) Do(pgCtrl *podGroupController, c cluster.Cluster, store storage.Store, ev *RuntimeEagleView) bool {
	var _err error
	start := time.Now()
	defer func() {
		pgCtrl.RLock()
		log.Infof("%s save last pod spec info, op=%+v, err=%v, duration=%s", pgCtrl, op, _err, time.Now().Sub(start))
		pgCtrl.RUnlock()
	}()
	if err := store.SetWithTTL(pgCtrl.lastPodSpecKey, op.spec, DefaultLastSpecCacheTTL, true); err != nil {
		log.Warnf("[Store] Failed to save last pod spec %s, %s", pgCtrl.lastPodSpecKey, err)
		_err = err
	}
	return false
}

func (op pgOperSaveStore) Do(pgCtrl *podGroupController, c cluster.Cluster, store storage.Store, ev *RuntimeEagleView) bool {
	var _err error
	start := time.Now()
	defer func() {
		pgCtrl.RLock()
		log.Infof("%s save, op=%+v, err=%v, duration=%s", pgCtrl, op, _err, time.Now().Sub(start))
		pgCtrl.RUnlock()
	}()
	pg := pgCtrl.Inspect()
	if pgCtrl.IsHealthy() {
		if err := store.Set(pgCtrl.storedKey, pg, op.force); err != nil {
			log.Warnf("[Store] Failed to save pod group %s, %s", pgCtrl.storedKey, err)
			_err = err
		}
	} else {
		log.Warnf("Some pods lost IP, will not save pod group %s", pgCtrl.storedKey)
	}

	return false
}

type pgOperRemoveStore struct{}

func (op pgOperRemoveStore) Do(pgCtrl *podGroupController, c cluster.Cluster, store storage.Store, ev *RuntimeEagleView) bool {
	var _err error
	start := time.Now()
	defer func() {
		pgCtrl.RLock()
		log.Infof("%s remove, op=%+v, err=%v, duration=%s", pgCtrl, op, _err, time.Now().Sub(start))
		pgCtrl.RUnlock()
	}()
	if err := store.Remove(pgCtrl.storedKey); err != nil {
		_err = err
	} else {
		store.TryRemoveDir(pgCtrl.storedKeyDir)
	}
	return false
}

type pgOperSnapshotEagleView struct {
	pgName string
}

func (op pgOperSnapshotEagleView) Do(pgCtrl *podGroupController, c cluster.Cluster, store storage.Store, ev *RuntimeEagleView) bool {
	var _err error
	start := time.Now()
	defer func() {
		pgCtrl.RLock()
		log.Infof("%s snapshot the swarm containers, op=%+v, err=%v, duration=%s", pgCtrl, op, _err, time.Now().Sub(start))
		pgCtrl.RUnlock()
	}()
	if pods, err := ev.RefreshPodGroup(c, op.pgName); err != nil {
		_err = err
	} else {
		snapshot := make(map[string]RuntimeEaglePod)
		for _, p := range pods {
			snapshot[p.Container.Id] = p
		}
		pgCtrl.evSnapshot = snapshot
		pgCtrl.cleanCorruptedContainers()
	}
	return false
}

type pgOperUpgradeInstance struct {
	instanceNo int
	version    int
	oldPodSpec PodSpec
	newPodSpec PodSpec
}

func (op pgOperUpgradeInstance) Do(pgCtrl *podGroupController, c cluster.Cluster, store storage.Store, ev *RuntimeEagleView) bool {
	start := time.Now()
	defer func() {
		pgCtrl.RLock()
		log.Infof("%s upgrade instance, iNo=%d, version=%d, duration=%s", pgCtrl, op.instanceNo, op.version, time.Now().Sub(start))
		pgCtrl.RUnlock()
	}()

	podCtrl := pgCtrl.podCtrls[op.instanceNo-1]
	newPodSpec := op.newPodSpec.Clone()
	newPodSpec.PrevState = podCtrl.spec.PrevState.Clone() // upgrade action, state should not changed
	prevNodeName := newPodSpec.PrevState.NodeName

	isLastPodHealthy := pgCtrl.waitLastPodHealth(op.instanceNo)
	if !isLastPodHealthy && op.instanceNo == 2 && pgCtrl.rollBack() {
		return false
	}
	log.Infof("upgrade instance : %d !", op.instanceNo)
	var lowOp pgOperation
	lowOp = pgOperRemoveInstance{op.instanceNo, op.oldPodSpec}
	lowOp.Do(pgCtrl, c, store, ev)
	time.Sleep(10 * time.Second)

	// FIXME: do we need to consider hard state flag on upgrade
	if op.oldPodSpec.IsStateful() && newPodSpec.IsStateful() && prevNodeName != "" {
		newPodSpec.Filters = append(newPodSpec.Filters, fmt.Sprintf("constraint:node==%s", prevNodeName))
	}
	podCtrl.spec = newPodSpec
	podCtrl.pod.State = RunStatePending
	podCtrl.pod.RestartCount = 0
	lowOp = pgOperDeployInstance{op.instanceNo, op.version}
	lowOp.Do(pgCtrl, c, store, ev)
	return false
}

type pgOperRefreshInstance struct {
	instanceNo int
	spec       PodGroupSpec
}

func (op pgOperRefreshInstance) Do(pgCtrl *podGroupController, c cluster.Cluster, store storage.Store, ev *RuntimeEagleView) bool {
	var runtime ImRuntime
	start := time.Now()
	defer func() {
		pgCtrl.RLock()
		log.Infof("%s refresh instance, iNo=%d, runtime=%+v, duration=%s", pgCtrl, op.instanceNo, runtime, time.Now().Sub(start))
		pgCtrl.RUnlock()
	}()

	if op.instanceNo > len(pgCtrl.podCtrls) {
		log.Warnf("Pod is not exists")
		return false
	}
	podCtrl := pgCtrl.podCtrls[op.instanceNo-1]

	podCtrl.Refresh(c)
	runtime = podCtrl.pod.ImRuntime

	// eagleview ids
	// in case of pod created failed
	evIds := make([]string, len(podCtrl.spec.Containers))
	evVersion := -1
	for _, podContainer := range pgCtrl.evSnapshot {
		if podContainer.InstanceNo == op.instanceNo {
			evVersion = podContainer.Version
			cId := podContainer.Container.Id
			cIndex := podContainer.ContainerIndex
			if cIndex >= 0 && cIndex < len(evIds) {
				evIds[cIndex] = cId
			}
		}
	}

	if len(podCtrl.pod.Containers) <= 0 {
		return false
	}
	// if some thing need change after refresh
	consistent := true
	container := podCtrl.pod.Containers[0]
	if (evVersion != -1 && op.spec.Version != evVersion) || podCtrl.spec.Version != op.spec.Version {
		log.Warnf("PodGroupCtrl %s, we found pod running with lower version, just upgrade it", op.spec)
		// the new spec should be in op.spec.Pod
		op := pgOperUpgradeInstance{op.instanceNo, op.spec.Version, podCtrl.spec, op.spec.Pod}
		op.Do(pgCtrl, c, store, ev)
		runtime = podCtrl.pod.ImRuntime
		consistent = false
	} else if runtime.State == RunStateSuccess {
		if runtime.Healthst == HealthStateUnHealthy {
			startAt := podCtrl.pod.Containers[0].Runtime.State.StartedAt
			checkTime := podCtrl.spec.GetSetupTime()
			if checkTime < DefaultSetUpTime {
				checkTime = DefaultSetUpTime
			}
			if time.Now().After(startAt.Add(time.Second * time.Duration(checkTime) * 5)) {
				log.Warnf("PodGroupCtrl %s, we found pod unhealthy", op.spec)
				ntfController.Send(NewNotifySpec(podCtrl.spec.Namespace, podCtrl.spec.Name,
					op.instanceNo, time.Now(), NotifyPodUnHealthy))
			}
		}
		if generics.Equal_StringSlice(evIds, podCtrl.pod.ContainerIds()) && op.spec.Version == evVersion {
			pod := podCtrl.pod.Clone()
			pgCtrl.emitChangeEvent("verify", podCtrl.spec, pod, pod.NodeName())
		}
	} else if runtime.State == RunStateMissing {
		if !FetchGuard().Working {
			return false
		}
		foundRuntime := false
		for i, cId := range evIds {
			if cId != "" {
				podCtrl.pod.Containers[i].Id = cId
				foundRuntime = true
			}
		}
		if foundRuntime {
			// recover from the runtime
			podCtrl.Refresh(c)
			runtime = podCtrl.pod.ImRuntime
			if runtime.State == RunStateSuccess {
				pod := podCtrl.pod.Clone()
				pgCtrl.emitChangeEvent("verify", podCtrl.spec, pod, pod.NodeName())
			}
		} else {
			log.Warnf("PodGroupCtrl %s, we found pod missing, just redeploy it", op.spec)
			// pod missing usually happended when agent was down, so no need to notify app owner
			newPodSpec := podCtrl.spec.Clone()
			prevNodeName := newPodSpec.PrevState.NodeName
			if newPodSpec.IsHardStateful() {
				// we don't do anything
				log.Warnf("PodGroupCtrl %s, we found hard state pod missing, will leave it there, please ping admins", op.spec)
				return false
			}
			if newPodSpec.IsStateful() && prevNodeName != "" {
				newPodSpec.Filters = append(newPodSpec.Filters, fmt.Sprintf("constraint:node==%s", prevNodeName))
			}
			podCtrl.spec = newPodSpec
			podCtrl.pod.State = RunStatePending
			// when found pod down and redeploy it we just regard it as a drift operation and make driftcount incr
			podCtrl.pod.DriftCount += 1
			op := pgOperDeployInstance{op.instanceNo, op.spec.Version}
			op.Do(pgCtrl, c, store, ev)
			runtime = podCtrl.pod.ImRuntime
			consistent = false
		}
	} else if runtime.State == RunStateExit || runtime.State == RunStateFail {
		if !pgCtrl.engine.config.Maintenance && podCtrl.pod.NeedRestart(op.spec.RestartPolicy) {
			if podCtrl.pod.RestartEnoughTimes() {
				ntfController.Send(NewNotifySpec(podCtrl.spec.Namespace, podCtrl.spec.Name,
					op.instanceNo, container.Runtime.State.FinishedAt, NotifyLetPodGo))
				return false
			}
			log.Warnf("PodGroupCtrl %s, we found pod down, just restart it", op.spec)
			if podCtrl.pod.OOMkilled {
				log.Errorf("pod down with oom:%v", op.spec)
				ntfController.Send(NewNotifySpec(podCtrl.spec.Namespace, podCtrl.spec.Name,
					op.instanceNo, container.Runtime.State.FinishedAt, NotifyPodDownOOM))
			} else {
				ntfController.Send(NewNotifySpec(podCtrl.spec.Namespace, podCtrl.spec.Name,
					op.instanceNo, container.Runtime.State.FinishedAt, NotifyPodDown))
			}

			podCtrl.Start(c)
			runtime = podCtrl.pod.ImRuntime
			if runtime.State == RunStateSuccess {
				pod := podCtrl.pod.Clone()
				pgCtrl.emitChangeEvent("verify", podCtrl.spec, pod, pod.NodeName())
			}
		}
	}
	if !consistent {
		op1 := pgOperSnapshotGroup{true}
		op1.Do(pgCtrl, c, store, ev)
		op2 := pgOperSaveStore{true}
		op2.Do(pgCtrl, c, store, ev)
	}

	return false
}

type pgOperVerifyInstanceCount struct {
	spec PodGroupSpec
}

func (op pgOperVerifyInstanceCount) Do(pgCtrl *podGroupController, c cluster.Cluster, store storage.Store, ev *RuntimeEagleView) bool {
	start := time.Now()
	var rmCount int
	defer func() {
		pgCtrl.RLock()
		log.Infof("%s verify instance count, rmCount=%d , duration=%s", pgCtrl, rmCount, time.Now().Sub(start))
		pgCtrl.RUnlock()
	}()

	for _, podContainer := range pgCtrl.evSnapshot {
		if podContainer.InstanceNo > op.spec.NumInstances {
			cId := podContainer.Container.Id
			log.Warnf("PodGroupCtrl %s we found container %s with iNo=%d, beyond the necessary instance counts %d, will remove it",
				op.spec, cId, podContainer.InstanceNo, op.spec.NumInstances)
			c.StopContainer(cId, op.spec.Pod.GetKillTimeout())
			c.RemoveContainer(cId, true, false)
			rmCount++
		}
	}
	return false
}

type pgOperDeployInstance struct {
	instanceNo int
	version    int
}

/*
 *  Deploy is happend when deployding, scheduling, foundMissing
 *  1. check if current pod is deployd in cluster
 *  2. if deployed, check if corrupted, if corrupted try recover it
 *  3. if not deployed, just deploy it
 *
 */
func (op pgOperDeployInstance) Do(pgCtrl *podGroupController, c cluster.Cluster, store storage.Store, ev *RuntimeEagleView) bool {
	var runtime ImRuntime
	start := time.Now()
	defer func() {
		pgCtrl.RLock()
		log.Infof("%s deploy instance, op=%+v, runtime=%+v, duration=%s", pgCtrl, op, runtime, time.Now().Sub(start))
		pgCtrl.RUnlock()
	}()
	podCtrl := pgCtrl.podCtrls[op.instanceNo-1]
	containerIds := make([]string, len(podCtrl.spec.Containers))
	foundDeployed := false
	for _, podContainer := range pgCtrl.evSnapshot {
		if podContainer.InstanceNo == op.instanceNo && podContainer.Version == op.version {
			cId := podContainer.Container.Id
			cIndex := podContainer.ContainerIndex
			if cIndex >= 0 && cIndex < len(podCtrl.spec.Containers) {
				containerIds[cIndex] = cId
				foundDeployed = true
			}
		}
	}
	if foundDeployed {
		corrupted := false
		if len(podCtrl.pod.Containers) != len(containerIds) {
			podCtrl.pod.Containers = make([]Container, len(containerIds))
			corrupted = true
		}
		for i, container := range podCtrl.pod.Containers {
			if container.Id != containerIds[i] {
				corrupted = true
				podCtrl.pod.Containers[i].Id = containerIds[i]
			}
		}
		if corrupted {
			pgCtrl.RLock()
			log.Warnf("%s we found some corrupted pod runtime instanceNo=%d, try to fix it", pgCtrl, op.instanceNo)
			pgCtrl.RUnlock()
			podCtrl.pod.State = RunStateInconsistent
		} else {
			pgCtrl.RLock()
			log.Infof("%s we found instance deployed, just refresh the current state", pgCtrl)
			pgCtrl.RUnlock()
		}
		podCtrl.Refresh(c)
		runtime = podCtrl.pod.ImRuntime
		if runtime.State == RunStateSuccess {
			// FIXME do we need this?
			pod := podCtrl.pod.Clone()
			pgCtrl.emitChangeEvent("verify", podCtrl.spec, pod, pod.NodeName())
		}
	} else {
		podCtrl.Deploy(c, []string{})
		runtime = podCtrl.pod.ImRuntime
		if runtime.State == RunStateSuccess {
			pod := podCtrl.pod.Clone()
			pgCtrl.emitChangeEvent("add", podCtrl.spec, pod, pod.NodeName())
		}
	}
	return false
}

type pgOperSnapshotGroup struct {
	updateTime bool
}

func (op pgOperSnapshotGroup) Do(pgCtrl *podGroupController, c cluster.Cluster, store storage.Store, ev *RuntimeEagleView) bool {
	pgCtrl.Lock()
	defer pgCtrl.Unlock()
	spec, group := pgCtrl.spec, pgCtrl.group
	start := time.Now()
	defer func() {
		log.Infof("%s snapshot group, op=%+v, runtime=%+v, duration=%s", pgCtrl, op, group.BaseRuntime, time.Now().Sub(start))
	}()

	group.State = RunStateSuccess
	group.Healthst = HealthStateHealthy
	group.LastError = ""
	group.Pods = make([]Pod, spec.NumInstances)
	for i, podCtrl := range pgCtrl.podCtrls {
		group.Pods[i] = podCtrl.pod
		if podCtrl.pod.State != RunStateSuccess {
			group.State = podCtrl.pod.State
			group.LastError = podCtrl.pod.LastError
		}
		if podCtrl.pod.Healthst != HealthStateHealthy {
			group.Healthst = podCtrl.pod.Healthst
		}
	}
	if op.updateTime {
		group.UpdatedAt = time.Now()
	}
	pgCtrl.group = group
	return false
}

type pgOperDriftInstance struct {
	instanceNo int
	fromNode   string
	toNode     string
	force      bool
}

func (op pgOperDriftInstance) Do(pgCtrl *podGroupController, c cluster.Cluster, store storage.Store, ev *RuntimeEagleView) bool {
	isDrifted := false
	var runtime ImRuntime
	start := time.Now()
	defer func() {
		if isDrifted {
			pgCtrl.RLock()
			log.Infof("%s drift instance, op=%+v, runtime=%+v, duration=%s", pgCtrl, op, runtime, time.Now().Sub(start))
			pgCtrl.RUnlock()
		}
	}()
	podCtrl := pgCtrl.podCtrls[op.instanceNo-1]
	oldSpec, oldPod := podCtrl.spec.Clone(), podCtrl.pod
	oldNodeName := oldPod.NodeName()

	isDrifted = podCtrl.Drift(c, op.fromNode, op.toNode, op.force)
	runtime = podCtrl.pod.ImRuntime
	if isDrifted {
		pgCtrl.emitChangeEvent("remove", oldSpec, oldPod, oldNodeName)
		pod := podCtrl.pod.Clone()
		pgCtrl.emitChangeEvent("add", podCtrl.spec, pod, pod.NodeName())
	}
	return false
}

type pgOperRemoveInstance struct {
	instanceNo int
	podSpec    PodSpec
}

func (op pgOperRemoveInstance) Do(pgCtrl *podGroupController, c cluster.Cluster, store storage.Store, ev *RuntimeEagleView) bool {
	start := time.Now()
	defer func() {
		pgCtrl.RLock()
		log.Infof("%s remove instance, instanceNo=%d, duration=%s", pgCtrl, op.instanceNo, time.Now().Sub(start))
		pgCtrl.RUnlock()
	}()
	podCtrl := pgCtrl.podCtrls[op.instanceNo-1]
	nodeName := podCtrl.pod.NodeName()
	podCtrl.Remove(c)
	pgCtrl.emitChangeEvent("remove", podCtrl.spec, podCtrl.pod, nodeName)
	return false
}

type pgOperPurge struct{}

func (op pgOperPurge) Do(pgCtrl *podGroupController, c cluster.Cluster, store storage.Store, ev *RuntimeEagleView) bool {
	start := time.Now()
	defer func() {
		pgCtrl.RLock()
		log.Infof("%s purge, op=%+v, duration=%s", pgCtrl, op, time.Now().Sub(start))
		pgCtrl.RUnlock()
	}()
	for _, podContainer := range pgCtrl.evSnapshot {
		cId := podContainer.Container.Id
		log.Warnf("%s still find some container alive, try to remove it", pgCtrl)
		for i := 0; i < 3; i += 1 {
			if err := c.RemoveContainer(cId, true, false); err == nil || adoc.IsNotFound(err) {
				break
			} else {
				time.Sleep(10 * time.Second)
				if i == 2 {
					log.Warnf("%s still cannot remove the container after max retry, please remove it manually", pgCtrl)
				}
			}
		}
	}
	pgCtrl.Lock()
	defer pgCtrl.Unlock()
	pgCtrl.group.State = RunStateRemoved
	return true // to shutdown the worker routine
}

type pgOperPushPodCtrl struct {
	spec PodSpec
}

func (op pgOperPushPodCtrl) Do(pgCtrl *podGroupController, c cluster.Cluster, store storage.Store, ev *RuntimeEagleView) bool {
	var pod Pod
	pod.InstanceNo = len(pgCtrl.podCtrls) + 1
	pod.State = RunStatePending
	podCtrl := &podController{
		spec: op.spec,
		pod:  pod,
	}
	podCtrl.spec.PrevState = NewPodPrevState(1) // set empty prevstate
	pgCtrl.podCtrls = append(pgCtrl.podCtrls, podCtrl)
	return false
}

type pgOperPopPodCtrl struct{}

func (op pgOperPopPodCtrl) Do(pgCtrl *podGroupController, c cluster.Cluster, store storage.Store, ev *RuntimeEagleView) bool {
	if len(pgCtrl.podCtrls) > 0 {
		pgCtrl.podCtrls = pgCtrl.podCtrls[:len(pgCtrl.podCtrls)-1]
	}
	return false
}

type pgOperLogOperation struct {
	msg string
}

func (op pgOperLogOperation) Do(pgCtrl *podGroupController, c cluster.Cluster, store storage.Store, ev *RuntimeEagleView) bool {
	pgCtrl.RLock()
	defer pgCtrl.RUnlock()
	return false
}

type pgOperSnapshotPrevState struct{}

func (op pgOperSnapshotPrevState) Do(pgCtrl *podGroupController, c cluster.Cluster, store storage.Store, ev *RuntimeEagleView) bool {
	defer func() {
		pgCtrl.RLock()
		log.Infof("%s snapshot prev state, op=%+v", pgCtrl, op)
		pgCtrl.RUnlock()
	}()

	newState := make([]PodPrevState, len(pgCtrl.podCtrls))
	for i, podCtrl := range pgCtrl.podCtrls {
		newState[i] = podCtrl.spec.PrevState.Clone()
	}
	pgCtrl.prevState = newState
	return false
}

type pgOperSendState struct {
	state string
}

func (op pgOperSendState) Do(pgCtrl *podGroupController, c cluster.Cluster, store storage.Store, ev *RuntimeEagleView) bool {
	pgCtrl.Lock()
	pgCtrl.group.LastError = op.state
	pgCtrl.Unlock()

	pgCtrl.opsChan <- pgOperSnapshotGroup{true}
	return false
}

type pgOperChangeState struct {
	op       string
	instance int
}

func (op pgOperChangeState) Do(pgCtrl *podGroupController, c cluster.Cluster, store storage.Store, ev *RuntimeEagleView) bool {
	start := time.Now()
	defer func() {
		pgCtrl.RLock()
		log.Infof("%s change instance state, op=%+v,  duration=%s", pgCtrl, op, time.Now().Sub(start))
		pgCtrl.RUnlock()
	}()
	podCtrl := pgCtrl.podCtrls[op.instance-1]
	switch op.op {
	case "start":
		podCtrl.Start(c)
		if podCtrl.pod.State != RunStateError {
			podCtrl.pod.ChangeTargetState(ExpectStateRun)
		}
	case "stop":
		podCtrl.Stop(c)
		if podCtrl.pod.State != RunStateError {
			podCtrl.pod.ChangeTargetState(ExpectStateStop)
		}
	case "restart":
		pgCtrl.waitLastPodHealth(op.instance)
		podCtrl.Restart(c)
		if podCtrl.pod.State != RunStateError {
			podCtrl.pod.ChangeTargetState(ExpectStateRun)
		}
	}
	log.Infof("podCtrl.pod.State:%v, target_state:%v", podCtrl.pod.State, podCtrl.pod.TargetState)
	return false
}

// Mark podgroup operated over
type pgOperOver struct {
}

func (op pgOperOver) Do(pgCtrl *podGroupController, c cluster.Cluster, store storage.Store, ev *RuntimeEagleView) bool {
	pgCtrl.OperateOver()
	return false
}
