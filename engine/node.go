package engine

import (
	"time"

	"github.com/laincloud/deployd/cluster"
	"github.com/mijia/sweb/log"
)

// remove a node should be in such steps show below
// 1. make target node in maintenance with constraint
// 2. fetch all containers in target node
// 3. drift all containers in target node Asynchronously
// (which can make cluster corrupted but eagle will correct it, don't worry!
//     in situation: schedule instance(generally shrink) and drift concurrently)
// 4. stop all process service for lain (generally by lainctl)
// 5. remove maintenance(generally by lainctl or called in add node phase)
func (engine *OrcEngine) RemoveNode(node string) error {
	// step 1
	constraint := ConstraintSpec{"node", false, node, true}
	cstController.SetConstraint(constraint, engine.store)
	// step 2
	pods, err := engine.eagleView.refreshPodsByNode(engine.cluster, []string{node})
	if err != nil {
		log.Warn("refreshPodsByNode err:%v", err)
		return err
	}
	log.Infof("pods %v will be drift", pods)
	// step 3
	for _, pod := range pods {
		engine.DriftNode(node, "", pod.Name, pod.InstanceNo, true)
	}
	return nil
}

// Fetch all containers in target nodes
func (ev *RuntimeEagleView) refreshPodsByNode(c cluster.Cluster, nodes []string) ([]RuntimeEaglePod, error) {
	totalContainers := 0
	start := time.Now()
	defer func() {
		log.Infof("<RuntimeEagleView> pods by node %v refreshed, #containers=%d, duration=%s",
			nodes, totalContainers, time.Now().Sub(start))
	}()
	nodeFilters := make([]string, len(nodes))
	for i, node := range nodes {
		nodeFilters[i] = node
	}
	filters := make(map[string][]string)
	labelFilters := []string{
		"com.docker.swarm.id",
	}
	filters["node"] = nodeFilters
	filters["label"] = labelFilters
	log.Infof("filters: %v", filters)
	pods, err := ev.refreshByFilters(c, filters)
	totalContainers = len(pods)
	return pods, err
}
