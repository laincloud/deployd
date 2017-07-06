package swarm

import (
	"fmt"
	"time"

	"github.com/laincloud/deployd/cluster"
	"github.com/mijia/adoc"
)

type SwarmCluster struct {
	*adoc.DockerClient
}

func (c *SwarmCluster) GetResources() ([]cluster.Node, error) {
	if info, err := c.DockerClient.SwarmInfo(); err != nil {
		return nil, err
	} else {
		nodes := make([]cluster.Node, len(info.Nodes))
		for i, node := range info.Nodes {
			nodes[i] = cluster.Node{
				Name:       node.Name,
				Address:    node.Address,
				Containers: node.Containers,
				CPUs:       node.CPUs,
				UsedCPUs:   node.UsedCPUs,
				Memory:     node.Memory,
				UsedMemory: node.UsedMemory,
			}
		}
		return nodes, nil
	}
}

func NewCluster(addr string, timeout, rwTimeout time.Duration) (cluster.Cluster, error) {
	docker, err := adoc.NewSwarmClientTimeout(addr, nil, timeout, rwTimeout)
	if err != nil {
		return nil, fmt.Errorf("Cannot connect swarm master[%s], %s", addr, err)
	}
	swarm := &SwarmCluster{}
	swarm.DockerClient = docker
	return swarm, nil
}
