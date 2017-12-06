package engine

import (
	"fmt"
	"sort"
)

// By is the type of a "less" function that defines the ordering of its Planet arguments.
type By func(c1, c2 *container) bool

// Sort is a method on the function type, By, that sorts the argument slice according to the function.
func (by By) Sort(containers []*container) {
	cs := &containerSorter{
		containers: containers,
		by:         by, // The Sort method's receiver is the function (closure) that defines the sort order.
	}
	sort.Sort(cs)
}

var ByVersionAndDriftCounter = func(c1, c2 *container) bool {
	return c1.version > c2.version ||
		(c1.version == c2.version && c1.driftCount > c2.driftCount)
}

type container struct {
	version    int
	instance   int
	driftCount int
	id         string
}

func (c *container) String() string {
	return fmt.Sprintf("id:%s, version:%d, instance:%d, driftCount:%d", c.id, c.version, c.instance, c.driftCount)
}

type containerSorter struct {
	containers []*container
	by         By
}

// Len is part of sort.Interface.
func (s *containerSorter) Len() int {
	return len(s.containers)
}

// Swap is part of sort.Interface.
func (s *containerSorter) Swap(i, j int) {
	s.containers[i], s.containers[j] = s.containers[j], s.containers[i]
}

// Less is part of sort.Interface.
func (s *containerSorter) Less(i, j int) bool {
	return s.by(s.containers[i], s.containers[j])
}
