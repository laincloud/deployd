package util

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/laincloud/deployd/utils/regex"
)

var (
	ErrContainerMatchFailed = errors.New("Container Match Failed!")
)

func ParseNameInstanceNo(containerName string) (string, int, error) {
	p := regex.MustCompile(`(.*)\.v([0-9]+)-i([0-9]+)-d([0-9]+)`)
	g := p.Match(containerName)
	if g == nil {
		return "", 0, ErrContainerMatchFailed
	}
	instance, err := strconv.Atoi(g.Group(3))
	if err != nil {
		return "", 0, ErrContainerMatchFailed
	}
	return g.Group(1), instance, nil
}

// pgname, version, instance, driftcount
func ParseContainerName(containerName string) (string, int, int, int, error) {
	p := regex.MustCompile(`(.*)\.v([0-9]+)-i([0-9]+)-d([0-9]+)`)
	g := p.Match(containerName)
	if g == nil {
		return "", 0, 0, 0, ErrContainerMatchFailed
	}
	version, err := strconv.Atoi(g.Group(2))
	if err != nil {
		return "", 0, 0, 0, ErrContainerMatchFailed
	}
	instance, err := strconv.Atoi(g.Group(3))
	if err != nil {
		return "", 0, 0, 0, ErrContainerMatchFailed
	}
	driftCount, err := strconv.Atoi(g.Group(4))
	if err != nil {
		return "", 0, 0, 0, ErrContainerMatchFailed
	}
	return g.Group(1), version, instance, driftCount, nil
}

func IpConflictErrorMatch(err string) string {
	p := regex.MustCompile(`IP assignment error, data: {IP:([0-9.]+) HandleID:(.*)}: Address already assigned in block`)
	g := p.Match(err)
	if g == nil {
		return ""
	}
	return g.Group(1)
}

func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}
	p := regex.MustCompile(`getsockopt: connection refused`)
	g := p.Match(err.Error())
	if g != nil {
		return true
	}
	switch err := err.(type) {
	case net.Error:
		return err.Timeout()
	case *url.Error:
		if err, ok := err.Err.(net.Error); ok {
			return err.Timeout()
		}
	}
	return false
}

func AddNodeConstraint(filters []string, nodeName string) []string {
	newFilters := make([]string, 0, len(filters))
	for _, filter := range filters {
		if strings.HasPrefix(filter, "constraint:node==") {
			continue
		}
		newFilters = append(newFilters, filter)
	}
	newFilters = append(newFilters, fmt.Sprintf("constraint:node==%s", nodeName))
	return newFilters
}
