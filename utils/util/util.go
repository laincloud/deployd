package util

import (
	"errors"
	"net"
	"net/url"
	"strconv"

	"github.com/laincloud/deployd/utils/regex"
)

func ParseNameInstanceNo(containerName string) (string, int, error) {
	if p, err := regex.Compile(`(.*)\.v([0-9]+)-i([0-9]+)-d([0-9]+)`); err != nil {
		return "", 0, err
	} else {
		g := p.Match(containerName)
		if g == nil {
			return "", 0, errors.New("Container Match Failed!")
		}
		instance, err := strconv.Atoi(g.Group(3))
		if err != nil {
			return "", 0, err
		}
		return g.Group(1), instance, nil
	}
}

func IsConnectionError(err error) bool {
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
