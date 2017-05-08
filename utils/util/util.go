package util

import (
	"errors"
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
