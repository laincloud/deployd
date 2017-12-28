package engine

import (
	"fmt"
)

var (
	ErrOperLockedFormat = "Another operation \"%s\" is progressing"
)

type LockedError interface {
	error
}

type OperLockedError struct {
	info string
}

func (ole OperLockedError) Error() string {
	return fmt.Sprintf(ErrOperLockedFormat, ole.info)
}
