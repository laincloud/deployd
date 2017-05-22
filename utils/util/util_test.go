package util

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_ParseNameInstanceNo(t *testing.T) {
	containerName := "webrouter.worker.worker.v0-i1-d0"

	name, incetance, err := ParseNameInstanceNo(containerName)
	assert.Equal(t, nil, err)
	assert.Equal(t, "webrouter.worker.worker", name)
	assert.Equal(t, 1, incetance)
}

func Test_IsConnectionError(t *testing.T) {
	err := errors.New("dial tcp 192.168.77.21:2376: getsockopt: connection refused")
	assert.Equal(t, true, IsConnectionError(err))
}

func Test_deepEqual(t *testing.T) {
	a := []string{"a", "b"}
	b := []string{"a", "b"}
	fmt.Println(reflect.DeepEqual(a, b))
}

func Test_timeFormat(t *testing.T) {
	fmt.Println(time.Now().Format("Jan  2 15:04:05"))
	// Jan  1 00:00:00
	// time.Now().Format("2006-01-02 15:04:05")
}
