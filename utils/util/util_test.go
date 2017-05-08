package util

import (
	"fmt"
	"testing"
)

func Test_ParseNameInstanceNo(t *testing.T) {
	containerName := "webrouter.worker.worker.v0-i1-d0"
	if name, incetance, err := ParseNameInstanceNo(containerName); err == nil {
		fmt.Printf("name:%v,incetance:%v\n", name, incetance)
	} else {
		fmt.Println("err:%v\n", err)
	}
}
