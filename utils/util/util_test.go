package util

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func Test_ParseNameInstanceNo(t *testing.T) {
	containerName := "webrouter.worker.worker.v0-i1-d0"
	if name, incetance, err := ParseNameInstanceNo(containerName); err == nil {
		fmt.Printf("name:%v,incetance:%v\n", name, incetance)
	} else {
		fmt.Println("err:%v\n", err)
	}
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
