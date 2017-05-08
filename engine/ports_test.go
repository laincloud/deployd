package engine

import (
	"fmt"
	"strconv"
	"testing"
)

func TestRegisterPorts(t *testing.T) {
	fmt.Println("Start")
	ConfigPostManager("http://127.0.0.1:4001")
	test := make([]*StreamProc, 0)
	for i := 0; i < 2; i++ {
		test = append(test, &StreamProc{
			StreamPort: StreamPort{
				SrcPort: 9001 + i,
				DstPort: 9001 + i,
				Proto:   "tcp",
			},
			NameSpace: "test" + strconv.Itoa(i+1),
			ProcName:  "test" + strconv.Itoa(i+1),
		})
	}
	ok, faileds := RegisterPorts(test...)
	if !ok {
		fmt.Printf("failed with ports:%v\n", faileds)
	}
	CancelPorts(test...)
}
