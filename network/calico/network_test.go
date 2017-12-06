package calico

import (
	"fmt"
	"testing"

	"github.com/projectcalico/libcalico-go/lib/net"
)

func Test_parseIp(t *testing.T) {
	fmt.Printf("ip: %v", net.ParseIP("127.0.0.1"))
}
