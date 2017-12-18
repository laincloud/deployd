package network

import (
	"errors"

	"github.com/laincloud/deployd/network/calico"
)

type NWMInterface interface {
	ReleaseIp(ip string) error
}

var (
	ni NWMInterface

	ErrNoNetworkMgrSupported = errors.New("No NetWork Manager Supported ")
)

// networkDriver could calico, other overlay etc.
// fixme: now we just support calico.
func InitNetWorkManager(networkDriver, endpoint string) {
	ni = calico.NewNetWorkMgr(endpoint)
}

func ReleaseIp(ip string) error {
	if ni == nil {
		return ErrNoNetworkMgrSupported
	}
	return ni.ReleaseIp(ip)
}
