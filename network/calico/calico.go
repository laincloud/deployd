package calico

import (
	calico "github.com/projectcalico/libcalico-go/lib/client"

	"github.com/mijia/sweb/log"
	"github.com/projectcalico/libcalico-go/lib/api"
	"github.com/projectcalico/libcalico-go/lib/net"
)

type NetWorkManager struct {
	calico *calico.Client
	ipam   calico.IPAMInterface
}

// FIx me: just support etcd endpoint now
func NewNetWorkMgr(endpoint string) *NetWorkManager {
	config := api.CalicoAPIConfig{
		Spec: api.CalicoAPIConfigSpec{
			DatastoreType: api.EtcdV2,
			EtcdConfig: api.EtcdConfig{
				EtcdEndpoints: endpoint,
			},
		},
	}
	c, err := calico.New(config)
	defer func() {
		if err == nil {
			log.Infof("Init calico network manager succeed")
		}
	}()
	if err != nil {
		log.Warnf("New Calico NetWork Manager Failed!!")
		return nil
	}
	nwm := &NetWorkManager{}
	nwm.calico = c
	nwm.ipam = c.IPAM()
	log.Infof("nwm.ipam: %v", nwm.ipam)
	return nwm
}

func (self *NetWorkManager) ReleaseIp(ip string) error {
	IP := net.ParseIP(ip)
	if IP == nil {
		log.Warnf("Ip %v is invalid to parse", ip)
		return nil
	}
	_, err := self.ipam.ReleaseIPs([]net.IP{*IP})
	return err
}
