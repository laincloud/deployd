package engine

import (
	"encoding/json"
	"fmt"
	"sync"

	etcd "github.com/coreos/etcd/client"
	"github.com/mijia/sweb/log"
	"golang.org/x/net/context"
)

const (
	KeyPrefixStreamPorts = "/lain/deployd/stream/ports"
)

var (
	pm *PortsManager
)

func ConfigPostManager(endpoint string) {
	pm = NewPortsManager(endpoint)
}

type StreamPort struct {
	SrcPort int    `json:"srcport"`
	DstPort int    `json:"dstport"`
	Proto   string `json:"proto"`
}

func (sp StreamPort) Equals(osp StreamPort) bool {
	return sp.SrcPort == osp.SrcPort &&
		sp.DstPort == osp.DstPort &&
		sp.Proto == osp.Proto
}

type StreamPorts struct {
	Ports []StreamPort `json:"ports"`
}

func (sp StreamPorts) Equals(osp StreamPorts) bool {
	if len(sp.Ports) != len(osp.Ports) {
		return false
	}
	for i, _ := range sp.Ports {
		if !sp.Ports[i].Equals(osp.Ports[i]) {
			return false
		}
	}
	return true
}

type StreamProc struct {
	StreamPort
	NameSpace string
	ProcName  string
}

type PortsManager struct {
	etcd *etcd.Client
	lock *sync.Mutex
}

func NewPortsManager(endpoint string) *PortsManager {
	cfg := etcd.Config{
		Endpoints: []string{endpoint},
		Transport: etcd.DefaultTransport,
	}
	c, err := etcd.New(cfg)
	if err != nil {
		log.Errorf("NewPortsManager with error:%v\n", err)
	}
	return &PortsManager{
		etcd: &c,
		lock: &sync.Mutex{},
	}
}

func (pm PortsManager) occupiedPorts(spArr ...*StreamProc) []int {
	occs := make([]int, 0)
	for _, sp := range spArr {
		key := fmt.Sprintf(KeyPrefixStreamPorts+"/%d", sp.SrcPort)
		if keyExists(pm.etcd, key) {
			occs = append(occs, sp.SrcPort)
		}
	}
	return occs
}

func (pm PortsManager) RegisterStreamPorts(spArr ...*StreamProc) (bool, []int) {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	succeedArr := make([]*StreamProc, 0, len(spArr))
	for _, sp := range spArr {
		if !pm.RegisterStreamPort(sp) {
			for _, succeed := range succeedArr {
				pm.CancelStreamPort(succeed)
			}
			return false, pm.occupiedPorts(spArr...)
		}
		succeedArr = append(succeedArr, sp)
	}
	return true, nil
}

func (pm PortsManager) CancelStreamPorts(spArr ...*StreamProc) {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	for _, sp := range spArr {
		pm.CancelStreamPort(sp)
	}
}

func (pm PortsManager) RegisterStreamPort(sp *StreamProc) bool {
	key := fmt.Sprintf(KeyPrefixStreamPorts+"/%d", sp.SrcPort)
	return putValue(pm.etcd, key, sp)
}

func (pm *PortsManager) CancelStreamPort(sp *StreamProc) bool {
	key := fmt.Sprintf(KeyPrefixStreamPorts+"/%d", sp.SrcPort)
	return delValue(pm.etcd, key)
}

func RegisterPorts(sps ...*StreamProc) (bool, []int) {
	return pm.RegisterStreamPorts(sps...)
}

func CancelPorts(sps ...*StreamProc) {
	pm.CancelStreamPorts(sps...)
}

func keyExists(e *etcd.Client, key string) bool {
	kapi := etcd.NewKeysAPI(*e)
	return storeOp(func() (bool, error) {
		resp, err := kapi.Get(context.Background(), key, &etcd.GetOptions{Quorum: true})
		if err != nil {
			if etcdErr, ok := err.(etcd.Error); ok {
				switch etcdErr.Code {
				case etcd.ErrorCodeKeyNotFound:
					return false, nil
				default:
					return false, err
				}
			}
			return false, err
		}
		if resp == nil || resp.Node == nil || resp.Node.Value == "" {
			return false, nil
		}
		return true, nil
	})
}

func putValue(e *etcd.Client, key string, value interface{}) bool {
	kapi := etcd.NewKeysAPI(*e)
	bytes, err := json.Marshal(value)
	if err != nil {
		return false
	}
	return storeOp(func() (bool, error) {
		_, err := kapi.Set(
			context.Background(),
			key, string(bytes),
			&etcd.SetOptions{PrevExist: etcd.PrevNoExist},
		)
		if err != nil {
			if etcdErr, ok := err.(etcd.Error); ok {
				switch etcdErr.Code {
				case etcd.ErrorCodeNodeExist:
					return false, nil
				default:
					log.Errorf("set key %v failed with error:%v", key, err)
				}
			}
			return false, err
		}
		return true, nil
	})
}

func delValue(e *etcd.Client, key string) bool {
	kapi := etcd.NewKeysAPI(*e)
	return storeOp(func() (bool, error) {
		_, err := kapi.Delete(context.Background(), key, nil)
		if err != nil {
			return false, err
		}
		return true, nil
	})
}

func storeOp(op func() (bool, error)) bool {
	succ, err := op()
	if err != nil {
		log.Errorf("store op err:%v", err)
		return false
	} else {
		return succ
	}
}
