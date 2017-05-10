package engine

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
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

func (pm PortsManager) occupiedProcs(spArr ...*StreamProc) []int {
	occs := make([]int, 0)
	for _, sp := range spArr {
		key := fmt.Sprintf(KeyPrefixStreamPorts+"/%d", sp.SrcPort)
		if keyExists(pm.etcd, key) {
			occs = append(occs, sp.SrcPort)
		}
	}
	return occs
}

func (pm PortsManager) occupiedPorts(ports ...int) []int {
	occs := make([]int, 0)
	for _, port := range ports {
		key := fmt.Sprintf(KeyPrefixStreamPorts+"/%d", port)
		if keyExists(pm.etcd, key) {
			occs = append(occs, port)
		}
	}
	return occs
}

func (pm PortsManager) Refresh(pgCtrls map[string]*podGroupController) {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	occs := make([]*StreamProc, 0)
	for _, pgCtrl := range pgCtrls {
		annotation := pgCtrl.spec.Pod.Annotation
		var sps StreamPorts
		if err := json.Unmarshal([]byte(annotation), &sps); err != nil {
			continue
		}
		for _, sp := range sps.Ports {
			occs = append(occs, &StreamProc{
				StreamPort: sp,
				NameSpace:  pgCtrl.spec.Namespace,
				ProcName:   pgCtrl.spec.Name,
			})
		}
	}

	for _, sp := range occs {
		key := fmt.Sprintf(KeyPrefixStreamPorts+"/%d", sp.SrcPort)
		putValue(pm.etcd, key, sp, true)
	}

	markedPorts, err := fetchAll(pm.etcd, KeyPrefixStreamPorts)
	if err != nil {
		return
	}
	portsSets := make(map[int]struct{})
	for _, port := range occs {
		portsSets[port.SrcPort] = struct{}{}
	}
	garbagePorts := make([]int, 0)
	for _, port := range markedPorts {
		if _, ok := portsSets[port]; !ok {
			garbagePorts = append(garbagePorts, port)
		}
	}
	for _, port := range garbagePorts {
		key := fmt.Sprintf(KeyPrefixStreamPorts+"/%d", port)
		delValue(pm.etcd, key)
	}
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
			return false, pm.occupiedProcs(spArr...)
		}
		succeedArr = append(succeedArr, sp)
	}
	return true, nil
}

func (pm PortsManager) UpdateStreamPorts(spArr ...*StreamProc) {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	for _, sp := range spArr {
		pm.UpdateStreamPort(sp)
	}
}

func (pm PortsManager) CancelStreamPorts(spArr ...*StreamProc) {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	for _, sp := range spArr {
		pm.CancelStreamPort(sp)
	}
}

func (pm PortsManager) FetchAllStreamPortsInfo() []StreamProc {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	ports, err := fetchAllInfo(pm.etcd, KeyPrefixStreamPorts)
	if err != nil {
		return nil
	}
	return ports
}

func (pm PortsManager) RegisterStreamPort(sp *StreamProc) bool {
	key := fmt.Sprintf(KeyPrefixStreamPorts+"/%d", sp.SrcPort)
	return putValue(pm.etcd, key, sp, false)
}

func (pm *PortsManager) UpdateStreamPort(sp *StreamProc) bool {
	key := fmt.Sprintf(KeyPrefixStreamPorts+"/%d", sp.SrcPort)
	return putValue(pm.etcd, key, sp, true)
}

func (pm *PortsManager) CancelStreamPort(sp *StreamProc) bool {
	key := fmt.Sprintf(KeyPrefixStreamPorts+"/%d", sp.SrcPort)
	return delValue(pm.etcd, key)
}

func RegisterPorts(sps ...*StreamProc) (bool, []int) {
	return pm.RegisterStreamPorts(sps...)
}

func UpdatePorts(sps ...*StreamProc) {
	pm.UpdateStreamPorts(sps...)
}

func CancelPorts(sps ...*StreamProc) {
	pm.CancelStreamPorts(sps...)
}

func FetchAllPortsInfo() []StreamProc {
	return pm.FetchAllStreamPortsInfo()
}

func OccupiedPorts(ports ...int) []int {
	return pm.occupiedPorts(ports...)
}

func RefreshPorts(pgCtrls map[string]*podGroupController) {
	pm.Refresh(pgCtrls)
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

func fetchAllInfo(e *etcd.Client, key string) ([]StreamProc, error) {
	kapi := etcd.NewKeysAPI(*e)
	resp, err := kapi.Get(context.Background(), key, &etcd.GetOptions{Recursive: true, Quorum: true})
	if err != nil {
		return nil, err
	}
	portsInfo := make([]StreamProc, 0, len(resp.Node.Nodes))

	var sp StreamProc
	for _, node := range resp.Node.Nodes {
		json.Unmarshal([]byte(node.Value), &sp)
		portsInfo = append(portsInfo, sp)
	}
	return portsInfo, nil
}

func fetchAll(e *etcd.Client, key string) ([]int, error) {
	kapi := etcd.NewKeysAPI(*e)
	resp, err := kapi.Get(context.Background(), key, &etcd.GetOptions{Recursive: true, Quorum: true})
	if err != nil {
		return nil, err
	}
	ports := make([]int, 0, len(resp.Node.Nodes))
	for _, node := range resp.Node.Nodes {
		key := node.Key
		infos := strings.Split(key, "/")
		if len(infos) > 0 {
			if port, err := strconv.Atoi(strings.Split(key, "/")[len(infos)-1]); err == nil {
				ports = append(ports, port)
			}
		}
	}
	return ports, nil
}

func putValue(e *etcd.Client, key string, value interface{}, force bool) bool {
	kapi := etcd.NewKeysAPI(*e)
	bytes, err := json.Marshal(value)
	if err != nil {
		return false
	}
	return storeOp(func() (bool, error) {
		var opts *etcd.SetOptions = nil
		if !force {
			opts = &etcd.SetOptions{PrevExist: etcd.PrevNoExist}
		}
		_, err := kapi.Set(
			context.Background(),
			key, string(bytes),
			opts,
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
