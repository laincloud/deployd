package engine

import (
	"encoding/json"

	"github.com/laincloud/deployd/storage"
	"github.com/mijia/sweb/log"
)

type Resource struct {
	Cpu    int    `json:"cpu"`
	Memory string `json:"memory"`
}

type Guard struct {
	Working bool `json:"Working"`
}

const (
	EtcdResourcesKey = "/lain/config/resources"

	EtcdGuardSwitchKey = "/lain/config/guardswitch"
)

var (
	resource = &Resource{Cpu: 8, Memory: "16G"}
	guard    = &Guard{Working: false}
)

func watchGuard(store storage.Store) {
	watcher(store, EtcdGuardSwitchKey, guard)
}

func watchResource(store storage.Store) {
	watcher(store, EtcdResourcesKey, resource)
}

func watcher(store storage.Store, key string, v interface{}) {
	rsCh := store.Watch(key)
	store.Get(key, v)
	go func() {
		for {
			select {
			case rsc := <-rsCh:
				if err := json.Unmarshal([]byte(rsc), v); err == nil {
					log.Infof("got value:%v", v)
				} else {
					log.Warnf("watcher faild with marshall error:%v", err)
				}
				break
			}
		}
	}()
}

func FetchResource() *Resource {
	return resource
}

func FetchGuard() *Guard {
	return guard
}

func GuardGotoSleep(store storage.Store) bool {
	g := &Guard{Working: false}
	if err := store.Set(EtcdGuardSwitchKey, g, true); err != nil {
		return false
	}
	guard = g
	return true
}

func GuardGotoWork(store storage.Store) bool {
	g := &Guard{Working: true}
	if err := store.Set(EtcdGuardSwitchKey, g, true); err != nil {
		return false
	}
	guard = g
	return true
}
