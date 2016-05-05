package elector

import (
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	etcdLibkv "github.com/docker/libkv/store/etcd"
	"github.com/mijia/sweb/log"
	"strings"
	"time"
)

const (
	LeaderKey      = "/lain/deployd/leader"
	defaultLockTTL = 20 * time.Second
)

type Elector struct {
	store  store.Store
	key    string
	value  string
	ttl    time.Duration
	leader bool
}

func init() {
	libkv.AddStore(store.ETCD, etcdLibkv.New)
}

func New(etcds []string, key string, value string) (*Elector, error) {
	for i, v := range etcds {
		if parts := strings.SplitN(v, "://", 2); len(parts) == 2 {
			etcds[i] = parts[1]
		}
	}
	st, err := libkv.NewStore(store.ETCD, etcds, nil)
	if err != nil {
		return nil, err
	}
	return &Elector{
		store:  st,
		key:    key,
		value:  value,
		leader: false,
		ttl:    defaultLockTTL,
	}, nil
}

func (e *Elector) Run(stop chan struct{}) chan string {
	stopWatchCh, leaderCh := make(chan struct{}), make(chan string)

	go e.elect(stopWatchCh, stop)
	go e.watch(leaderCh, stopWatchCh)

	return leaderCh
}

func (e *Elector) IsLeader() bool {
	return e.leader
}

func (e *Elector) watch(leaderCh chan string, stop chan struct{}) {
	defer close(leaderCh)

	var (
		current string = ""
		ch      <-chan *store.KVPair
		err     error
		retry   int = 0
	)
	// watch would be failed if leader key not exist
	// sometimes it should wait for elect() to create(lock) leader key
	// try 3 times, waiting for the leader key created
	for {
		ch, err = e.store.Watch(e.key, stop)
		if err != nil {
			if retry >= 3 {
				log.Fatalf("Fail to watch leader key[%s] for 3 times, exit", e.key)
			}
			log.Warnf("Fail to watch leader key[%s], %s, try again",
				e.key, err.Error())
			time.Sleep(time.Second)
			retry += 1
			continue
		}
		retry = 0

		for kv := range ch {
			value := string(kv.Value)
			log.Debugf("Get watch event, leader value changed to %s", value)
			if current != value && value != "" {
				current = value
				leaderCh <- value
			}
		}
		select {
		case <-stop: // real stop
			return
		default:
			log.Warnf("elector's watcher stoped for some unkown reason, retry")
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (e *Elector) elect(stopWatchCh chan struct{}, stop chan struct{}) {
	defer close(stopWatchCh)
	lock, err := e.store.NewLock(e.key, &store.LockOptions{[]byte(e.value), defaultLockTTL, nil})
	if err != nil {
		log.Fatalf("Fail to create distribution locker, %s", err.Error())
	}
	for {
		e.leader = false
		log.Debug("Try to get the lock for becoming a leader")
		// follower will block here waiting for lock
		lostCh, err := lock.Lock(nil)
		if err != nil {
			log.Errorf("Fail to lock %s:%s", e.key, err.Error())
			time.Sleep(time.Second * 3) // sleep for a while to try again
			continue
		}

		log.Debug("Becomming a leader")
		// leader will block here until it stoped or others becoming leader
		e.leader = true
		select {
		case <-stop:
			// stop election
			log.Debug("Get a stop-signal, stop election routine")
			if err := lock.Unlock(); err != nil {
				log.Errorf("Fail to give up the leader identity, %s", err.Error())
			}
			return
		case <-lostCh:
			// lost leader key, try to elect again
		}
	}
}
