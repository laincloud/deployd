package etcd

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"

	"github.com/coreos/etcd/client"
	"github.com/laincloud/deployd/storage"
	"golang.org/x/net/context"
)

type EtcdStore struct {
	keysApi client.KeysAPI
	ctx     context.Context

	sync.RWMutex
	keyHashes map[string]uint64
}

func (store *EtcdStore) Get(key string, v interface{}) error {
	if resp, err := store.keysApi.Get(store.ctx, key, &client.GetOptions{Quorum: true}); err != nil {
		if cerr, ok := err.(client.Error); ok && cerr.Code == client.ErrorCodeKeyNotFound {
			return storage.ErrNoSuchKey
		}
		return err
	} else {
		if resp.Node == nil {
			return fmt.Errorf("Etcd Store returns a nil node")
		}
		if resp.Node.Dir {
			return fmt.Errorf("Etcd Store returns this is a directory node")
		}
		value := resp.Node.Value
		if err := json.Unmarshal([]byte(value), v); err != nil {
			return err
		}
	}
	return nil
}

func (store *EtcdStore) KeysByPrefix(prefix string) ([]string, error) {
	// Prefix should corresponding to a directory name, and will return all the nodes inside the directory
	keys := make([]string, 0)
	if resp, err := store.keysApi.Get(store.ctx, prefix, &client.GetOptions{Quorum: true}); err != nil {
		if cerr, ok := err.(client.Error); ok && cerr.Code == client.ErrorCodeKeyNotFound {
			return keys, storage.ErrNoSuchKey
		}
		return keys, err
	} else {
		if resp.Node == nil {
			return keys, fmt.Errorf("Etcd store returns a nil node")
		}
		if !resp.Node.Dir {
			return keys, fmt.Errorf("Etcd store returns a non-directory node")
		}
		for _, node := range resp.Node.Nodes {
			if node != nil {
				keys = append(keys, node.Key)
			}
		}
	}
	return keys, nil
}

func (store *EtcdStore) Set(key string, v interface{}, force ...bool) error {
	if data, err := json.Marshal(v); err != nil {
		return err
	} else {
		h := fnv.New64a()
		h.Write(data)
		dataHash := h.Sum64()
		forceSave := false
		if len(force) > 0 {
			forceSave = force[0]
		}

		store.Lock()
		defer store.Unlock()
		if !forceSave {
			if lastHash, ok := store.keyHashes[key]; ok && lastHash == dataHash {
				return nil
			}
		}
		_, err := store.keysApi.Set(store.ctx, key, string(data), nil)
		if err == nil {
			store.keyHashes[key] = dataHash
		}
		return err
	}
}

func (store *EtcdStore) Remove(key string) error {
	_, err := store.keysApi.Delete(store.ctx, key, nil)
	if err != nil {
		store.Lock()
		delete(store.keyHashes, key)
		store.Unlock()
	}
	return err
}

func (store *EtcdStore) RemoveDir(key string) error {
	err := store.deleteDir(key, true)
	return err
}

func (store *EtcdStore) TryRemoveDir(key string) {
	store.deleteDir(key, false)
}

func (store *EtcdStore) deleteDir(key string, recursive bool) error {
	opts := client.DeleteOptions{
		Recursive: recursive,
		Dir:       true,
	}
	_, err := store.keysApi.Delete(store.ctx, key, &opts)
	return err
}

func NewStore(addr string, isDebug bool) (storage.Store, error) {
	c, err := client.New(client.Config{
		Endpoints: strings.Split(addr, ","),
	})
	if err != nil {
		return nil, err
	}
	if false && isDebug {
		client.EnablecURLDebug()
	}
	s := &EtcdStore{
		keysApi:   client.NewKeysAPI(c),
		ctx:       context.Background(),
		keyHashes: make(map[string]uint64),
	}
	return s, nil
}
