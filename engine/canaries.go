package engine

import (
	"strings"

	"github.com/laincloud/deployd/storage"
	"github.com/mijia/sweb/log"
)

type Strategy struct {
	DivType  string        `json:"Type"`
	DivDatas []interface{} `json:"Rule"`
}

type Canary struct {
	Strategies []*Strategy
}

func (canary *Canary) Equal(nCanary *Canary) bool {
	if len(canary.Strategies) != len(nCanary.Strategies) {
		return false
	}
	for i, strategy := range canary.Strategies {
		if strategy.DivType != nCanary.Strategies[i].DivType {
			return false
		}
	}
	return true
}

type CanaryPodsWithSpec struct {
	PodGroupSpec
	Canary *Canary
}

func (spec *CanaryPodsWithSpec) SaveCanary(store storage.Store) error {
	key := strings.Join([]string{kLainDeploydRootKey, kLainCanaryKey, spec.Namespace, spec.Name}, "/")
	if err := store.Set(key, spec.Canary, true); err != nil {
		log.Warnf("[Store] Failed to save pod group canary info %s, %s", key, err)
		return err
	}
	return nil
}

func (spec *PodGroupSpec) RemoveCanary(store storage.Store) error {
	key := strings.Join([]string{kLainDeploydRootKey, kLainCanaryKey, spec.Namespace, spec.Name}, "/")
	if err := store.Remove(key); err != nil {
		log.Warnf("[Store] Failed to remove pod group canary info %s, %s", key, err)
		return err
	}
	return nil
}

func (spec *PodGroupSpec) FetchCanary(store storage.Store) (*Canary, error) {
	key := strings.Join([]string{kLainDeploydRootKey, kLainCanaryKey, spec.Namespace, spec.Name}, "/")
	var canary Canary
	if err := store.Get(key, &canary); err != nil {
		log.Warnf("[Store] Failed to fetch pod group canary info %s, %s", key, err)
		return nil, err
	}
	return &canary, nil
}

func (spec *PodGroupSpec) UpdateCanary(store storage.Store, canary *Canary) error {
	key := strings.Join([]string{kLainDeploydRootKey, kLainCanaryKey, spec.Namespace, spec.Name}, "/")
	if err := store.Set(key, canary, true); err != nil {
		log.Warnf("[Store] Failed to update pod group canary info %s, %s", key, err)
		return err
	}
	return nil
}
