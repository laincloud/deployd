package engine

import (
	"fmt"
	"sync"

	"github.com/laincloud/deployd/storage"
	"github.com/mijia/sweb/log"
)

type ConstraintSpec struct {
	Type  string
	Equal bool
	Value string
	Soft  bool
}

type constraintController struct {
	sync.RWMutex

	constraints map[string]ConstraintSpec
}

func NewConstraintController() *constraintController {
	cc := &constraintController{
		constraints: make(map[string]ConstraintSpec),
	}
	return cc
}

func (cc *constraintController) LoadConstraints(store storage.Store) error {
	constraints := make(map[string]ConstraintSpec)
	cstKey := fmt.Sprintf("%s/%s", kLainDeploydRootKey, kLainConstraintKey)
	if cstNames, err := store.KeysByPrefix(cstKey); err != nil {
		if err != storage.ErrNoSuchKey {
			return err
		}
	} else {
		for _, cstName := range cstNames {
			var cstSpec ConstraintSpec
			if err := store.Get(cstName, &cstSpec); err != nil {
				log.Errorf("Failed to load constraint %s from storage, %s", cstName, err)
				return err
			}
			constraints[cstSpec.Type] = cstSpec
			log.Infof("Loaded constraint %s from storage, %s", cstSpec.Type, cstSpec)
		}
	}
	cc.constraints = constraints
	return nil
}

func (cc *constraintController) LoadFilterFromConstrain(cstSpec ConstraintSpec) string {
	operator := "=="
	if !cstSpec.Equal {
		operator = "!="
	}
	if cstSpec.Soft {
		operator += "~"
	}
	return fmt.Sprintf("constraint:%s%s%s", cstSpec.Type, operator, cstSpec.Value)
}

func (cc *constraintController) GetAllConstraints() map[string]ConstraintSpec {
	cc.RLock()
	defer cc.RUnlock()
	return cc.constraints
}

func (cc *constraintController) GetConstraint(cstType string) (ConstraintSpec, bool) {
	cc.RLock()
	defer cc.RUnlock()
	if cstSpec, ok := cc.constraints[cstType]; !ok {
		return ConstraintSpec{}, false
	} else {
		return cstSpec, true
	}
}

func (cc *constraintController) SetConstraint(cstSpec ConstraintSpec, store storage.Store) error {
	cc.Lock()
	defer cc.Unlock()
	constraintKey := fmt.Sprintf("%s/%s/%s", kLainDeploydRootKey, kLainConstraintKey, cstSpec.Type)
	if err := store.Set(constraintKey, cstSpec); err != nil {
		log.Warnf("Failed to set constraint key %s, %s", constraintKey, err)
		return err
	}
	cc.constraints[cstSpec.Type] = cstSpec
	return nil
}

func (cc *constraintController) RemoveConstraint(cstType string, store storage.Store) error {
	cc.Lock()
	defer cc.Unlock()
	constraintKey := fmt.Sprintf("%s/%s/%s", kLainDeploydRootKey, kLainConstraintKey, cstType)
	if err := store.Remove(constraintKey); err != nil {
		log.Warnf("Failed to remove constraint key %s, %s", constraintKey, err)
		return err
	}
	delete(cc.constraints, cstType)
	return nil
}
