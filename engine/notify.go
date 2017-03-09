package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/laincloud/deployd/storage"
	"github.com/mijia/sweb/log"
)

var (
	NotifyPodMissing = "LAIN found pod missing, ready to redeployd it"
	NotifyPodDown    = "LAIN found pod down, ready to restart it"
	NotifyLetPodGo   = "LAIN found pod restart too many times in a short period, will let it go"
	NotifyPodIPLost  = "LAIN found pod lost IP, please inform the SA team"
)

type notifyController struct {
	sync.RWMutex

	callbacks map[string]string

	callbackChan chan NotifySpec
}

type NotifySpec struct {
	Level      string
	Namespace  string
	PodName    string
	InstanceNo int
	Timestamp  time.Time
	Message    string
}

func NewNotifySpec(namespace string, podName string, instanceNo int, message string) NotifySpec {
	notifySpec := NotifySpec{
		Level:      "Error",
		Namespace:  namespace,
		PodName:    podName,
		InstanceNo: instanceNo,
		Timestamp:  time.Now(),
		Message:    message,
	}
	return notifySpec
}

func NewNotifyController(stop chan struct{}) *notifyController {
	nc := &notifyController{
		callbacks:    make(map[string]string),
		callbackChan: make(chan NotifySpec, 500),
	}
	nc.Activate(stop)
	return nc
}

func (nc *notifyController) LoadNotifies(store storage.Store) error {
	nc.Lock()
	defer nc.Unlock()
	callbacks := []string{}
	notifyKey := fmt.Sprintf("%s/%s", kLainDeploydRootKey, kLainNotifyKey)
	if err := store.Get(notifyKey, &callbacks); err != nil {
		if err != storage.ErrNoSuchKey {
			log.Errorf("Failed to load nofities from storage, %s", err)
			return err
		}
	}
	for i := 0; i < len(callbacks); i++ {
		nc.callbacks[callbacks[i]] = ""
	}
	return nil
}

func (nc *notifyController) GetAllNotifies() map[string]string {
	nc.RLock()
	defer nc.RUnlock()
	return nc.callbacks
}

func (nc *notifyController) AddNotify(callback string, store storage.Store) error {
	nc.Lock()
	defer nc.Unlock()
	notifyKey := fmt.Sprintf("%s/%s", kLainDeploydRootKey, kLainNotifyKey)
	notifyMap := make(map[string]string)
	for k, v := range nc.callbacks {
		notifyMap[k] = v
	}
	notifyMap[callback] = ""
	log.Infof("ready to set Notify val %s", notifyMap)
	if err := store.Set(notifyKey, nc.CallbackList(notifyMap)); err != nil {
		log.Warnf("Failed to set Notify val %s, %s", callback, err)
		return err
	} else {
		log.Infof("Success set Notify val %s", callback)
	}
	nc.callbacks[callback] = ""
	return nil
}

func (nc *notifyController) RemoveNotify(callback string, store storage.Store) error {
	nc.Lock()
	defer nc.Unlock()
	NotifyKey := fmt.Sprintf("%s/%s", kLainDeploydRootKey, kLainNotifyKey)
	notifyMap := make(map[string]string)
	for k, v := range nc.callbacks {
		notifyMap[k] = v
	}
	delete(notifyMap, callback)
	if err := store.Set(NotifyKey, nc.CallbackList(notifyMap)); err != nil {
		log.Warnf("Failed to remove Notify %s, %s", callback, err)
		return err
	} else {
		log.Infof("Success remove Notify %s", callback)
	}
	delete(nc.callbacks, callback)
	return nil
}

func (nc *notifyController) CallbackList(callbackMap map[string]string) []string {
	notifyValue := []string{}
	for callback, _ := range callbackMap {
		notifyValue = append(notifyValue, callback)
	}
	return notifyValue
}

func (nc *notifyController) Send(notifySpec NotifySpec) {
	log.Infof("Receiving nofity request: %s", notifySpec)
	nc.callbackChan <- notifySpec
}

func (nc *notifyController) Notify(notifySpec NotifySpec) {
	nc.Lock()
	defer nc.Unlock()
	log.Infof("Ready sending notify request: %s", notifySpec)
	callbackList := nc.CallbackList(nc.callbacks)
	for i := 0; i < len(callbackList); i++ {
		uri := callbackList[i]
		if err := nc.Callback(uri, notifySpec); err != nil {
			log.Errorf("Fail notify spec %s to %s: %s", notifySpec, uri, err)
		}
	}
}

func (nc *notifyController) Callback(uri string, notifySpec NotifySpec) error {
	if body, err := json.Marshal(notifySpec); err != nil {
		return err
	} else {
		req, err := http.NewRequest("POST", uri, bytes.NewBuffer(body))
		if err != nil {
			return err
		}
		req.Header.Add("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode >= 300 {
			log.Infof("Error response from %s: status %s", resp.StatusCode)
			var errMsg []byte
			var cbErr error
			defer resp.Body.Close()
			if errMsg, cbErr = ioutil.ReadAll(resp.Body); cbErr != nil {
				return cbErr
			}
			return errors.New(strings.TrimSpace(string(errMsg)))
		}
		return nil
	}
}

func (nc *notifyController) Activate(stop chan struct{}) {
	log.Infof("Ready listen notify request...")
	go func() {
		for {
			select {
			case notifySpec := <-nc.callbackChan:
				nc.Notify(notifySpec)
			case <-stop:
				if len(nc.callbackChan) == 0 {
					log.Infof("Stop listen notify request...")
					return
				}
			}
		}
	}()
}
