package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/laincloud/deployd/apiserver"
	"github.com/laincloud/deployd/engine"
	"github.com/laincloud/deployd/utils/elector"
	"github.com/laincloud/deployd/utils/proxy"
	"github.com/mijia/sweb/log"
)

const (
	VERSION = "2.1.2"
)

func main() {
	var webAddr, swarmAddr, etcdAddr, advertise string
	var isDebug, version bool
	var refreshInterval, dependsGCTime, maxRestartTimes, restartInfoClearInterval int

	flag.StringVar(&advertise, "advertise", "", "The address advertise to other peers, this will open HA mode")
	flag.StringVar(&webAddr, "web", ":9000", "The address which lain-deployd is listenning on")
	flag.StringVar(&swarmAddr, "swarm", "", "The tcp://<SWRAM_IP>:<SWARM_PORT> address that Swarm master is deployed")
	flag.StringVar(&etcdAddr, "etcd", "", "The etcd cluster access points, e.g. http://127.0.0.1:4001")
	flag.IntVar(&dependsGCTime, "dependsGCTime", 5, "The depends garbage collection time (minutes)")
	flag.IntVar(&refreshInterval, "refreshInterval", 90, "The refresh interval time (seconds)")
	flag.IntVar(&maxRestartTimes, "maxRestartTimes", 3, "The max restart times for pod")
	flag.IntVar(&restartInfoClearInterval, "restartInfoClearInterval", 30, "The interval to clear restart info (minutes)")
	flag.BoolVar(&isDebug, "debug", false, "Debug mode switch")
	flag.BoolVar(&version, "v", false, "Show version")
	flag.Parse()

	if version {
		println("deployd", VERSION)
		return
	}

	usage(swarmAddr != "", "Please provide the swarm master address!")
	usage(etcdAddr != "", "Please provide the etcd access points address!")

	if isDebug {
		log.EnableDebug()
	}

	engine.DependsGarbageCollectTimeout = time.Duration(dependsGCTime) * time.Minute
	engine.RefreshInterval = refreshInterval
	engine.RestartMaxCount = maxRestartTimes
	engine.RestartInfoClearInterval = time.Duration(restartInfoClearInterval) * time.Minute

	server := apiserver.New(swarmAddr, etcdAddr, isDebug)

	engine.ConfigPostManager(etcdAddr)

	if advertise == "" {
		// no advertise, running without election
		go server.ListenAndServe(webAddr)
	} else {
		// running with election, make deploy service HA
		elec, err := elector.New(strings.Split(etcdAddr, ","), elector.LeaderKey, advertise)
		if err != nil {
			log.Fatal(err.Error())
		}
		stop := make(chan struct{})
		defer close(stop)
		leaderCh := elec.Run(stop)

		p := proxy.New(webAddr, "")

		// run api server
		go func() {
			for leader := range leaderCh {
				if leader == advertise {
					log.Infof("Becomming a leader, shutdown proxy server")
					p.Shutdown() // stop proxy
					log.Infof("Starting api server")
					go server.ListenAndServe(webAddr) // start api server
				} else {
					log.Infof("Becomming a follower, shutdown api server")
					server.Shutdown() // stop api server
					log.Infof("Starting proxy server")
					p.SetDest(leader)
					go p.Run() // start proxy
				}
			}
		}()
	}

	waitSignal()
}

func waitSignal() {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	// do some ending job
	log.Infof("Get signal %s, exit.", <-ch)
}

func usage(condition bool, msg string) {
	if !condition {
		fmt.Println("lain-deployd:")
		fmt.Println("    " + msg)
		flag.Usage()
		os.Exit(1)
	}
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}
