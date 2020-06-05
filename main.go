package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	barmanCheckExitCode = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "barman_check_exit_code",
			Help: "barman check command exit code result.",
		},
		[]string{"backup"},
	)

	sudoExecPath, barmanExecPath, barmanUserName, barmanConfigDir string

	mux sync.Mutex
)

// periodic task for metrics values update
func barmanCheck() {
	go func() {
		for {
			mux.Lock()

			barmanUpdateMetrics()

			mux.Unlock()

			time.Sleep(30 * time.Second)
		}
	}()
}

func barmanReset(event fsnotify.Event) {
	log.Println(event.Op)

	if event.Op <= 4 {
		log.Println("reset metrics started")

		mux.Lock()

		// reset metrics if files changed
		barmanCheckExitCode.Reset()

		// and then update metrics
		barmanUpdateMetrics()

		mux.Unlock()

		log.Println("reset metrics completed")
	}
}

// check config directory changes
// and reset metrics if files removed or added
func barmanConfigDirectoryCheck() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	done := make(chan struct{})
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				log.Println("fsnotify event:", event)
				barmanReset(event)
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("fsnotify error:", err)
			}
		}
	}()

	err = watcher.Add(barmanConfigDir)
	if err != nil {
		log.Fatal(err)
	}

	<-done
}

// create metrics or override them's values
func barmanUpdateMetrics() {
	log.Printf(
		"execute: %s --user=%s %s list-server --minimal\n",
		sudoExecPath,
		barmanUserName,
		barmanExecPath,
	)

	cmd := exec.Command(
		sudoExecPath,
		fmt.Sprintf("--user=%s", barmanUserName),
		barmanExecPath,
		"list-server", "--minimal")

	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("giving databases list failed: %s\n", err)
	}

	dbs := strings.Split(string(out), "\n")
	dbs = dbs[:len(dbs)-1]

	log.Printf("prepared backups list: %v\n", dbs)

	for _, db := range dbs {
		cmd := exec.Command(
			sudoExecPath,
			fmt.Sprintf("--user=%s", barmanUserName),
			barmanExecPath,
			"check", db)

		err := cmd.Run()

		if err != nil {
			log.Printf("check failed: %s\n", err)
			barmanCheckExitCode.WithLabelValues(db).Set(1)
		} else {
			barmanCheckExitCode.WithLabelValues(db).Set(0)
		}
	}

	log.Printf("check completed")
}

// initialize
func init() {
	// override sudo executable path
	sudoExecPath = os.Getenv("SUDO_BINARY_PATH")
	if sudoExecPath == "" {
		sudoExecPath = "/usr/bin/sudo"
	}

	// override barman executable path
	barmanExecPath = os.Getenv("BARMAN_BINARY_PATH")
	if barmanExecPath == "" {
		barmanExecPath = "/usr/bin/barman"
	}

	// override barman user name
	barmanUserName = os.Getenv("BARMAN_USER_NAME")
	if barmanUserName == "" {
		barmanUserName = "barman"
	}

	barmanConfigDir = "/etc/barman.d"

	prometheus.MustRegister(barmanCheckExitCode)
}

func main() {
	go barmanConfigDirectoryCheck()
	barmanCheck()

	http.Handle("/metrics", promhttp.Handler())
	log.Println("exporter started")
	err := http.ListenAndServe(":9706", nil)
	if err != nil {
		log.Printf("http listener failed with error: %s\n", err)
	}
}
