package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/spf13/pflag"

	"github.com/fsnotify/fsnotify"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	flagUsageMessage = `Supported env variables:
	SUDO_BINARY_PATH   - path to sudo binary (default: /usr/bin/sudo)
	BARMAN_BINARY_PATH - path to barman binary path (default: /usr/bin/barman)
	BARMAN_USER_NAME   - username, which used for barman execution (default: barman)
	BARMAN_CONFIG_DIR  - path to dir with user defined config files (default: /etc/barman.d)

Allowed flags:`
)

var (
	sudoExecPath, barmanExecPath, barmanUserName, barmanConfigDir string

	mux sync.Mutex

	version       string
	commitID      string
	checkExitCode *prometheus.GaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "barman_check_exit_code",
			Help: "barman check command exit code result.",
		},
		[]string{"backup"},
	)

	flagSet            *pflag.FlagSet = pflag.NewFlagSet("barman_exporter", pflag.ExitOnError)
	flagVersion        bool
	flagParallelCheck  bool
	flagScrapeInterval int
)

// periodic task for metrics values update
func periodicCheck() {
	for {
		// mux.Lock()

		barmanUpdateMetrics(0)

		// mux.Unlock()

		time.Sleep(time.Duration(flagScrapeInterval) * time.Second)
	}
}

func resetMetrics(event fsnotify.Event) {
	log.Println(event.Op)

	if event.Op <= 4 {
		timestamp := time.Now().Unix()

		log.Printf("[%d] reset metrics started", timestamp)

		mux.Lock()

		// reset metrics if files changed
		checkExitCode.Reset()

		mux.Unlock()

		// and then update metrics
		barmanUpdateMetrics(timestamp)

		log.Printf("[%d] reset metrics completed", timestamp)
	}
}

// check config directory changes
// and reset metrics if files removed or added
func configDirectoryCheck() {
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
				resetMetrics(event)
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

// write new metrics values
func writeMetircValue(timestamp int64, db string) {
	log.Printf("[%d] get metric for db: %s", timestamp, db)

	cmd := exec.Command(
		sudoExecPath,
		fmt.Sprintf("--user=%s", barmanUserName),
		barmanExecPath,
		"check", db)

	err := cmd.Run()

	log.Printf("[%d] got metric for db: %s", timestamp, db)

	mux.Lock()

	if err != nil {
		log.Printf("[%d] check failed: %s\n", timestamp, err)
		checkExitCode.WithLabelValues(db).Set(1)
	} else {
		checkExitCode.WithLabelValues(db).Set(0)
	}

	mux.Unlock()
}

// create metrics or override them's values
func barmanUpdateMetrics(timestamp int64) {
	if timestamp == 0 {
		timestamp = time.Now().Unix()
	}

	log.Printf(
		"[%d] execute: %s --user=%s %s list-server --minimal\n",
		timestamp,
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
		log.Printf("[%d] giving databases list failed: %s\n", timestamp, err)
	}

	dbs := strings.Split(string(out), "\n")
	dbs = dbs[:len(dbs)-1]

	log.Printf("[%d] prepared backups list: %v\n", timestamp, dbs)

	for _, db := range dbs {
		if flagParallelCheck {
			go writeMetircValue(timestamp, db)
		} else {
			writeMetircValue(timestamp, db)
		}
	}

	log.Printf("[%d] check completed", timestamp)
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

	// override /etc/barman.d
	barmanConfigDir = os.Getenv("BARMAN_CONFIG_DIR")
	if barmanConfigDir == "" {
		barmanConfigDir = "/etc/barman.d"
	}

	prometheus.MustRegister(checkExitCode)
}

// func flagUsage(f *pflag.FlagSet) {
func flagUsage() {
	fmt.Println(flagUsageMessage)
	flagSet.PrintDefaults()
}

func main() {
	flagSet.Usage = flagUsage
	flagSet.BoolVar(&flagVersion, "version", false, "show current version")
	flagSet.BoolVar(&flagParallelCheck, "parallel-check", false, "check different databases in parallel")
	flagSet.IntVar(&flagScrapeInterval, "scrape-interval", 60, "exporter metrics update interval")
	flagSet.Parse(os.Args[1:])

	if flagVersion {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		if version != "" {
			fmt.Fprintf(w, "version:\t%s\n", version)
		}
		fmt.Fprintf(w, "git commit:\t%s\n", commitID)
		w.Flush()
		os.Exit(0)
	}

	go configDirectoryCheck()
	go periodicCheck()

	http.Handle("/metrics", promhttp.Handler())
	log.Println("exporter started")
	err := http.ListenAndServe(":9706", nil)
	if err != nil {
		log.Printf("http listener failed with error: %s\n", err)
	}
}
