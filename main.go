package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

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
)

func barmanCheck() {
	// override sudo executable path
	sudoExecPath := os.Getenv("SUDO_BINARY_PATH")
	if sudoExecPath == "" {
		sudoExecPath = "/usr/bin/sudo"
	}

	// override barman executable path
	barmanExecPath := os.Getenv("BARMAN_BINARY_PATH")
	if barmanExecPath == "" {
		barmanExecPath = "/usr/bin/barman"
	}

	// override barman user name
	barmanUserName := os.Getenv("BARMAN_USER_NAME")
	if barmanUserName == "" {
		barmanUserName = "barman"
	}

	go func() {
		for {
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

			time.Sleep(30 * time.Second)
		}
	}()
}

func init() {
	prometheus.MustRegister(barmanCheckExitCode)
}

func main() {
	barmanCheck()

	http.Handle("/metrics", promhttp.Handler())
	log.Println("exporter started")
	http.ListenAndServe(":9706", nil)
}
