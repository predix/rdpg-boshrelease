package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/starkandwayne/rdpgd/admin"
	"github.com/starkandwayne/rdpgd/cfsb"
	"github.com/starkandwayne/rdpgd/globals"
	"github.com/starkandwayne/rdpgd/gpb"
	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/rdpg"
	"github.com/starkandwayne/rdpgd/tasks"
)

//var - entry point for configuring a cluster
var (
	VERSION string
	pidFile string
)

func init() {
	pidFile = os.Getenv("RDPGD_PIDFILE")
}

func main() {
	go writePidFile()

	parseArgs()

	switch globals.ServiceRole {
	case "manager":
		manager()
	case "service":
		service()
	case "pgbouncer":
		pgbouncer()
	default:
		if len(globals.ServiceRole) > 0 {
			fmt.Fprintf(os.Stderr, `ERROR: Unknown Role: %s, valid Roles: manager / service`, globals.ServiceRole)
			usage()
			os.Exit(1)
		} else {
			fmt.Fprintf(os.Stderr, `ERROR: Role must be specified on the command line.`)
			usage()
			os.Exit(1)
		}
	}
}

func parseArgs() {
	for index, arg := range os.Args {
		if index == 0 {
			continue
		}

		switch arg {
		case "manager":
			globals.ServiceRole = "manager"
		case "service":
			globals.ServiceRole = "service"
		case "pgbouncer":
			globals.ServiceRole = "pgbouncer"
		case "version", "--version", "-version":
			fmt.Fprintf(os.Stdout, "%s\n", VERSION)
			os.Exit(0)
		case "help", "-h", "?", "--help":
			usage()
			os.Exit(0)
		default:
			usage()
			os.Exit(1)
		}
	}
}

func usage() {
	fmt.Fprintf(os.Stdout, `
rdpgd - Reliable Distributed PostgreSQL Daemon

Usage:

	rdpgd [Flag(s)] <Action>

Actions:

	manager   Run in Management Cluster mode.
	service   Run in Service Cluster mode.
	bootstrap Bootstrap RDPG schemas, filesystem etc...
	version   print rdpg version
	help      print this message

Flags:

	--version  print rdpgd version and exit
	--help     print this message and exit

	`)
}

func manager() (err error) {
	log.Info(`Starting with 'manager' role...`)
	err = bootstrap()
	if err != nil {
		log.Error(fmt.Sprintf(`main.manager() bootstrap() ! %s`, err))
		os.Exit(1)
	}
	go admin.API()
	go cfsb.API()
	go tasks.Scheduler()
	go tasks.Work()
	err = signalHandler()
	return
}

func service() (err error) {
	log.Info(`Starting with 'service' role...`)
	err = bootstrap()
	if err != nil {
		log.Error(fmt.Sprintf(`main.service() bootstrap() ! %s`, err))
		os.Exit(1)
	}
	go admin.API()
	go tasks.Scheduler()
	go tasks.Work()
	err = signalHandler()
	return
}

func pgbouncer() (err error) {
	log.Info(`Starting with 'pgbouncer' role...`)
	go gpb.Work()
	err = signalHandler()
	return
}

func bootstrap() (err error) {
	err = rdpg.Bootstrap()
	if err != nil {
		log.Error(fmt.Sprintf(`Bootstrap(%s) failed`, globals.ServiceRole))
		proc, _ := os.FindProcess(os.Getpid())
		proc.Signal(syscall.SIGTERM)
	}
	return
}

func writePidFile() {
	if pidFile != "" {
		pid := os.Getpid()
		log.Trace(fmt.Sprintf(`main.writePidFile() Writing pid %d to %s`, pid, pidFile))
		err := ioutil.WriteFile(pidFile, []byte(strconv.Itoa(pid)), 0644)
		if err != nil {
			log.Error(fmt.Sprintf(`main.writePidFile() Error while writing pid '%d' to '%s' :: %s`, pid, pidFile, err))
			os.Exit(1)
		}
	}
	return
}

func signalHandler() (err error) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	for sig := range ch {
		log.Info(fmt.Sprintf("main.signalHandler() Received signal %v, shutting down gracefully...", sig))
		if _, err := os.Stat(pidFile); err == nil {
			if err := os.Remove(pidFile); err != nil {
				log.Error(err.Error())
				os.Exit(1)
			}
		}
		os.Exit(0)
	}
	return
}
