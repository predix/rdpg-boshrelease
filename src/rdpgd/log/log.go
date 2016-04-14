package log

import (
	"fmt"
	"os"
	"strings"
	"time"
)

var (
	level int
)

func init() {
	lvl := os.Getenv("RDPGD_LOG_LEVEL")
	if lvl != "" {
		level = code(lvl)
	} else {
		level = code("info")
	}
}

func code(lvl string) int {
	switch strings.ToLower(lvl) {
	case "all":
		return 0
	case "trace":
		return 10
	case "debug":
		return 20
	case "error", "err":
		return 30
	case "fatal", "crit", "critical":
		return 40
	case "info":
		return 50
	case "warn", "warning":
		return 60
	case "off":
		return 100
	default:
		return 50
	}
}

// create msg chan variable with some buffer
func init() {
	// start background worker listening on msg chan
}

// TODO: Logs also go to database for ops dashboard inspection.
func log(lvl, msg string) {
	c := code(lvl)
	if level <= c {
		ts := time.Now().Format(time.RFC3339)
		if c < 50 { // Log stderr messages direct and immediately
			fmt.Fprintf(os.Stderr, "%s rdpg %s %s\n", ts, lvl, msg)
		} else {
			// TODO: background worker for stdout messages via channel async
			fmt.Fprintf(os.Stdout, "%s rdpg %s %s\n", ts, lvl, msg)
		}
	}
}

// TODO: Alter log functions to be able to pass args through to fmt.Sprintf()
func Trace(msg string) {
	log("trace", msg)
}

func Debug(msg string) {
	log("debug", msg)
}

func Error(msg string) {
	log("error", msg)
}

func Fatal(msg string) {
	log("fatal", msg)
}

func Info(msg string) {
	log("info", msg)
}

func Warn(msg string) {
	log("warn", msg)
}
