package main

import (
	"fmt"
	"github.com/rs/zerolog"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var logger zerolog.Logger
var stdOutLogger = makeLogger("MAIN", true, false)

func errOrDebug(inpLog *zerolog.Logger, err error) *zerolog.Event {
	if err != nil {
		return inpLog.Error().Err(err)
	} else {
		return inpLog.Debug()
	}
}

func makeLogDirOrDie() string {
	usr := getUserOrDie()
	logDir := filepath.Join(usr.HomeDir, "logs")
	panicOnErr(os.MkdirAll(logDir, 0755))
	return logDir
}

// TODO: Im pretty sure there is a better way to do logging.
func makeLogger(name string, logCaller bool, logToFile bool) zerolog.Logger {
	var logFile = os.Stdout
	if logToFile {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
		logDir := makeLogDirOrDie()
		var err error
		logFile, err = os.OpenFile(filepath.Join(logDir, "fusefs.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		panicOnErr(err)
	}

	output := zerolog.ConsoleWriter{Out: logFile, TimeFormat: time.RFC3339}
	output.FormatLevel = func(i interface{}) string {
		if i == nil {
			i = "na"
		}
		return strings.ToUpper(fmt.Sprintf("| %-6s|", i))
	}
	ctx := zerolog.New(output).
		With().
		Timestamp().
		Str("Name", name)
	if logCaller {
		ctx = ctx.Caller()
	}
	l := ctx.Logger().Level(zerolog.InfoLevel)
	return l
}

func makeStdLogger(name string, logToFile bool) *log.Logger {
	zlog := makeLogger(name, false, logToFile)
	ret := log.New(os.Stdout, "", 0)
	ret.SetFlags(0)
	ret.SetPrefix("")
	ret.SetOutput(zlog)
	return ret
}
