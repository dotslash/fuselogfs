package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/rs/zerolog"
	stdlog "log"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

func getUserOrDie() *user.User {
	usr, err := user.Current()
	panicOnErr(err)
	return usr
}

func makeLogDirOrDie() string {
	usr := getUserOrDie()
	logDir := filepath.Join(usr.HomeDir, "logs")
	panicOnErr(os.MkdirAll(logDir, 0755))
	return logDir
}

func makeLogger(component string, caller bool) zerolog.Logger {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	logDir := makeLogDirOrDie()
	logFile, err := os.OpenFile(filepath.Join(logDir, "fusefs.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	panicOnErr(err)

	output := zerolog.ConsoleWriter{Out: logFile, TimeFormat: time.RFC3339}
	output.FormatLevel = func(i interface{}) string {
		if i == nil {
			i = "na"
		}
		return strings.ToUpper(fmt.Sprintf("| %-6s|", i))
	}
	ctx := zerolog.
		New(output).
		With().
		Timestamp().
		Str("Name", component)
	if caller {
		ctx = ctx.Caller()
	}
	l:= ctx.Logger()
	return l
}



func makeStdLogger(component string) *stdlog.Logger {
	zlog := makeLogger(component, false)
	zlog = zlog.Level(zerolog.InfoLevel)
	ret := stdlog.New(os.Stdout, "", 0)
	ret.SetFlags(0)
	ret.SetPrefix("")
	ret.SetOutput(zlog)
	return ret
}

var logger = makeLogger("MAIN", true)

func registerSIGINTHandler(mountDir string) {
	// Register for SIGINT.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	// Start a goroutine that will unmount when the signal is received.
	go func() {
		for {
			s := <-signalChan
			if s == syscall.SIGTERM {
				logger.Printf("Received %v", s)
				err := fuse.Unmount(mountDir)
				if err != nil {
					logger.Printf("Failed to unmount. err: %v", err)
				}
			}
		}
	}()
}

func main() {
	mountDir := flag.String("mount_dir", "/tmp/fool", "Mount destination")
	stageDir := flag.String("stage_dir", "/tmp/foolstage", "Mount destination")
	flag.Parse()

	fsname := "fuselogfs-at-" + *mountDir
	mountcfg := fuse.MountConfig{
		OpContext:               nil,
		FSName:                  fsname,
		ReadOnly:                false,
		ErrorLogger:             makeStdLogger("fuse err"),
		// DebugLogger:             makeStdLogger("fuse debug"),
		DisableWritebackCaching: false,
		EnableVnodeCaching:      false,
		EnableSymlinkCaching:    false,
		EnableNoOpenSupport:     false,
		EnableNoOpendirSupport:  false,
		VolumeName:              fsname,
		Options:                 nil,
		Subtype:                 "",
	}
	fs, err := NewFuseLogFs(*stageDir, getUserOrDie())
	panicOnErr(err)
	server := fuseutil.NewFileSystemServer(fs)
	mfs, err := fuse.Mount(*mountDir, server, &mountcfg)
	if err != nil {
		fmt.Printf("Failed to mount: err=%v \n", err)
		return
	}
	// registerSIGINTHandler(*mountDir)
	err = mfs.Join(context.Background())
	logger.Err(err)
}
