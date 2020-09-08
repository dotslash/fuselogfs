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

// TODO: Im pretty sure there is a better way to do logging.
func makeLogger(component string, caller bool, logToFile bool) zerolog.Logger {
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
	ctx := zerolog.
		New(output).
		With().
		Timestamp().
		Str("Name", component)
	if caller {
		ctx = ctx.Caller()
	}
	l := ctx.Logger()
	return l
}

func makeStdLogger(component string, logToFile bool) *stdlog.Logger {
	zlog := makeLogger(component, false, logToFile)
	zlog = zlog.Level(zerolog.InfoLevel)
	ret := stdlog.New(os.Stdout, "", 0)
	ret.SetFlags(0)
	ret.SetPrefix("")
	ret.SetOutput(zlog)
	return ret
}

var logger zerolog.Logger

func registerSIGINTHandler(mountDir string) {
	// Register for SIGINT.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	// Start a goroutine that will unmount when the signal is received.
	go func() {
		attemptNum := 0
		for {
			s := <-signalChan
			attemptNum++
			logger.Warn().Msgf("Received %v attemptNum:%v", s, attemptNum)

			if attemptNum >= 3 {
				logger.Panic().Msg("Previous 2 attempts to unmount might not have worked. Panicking")
			}

			// Unmount asynchronously so that the user can issue a ^c again if the first one
			// is stuck.
			// TODO: When does unmount get "stuck"?
			go func() {
				logger.Warn().Msg("Attempting to unmount fuse")
				err := fuse.Unmount(mountDir)
				if err != nil {
					logger.Err(err).Msg("Failed to unmount")
				} else {
					logger.Warn().Msg("Unmounting fuse successful")
				}
			}()
			// Sleep just to make sure we dont panic if the user does ^c impatiently
			time.Sleep(time.Second)
		}
	}()
}

func main() {
	mountDir := flag.String("mount_dir", "/tmp/fool", "Mount destination")
	stageDir := flag.String("stage_dir", "/tmp/foolstage", "Mount destination")
	logToFile := flag.Bool("log_to_file", true, "Whether to log to file or not")

	flag.Parse()
	logger = makeLogger("MAIN", true, false)

	fsName := "fuselogfs-at-" + *mountDir
	mountConfig := fuse.MountConfig{
		// Most of these fields are set to default. Setting them explicitly
		// for documentation purposes.
		OpContext:               nil,
		FSName:                  fsName,
		ReadOnly:                false,
		ErrorLogger:             makeStdLogger("fuse err", *logToFile),
		DebugLogger:             nil,
		DisableWritebackCaching: false,
		EnableVnodeCaching:      false,
		EnableSymlinkCaching:    false,
		EnableNoOpenSupport:     false,
		EnableNoOpendirSupport:  false,
		VolumeName:              fsName,
		Options:                 nil,
		Subtype:                 "",
	}
	fs, err := NewFuseLogFs(*stageDir, getUserOrDie())
	panicOnErr(err)
	server := fuseutil.NewFileSystemServer(fs)
	mfs, err := fuse.Mount(*mountDir, server, &mountConfig)
	if err != nil {
		fmt.Printf("Failed to mount: err=%v \n", err)
		return
	}
	registerSIGINTHandler(*mountDir)
	err = mfs.Join(context.Background())
	logger.Err(err)
}
