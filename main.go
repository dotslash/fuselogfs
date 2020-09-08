package main

import (
	"context"
	"flag"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/sevlyar/go-daemon"
	"os"
	"os/signal"
	"os/user"
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

func unmount(mountDir string) {
	logger.Warn().Msg("Attempting to unmount fuse")
	err := fuse.Unmount(mountDir)
	logger.Err(err).Msg("Unmount finished")
}

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
			go unmount(mountDir)
			// Sleep just to make sure we dont panic if the user does ^c impatiently
			time.Sleep(time.Second)
		}
	}()
}

// "Forks" the current process.
// - child process will return a daemon.Context which eventually needs to be "Release"d
// - parent process will return nil.
func fork() *daemon.Context {
	ctx := new(daemon.Context)
	child, err := ctx.Reborn()
	if err != nil {
		stdOutLogger.Err(err).Msg("daemon::Reborn failed")
	}

	if child != nil {
		stdOutLogger.Info().Msgf("Launched child process pid:%v", child.Pid)
		return nil
	} else {
		return ctx
	}
}

func main() {
	mountDir := flag.String("mount_dir", "/tmp/fool", "Mount destination")
	stageDir := flag.String("stage_dir", "/tmp/foolstage", "Original location of the files exported by fuse")
	logToFile := flag.Bool("log_to_file", true, "Whether to log to file or not")
	daemonMode := flag.Bool("daemon", true,
		"Whether to run as daemon or not. If running as daemon log_to_file should be true (default)")

	flag.Parse()

	logger = makeLogger("MAIN", true, *logToFile)

	if *daemonMode {
		daemonCtx := fork()
		if daemonCtx == nil {
			return
		}
		defer func() {
			err := daemonCtx.Release()
			logger.Err(err).Msg("Release daemon context")
		}()
	}

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
		logger.Err(err).Msg("Failed to mount")
		return
	}
	registerSIGINTHandler(*mountDir)
	err = mfs.Join(context.Background())
	logger.Err(err).Msg("Join returned on mounted file system")

	// TODO: Sleep for one sec for logs to flush. Does that make any sense?
	//  From anecdotal evidence it seems that way.
	time.Sleep(time.Second)
}
