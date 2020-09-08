package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"
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
	if err != nil {
		logger.Err(err).Msg("Failed to unmount")
	} else {
		logger.Warn().Msg("Unmounting fuse successful")
	}
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

func main() {
	mountDir := flag.String("mount_dir", "/tmp/fool", "Mount destination")
	stageDir := flag.String("stage_dir", "/tmp/foolstage", "Original location of the files exported by fuse")
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
	if err != nil {
		logger.Err(err).Msg("Program exited with error")
	}
}
