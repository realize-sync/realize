package internal

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

type Options struct {
	// If true, just print the soft links that were found.
	DryRun bool

	// If true, delete the remote file after realizing the soft links.
	// Also deletes the containing directory, if it is empty.
	Delete bool

	// If true, delete containing directory and leftover files after
	// deleting the file.
	DeleteDirAndLeftover bool

	// If non-nil, limit read rate (in bytes).
	RateLimiter *rate.Limiter
}

// RunRealize transforms soft links to remote in root into real links
func RunRealize(ctx context.Context, root string, remote string, options Options) error {
	logrus.Debugf("Looking for symlinks to %s in %s...", remote, root)
	processed := make(map[string]bool)
	containingDirs := make(map[string]bool)
	errorCount := 0
	for {
		targets, err := collectTargets(root, remote)
		if err != nil {
			return err
		}
		processedThisRound := 0
		for _, target := range targets {
			if _, ok := processed[target.localPath]; ok {
				continue
			}
			processed[target.localPath] = true
			processedThisRound++
			if options.DryRun {
				fmt.Printf("realize %s <- %s\n", target.localPath, target.remotePath)
				continue
			}
			logrus.Debugf("realize %s <- %s\n", target.localPath, target.remotePath)
			dir := filepath.Dir(target.remotePath)
			if err := realize(ctx, target.remotePath, target.localPath, options.RateLimiter); err != nil {
				if err == context.DeadlineExceeded || err == context.Canceled {
					return fmt.Errorf("Timed out or cancelled. Realized %d files.", len(processed)-1)
				}
				logrus.Errorf("failed to realize %s: %s", target.localPath, err)
				errorCount++
				containingDirs[dir] = false
				continue
			}
			if options.Delete {
				err := os.Remove(target.remotePath)
				logrus.Debugf("Deleted %s: %s", target.remotePath, errorToString(err))
				if _, ok := containingDirs[dir]; !ok {
					containingDirs[dir] = true
				}
			}
		}
		if processedThisRound == 0 {
			break
		}
		if options.DeleteDirAndLeftover {
			for dir, shouldDelete := range containingDirs {
				if !shouldDelete {
					continue
				}
				err := os.RemoveAll(dir)
				logrus.Debugf("Deleted dir %s: %s", dir, errorToString(err))
			}
		}
	}
	if errorCount > 0 {
		return fmt.Errorf("Failed to realize %d/%d files.", errorCount, len(processed))
	}
	logrus.Debugf("Successfully realized %d files.", len(processed))
	return nil
}

type target struct {
	remotePath string
	localPath  string
}

// collectTargets returns soft links within roots that point within remote
func collectTargets(root string, remote string) ([]*target, error) {
	remotePrefix := remote
	if !strings.HasSuffix(remotePrefix, "/") {
		remotePrefix += "/"
	}
	var targets []*target
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if (d.Type() & fs.ModeSymlink) != 0 {
			dest, _ := os.Readlink(path)
			if strings.HasPrefix(dest, remotePrefix) {
				targets = append(targets, &target{
					remotePath: dest,
					localPath:  path,
				})
			}
		}
		return nil
	})
	return targets, err
}

// realize copies remotePath to localPath
func realize(ctx context.Context, remotePath string, localPath string, limiter *rate.Limiter) error {
	tempPath := localPath + ".part"
	defer os.Remove(tempPath)

	startTime := time.Now()
	byteCount, err := copy(ctx, remotePath, tempPath, limiter)
	if err != nil {
		return err
	}
	if err = os.Rename(tempPath, localPath); err != nil {
		return err
	}
	bytesPerSecond := float64(0)
	duration := time.Since(startTime)
	if duration != 0 {
		bytesPerSecond = float64(byteCount) / float64(duration) * float64(time.Second)
	}
	logrus.Infof("%s: OK [%s, %s/s]",
		localPath,
		bytefmt.ByteSize(byteCount),
		bytefmt.ByteSize(uint64(bytesPerSecond)))
	return nil
}

// copy does a rate-limited copy of a file.
func copy(ctx context.Context, fromPath string, toPath string, limiter *rate.Limiter) (uint64, error) {
	source, err := os.Open(fromPath)
	if err != nil {
		return 0, err
	}
	defer source.Close()
	dest, err := os.Create(toPath)
	if err != nil {
		return 0, err
	}
	defer dest.Close()
	buf := make([]byte, 16*bytefmt.KILOBYTE)
	byteCount := uint64(0)
	for {
		if ctx.Err() != nil {
			return byteCount, ctx.Err()
		}
		if limiter != nil {
			if err = limiter.WaitN(ctx, len(buf)); err != nil {
				// This might fails when the deadline is about to be
				// exceeded. Report it as if the deadline was exceeded
				// already.
				return byteCount, context.DeadlineExceeded
			}
		}
		n, err := source.Read(buf)
		if err != nil && err != io.EOF {
			return byteCount, err
		}
		if n == 0 {
			return byteCount, nil
		}
		if n > 0 {
			byteCount += uint64(n)
		}
		if _, err := dest.Write(buf[:n]); err != nil {
			return byteCount, err
		}
	}
}

// errorToString converts an error into a string.
func errorToString(err error) string {
	if err == nil {
		return "OK"
	}
	return err.Error()
}
