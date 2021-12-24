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
	RateLimiter *rate.Limiter
	Delete      bool
	DryRun      bool
}

func RunRealize(ctx context.Context, root string, remote string, options Options) error {
	processed := make(map[string]bool)
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
				fmt.Printf("%s -> %s\n", target.remotePath, target.localPath)
				continue
			}

			if err := realize(ctx, target.remotePath, target.localPath, options.RateLimiter); err != nil {
				logrus.Warnf("%s: failed to realize: %s", target.localPath, err)
				errorCount++
				continue
			}
			if options.Delete {
				err := os.Remove(target.remotePath)
				logrus.Debugf("Delete %s -> %s", target.remotePath, err)
				containingDir := filepath.Dir(target.remotePath)
				if empty, _ := isEmpty(containingDir); empty {
					err := os.Remove(containingDir)
					logrus.Debugf("Delete dir %s -> %s", containingDir, err)
				}
			}
		}
		if processedThisRound == 0 {
			break
		}
	}
	if errorCount > 0 {
		return fmt.Errorf("Failed to realize %d/%d files.", errorCount, len(processed))
	}
	return nil
}

type targetDefinition struct {
	remotePath string
	localPath  string
}

func collectTargets(root string, remote string) ([]*targetDefinition, error) {
	remotePrefix := remote
	if !strings.HasSuffix(remotePrefix, "/") {
		remotePrefix += "/"
	}
	var targets []*targetDefinition
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || (d.Type()&fs.ModeSymlink) == 0 {
			return nil
		}
		dest, _ := os.Readlink(path)
		if strings.HasPrefix(dest, remotePrefix) {
			targets = append(targets, &targetDefinition{
				remotePath: dest,
				localPath:  path,
			})
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
	logrus.Debugf("%s -> %s...", remotePath, localPath)
	byteCount, err := copy(ctx, remotePath, tempPath, limiter)
	if err != nil {
		return err
	}
	if err = os.Rename(tempPath, localPath); err != nil {
		return err
	}
	bytePerSecond := uint64(byteCount / uint64(time.Since(startTime)/time.Second))
	logrus.Infof("%s: OK [%s/s]", localPath, bytefmt.ByteSize(bytePerSecond))
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
		var err error
		if limiter != nil {
			err = limiter.WaitN(ctx, len(buf))
		} else {
			err = ctx.Err()
		}
		if err != nil {
			return byteCount, err
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

func isEmpty(name string) (bool, error) {
	h, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer h.Close()

	if _, err = h.Readdir(1); err == io.EOF {
		return true, nil
	}
	return false, err
}
