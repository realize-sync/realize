package internal

import (
	"context"
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
)

const bufSize = 16

type targetDefinition struct {
	remotePath string
	localPath  string
}

func RunRealize(ctx context.Context, root string, remote string, bwLimit int, delete bool, dryRun bool) error {
	limiter := rate.NewLimiter(rate.Limit(bwLimit), bufSize)
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
			if dryRun {
				fmt.Printf("%s -> %s\n", target.remotePath, target.localPath)
				continue
			}

			if err := realize(ctx, target.remotePath, target.localPath, limiter); err != nil {
				logrus.Warnf("%s: failed to realize: %s", target.localPath, err)
				errorCount++
				continue
			}
			if delete {
				os.Remove(target.remotePath)
				// TODO: delete containing directory, if it's now empty
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

	logrus.Debugf("%s -> %s...", remotePath, localPath)
	if err := copy(ctx, remotePath, tempPath, limiter); err != nil {
		return err
	}
	if err := os.Rename(tempPath, localPath); err != nil {
		return err
	}
	logrus.Infof("%s: OK", localPath)
	return nil
}

// copy does a rate-limited copy of a file.
func copy(ctx context.Context, fromPath string, toPath string, limiter *rate.Limiter) error {
	source, err := os.Open(fromPath)
	if err != nil {
		return err
	}
	defer source.Close()
	dest, err := os.Create(toPath)
	if err != nil {
		return err
	}
	defer dest.Close()
	buf := make([]byte, bufSize*1024)
	for {
		limiter.WaitN(ctx, bufSize)
		n, err := source.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			return nil
		}

		if _, err := dest.Write(buf[:n]); err != nil {
			return err
		}
	}
}
