package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/sirupsen/logrus"
	realize "github.com/szermatt/realize/internal"
	"golang.org/x/time/rate"
)

var (
	verboseArg   = flag.Bool("v", false, "Enable verbose output.")
	debugArg     = flag.Bool("debug", false, "Enable debug output.")
	dryRunArg    = flag.Bool("n", false, "Enable dry-run mode.")
	rootArg      = flag.String("root", "", "Directory to look for soft links in")
	remoteArg    = flag.String("remote", "", "Remote directory to realize soft links for")
	deleteArg    = flag.Bool("delete", false, "Delete linked file after copying")
	deleteDirArg = flag.String("deletedir", "", "After deleting the file, delete containing directory if pattern matches")
	bwLimitArg   = flag.String("bwlimit", "", "Bandwidth limit per second. Units: B (default), K, M, G")
	timeout      = flag.Duration("timeout", time.Duration(0), "Give up after that much time has passed.")
)

func main() {
	flag.Parse()

	if *debugArg {
		logrus.SetLevel(logrus.DebugLevel)
	} else if *verboseArg {
		logrus.SetLevel(logrus.InfoLevel)
	} else {
		logrus.SetLevel(logrus.WarnLevel)
	}
	logrus.SetFormatter(&PlainTextFormatter{})

	if *dryRunArg {
		logrus.Warn("Dry-run mode on")
	}

	options := realize.Options{
		Delete:        *deleteArg,
		DeleteDirGlob: *deleteDirArg,
		DryRun:        *dryRunArg,
	}
	if len(*bwLimitArg) > 0 {
		limitBytes := uint64(0)
		var err error
		limitBytes, err = bytefmt.ToBytes(*bwLimitArg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: invalid format for --bwlimit argument: %s", err)
			os.Exit(1)
		}
		burstSize := min(int(limitBytes/10), 32*bytefmt.KILOBYTE)
		options.RateLimiter = rate.NewLimiter(rate.Limit(limitBytes), burstSize)
		logrus.Debugf("Bandwidth limit: %s/s", bytefmt.ByteSize(limitBytes))
	}
	ctx := context.Background()
	if *timeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, *timeout)
		defer cancel()
		ctx = timeoutCtx
		deadline, _ := ctx.Deadline()
		logrus.Debugf("Stopping at %s", deadline.Local())
	}
	if err := realize.RunRealize(ctx, *rootArg, *remoteArg, options); err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}
}

func min(a int, b int) int {
	if b < a {
		return b
	}
	return a
}

type PlainTextFormatter struct{}

func (f *PlainTextFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	prefix := ""
	switch entry.Level {
	case logrus.FatalLevel:
		prefix = "error: "
	case logrus.ErrorLevel:
		prefix = "error: "
	case logrus.WarnLevel:
		prefix = "warning: "
	}
	return []byte(prefix + entry.Message + "\n"), nil
}
