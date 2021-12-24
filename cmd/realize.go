package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	realize "github.com/szermatt/realize/internal"
)

var (
	verboseArg = flag.Bool("v", false, "Enable verbose output.")
	debugArg   = flag.Bool("debug", false, "Enable debug output.")
	dryRunArg  = flag.Bool("n", false, "Enable dry-run mode.")
	rootArg    = flag.String("root", "", "Directory to look for soft links in")
	remoteArg  = flag.String("remote", "", "Remote directory to realize soft links for")
	deleteArg  = flag.Bool("delete", false, "Delete linked file after copying")
	bwLimitArg = flag.Int("bwlimit", 500, "Bandwidth limit, in kb/s")
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

	if err := realize.RunRealize(
		context.Background(), *rootArg, *remoteArg, *bwLimitArg, *deleteArg, *dryRunArg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(10)
	}
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
