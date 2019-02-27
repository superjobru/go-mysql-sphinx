package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/juju/errors"
	"github.com/superjobru/go-mysql-sphinx/river"
	"github.com/thejerf/suture"
	"gopkg.in/birkirb/loggers.v1/log"
)

type strList []string

var version string

func (s *strList) String() string {
	return strings.Join(*s, " ")
}

func (s *strList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func getVersion() string {
	return fmt.Sprintf("go-mysql-sphinx %s ; go runtime %s", version, runtime.Version())
}

func run() (err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flags := flag.NewFlagSet("", flag.ContinueOnError)

	var configFile string
	var dataDir string
	var myAddr string
	var sphAddr strList
	var logLevel string
	var logFile string
	var rebuildAndExit bool
	var showVersion bool

	flags.StringVar(&configFile, "config", "./etc/river.toml", "config file")
	flags.StringVar(&dataDir, "data-dir", "", "directory for storing local application state")
	flags.StringVar(&myAddr, "my-addr", "", "MySQL replica address")
	flags.Var(&sphAddr, "sph-addr", "Sphinx address")
	flags.StringVar(&logLevel, "log-level", "info", "log level")
	flags.StringVar(&logFile, "log-file", "", "log file; will log to stdout if empty")
	flags.BoolVar(&rebuildAndExit, "rebuild-and-exit", false, "rebuild all configured indexes and exit")
	flags.BoolVar(&showVersion, "version", false, "show program version and exit")

	if err = flags.Parse(os.Args[1:]); err != nil {
		return err
	}

	if showVersion {
		_, err = fmt.Printf("%s\n", getVersion())
		return err
	}

	if err = initLogger(logLevel, logFile); err != nil {
		return err
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	cfg, err := river.NewConfigWithFile(configFile)
	if err != nil {
		return err
	}

	if dataDir != "" {
		cfg.DataDir = dataDir
	}

	if myAddr != "" {
		cfg.MyAddr = myAddr
	}

	if len(sphAddr) > 0 {
		cfg.SphAddr = sphAddr
	}

	r, err := river.NewRiver(cfg, log.Logger, rebuildAndExit)
	if err != nil {
		return err
	}

	rootSup := suture.New("root", suture.Spec{
		FailureThreshold: -1,
		FailureBackoff:   10 * time.Second,
		Timeout:          time.Minute,
		Log: func(msg string) {
			log.WithFields("library", "suture").Info(msg)
		},
	})

	rootSup.Add(r.StatService)
	rootSup.Add(r)
	rootSup.ServeBackground()

	select {
	case n := <-sc:
		log.Infof("received signal %v, exiting", n)
	case err = <-r.FatalErrC:
		if errors.Cause(err) == river.ErrRebuildAndExitFlagSet {
			log.Info(err.Error())
			err = nil
		}
	}

	rootSup.Stop()
	return err
}

func main() {
	err := run()
	if err != nil {
		// Fatalf also exits with exit-code 1
		log.Fatalf(errors.ErrorStack(err))
	}
}
