package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"syscall"
	"text/tabwriter"

	"github.com/mmadfox/spinix/internal/cluster"

	"google.golang.org/grpc"

	"github.com/mmadfox/run"
	"github.com/mmadfox/spinix/internal/config"
)

func main() {
	fs := flag.NewFlagSet("spinix", flag.ExitOnError)
	var (
		confFilename = fs.String("config", "spinix.yml", "Sets configuration filename. Default is spinix.yaml in the current folder.")
	)
	fs.Usage = usageFor(fs, os.Args[0]+" [flags]")
	if err := fs.Parse(os.Args[1:]); err != nil {
		fmt.Printf("[ERROR] fs.Parse(%v) => %v\n", os.Args[1:], err)
		os.Exit(1)
	}

	envConfFilename := os.Getenv("CONFIG")
	if len(envConfFilename) > 0 {
		*confFilename = envConfFilename
	}
	conf, err := config.FromFile(*confFilename)
	if err != nil {
		fmt.Printf("[ERROR] config.FromFile(%s) => %v\n", *confFilename, err)
		os.Exit(1)
	}

	logger, err := conf.BuildLogger()
	if err != nil {
		fmt.Printf("[ERROR] conf.BuildLogger() => %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()
	sugarLogger := logger.Sugar()

	ctx := context.Background()

	grpcListener, err := net.Listen("tcp", conf.GRPCAddr())
	if err != nil {
		sugarLogger.Errorf("failed to listen: %v", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	c, err := cluster.New(grpcServer, logger, conf.ClusterOptions())
	if err != nil {
		sugarLogger.Errorf("failed to init cluster: %v", err)
		os.Exit(1)
	}

	var g run.PGroup
	{
		g.Add(func() error {
			sugarLogger.Infof("Run: GRPC server on %s", conf.GRPCAddr())
			return grpcServer.Serve(grpcListener)
		}, func(err error) {
			grpcServer.GracefulStop()
			_ = grpcListener.Close()
			logger.Info("Shutdown: GRPC server")
		}, interruptPosition(1))
	}

	{
		g.Add(func() error {
			sugarLogger.Infof("Run: Cluster service on %s", conf.ClusterAddr())
			return c.Run()
		}, func(err error) {
			if err := c.Shutdown(); err != nil {
				sugarLogger.Error(err)
			}
			logger.Info("Shutdown: Cluster service")
		}, interruptPosition(2))
	}

	{
		g.Add(terminate(ctx))
	}

	sugarLogger.Infof("Exit: %v\n", g.Run())
}

func terminate(ctx context.Context) (execute func() error, interrupt func(error), interruptOrder int) {
	execute, interrupt = run.SignalHandler(ctx, syscall.SIGINT, syscall.SIGTERM)
	return execute, interrupt, 0
}

func interruptPosition(n int) int {
	return n
}

func usageFor(fs *flag.FlagSet, short string) func() {
	return func() {
		fmt.Fprintf(os.Stderr, "USAGE\n")
		fmt.Fprintf(os.Stderr, "  %s\n", short)
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "FLAGS\n")
		w := tabwriter.NewWriter(os.Stderr, 0, 2, 2, ' ', 0)
		fs.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "\t-%s %s\t%s\n", f.Name, f.DefValue, f.Usage)
		})
		w.Flush()
		fmt.Fprintf(os.Stderr, "\n")
	}
}
