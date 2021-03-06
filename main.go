package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	bench "github.com/5kbpers/bench-append-only/bench"
)

var (
	tables    = flag.Uint64("tables", 1, "The number of tables")
	base      = flag.Uint64("base", 0, "The base number of inserted rows")
	threads   = flag.Uint64("threads", 64, "The number of threads")
	batchSize = flag.Uint64("batch", 100, "The number of rows for each insert")
	dsn       = flag.String("dsn", "", "The DSN of the target database")
)

func main() {
	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())

	closeSignalChan := make(chan os.Signal, 1)
	signal.Notify(closeSignalChan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		sig := <-closeSignalChan
		fmt.Printf("got signal %s to exit\n", sig)
		cancel()
	}()

	b := bench.NewBenchmark(*base, *threads, *tables, *batchSize, *dsn)
	b.Run(ctx)
}
