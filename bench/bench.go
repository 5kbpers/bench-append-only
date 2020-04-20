package bench

import (
	"context"
	"fmt"
	"sync"
)

type worker struct {
	db     DB
	number uint64
}

func newWorker(number uint64, tables uint64, dsn string) (*worker, error) {
	db, err := newMySQLConn(tables, dsn)
	if err != nil {
		return nil, err
	}
	return &worker{
		db:     db,
		number: number,
	}, nil
}

func (w *worker) run(ctx context.Context, base uint64, pace uint64) {
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("[worker %d] worker exited, current number is %d\n", w.number, base)
			return
		default:
			err := w.db.Insert(base)
			if err != nil {
				panic(err)
			}
			base += pace
		}
	}
}

type Benchmark struct {
	dsn     string
	base    uint64
	threads uint64
	tables  uint64
}

func (b *Benchmark) Run(ctx context.Context) {
	db, err := newMySQLConn(b.tables, b.dsn)
	if err != nil {
		panic(err)
	}
	err = db.CreateTables()
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	for i := uint64(0); i < b.threads; i++ {
		worker, err := newWorker(i, b.tables, b.dsn)
		if err != nil {
			panic(err)
		}
		wg.Add(1)
		go func(base uint64, pace uint64) {
			worker.run(ctx, base, pace)
			wg.Done()
		}(b.base+i, b.threads)
	}

	wg.Wait()
}

func NewBenchmark(base uint64, threads uint64, tables uint64, dsn string) *Benchmark {
	return &Benchmark{
		base:    base,
		threads: threads,
		tables:  tables,
		dsn:     dsn,
	}
}
