package bench

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type worker struct {
	db     DB
	number uint64
}

func newWorker(number uint64, dsn string) (*worker, error) {
	db, err := newMySQLConn(dsn)
	if err != nil {
		return nil, err
	}
	return &worker{
		db:     db,
		number: number,
	}, nil
}

func (w *worker) run(ctx context.Context) {
	var count int64
	lastTime := time.Now()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("[worker %d] worker exited, count %d\n", w.number, count)
			return
		default:
			id := r.Int63n(100000000) + 1
			// fmt.Printf("[worker %d] random id %d\n", w.number, id)
			err := w.db.RandomUpdate(id)
			if err != nil {
				panic(err)
			}
			count++
			if count%10000 == 0 {
				now := time.Now()
				fmt.Printf("[worker %d] insert 10000 records, cost %ds\n", w.number, now.Unix()-lastTime.Unix())
				lastTime = now
			}
		}
	}
}

type Benchmark struct {
	dsn     string
	threads uint64
}

func (b *Benchmark) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := uint64(0); i < b.threads; i++ {
		worker, err := newWorker(i, b.dsn)
		if err != nil {
			panic(err)
		}
		wg.Add(1)
		go func() {
			worker.run(ctx)
			wg.Done()
		}()
	}

	wg.Wait()
}

func NewBenchmark(threads uint64, dsn string) *Benchmark {
	return &Benchmark{
		threads: threads,
		dsn:     dsn,
	}
}
