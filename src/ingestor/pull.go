package ingestor

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/redis/go-redis/v9"
)

const (
	pullLockKey     = "lock:ingestor:pull"
	pullLockTTL     = 3 * time.Minute
	drainTimeout    = 100 * time.Second
	drainIdleWindow = 5 * time.Second

	// pullMaxOutstandingMessages raises Pub/Sub's own flow-control ceiling
	// (client default: 1000 concurrently delivered-but-unacked messages)
	// well above what this ingestor can hold received-but-not-yet-acked at
	// once (pipelineChannelBuffer plus one batch in flight). Acks only
	// happen when a batch flushes, not per message — measured flush stats
	// showed exec itself is fast (~3-10ms), but without raising this, the
	// batching delay alone trips Pub/Sub's flow control and throttles
	// delivery, independent of how fast Redis or the flush loop actually is.
	pullMaxOutstandingMessages = 20000

	// pipelineBatchSize caps how many ZADDs accumulate before being flushed
	// as a single Redis pipeline. A single accumulate/flush loop is enough:
	// measured pipeline execs took ~3-10ms, so one loop can push far more
	// throughput than this pipeline needs without concurrent flushers.
	pipelineBatchSize = 500

	// pipelineChannelBuffer decouples Receive's many concurrent callback
	// goroutines from the single flush loop, so add() doesn't block callers
	// under normal load.
	pipelineChannelBuffer = 2000

	// pipelineFlushInterval bounds how long a partial batch can sit before
	// being flushed anyway, so a quiet tail of messages doesn't wait for a
	// full batch that will never come.
	pipelineFlushInterval = 100 * time.Millisecond
)

var (
	pullSub     *pubsub.Subscriber
	windowerURL string
)

// batchItem pairs a parsed Redis write with the pubsub message it came from,
// so the message can be acked/nacked once the batch it landed in is flushed.
type batchItem struct {
	rec eventRecord
	msg *pubsub.Message
}

// writeBatcher accumulates parsed events on a single background goroutine
// and flushes them to Redis as pipelined, grouped ZADDs. A single loop is
// enough — measured Exec latency is only ~3-10ms, so it can push far more
// throughput than this pipeline needs without needing concurrent flushers.
// Call add() from any goroutine; call close() once (after Receive returns)
// to flush whatever's left and wait for the loop to finish.
type writeBatcher struct {
	items     chan batchItem
	done      chan struct{}
	processed *int64
	failed    *int64

	// Diagnostics: cumulative flush count/latency/items, so ingestPull can
	// log aggregated per-flush timing periodically instead of once per
	// flush (which at high flush rates would be its own logging overhead).
	flushCount    int64
	flushItemsSum int64
	flushNanosSum int64
	flushNanosMax int64
}

func newWriteBatcher(processed, failed *int64) *writeBatcher {
	b := &writeBatcher{
		items:     make(chan batchItem, pipelineChannelBuffer),
		done:      make(chan struct{}),
		processed: processed,
		failed:    failed,
	}
	go b.run()
	return b
}

func (b *writeBatcher) add(item batchItem) {
	b.items <- item
}

func (b *writeBatcher) recordFlush(items int, elapsed time.Duration) {
	atomic.AddInt64(&b.flushCount, 1)
	atomic.AddInt64(&b.flushItemsSum, int64(items))
	atomic.AddInt64(&b.flushNanosSum, int64(elapsed))
	for {
		cur := atomic.LoadInt64(&b.flushNanosMax)
		if int64(elapsed) <= cur || atomic.CompareAndSwapInt64(&b.flushNanosMax, cur, int64(elapsed)) {
			break
		}
	}
}

// logStats reports cumulative flush diagnostics: how many flushes have run,
// the average and worst-case Exec latency, and the average batch size
// actually achieved (as opposed to pipelineBatchSize, the cap).
func (b *writeBatcher) logStats() {
	count := atomic.LoadInt64(&b.flushCount)
	if count == 0 {
		return
	}
	itemsSum := atomic.LoadInt64(&b.flushItemsSum)
	nanosSum := atomic.LoadInt64(&b.flushNanosSum)
	nanosMax := atomic.LoadInt64(&b.flushNanosMax)
	log.Printf("[PullIngestor] flush stats: count=%d avg_items=%.1f avg_exec=%s max_exec=%s",
		count, float64(itemsSum)/float64(count), time.Duration(nanosSum/count), time.Duration(nanosMax))
}

// close stops accepting new items, flushes whatever's buffered, and waits
// for the loop to drain. Must only be called once, after all add() calls
// have returned.
func (b *writeBatcher) close() {
	close(b.items)
	<-b.done
}

func (b *writeBatcher) run() {
	defer close(b.done)

	batch := make([]batchItem, 0, pipelineBatchSize)
	ticker := time.NewTicker(pipelineFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case item, ok := <-b.items:
			if !ok {
				b.flush(batch)
				return
			}
			batch = append(batch, item)
			if len(batch) >= pipelineBatchSize {
				b.flush(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				b.flush(batch)
				batch = batch[:0]
			}
		}
	}
}

// flush pipelines batch to Redis and acks/nacks each message by its group's
// result. It uses its own context rather than the tick's drainCtx, since a
// flush triggered near/after the drain deadline must still be able to
// complete and ack — it isn't part of the pull itself, just bookkeeping for
// messages already received.
func (b *writeBatcher) flush(batch []batchItem) {
	if len(batch) == 0 {
		return
	}

	// Group by target key so events bound for the same sorted set collapse
	// into a single ZADD with many score/member pairs, instead of one ZADD
	// per event. Pipelining already cut round-trips; Redis still counts and
	// processes each pipelined command separately, so this is what actually
	// cuts the number of commands the (single-threaded) server has to work
	// through — the thing INFO stats showed was the real ceiling.
	groups := make(map[string][]batchItem, 1)
	for _, it := range batch {
		groups[it.rec.key] = append(groups[it.rec.key], it)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pipe := rdb.Pipeline()
	cmds := make(map[string]*redis.IntCmd, len(groups))
	for key, items := range groups {
		members := make([]redis.Z, len(items))
		for i, it := range items {
			members[i] = it.rec.z
		}
		cmds[key] = pipe.ZAdd(ctx, key, members...)
	}
	execStart := time.Now()
	_, err := pipe.Exec(ctx)
	execElapsed := time.Since(execStart)
	if err != nil && !errors.Is(err, redis.Nil) {
		log.Printf("[PullIngestor] pipeline exec error (batch=%d groups=%d elapsed=%s): %v", len(batch), len(groups), execElapsed, err)
	}
	b.recordFlush(len(batch), execElapsed)

	for key, items := range groups {
		err := cmds[key].Err()
		for _, it := range items {
			if err != nil {
				log.Printf("[PullIngestor] nacking message: redis zadd failed: %v", err)
				it.msg.Nack()
				atomic.AddInt64(b.failed, 1)
				continue
			}
			it.msg.Ack()
			atomic.AddInt64(b.processed, 1)
		}
	}
}

func init() {
	// init() runs unconditionally for every entry point built from this source
	// dir (main.go's IngestEvent included) regardless of which one --entry-point
	// actually selects at deploy time. The push ingestor's env file doesn't set
	// these, so treat their absence as "this deployment isn't using the pull
	// entry point" and skip registration, rather than log.Fatal-ing the whole
	// process before it can bind PORT 8080.
	projectID := os.Getenv("PUBSUB_PROJECT_ID")
	subID := os.Getenv("PUBSUB_PULL_SUBSCRIPTION_ID")
	windowerURL = os.Getenv("WINDOWER_URL")
	if projectID == "" || subID == "" || windowerURL == "" {
		return
	}

	client, err := pubsub.NewClient(context.Background(), projectID)
	if err != nil {
		log.Fatalf("pubsub client init failed: %v", err)
	}
	pullSub = client.Subscriber(subID)
	pullSub.ReceiveSettings.MaxOutstandingMessages = pullMaxOutstandingMessages

	functions.HTTP("IngestPull", ingestPull)
}

// ingestPull is a Tick: it drains the pull subscription until idle, writes everything
// to Redis (same processMessage path as the push ingestor), then nudges windower.
// A Redis SetNX lock guards against overlapping ticks if a drain runs long.
func ingestPull(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	locked, err := rdb.SetNX(ctx, pullLockKey, "locked", pullLockTTL).Result()
	if err != nil {
		http.Error(w, "lock check failed", http.StatusInternalServerError)
		log.Printf("[PullIngestor] lock check failed: %v", err)
		return
	}
	if !locked {
		log.Printf("[PullIngestor] previous tick still draining, skipping this tick")
		w.WriteHeader(http.StatusOK)
		return
	}
	defer rdb.Del(context.Background(), pullLockKey)

	drainStart := time.Now()
	drainCtx, cancel := context.WithTimeout(ctx, drainTimeout)
	defer cancel()

	var processed, failed int64
	batcher := newWriteBatcher(&processed, &failed)
	idle := time.AfterFunc(drainIdleWindow, cancel)
	defer idle.Stop()

	// Diagnostic: log the Redis connection pool's actual concurrency during
	// the drain, to see whether PoolSize:50 is really achieving many
	// concurrent connections inside this environment, or whether something
	// (e.g. Direct VPC egress connection setup) is keeping it much lower
	// than what redis-benchmark achieved from the bastion.
	statsDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s := rdb.PoolStats()
				log.Printf("[PullIngestor] redis pool: total=%d idle=%d stale=%d hits=%d misses=%d timeouts=%d",
					s.TotalConns, s.IdleConns, s.StaleConns, s.Hits, s.Misses, s.Timeouts)
				batcher.logStats()
			case <-statsDone:
				return
			}
		}
	}()
	defer close(statsDone)

	err = pullSub.Receive(drainCtx, func(msgCtx context.Context, msg *pubsub.Message) {
		idle.Reset(drainIdleWindow)

		rec, ok, procErr := parseEvent(msg.Data)
		if procErr != nil {
			log.Printf("[PullIngestor] nacking message: %v", procErr)
			msg.Nack()
			atomic.AddInt64(&failed, 1)
			return
		}
		if !ok {
			// Malformed/unroutable: ack directly, nothing to write.
			msg.Ack()
			atomic.AddInt64(&processed, 1)
			return
		}
		batcher.add(batchItem{rec: rec, msg: msg})
	})
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		http.Error(w, "drain failed", http.StatusInternalServerError)
		log.Printf("[PullIngestor] drain failed: %v", err)
		return
	}

	// Receive only returns once nothing is left to deliver, but any partial
	// batches are still sitting in the workers' buffers — flush and wait for
	// them before reporting the tick done and releasing the lock.
	batcher.close()

	// drainIdleWindow always contributes a trailing wait once the last message
	// is processed (Receive only returns once drainIdleWindow passes with no
	// new message) — subtract it out to report time actually spent moving
	// events into Redis, not time spent confirming the subscription was empty.
	drainElapsed := time.Since(drainStart)
	adjustedElapsed := drainElapsed - drainIdleWindow
	if adjustedElapsed < 0 {
		adjustedElapsed = 0
	}
	log.Printf("[PullIngestor] tick complete: processed=%d failed=%d drain_elapsed=%s adjusted_elapsed=%s",
		processed, failed, drainElapsed, adjustedElapsed)

	// Cloud Run freezes the container once the HTTP response is sent, so this must complete
	// before we respond rather than being kicked off as a background goroutine. Fire-and-forget
	// here means the windower call's outcome doesn't gate this tick's success/lock, not that we
	// return before it's actually been dispatched.
	triggerWindower()

	w.WriteHeader(http.StatusOK)
}

func triggerWindower() {
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Post(windowerURL, "application/json", nil)
	if err != nil {
		log.Printf("[PullIngestor] windower trigger failed: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Printf("[PullIngestor] windower trigger returned status %d", resp.StatusCode)
	}
}
