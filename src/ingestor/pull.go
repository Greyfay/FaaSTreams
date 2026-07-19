package ingestor

import (
	"context"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
)

const (
	pullLockKey        = "lock:ingestor:pull"
	pullLockTTL        = 7 * time.Second
	drainIdleWindow    = 2000 * time.Millisecond
	maxSessionDuration = 50 * time.Second
	interval           = 5 * time.Second
	maxDelay           = 1500 * time.Millisecond
)

var (
	pullSub     *pubsub.Subscriber
	windowerURL string
)

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

	// CRITICAL: We only delete the lock here if the normal flow didn't release it yet
	lockReleased := false
	defer func() {
		if !lockReleased {
			rdb.Del(context.Background(), pullLockKey)
		}
	}()

	sessionCtx, cancelSession := context.WithTimeout(ctx, maxSessionDuration)
	defer cancelSession()

	// `isShuttingDown` acts as a thread-safe coordination barrier (0 = active, 1 = stopping).
	// It ensures the background ticker goroutine completely halts its triggers once the
	// SubPub-Pulling terminates
	var processedSinceLastTick, failedSinceLastTick, isShuttingDown int64

	// to ensure that the last Windower trigger strictly adheres to the interval of `interval` (5s) seconds
	var lastTickTime atomic.Value
	lastTickTime.Store(time.Now())

	// autonomous 5-second windower ticker
	// internal ticker to guarantee that the Windower is triggered exactly
	// every `interval` (5s) seconds from within this container, completely decoupling from the
	// external scheduler as long as data is flowing.
	windowerTicker := time.NewTicker(interval)
	defer windowerTicker.Stop()

	// triggering the windower every `interval` seconds,
	// until the maximum session duration (`maxSessionDuration`) is reached.
	go func() {
		for {
			select {
			case <-windowerTicker.C:
				if atomic.LoadInt64(&isShuttingDown) == 1 {
					return
				}

				lastTickTime.Store(time.Now())

				rdb.Expire(sessionCtx, pullLockKey, pullLockTTL)

				currentProcessed := atomic.SwapInt64(&processedSinceLastTick, 0)
				currentFailed := atomic.SwapInt64(&failedSinceLastTick, 0)

				if currentFailed > 0 {
					log.Printf("[PullIngestor] %d messages failed", currentFailed)
				}

				// There could be cases where a window is 2xinterval (10s) long,
				//in which case the last `interval` (5s) are not sufficient to determine whether the window should be triggered
				log.Printf("[PullIngestor] processed %d messages", currentProcessed)
				triggerWindower()

			case <-sessionCtx.Done():
				return
			}
		}
	}()

	// Idle Guardian
	// creates a child context for message pulling.
	// If the queue goes empty and no messages arrive within `drainIdleWindow`,
	// it triggers a background Goroutine to call cancelReceive().
	// This breaks pullSub.Receive immediately
	receiveCtx, cancelReceive := context.WithCancel(sessionCtx)
	idleTimer := time.AfterFunc(drainIdleWindow, func() {
		log.Printf("[PullIngestor] %v of absolute silence (queue empty). Terminating", drainIdleWindow)
		cancelReceive()
	})

	log.Printf("[PullIngestor] starting continuous message consumption with internal %v ticker", interval)

	err = pullSub.Receive(receiveCtx, func(msgCtx context.Context, msg *pubsub.Message) {
		idleTimer.Reset(drainIdleWindow)

		if procErr := processMessage(msgCtx, msg.Data); procErr != nil {
			log.Printf("[PullIngestor] nacking message: %v", procErr)
			msg.Nack()
			atomic.AddInt64(&failedSinceLastTick, 1)
			return
		}
		msg.Ack()
		atomic.AddInt64(&processedSinceLastTick, 1)
	})

	atomic.StoreInt64(&isShuttingDown, 1)

	idleTimer.Stop()
	cancelReceive()

	// Release the Redis lock early. Since the PubSub-Pulling
	// has already terminated, this instance will not fetch any more data.
	// Releasing `pullLockKey` now allows the next scheduled container instance
	// to start working immediately without blockages.
	// We explicitly set `lockReleased = true`. This prevents the
	// top-level `defer` function from executing a second `rdb.Del`.
	rdb.Del(context.Background(), pullLockKey)
	lockReleased = true

	lastTick := lastTickTime.Load().(time.Time)
	timeSinceLastTick := time.Since(lastTick)

	// ensures that the Windower is triggered every `interval`
	if timeSinceLastTick < interval {
		remainingWait := interval - timeSinceLastTick
		log.Printf("[PullIngestor] Strict 5s pacing enforced. Sleeping for %v", remainingWait)
		time.Sleep(remainingWait)
	}

	absoluteDeadline := lastTick.Add(interval).Add(maxDelay)

	// If this goroutine suffered from a massive delay
	// the next container instance might already be active and triggering Windowers.
	// Therefore, this final window trigger is dropped to prevent duplicates.
	if time.Now().After(absoluteDeadline) {
		log.Printf("[PullIngestor] WARNING: Slept too long! Expected total ~%v, but actually passed %v. Dropping final trigger to prevent duplicate window.", interval, time.Since(lastTick))
		w.WriteHeader(http.StatusOK)
		return
	}

	finalProcessed := atomic.SwapInt64(&processedSinceLastTick, 0)

	log.Printf("[PullIngestor] processed %d messages", finalProcessed)
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
