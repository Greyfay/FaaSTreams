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
)

const (
	pullLockKey     = "lock:ingestor:pull"
	pullLockTTL     = 3 * time.Minute
	drainTimeout    = 100 * time.Second
	drainIdleWindow = 5 * time.Second
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
	defer rdb.Del(context.Background(), pullLockKey)

	drainCtx, cancel := context.WithTimeout(ctx, drainTimeout)
	defer cancel()

	var processed, failed int64
	idle := time.AfterFunc(drainIdleWindow, cancel)
	defer idle.Stop()

	err = pullSub.Receive(drainCtx, func(msgCtx context.Context, msg *pubsub.Message) {
		idle.Reset(drainIdleWindow)

		if procErr := processMessage(msgCtx, msg.Data); procErr != nil {
			log.Printf("[PullIngestor] nacking message: %v", procErr)
			msg.Nack()
			atomic.AddInt64(&failed, 1)
			return
		}
		msg.Ack()
		atomic.AddInt64(&processed, 1)
	})
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		http.Error(w, "drain failed", http.StatusInternalServerError)
		log.Printf("[PullIngestor] drain failed: %v", err)
		return
	}

	log.Printf("[PullIngestor] tick complete: processed=%d failed=%d", processed, failed)

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
