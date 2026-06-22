package coordinatorcore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/faastreams/coordinator/config"

	"cloud.google.com/go/pubsub"
	"github.com/redis/go-redis/v9"
)

var redisStreamKey = getEnvDefault("REDIS_KEY", "mod-stream")
var coordinatorKeyPrefix = getEnvDefault("COORDINATOR_KEY_PREFIX", "coordinator")

func getEnvDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

type Event struct {
	Timestamp time.Time
	Raw       map[string]string
}

// TODO: Currently only supports tumbling windows, add support for other window types
type Coordinator struct {
	redisClient  *redis.Client
	windowSize   time.Duration
	query        config.Query
	windowEndKey string
}

func NewCoordinator(redisClient *redis.Client, queryConfig config.Query) *Coordinator {
	windowSize := time.Duration(queryConfig.WindowSize) * time.Second

	coordinator := &Coordinator{
		redisClient:  redisClient,
		windowSize:   windowSize,
		query:        queryConfig,
		windowEndKey: fmt.Sprintf("%s:%s:window_end", coordinatorKeyPrefix, queryConfig.Name),
	}

	return coordinator
}

// ParseEvent parses a raw data map into an Event struct.
func (c *Coordinator) ParseEvent(data map[string]string) *Event {
	timestamp, err := time.Parse("02/01/2006 15:04:05", data["# Timestamp"])
	if err != nil {
		return nil
	}

	return &Event{
		Timestamp: timestamp,
		Raw:       data,
	}
}

// StoreEvent writes a single raw event into the shared Redis sorted set.
// This should be called once per message, before fanning out to CheckWindow.
func (c *Coordinator) StoreEvent(ctx context.Context, rawData []byte, score float64) {
	c.redisClient.ZAdd(ctx, redisStreamKey, redis.Z{
		Score:  score,
		Member: string(rawData),
	})
}

// Cleanup removes events from the shared stream older than upperBound.
// Should be called once per message using the minimum bound across all coordinators.
func (c *Coordinator) Cleanup(ctx context.Context, upperBound time.Time) {
	c.redisClient.ZRemRangeByScore(ctx, redisStreamKey, "-inf", strconv.FormatInt(upperBound.Unix(), 10))
}

// CheckWindow tracks window state for this coordinator's query and triggers a worker
// when the window closes. Returns the cleanup upper bound if a window closed, zero time otherwise.
func (c *Coordinator) CheckWindow(ctx context.Context, event *Event) time.Time {
	windowEnd := c.getWindowEnd(ctx)
	if windowEnd.IsZero() {
		windowEnd = event.Timestamp.Add(c.windowSize)
		c.setWindowEnd(ctx, windowEnd)
		log.Printf("[Coordinator:%s] First window ends at: %s\n", c.query.Name, windowEnd.Format("15:04:05"))
		return time.Time{}
	}

	if event.Timestamp.After(windowEnd) {
		windowStart := windowEnd.Add(-c.windowSize)
		c.triggerWorker(ctx, windowStart, windowEnd)
		cleanupBound := windowEnd.Add(-2 * c.windowSize)
		windowEnd = windowEnd.Add(c.windowSize)
		c.setWindowEnd(ctx, windowEnd)
		return cleanupBound
	}
	return time.Time{}
}

func (c *Coordinator) getWindowEnd(ctx context.Context) time.Time {
	val, err := c.redisClient.Get(ctx, c.windowEndKey).Result()
	if err == redis.Nil {
		log.Printf("[Coordinator:%s] window_end key missing in Redis, treating as first window\n", c.query.Name)
		return time.Time{}
	}
	if err != nil {
		log.Printf("[Coordinator:%s] ERROR reading window_end from Redis: %v\n", c.query.Name, err)
		return time.Time{}
	}
	unix, _ := strconv.ParseInt(val, 10, 64)
	return time.Unix(unix, 0)
}

func (c *Coordinator) setWindowEnd(ctx context.Context, t time.Time) {
	c.redisClient.Set(ctx, c.windowEndKey, t.Unix(), 0)
}

func (c *Coordinator) triggerWorker(ctx context.Context, windowStart time.Time, windowEnd time.Time) {
	lockKey := fmt.Sprintf("%s:%s:lock:%d", coordinatorKeyPrefix, c.query.Name, windowEnd.Unix())

	locked, err := c.redisClient.SetNX(ctx, lockKey, "1", 5*time.Minute).Result()
	if err != nil || !locked {
		log.Printf("[Coordinator:%s] Window %s already being processed, skipping\n", c.query.Name, windowEnd.Format("15:04:05"))
		return
	}

	minScore := strconv.FormatInt(windowStart.Unix(), 10)
	maxScore := strconv.FormatInt(windowEnd.Unix(), 10)

	log.Printf("[Coordinator:%s] Triggering worker for window (scores): %s(%s) - %s(%s)\n", c.query.Name, minScore, windowStart.Format("15:04:05"), maxScore, windowEnd.Format("15:04:05"))
	workerURL := os.Getenv("WORKER_URL")

	data := map[string]interface{}{
		"window_start": windowStart.Unix(),
		"window_end":   windowEnd.Unix(),
		"query":        c.query.Query,
		"query_name":   c.query.Name,
		"return_type":  c.query.ReturnType,
	}

	dataBytes, _ := json.Marshal(data)

	go func() {
		resp, err := http.Post(workerURL, "application/json", bytes.NewBuffer(dataBytes))
		if err != nil {
			log.Printf("[Coordinator:%s] Failed to spawn worker: %v\n", c.query.Name, err)
			return
		}
		defer resp.Body.Close()
		log.Printf("[Coordinator:%s] Worker spawned\n", c.query.Name)
	}()
}

// HandleSubscriptionMessage is used in local subscription mode to process a single
// Pub/Sub message across all coordinators with a single Redis write and cleanup.
func HandleSubscriptionMessage(ctx context.Context, coordinators []*Coordinator, msg *pubsub.Message) {
	var data map[string]string
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return
	}

	if len(coordinators) == 0 {
		return
	}

	event := coordinators[0].ParseEvent(data)
	if event == nil {
		return
	}

	coordinators[0].StoreEvent(ctx, msg.Data, float64(event.Timestamp.Unix()))

	var minCleanupBound time.Time
	for _, c := range coordinators {
		bound := c.CheckWindow(ctx, event)
		if !bound.IsZero() && (minCleanupBound.IsZero() || bound.Before(minCleanupBound)) {
			minCleanupBound = bound
		}
	}

	if !minCleanupBound.IsZero() {
		coordinators[0].Cleanup(ctx, minCleanupBound)
	}
}
