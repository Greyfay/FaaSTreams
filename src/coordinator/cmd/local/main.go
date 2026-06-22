package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/faastreams/coordinator/internal/coordinatorcore"

	"cloud.google.com/go/pubsub"
)

func main() {
	ctx := context.Background()

	coordinators := coordinatorcore.SetupFromEnv(ctx)
	_, _, subscription := coordinatorcore.SetupPubSub(ctx)

	mode := os.Getenv("RUN_MODE")
	if mode == "http" {
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
		}
		log.Println("[Main] Starting HTTP server...")
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)

			var pushRequest struct {
				Message struct {
					Data []byte `json:"data"`
				} `json:"message"`
			}
			json.Unmarshal(body, &pushRequest)

			var data map[string]string
			json.Unmarshal(pushRequest.Message.Data, &data)

			if len(coordinators) == 0 {
				w.WriteHeader(http.StatusOK)
				return
			}

			event := coordinators[0].ParseEvent(data)
			if event == nil {
				w.WriteHeader(http.StatusOK)
				return
			}

			coordinators[0].StoreEvent(r.Context(), pushRequest.Message.Data, float64(event.Timestamp.Unix()))

			var minCleanupBound time.Time
			for _, c := range coordinators {
				bound := c.CheckWindow(r.Context(), event)
				if !bound.IsZero() && (minCleanupBound.IsZero() || bound.Before(minCleanupBound)) {
					minCleanupBound = bound
				}
			}

			if !minCleanupBound.IsZero() {
				coordinators[0].Cleanup(r.Context(), minCleanupBound)
			}

			w.WriteHeader(http.StatusOK)
		})
		http.ListenAndServe(":"+port, nil)
	} else {
		log.Println("[Main] Starting subscription receiver...")
		subscription.ReceiveSettings.MaxOutstandingMessages = 1
		subscription.ReceiveSettings.NumGoroutines = 1
		subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			coordinatorcore.HandleSubscriptionMessage(ctx, coordinators, msg)
			msg.Ack()
		})
	}
}
