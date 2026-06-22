package coordinator

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/faastreams/coordinator/internal/coordinatorcore"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
)

var coordinators []*coordinatorcore.Coordinator

func init() {
	coordinators = coordinatorcore.SetupFromEnv(context.Background())
	functions.HTTP("Handler", Handler)
}

// Handler is the Cloud Functions entry point, receiving Pub/Sub push messages over HTTP.
func Handler(w http.ResponseWriter, r *http.Request) {
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
		if bound.IsZero() {
			minCleanupBound = time.Time{}
			break
		}
		if minCleanupBound.IsZero() || bound.Before(minCleanupBound) {
			minCleanupBound = bound
		}
	}

	if !minCleanupBound.IsZero() {
		coordinators[0].Cleanup(r.Context(), minCleanupBound)
	}

	w.WriteHeader(http.StatusOK)
}
