#!/bin/bash
set -e

# Purge whatever's still queued in Pub/Sub from previous test runs. Resetting
# Redis alone doesn't touch this — Eventarc's push subscription retries failed
# deliveries with backoff instead of dropping them, so old test runs can leave
# millions of stale messages queued up, silently polluting the next run.
SUBSCRIPTION=$(gcloud pubsub subscriptions list --filter="topic:ais-stream" --format="value(name)" | head -1)
if [ -z "$SUBSCRIPTION" ]; then
  echo "Could not find Pub/Sub subscription for topic ais-stream — skipping backlog purge"
else
  echo "Purging Pub/Sub backlog on $SUBSCRIPTION..."
  gcloud pubsub subscriptions seek "$SUBSCRIPTION" --time="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
  echo "Backlog purged."
fi

gcloud compute ssh redis-bastion --zone europe-west3-a --command "
  redis-cli -h 10.101.64.19 -p 6379 DEL \
    data:ais_data_v1 \
    analytics-results \
    window:next:ais_data_v1 \
    lock:ais_data_v1:hazard_zones_proximity_alerts
"

echo "Keys deleted. Run the simulation now, then press Enter to seed the window pointer..."
read

gcloud compute ssh redis-bastion --zone europe-west3-a --command "
  SCORE=\$(redis-cli -h 10.101.64.19 -p 6379 ZRANGE data:ais_data_v1 0 0 WITHSCORES | tail -1)
  if [ -z \"\$SCORE\" ]; then
    echo 'No data found in data:ais_data_v1 — seed skipped'
  else
    redis-cli -h 10.101.64.19 -p 6379 ZADD window:next:ais_data_v1 \$SCORE hazard_zones_proximity_alerts
    echo \"Seeded window:next:ais_data_v1 with score \$SCORE\"
  fi
"
