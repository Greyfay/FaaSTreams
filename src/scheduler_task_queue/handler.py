import functions_framework
from google.cloud import tasks_v2
import time
import json

@functions_framework.http
def windower_sub_1_trigger(request):
    project_id = "faastreams"
    project_region = "europe-west3"
    queue_id = "faastreams-queue"
    worker_url = "https://europe-west3-faastreams.cloudfunctions.net/windower"

    client = tasks_v2.CloudTasksClient()
    queue_path = client.queue_path(project_id, project_region, queue_id)

    current_time = int(time.time())

    for i in range(12):
        delay = i * 5
        execution_time = current_time + delay

        # Create the task dictionary exactly like the basic GCP documentation examples
        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": worker_url,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"step": i, "delay": delay}).encode("utf-8")
            },
            "schedule_time": {
                "seconds": execution_time
            }
        }

        client.create_task(request={"parent": queue_path, "task": task})
        human_readable_time = time.ctime(execution_time)
        print(f"[Scheduler Queue Tasks] Sent task {i} scheduled for {human_readable_time}")

    return "All tasks succesfully triggered", 200