import requests

# Set your Databricks workspace URL and personal access token
DATABRICKS_INSTANCE = "https://<your-databricks-instance>"  # e.g., https://adb-1234567890123456.7.azuredatabricks.net
TOKEN = "<your-personal-access-token>"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}"
}

def pause_all_jobs():
    """Pause all scheduled jobs by removing their schedule."""
    jobs_url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/list"
    jobs = requests.get(jobs_url, headers=HEADERS).json().get("jobs", [])
    for job in jobs:
        job_id = job["job_id"]
        update_url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/update"
        payload = {
            "job_id": job_id,
            "schedule": None
        }
        resp = requests.post(update_url, headers=HEADERS, json=payload)
        print(f"Paused job {job_id}: {resp.status_code}")

def pause_all_pipelines():
    """Stop all Delta Live Tables pipelines."""
    pipelines_url = f"{DATABRICKS_INSTANCE}/api/2.0/pipelines"
    pipelines = requests.get(pipelines_url, headers=HEADERS).json().get("statuses", [])
    for pipeline in pipelines:
        pipeline_id = pipeline["pipeline_id"]
        stop_url = f"{DATABRICKS_INSTANCE}/api/2.0/pipelines/{pipeline_id}/stop"
        resp = requests.post(stop_url, headers=HEADERS)
        print(f"Stopped pipeline {pipeline_id}: {resp.status_code}")

if __name__ == "__main__":
    pause_all_jobs()
    pause_all_pipelines()
