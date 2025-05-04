#!/usr/bin/env python3
"""
Cloud Function: final_spark_submit
Triggers a Dataproc PySpark job to load the enriched IPL weather CSV into BigQuery.
"""
import logging
import functions_framework
from flask import Request, make_response
from google.cloud import dataproc_v1

# ─── LOGGING SETUP ─────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── CONFIG ───────────────────────────────────────────────────────────
PROJECT_ID   = "data-management-2-manoj"
REGION       = "europe-west3"
CLUSTER_NAME = "my-cluster"
PYSPARK_URI  = "gs://cricket_analytics_src/code/load_weather_to_bq.py"

@functions_framework.http
def trigger_spark_job(request: Request):
    """
    HTTP Cloud Function entry point.
    Submits a PySpark job to Dataproc.
    """
    try:
        logger.info(f"Using project={PROJECT_ID}, region={REGION}, cluster={CLUSTER_NAME}")
        job_client = dataproc_v1.JobControllerClient(
            client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
        )
        job = {
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
        }
        operation = job_client.submit_job_as_operation(
            request={"project_id": PROJECT_ID, "region": REGION, "job": job}
        )
        result = operation.result()  # waits until the submission is accepted
        job_id = result.reference.job_id
        logger.info(f"Submitted job ID: {job_id}")
        return make_response(f"Spark job submitted: {job_id}", 200)

    except Exception as e:
        logger.exception("Error submitting Spark job")
        return make_response(f"Error: {e}", 500)
