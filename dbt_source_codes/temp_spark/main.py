import functions_framework
from google.cloud import dataproc_v1

@functions_framework.http
def trigger_spark_job(request):
    project_id   = "data-management-2-manoj"
    region       = "europe-west3"
    cluster_name = "my-cluster"
    pyspark_uri  = "gs://cricket_analytics_src/code/load_weather_to_bq.py"

    client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {"main_python_file_uri": pyspark_uri}
    }

    result = client.submit_job(
        project_id=project_id,
        region=region,
        job=job
    )
    return f"Submitted PySpark job {result.reference.job_id}"
