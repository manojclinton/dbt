#!/usr/bin/env python3
"""
Load all JSON files from gs://cricket_analytics_src into
BigQuery table
  data-management-2-manoj.cricket_raw.cricket_match_raw
with three columns:
  1) file_name             (STRING)
  2) content               (JSON)
  3) file_upload_timestamp (TIMESTAMP)

Only new files (by name) are loaded; existing names are skipped.
Authentication is done via the service‐account JSON key file
in the same folder as this script.
"""

import os
import json
from datetime import datetime, timezone

from google.cloud import storage, bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

# --- CONFIG ---
PROJECT_ID  = "data-management-2-manoj"
BUCKET_NAME = "cricket_analytics_src"
PREFIX      = ""                   # root of bucket; change if needed
DATASET_ID  = "cricket_raw"
TABLE_ID    = "cricket_match_raw"
# --------------

# Path to your service-account key file (next to this script)
KEY_PATH = os.path.join(
    os.path.dirname(__file__),
    "data-management-2-manoj-67d7f9a199ea.json"
)

# Build credentials & clients
creds          = service_account.Credentials.from_service_account_file(KEY_PATH)
storage_client = storage.Client(credentials=creds, project=PROJECT_ID)
bq_client      = bigquery.Client(credentials=creds, project=PROJECT_ID)


def ensure_table(dataset_id: str, table_id: str) -> bigquery.TableReference:
    """Make sure the target table exists with the proper schema."""
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, dataset_id)
    table_ref   = dataset_ref.table(table_id)

    try:
        bq_client.get_table(table_ref)
        print(f"Table `{PROJECT_ID}.{dataset_id}.{table_id}` already exists.")
    except NotFound:
        schema = [
            bigquery.SchemaField("file_name",              "STRING",    mode="REQUIRED"),
            bigquery.SchemaField("content",                "JSON",      mode="REQUIRED"),
            bigquery.SchemaField("file_upload_timestamp",  "TIMESTAMP", mode="REQUIRED"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        print(f"Created table `{PROJECT_ID}.{dataset_id}.{table_id}`.")
    return table_ref


def load_json_files_to_bq(
    bucket_name: str,
    prefix: str,
    dataset_id: str,
    table_id: str
) -> None:
    # 1) Ensure the table exists
    table_ref = ensure_table(dataset_id, table_id)

    # 2) Fetch already-loaded file names from BigQuery
    print("Fetching list of already-loaded file names from BigQuery…")
    existing_files = set()
    query = f"""
      SELECT file_name
      FROM `{PROJECT_ID}.{dataset_id}.{table_id}`
    """
    for row in bq_client.query(query).result():
        existing_files.add(row.file_name)
    print(f"  → {len(existing_files)} files already in table; new ones will be loaded.")

    # 3) List GCS blobs, filter to only new JSONs, validate & prepare rows
    bucket         = storage_client.bucket(bucket_name)
    blobs          = bucket.list_blobs(prefix=prefix)
    rows_to_insert = []

    for blob in blobs:
        # skip non-JSON
        if not blob.name.lower().endswith(".json"):
            continue

        # strip prefix if used
        fn = blob.name if not prefix else blob.name[len(prefix):]
        if fn in existing_files:
            print(f"Skipping `{fn}` (already loaded).")
            continue

        print(f"Reading new file gs://{bucket_name}/{blob.name}…")
        raw = blob.download_as_text()

        # validate JSON
        try:
            json.loads(raw)
        except json.JSONDecodeError as e:
            print(f"  → skip `{fn}`: invalid JSON ({e})")
            continue

        # **CONVERT datetime to RFC3339 string** before inserting**
        rows_to_insert.append({
            "file_name":             fn,
            "content":               raw,
            "file_upload_timestamp": datetime.now(timezone.utc)
                                      .isoformat().replace("+00:00", "Z")
        })

    if not rows_to_insert:
        print("No new JSON files to insert.")
        return

    # 4) Insert into BigQuery
    errors = bq_client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        print("Encountered errors during insert:")
        for err in errors:
            print(err)
    else:
        print(f"Successfully inserted {len(rows_to_insert)} new files into "
              f"`{PROJECT_ID}.{dataset_id}.{table_id}`.")


if __name__ == "__main__":
    load_json_files_to_bq(BUCKET_NAME, PREFIX, DATASET_ID, TABLE_ID)
