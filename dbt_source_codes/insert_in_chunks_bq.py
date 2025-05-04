#!/usr/bin/env python3
"""
Cloud Function: raw_json_ingestion_bq

Reads JSON files from GCS and loads new ones into BigQuery with metadata.
Uses ADC, infers project from environment, adds structured logging.
"""
import os
import json
import logging
from datetime import datetime, timezone

import pandas as pd
from flask import Request, make_response
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound

# ─── CONFIG ────────────────────────────────────────────────────────────────────
# Project is inferred from ADC; explicit PROJECT_ID not required
DATASET_ID  = os.getenv("BQ_DATASET", "cricket_raw")  # overrideable via env
TABLE_ID    = os.getenv("BQ_TABLE", "cricket_match_raw")
BUCKET_NAME = os.getenv("BUCKET_NAME", "cricket_analytics_src")
PREFIX      = os.getenv("JSON_PREFIX", "")
BATCH_SIZE  = int(os.getenv("BATCH_SIZE", "75"))

# ─── LOGGING SETUP ─────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── CLIENTS (ADC) ─────────────────────────────────────────────────────────────
storage_client = storage.Client()
bq_client      = bigquery.Client()


def ensure_table(dataset_id: str, table_id: str):
    project = bq_client.project
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref   = dataset_ref.table(table_id)
    try:
        bq_client.get_table(table_ref)
        logger.info(f"BQ table {project}.{dataset_id}.{table_id} exists.")
    except NotFound:
        logger.info(f"Creating dataset {project}.{dataset_id}")
        try:
            bq_client.get_dataset(dataset_ref)
        except NotFound:
            bq_client.create_dataset(dataset_ref)
            logger.info(f"Created dataset {project}.{dataset_id}.")
        schema = [
            bigquery.SchemaField("file_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("content", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("file_upload_timestamp", "TIMESTAMP", mode="REQUIRED"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        logger.info(f"Created BQ table {project}.{dataset_id}.{table_id}.")
    return table_ref


def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def load_json_files_to_bq():
    # 1) Ensure table
    table_ref = ensure_table(DATASET_ID, TABLE_ID)

    # 2) Fetch existing file_names
    project = bq_client.project
    query = f"SELECT file_name FROM `{project}.{DATASET_ID}.{TABLE_ID}`"
    existing = {row.file_name for row in bq_client.query(query).result()}
    logger.info(f"Found {len(existing)} existing files in BQ.")

    # 3) List GCS blobs
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs  = bucket.list_blobs(prefix=PREFIX)
    rows = []
    for blob in blobs:
        if not blob.name.lower().endswith('.json'):
            continue
        fn = blob.name if not PREFIX else blob.name[len(PREFIX):]
        if fn in existing:
            logger.debug(f"Skipping already-loaded {fn}")
            continue
        logger.info(f"Reading new file: {fn}")
        raw = blob.download_as_text()
        try:
            json.loads(raw)
        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON {fn}: {e}, skipping.")
            continue
        rows.append({
            "file_name": fn,
            "content": raw,
            "file_upload_timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
        })
    if not rows:
        logger.info("No new JSON files to load.")
        return

    # 4) Insert in batches
    total, errs = 0, 0
    for i, batch in enumerate(chunked(rows, BATCH_SIZE), start=1):
        logger.info(f"Inserting batch {i} of size {len(batch)}")
        errors = bq_client.insert_rows_json(table_ref, batch)
        if errors:
            logger.error(f"Errors in batch {i}: {errors}")
            errs += len(errors)
        else:
            total += len(batch)
    logger.info(f"Insert complete: {total} rows, {errs} errors.")


def insert_jsons_to_bq_fn(request: Request):
    """
    HTTP Cloud Function entry point for loading JSONs into BigQuery.
    """
    try:
        load_json_files_to_bq()
        return make_response("JSON load completed.", 200)
    except Exception as e:
        logger.exception("Error loading JSONs to BQ")
        return make_response(f"Error: {e}", 500)
