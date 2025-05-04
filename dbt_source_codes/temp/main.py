#!/usr/bin/env python3
import os
import io
import time
import datetime
import requests
import pandas as pd
import logging

from google.cloud import storage
from flask import Request, make_response

# ─── CONFIG ────────────────────────────────────────────────────────────────────
PROJECT_ID    = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
BUCKET_NAME   = "cricket_analytics_src"
SCHEDULE_PATH = "schedule/ipl_full_schedule.csv"
OUTPUT_PATH   = "schedule/ipl_full_schedule_with_weather.csv"

HOURLY_VARS = [
    "temperature_2m",
    "relativehumidity_2m",
    "pressure_msl",
    "cloudcover",
    "rain",
    "wind_speed_10m",
]
TIMEZONE    = "Asia/Kolkata"
PAUSE_SEC   = 1.0
MAX_RETRIES = 3  # number of fetch retries

# ─── LOGGING SETUP ─────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── GCS CLIENT SETUP ──────────────────────────────────────────────────────────
client = storage.Client(project=PROJECT_ID)
bucket = client.bucket(BUCKET_NAME)


def fetch_hourly_archive(lat, lon, date_iso, vars_list, timezone):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": date_iso,
        "end_date":   date_iso,
        "hourly":     ",".join(vars_list),
        "timezone":   timezone,
    }
    logger.debug(f"Requesting weather API for {lat},{lon} @ {date_iso}")
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def safe_fetch(lat, lon, date_iso, vars_list, timezone):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            data = fetch_hourly_archive(lat, lon, date_iso, vars_list, timezone)
            return data
        except requests.exceptions.RequestException as e:
            logger.warning(f"Fetch attempt {attempt} failed for {lat},{lon} on {date_iso}: {e}")
            if attempt < MAX_RETRIES:
                wait = 2 ** (attempt - 1)
                logger.info(f"Retrying in {wait} seconds...")
                time.sleep(wait)
            else:
                logger.error(f"All {MAX_RETRIES} fetch attempts failed for {lat},{lon} on {date_iso}")
                return None


def parse_dt(date_str, time_str):
    dt = datetime.datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M")
    logger.debug(f"Parsed datetime string {date_str} {time_str} into {dt}")
    return dt


def download_csv_from_gcs(blob_path):
    logger.info(f"Downloading CSV from gs://{BUCKET_NAME}/{blob_path}")
    blob = bucket.blob(blob_path)
    if not blob.exists(client):
        logger.warning(f"Blob not found: {blob_path}")
        return None
    data = blob.download_as_string()
    return pd.read_csv(io.StringIO(data.decode("utf-8")))


def upload_df_to_gcs(df, blob_path):
    logger.info(f"Uploading DataFrame to gs://{BUCKET_NAME}/{blob_path}")
    blob = bucket.blob(blob_path)
    if blob.exists(client):
        logger.info(f"Deleting existing blob before upload: {blob_path}")
        blob.delete(client)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    blob.upload_from_string(csv_bytes, content_type="text/csv")


def main():
    # 1) Download schedule
    df_sched = download_csv_from_gcs(SCHEDULE_PATH)
    if df_sched is None:
        logger.error("Schedule file not found; aborting.")
        raise RuntimeError("Schedule file not found in GCS.")
    logger.info(f"Loaded {len(df_sched)} schedule rows.")

    # 2) Load existing enriched (explicit check for None)
    df_old_raw = download_csv_from_gcs(OUTPUT_PATH)
    if df_old_raw is None:
        df_old = pd.DataFrame(columns=[*df_sched.columns.tolist(),
                                       "datetime","temp_C","humidity_%","pressure_hPa",
                                       "cloudcover_%","rain_mm","wind_m_s"] )
        logger.info("No existing enriched file: starting with empty DataFrame.")
    else:
        df_old = df_old_raw
        logger.info(f"Found {len(df_old)} existing enriched rows.")

    # 3) Filter past-or-today matches
    now = datetime.datetime.now()
    df_sched["dt_obj"] = df_sched.apply(
        lambda r: parse_dt(r["match_date"], r["match_time"]), axis=1
    )
    df_past = df_sched[df_sched["dt_obj"] <= now]
    logger.info(f"{len(df_past)} matches on or before {now} to process.")

    # 4) Skip already fetched
    done_ids = set(df_old["match_id"].astype(str).tolist())
    df_new = df_past[~df_past["match_id"].astype(str).isin(done_ids)]
    logger.info(f"{len(df_new)} new matches to enrich.")
    if df_new.empty:
        logger.info("No new matches; exiting.")
        return

    # 5) Batch by season & fetch
    records = []
    for season, grp in df_new.groupby("season"):
        logger.info(f"Processing season {season} with {len(grp)} matches.")
        for _, row in grp.iterrows():
            dt      = row["dt_obj"]
            iso_dt  = dt.strftime("%Y-%m-%dT%H:00")
            date_iso= dt.date().isoformat()
            lat, lon= row["latitude"], row["longitude"]

            logger.debug(f"Fetching weather for match {row['match_id']} @ {iso_dt}")
            data = safe_fetch(lat, lon, date_iso, HOURLY_VARS, TIMEZONE)
            if not data:
                continue

            times = data.get("hourly", {}).get("time", [])
            if iso_dt not in times:
                logger.warning(f"Hour {iso_dt} not in API response; skipping.")
                continue

            idx = times.index(iso_dt)
            rec = row.drop(["dt_obj"]).to_dict()
            rec.update({
                "datetime":   iso_dt,
                "temp_C":     data["hourly"]["temperature_2m"][idx],
                "humidity_%": data["hourly"]["relativehumidity_2m"][idx],
                "pressure_hPa": data["hourly"]["pressure_msl"][idx],
                "cloudcover_%": data["hourly"]["cloudcover"][idx],
                "rain_mm":      data["hourly"]["rain"][idx],
                "wind_m_s":     data["hourly"]["wind_speed_10m"][idx],
            })
            records.append(rec)
            logger.info(f"Weather fetched for match {row['match_id']}")
            time.sleep(PAUSE_SEC)

    # 6) Combine, reorder & upload
    df_new_enriched = pd.DataFrame(records)
    df_combined = pd.concat([df_old, df_new_enriched], ignore_index=True)
    schedule_cols = ["season","match_id","city","match_num","venue","match_date","match_time","team1","team2","venue_id","latitude","longitude"]
    weather_cols  = ["datetime","temp_C","humidity_%","pressure_hPa","cloudcover_%","rain_mm","wind_m_s"]
    df_combined = df_combined[schedule_cols + weather_cols]

    logger.info(f"Uploading total {len(df_combined)} rows to GCS.")
    upload_df_to_gcs(df_combined, OUTPUT_PATH)
    logger.info("Upload complete.")


# ─── CLOUD FUNCTION ENTRY POINT ────────────────────────────────────────────────
def fetch_schedule_weather(request: Request):
    try:
        main()
        return make_response("Weather enrichment completed.", 200)
    except Exception as e:
        logger.exception("Error in weather enrichment")
        return make_response(f"Error: {e}", 500)
