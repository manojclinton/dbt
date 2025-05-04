#!/usr/bin/env python3
"""
fetch_schedule_weather.py

1) Reads the IPL schedule from GCS.
2) Skips any match already in the enriched output.
3) Only processes matches whose datetime ≤ today.
4) Batches by season to pace API calls.
5) Fetches hourly weather via Open-Meteo with retry/backoff.
6) Deletes and rewrites the enriched CSV in GCS with a fixed column order.
"""

import os
import io
import time
import datetime
import requests
import pandas as pd

from google.oauth2 import service_account
from google.cloud import storage

# ─── CONFIG ────────────────────────────────────────────────────────────────────
PROJECT_ID    = "data-management-2-manoj"
BUCKET_NAME   = "cricket_analytics_src"
SCHEDULE_PATH = "schedule/ipl_full_schedule.csv"
OUTPUT_PATH   = "schedule/ipl_full_schedule_with_weather.csv"
KEY_PATH      = os.path.join(
    os.path.dirname(__file__),
    "data-management-2-manoj-67d7f9a199ea.json"
)

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

# ─── GCS CLIENT SETUP ──────────────────────────────────────────────────────────
creds  = service_account.Credentials.from_service_account_file(KEY_PATH)
client = storage.Client(project=PROJECT_ID, credentials=creds)
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
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def safe_fetch(lat, lon, date_iso, vars_list, timezone):
    for attempt in range(1, MAX_RETRIES+1):
        try:
            return fetch_hourly_archive(lat, lon, date_iso, vars_list, timezone)
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES:
                wait = 2 ** (attempt - 1)
                print(f" ⚠️ Fetch attempt {attempt} failed: {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f" ❌ All {MAX_RETRIES} fetch attempts failed: {e}. Skipping this match.")
                return None


def parse_dt(date_str, time_str):
    # date_str="DD/MM/YYYY", time_str="HH:MM"
    return datetime.datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M")


def download_csv_from_gcs(blob_path):
    blob = bucket.blob(blob_path)
    if not blob.exists(client):
        return None
    data = blob.download_as_string()
    return pd.read_csv(io.StringIO(data.decode("utf-8")))


def upload_df_to_gcs(df, blob_path):
    blob = bucket.blob(blob_path)
    # delete old file to avoid overwrite issues
    if blob.exists(client):
        print(f"ℹ️ Deleting existing blob: {blob_path}")
        blob.delete(client)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    blob.upload_from_string(csv_bytes, content_type="text/csv")


def main():
    # 1) Download schedule
    print(f"⏳ Downloading schedule from gs://{BUCKET_NAME}/{SCHEDULE_PATH}")
    df_sched = download_csv_from_gcs(SCHEDULE_PATH)
    if df_sched is None:
        raise SystemExit("❌ Schedule file not found in GCS.")
    print(f"✔️ Loaded {len(df_sched)} matches.")

    # 2) Load existing enriched
    df_old = download_csv_from_gcs(OUTPUT_PATH)
    if df_old is None:
        print("ℹ️ No enriched file exists: starting fresh.")
        df_old = pd.DataFrame(columns=[
            "season","match_id","city","match_num","venue",
            "match_date","match_time","team1","team2","venue_id",
            "latitude","longitude","datetime","temp_C","humidity_%",
            "pressure_hPa","cloudcover_%","rain_mm","wind_m_s"
        ])
    else:
        print(f"ℹ️ Found {len(df_old)} enriched rows.")

    # 3) Filter past-or-today matches
    now = datetime.datetime.now()
    df_sched["dt_obj"] = df_sched.apply(
        lambda r: parse_dt(r["match_date"], r["match_time"]), axis=1)
    df_past = df_sched[df_sched["dt_obj"] <= now].copy()
    print(f"✔️ {len(df_past)} matches ≤ today.")

    # 4) Skip already fetched
    done_ids = set(df_old["match_id"].astype(str))
    df_new = df_past[~df_past["match_id"].astype(str).isin(done_ids)].copy()
    print(f"✔️ {len(df_new)} new matches to enrich.")
    if df_new.empty:
        print("✅ Nothing new to fetch; exiting.")
        return

    # 5) Batch by season
    records = []
    for season, grp in df_new.groupby("season"):
        print(f"\n--- Season {season} ({len(grp)} matches) ---")
        for _, row in grp.iterrows():
            dt = row["dt_obj"]
            iso_dt = dt.strftime("%Y-%m-%dT%H:00")
            date_iso = dt.date().isoformat()
            lat, lon = row["latitude"], row["longitude"]

            print(f"Fetching match {row['match_id']} @ {iso_dt}…", end="")
            data = safe_fetch(lat, lon, date_iso, HOURLY_VARS, TIMEZONE)
            if not data:
                print(" skipped.")
                continue

            times = data.get("hourly", {}).get("time", [])
            if iso_dt not in times:
                print(" ⚠️ missing hour, skipped.")
                continue

            idx = times.index(iso_dt)
            rec = row.drop(["dt_obj"]).to_dict()
            rec.update({
                "datetime":     iso_dt,
                "temp_C":       data["hourly"]["temperature_2m"][idx],
                "humidity_%":   data["hourly"]["relativehumidity_2m"][idx],
                "pressure_hPa": data["hourly"]["pressure_msl"][idx],
                "cloudcover_%": data["hourly"]["cloudcover"][idx],
                "rain_mm":      data["hourly"]["rain"][idx],
                "wind_m_s":     data["hourly"]["wind_speed_10m"][idx],
            })
            records.append(rec)
            print(" done.")
            time.sleep(PAUSE_SEC)

    # 6) Combine and enforce order
    df_new_enriched = pd.DataFrame(records)
    print(f"\n✔️ Fetched weather for {len(df_new_enriched)} matches.")

    df_combined = pd.concat([df_old, df_new_enriched], ignore_index=True)
    schedule_cols = [
        "season","match_id","city","match_num","venue",
        "match_date","match_time","team1","team2","venue_id",
        "latitude","longitude"
    ]
    weather_cols = [
        "datetime","temp_C","humidity_%","pressure_hPa",
        "cloudcover_%","rain_mm","wind_m_s"
    ]
    df_combined = df_combined[schedule_cols + weather_cols]

    # 7) Upload
    print(f"⏳ Uploading {len(df_combined)} rows to gs://{BUCKET_NAME}/{OUTPUT_PATH}")
    upload_df_to_gcs(df_combined, OUTPUT_PATH)
    print(f"✅ Done.")

if __name__ == "__main__":
    main()
