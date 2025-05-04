#!/usr/bin/env python3
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage
from google.oauth2 import service_account

# --- CONFIG ---
PROJECT_ID      = "data-management-2-manoj"    # can be omitted for storage-only
BUCKET_NAME     = "cricket_analytics_src"
PREFIX          = ""                           # e.g. "matches/" or "" for root
LOCAL_JSON_DIR  = "C:/Users/Manoj/Documents/DBT/ipl_json"    # ◀─ MAKE SURE this is correct!
KEY_PATH        = os.path.join(os.path.dirname(__file__),
                               "data-management-2-manoj-67d7f9a199ea.json")

# Build creds & client
creds          = service_account.Credentials.from_service_account_file(KEY_PATH)
storage_client = storage.Client(credentials=creds)
bucket         = storage_client.bucket(BUCKET_NAME)


def get_existing_files():
    existing = set()
    for blob in bucket.list_blobs(prefix=PREFIX):
        name = blob.name
        if PREFIX and name.startswith(PREFIX):
            name = name[len(PREFIX):]
        existing.add(name)
    return existing


def get_local_files():
    return {
        fn for fn in os.listdir(LOCAL_JSON_DIR)
        if fn.lower().endswith(".json")
    }


def upload_file(filename: str):
    source = os.path.join(LOCAL_JSON_DIR, filename)
    dest   = f"{PREFIX}{filename}" if PREFIX else filename
    blob   = bucket.blob(dest)
    blob.upload_from_filename(source)
    print(f"✅ Uploaded {filename}")


def main():
    print("🔍 Local JSON dir is:", LOCAL_JSON_DIR)
    local      = get_local_files()
    print(f"📂 Local files ({len(local)}):", sorted(local)[:10], "…")
    
    existing   = get_existing_files()
    print(f"☁️  Existing in GCS ({len(existing)}):", sorted(existing)[:10], "…")
    
    to_upload  = local - existing
    print(f"🆕 To upload ({len(to_upload)}):", sorted(to_upload))
    
    if not to_upload:
        print("🎉 Nothing to upload, exiting.")
        return

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = { pool.submit(upload_file, fn): fn for fn in to_upload }
        for fut in as_completed(futures):
            fn = futures[fut]
            try:
                fut.result()
            except Exception as e:
                print(f"❌ Error uploading {fn}: {e}")

    print("✅ All done.")


if __name__ == "__main__":
    main()
