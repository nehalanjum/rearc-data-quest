# Databricks notebook source
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.rearc")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.rearc.raw")

# COMMAND ----------

import os, re, json, time, hashlib, shutil, requests
from urllib.parse import urljoin
from datetime import datetime, timezone

CONTACT_EMAIL = "nehalanjuman@gmail.com"
HEADERS = {"User-Agent": f"RearcDataQuest/1.0 (contact: {CONTACT_EMAIL})"}   # fixes the BLS 403

BLS_URL = "https://download.bls.gov/pub/time.series/pr/"
POP_URL = ("https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
           "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population")

BASE = "/Volumes/workspace/rearc/raw"
BLS_DIR = f"{BASE}/bls/pr"
POP_DIR = f"{BASE}/datausa/population"
TMP_DIR = f"{BASE}/_tmp"
ARCHIVE_DIR = f"{BASE}/_archive"
MANIFEST_PATH = f"{BASE}/_manifest/manifest.json"

for d in (BLS_DIR, POP_DIR, TMP_DIR, ARCHIVE_DIR, os.path.dirname(MANIFEST_PATH)):
    os.makedirs(d, exist_ok=True)

session = requests.Session()
session.headers.update(HEADERS)

def load_manifest():
    if os.path.exists(MANIFEST_PATH):
        with open(MANIFEST_PATH) as f:
            return json.load(f)
    return {"bls": {}, "population": {}}

def save_manifest(m):
    with open(MANIFEST_PATH, "w") as f:
        json.dump(m, f, indent=2)

def snapshot_id():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")   # sortable, unique

# COMMAND ----------

def list_remote_files():
    r = session.get(BLS_URL, timeout=60)
    r.raise_for_status()
    files = {}
    for href in re.findall(r'href="([^"]+)"', r.text, flags=re.I):
        url = urljoin(BLS_URL, href)
        if url.startswith(BLS_URL) and url != BLS_URL and not url.endswith("/"):
            files[os.path.basename(url)] = url
    return files

def download_with_hash(url, tmp_path):
    h = hashlib.sha256()
    with session.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                f.write(chunk)
                h.update(chunk)
    return h.hexdigest()

manifest = load_manifest()
remote = list_remote_files()
print(f"Files at source: {len(remote)} -> {sorted(remote)}")

downloaded, skipped = [], []
for name, url in remote.items():
    time.sleep(1)                                   # be polite: BLS rate limits
    tmp = f"{TMP_DIR}/{name}.part"
    digest = download_with_hash(url, tmp)
    if manifest["bls"].get(name, {}).get("sha256") == digest:
        os.remove(tmp)
        skipped.append(name)
        continue
    snap = snapshot_id()
    dest_dir = f"{BLS_DIR}/{name}/{snap}"
    os.makedirs(dest_dir, exist_ok=True)
    shutil.move(tmp, f"{dest_dir}/{name}")
    manifest["bls"][name] = {"sha256": digest, "snapshot": snap}
    save_manifest(manifest)                         # save after every file: safe if the run dies midway
    downloaded.append(name)

# files that disappeared from the source: archive them (never silently lose raw history)
removed = []
if remote:   # safety: an empty listing means something went wrong, not "everything was deleted"
    for name in [n for n in manifest["bls"] if n not in remote]:
        src = f"{BLS_DIR}/{name}"
        if os.path.exists(src):
            shutil.move(src, f"{ARCHIVE_DIR}/{name}_{snapshot_id()}")
        del manifest["bls"][name]
        removed.append(name)
    save_manifest(manifest)

print("Downloaded:", downloaded)
print("Skipped (unchanged):", skipped)
print("Archived (removed at source):", removed)

# COMMAND ----------

r = session.get(POP_URL, timeout=60)
r.raise_for_status()
payload = r.json()
digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()

manifest = load_manifest()
if manifest["population"].get("sha256") == digest:
    print("Population unchanged, skipped")
else:
    snap = snapshot_id()
    os.makedirs(f"{POP_DIR}/{snap}", exist_ok=True)
    with open(f"{POP_DIR}/{snap}/population.json", "w") as f:
        json.dump(payload, f)
    manifest["population"] = {"sha256": digest, "snapshot": snap}
    save_manifest(manifest)
    print("Population saved")

print("Top-level type/keys:", list(payload.keys()) if isinstance(payload, dict) else type(payload))
records = payload["data"] if isinstance(payload, dict) and "data" in payload else payload
print("First record:", records[0])

# COMMAND ----------

display(dbutils.fs.ls(BLS_DIR))
display(dbutils.fs.ls(POP_DIR))
