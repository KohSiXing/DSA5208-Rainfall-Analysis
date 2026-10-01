#!/usr/bin/env python3
"""
Download the Historical Rainfall across Singapore datasets (collection 2279)
from data.gov.sg for 2017-2024.

    DSA5208 Rainfall Project

Nothing is hardcoded: the script asks the API which datasets are in the
collection, asks each one what year it covers, and downloads the ones in range.
If data.gov.sg adds 2025 later, change YEAR_TO and re-run.

HOW TO RUN (VS Code)
--------------------
1. In the terminal:   pip install requests
2. Edit SETTINGS below if you want a different year range.
3. Hit Run, or:       python3 download_rainfall.py

WHAT TO EXPECT
--------------
  * 8 files, several GB in total (2024 alone is ~1.2 GB). Budget 20-40 min on a
    home connection; it is almost entirely download time, not API waiting.
  * Files already downloaded are skipped, so if it dies partway through, just
    run it again.
  * Pauses between API calls are the rate limit (2 requests per 10 seconds
    without an API key), not a hang.
"""

import re
import sys
import time
from pathlib import Path

import requests

# ==========================================================================
# SETTINGS
# ==========================================================================

YEAR_FROM = 2017
YEAR_TO = 2024

OUTDIR = "rainfall-data"      # saved next to this script

LIST_ONLY = False             # True = show what's in the collection, download nothing

# ==========================================================================
# Constants
# ==========================================================================

COLLECTION_ID = 2279

COLLECTION_META = "https://api-production.data.gov.sg/v2/public/api/collections/{}/metadata"
DATASET_META = "https://api-production.data.gov.sg/v2/public/api/datasets/{}/metadata"
INITIATE = "https://api-open.data.gov.sg/v1/public/api/datasets/{}/initiate-download"
POLL = "https://api-open.data.gov.sg/v1/public/api/datasets/{}/poll-download"

# Dataset downloads are limited to 2 requests per 10s without an API key.
SLEEP = 6
MAX_POLLS = 10


def get(url, tries=3):
    """GET with a retry on 429 (rate limited)."""
    for attempt in range(tries):
        r = requests.get(url, timeout=60)
        if r.status_code == 429:
            print("    rate limited, waiting 30s", file=sys.stderr)
            time.sleep(30)
            continue
        r.raise_for_status()
        return r
    raise SystemExit(f"Giving up on {url} after {tries} attempts")


# ==========================================================================
# Discovery
# ==========================================================================

def list_dataset_ids():
    """Ask the collection which datasets it contains."""
    body = get(COLLECTION_META.format(COLLECTION_ID)).json()
    ids = body["data"]["collectionMetadata"]["childDatasets"]
    if not ids:
        raise SystemExit("Collection returned no child datasets.")
    return ids


def get_dataset_name(dataset_id):
    """Ask a dataset for its name, e.g. 'Historical Rainfall across Singapore (2017)'."""
    body = get(DATASET_META.format(dataset_id)).json()
    meta = body.get("data", {})
    # The name sits under a slightly different key depending on the response.
    for container in (meta.get("datasetMetadata", {}), meta):
        name = container.get("name") or container.get("datasetName")
        if name:
            return name
    return ""


def year_of(name):
    """Pull a 4-digit year out of a dataset name. Returns None if absent."""
    found = re.findall(r"\b(19\d{2}|20\d{2})\b", name)
    return int(found[-1]) if found else None


# ==========================================================================
# Download
# ==========================================================================

def download(dataset_id, year, outdir):
    """initiate-download, poll until a URL appears, then stream to disk."""
    target = outdir / f"rainfall_{year}.csv"

    if target.exists() and target.stat().st_size > 0:
        print(f"  already have {target.name} "
              f"({target.stat().st_size / 1e6:.0f} MB), skipping")
        return target

    print(f"  initiating...")
    get(INITIATE.format(dataset_id))

    for _ in range(MAX_POLLS):
        time.sleep(SLEEP)
        body = get(POLL.format(dataset_id)).json().get("data", {})
        url = body.get("url")

        if url:
            print(f"  downloading -> {target.name}")
            tmp = target.with_suffix(".part")
            with requests.get(url, stream=True, timeout=600) as resp:
                resp.raise_for_status()
                got = 0
                with open(tmp, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1 << 20):
                        fh.write(chunk)
                        got += len(chunk)
                        if got % (50 << 20) < (1 << 20):      # every ~50 MB
                            print(f"    {got / 1e6:.0f} MB", end="\r", flush=True)
            tmp.rename(target)                                 # only on success
            print(f"  done ({target.stat().st_size / 1e6:.0f} MB)        ")
            return target

        print(f"  not ready (status={body.get('status')}), retrying")

    print(f"  GAVE UP on {year} after {MAX_POLLS} polls", file=sys.stderr)
    return None


# ==========================================================================
# Main
# ==========================================================================

def main():
    outdir = Path(__file__).parent / OUTDIR
    outdir.mkdir(parents=True, exist_ok=True)

    print(f"Reading collection {COLLECTION_ID}...")
    ids = list_dataset_ids()
    print(f"  {len(ids)} datasets in the collection\n")

    print("Looking up what year each covers...")
    catalogue = []
    for did in ids:
        name = get_dataset_name(did)
        y = year_of(name)
        catalogue.append((did, name, y))
        print(f"  {y or '????'}  {did}  {name}")
        time.sleep(SLEEP)

    wanted = [(d, n, y) for d, n, y in catalogue
              if y is not None and YEAR_FROM <= y <= YEAR_TO]
    wanted.sort(key=lambda t: t[2])

    missing = set(range(YEAR_FROM, YEAR_TO + 1)) - {y for _, _, y in wanted}
    if missing:
        print(f"\nWARNING: no dataset found for {sorted(missing)}", file=sys.stderr)

    print(f"\n{len(wanted)} datasets in range {YEAR_FROM}-{YEAR_TO}:")
    for _, _, y in wanted:
        print(f"  {y}")

    if LIST_ONLY:
        print("\nLIST_ONLY is True — nothing downloaded.")
        return

    print(f"\nDownloading. Several GB total, so this will take a while.\n")

    saved = []
    for i, (did, name, y) in enumerate(wanted, 1):
        print(f"[{i}/{len(wanted)}] {y}")
        f = download(did, y, outdir)
        if f:
            saved.append(f)
        time.sleep(SLEEP)

    total = sum(f.stat().st_size for f in saved) / 1e9
    print(f"\nSaved {len(saved)}/{len(wanted)} files to {outdir}  ({total:.2f} GB)")

    if len(saved) < len(wanted):
        print("Some failed. Run again — finished files are skipped.", file=sys.stderr)


if __name__ == "__main__":
    main()
