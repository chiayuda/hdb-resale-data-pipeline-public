"""Requirement 1 (extraction half) and the Raw output group.

Dataset IDs are never hardcoded: discovered from the collection's own
metadata, then filtered to the ones whose published coverage overlaps the
configured window. A local manifest (sha256 + last-updated) avoids
re-downloading, and avoids re-resolving a download URL for a dataset already
cached, since resolving the URL is itself the rate-limited call.
"""
import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from io import StringIO
from typing import Any

import pandas as pd
import requests

MAX_ATTEMPTS = 4
INITIAL_BACKOFF_SECONDS = 10  # the documented reset window for data.gov.sg's 429 response
BACKOFF_MULTIPLIER = 2
RAW_DIR = "data/raw"
MANIFEST_PATH = os.path.join(RAW_DIR, "_manifest.json")


def _sha256_of(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def discover_datasets(config: dict[str, Any]) -> list[str]:
    url = config["urls"]["collection_metadata"].format(collection_id=config["collection_id"])
    response = requests.get(url)
    if response.status_code >= 300:
        raise RuntimeError(f"Request failed with status {response.status_code}: {response.text}")
    return response.json()["data"]["collectionMetadata"]["childDatasets"]


def filter_to_window(config: dict[str, Any], dataset_ids: list[str]) -> tuple[list[str], dict[str, str]]:
    window_start, window_end = config["window_start"], config["window_end"]
    dataset_ids_in_window = []
    dataset_last_updated = {}

    for dataset_id in dataset_ids:
        url = config["urls"]["dataset_metadata"].format(dataset_id=dataset_id)
        response = requests.get(url)
        if response.status_code >= 300:
            raise RuntimeError(f"Request failed with status {response.status_code}: {response.text}")

        data = response.json()["data"]
        coverage_start = data.get("coverageStart", "")[:7]
        coverage_end = data.get("coverageEnd", "")[:7]
        in_window = coverage_start <= window_end and coverage_end >= window_start

        if in_window:
            dataset_ids_in_window.append(dataset_id)
            dataset_last_updated[dataset_id] = data.get("lastUpdatedAt", "")

    return dataset_ids_in_window, dataset_last_updated


def _load_manifest() -> dict[str, dict[str, str]]:
    return json.load(open(MANIFEST_PATH)) if os.path.exists(MANIFEST_PATH) else {}


def _already_cached(
    manifest: dict[str, dict[str, str]],
    dataset_ids_in_window: list[str],
    dataset_last_updated: dict[str, str],
    refresh_on_update: bool,
) -> set[str]:
    cached: set[str] = set()
    for dataset_id in dataset_ids_in_window:
        entry = manifest.get(dataset_id)
        if not entry or not os.path.exists(entry["path"]) or _sha256_of(entry["path"]) != entry["sha256"]:
            continue

        current_last_updated = dataset_last_updated.get(dataset_id, "")
        is_stale = bool(current_last_updated) and current_last_updated != entry.get("last_updated_at", "")
        if is_stale and refresh_on_update:
            continue  # falls through to a fresh download

        cached.add(dataset_id)
    return cached


def _resolve_download_urls(
    config: dict[str, Any], dataset_ids_in_window: list[str], cached: set[str]
) -> dict[str, str]:
    headers = {"x-api-key": config["_api_key"]} if config["_api_key"] else {}
    download_urls: dict[str, str] = {}

    for dataset_id in dataset_ids_in_window:
        if dataset_id in cached:
            continue

        url = config["urls"]["poll_download"].format(dataset_id=dataset_id)
        backoff = INITIAL_BACKOFF_SECONDS

        for attempt in range(1, MAX_ATTEMPTS + 1):
            response = requests.get(url, headers=headers)
            if response.status_code < 300:
                break
            if response.status_code == 429 and attempt < MAX_ATTEMPTS:
                time.sleep(backoff)
                backoff *= BACKOFF_MULTIPLIER
                continue
            raise RuntimeError(f"Request failed with status {response.status_code}: {response.text}")

        download_urls[dataset_id] = response.json()["data"]["url"]

    return download_urls


def extract_raw_frames(
    config: dict[str, Any], dataset_ids_in_window: list[str], dataset_last_updated: dict[str, str]
) -> dict[str, pd.DataFrame]:
    """Downloads (or reuses cached copies of) every dataset in the window,
    writes each byte-for-byte to data/raw/ (the Raw output group), and
    returns {dataset_id: DataFrame} for the combine step."""
    os.makedirs(RAW_DIR, exist_ok=True)
    manifest = _load_manifest()
    refresh_on_update = bool(config.get("source", {}).get("refresh_on_update", False))

    cached = _already_cached(manifest, dataset_ids_in_window, dataset_last_updated, refresh_on_update)
    download_urls = _resolve_download_urls(config, dataset_ids_in_window, cached)

    # Iterated in dataset_ids_in_window's own order, not cached's: cached is
    # a set, and set iteration order is not guaranteed stable across runs
    # (hash randomisation), which would otherwise make the column order of
    # the unioned master dataset non-deterministic run to run.
    raw_frames: dict[str, pd.DataFrame] = {}
    for dataset_id in dataset_ids_in_window:
        if dataset_id not in cached:
            continue
        entry = manifest[dataset_id]
        raw_frames[dataset_id] = pd.read_csv(entry["path"])

    fetched_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    for dataset_id, url in download_urls.items():
        response = requests.get(url)
        if response.status_code >= 300:
            raise RuntimeError(f"Request failed with status {response.status_code}: {response.text}")

        content_disposition = response.headers.get("Content-Disposition", "")
        match = re.search(r'filename="([^"]+)"', content_disposition)
        original_name = match.group(1) if match else f"{dataset_id}.csv"
        name, ext = os.path.splitext(original_name)
        raw_path = os.path.join(RAW_DIR, f"{name}_{fetched_at}{ext}")

        with open(raw_path, "wb") as f:
            f.write(response.content)

        manifest[dataset_id] = {
            "path": raw_path,
            "sha256": hashlib.sha256(response.content).hexdigest(),
            "last_updated_at": dataset_last_updated.get(dataset_id, ""),
            "fetched_at": fetched_at,
        }
        raw_frames[dataset_id] = pd.read_csv(StringIO(response.text))

    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2)

    return raw_frames


def combine_master(raw_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Requirement 1: union of columns across all files, not the
    intersection, so an attribute absent from one file (remaining_lease)
    fills NaN there rather than being silently dropped."""
    frames = []
    for dataset_id, df in raw_frames.items():
        df = df.copy()
        df["source_dataset_id"] = dataset_id
        frames.append(df)
    return pd.concat(frames, ignore_index=True, sort=False)
