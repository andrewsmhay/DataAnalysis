#!/usr/bin/env python3
"""
Hevo API Script: Retrieve all Objects and Schema Mappings for a Pipeline.

This script uses the Hevo Public API v2.0 to:
  1. Fetch all objects for a given Pipeline ID.
  2. For each object, fetch its associated schema mapping.
  3. Output a combined summary to the console.

Usage:
  1. Set the environment variables HEVO_API_KEY and HEVO_API_SECRET.
  2. Optionally set HEVO_REGION (default: "us"). Valid values:
     us, us2, us-gcp, eu, in, asia, au
  3. Run: python hevo_pipeline_objects.py

Reference: https://api-docs.hevodata.com/reference/introduction
"""

import base64
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PIPELINE_ID = 100

REGION_BASE_URLS = {
    "us": "https://us.hevodata.com",
    "us2": "https://us2.hevodata.com",
    "us-gcp": "https://us-gcp.hevodata.com",
    "eu": "https://eu.hevodata.com",
    "in": "https://in.hevodata.com",
    "asia": "https://asia.hevodata.com",
    "au": "https://au.hevodata.com",
}

API_PREFIX = "/api/public/v2.0"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_auth_header(api_key: str, api_secret: str) -> dict:
    """Build the Basic-Auth header required by the Hevo API."""
    token = base64.b64encode(f"{api_key}:{api_secret}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
    }


def get_base_url(region: str) -> str:
    """Return the base URL for the specified Hevo region."""
    region = region.lower()
    if region not in REGION_BASE_URLS:
        sys.exit(
            f"[ERROR] Unknown region '{region}'. "
            f"Valid options: {', '.join(REGION_BASE_URLS.keys())}"
        )
    return REGION_BASE_URLS[region]


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------
def get_all_pipeline_objects(
    base_url: str, headers: dict, pipeline_id: int
) -> list:
    """
    GET /pipelines/{id}/objects

    Paginates through all objects for the given pipeline using the
    'starting_after' cursor until no more results are returned.
    """
    all_objects = []
    starting_after = None
    limit = 100

    while True:
        params: dict = {"limit": limit}
        if starting_after:
            params["starting_after"] = starting_after

        url = f"{base_url}{API_PREFIX}/pipelines/{pipeline_id}/objects"
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 404:
            sys.exit(f"[ERROR] Pipeline {pipeline_id} not found.")
        response.raise_for_status()

        body = response.json()

        # The API wraps the list inside a "data" key with a separate
        # "pagination" object.
        objects = body.get("data", body) if isinstance(body, dict) else body
        pagination = body.get("pagination", {}) if isinstance(body, dict) else {}

        if not objects:
            break

        all_objects.extend(objects)

        # If there is a pagination cursor, use it; otherwise we are done.
        next_cursor = pagination.get("starting_after")
        if not next_cursor or len(objects) < limit:
            break

        starting_after = next_cursor

    return all_objects


def get_schema_mapping(
    base_url: str, headers: dict, pipeline_id: int, event_type: str,
    max_retries: int = 5,
) -> dict | None:
    """
    GET /pipelines/{id}/mappings/{event_type}

    Returns the schema mapping for a single event type (object) within the
    pipeline, or None if not found.  Includes retry with exponential backoff
    for HTTP 429 (rate-limit) responses.
    """
    url = (
        f"{base_url}{API_PREFIX}/pipelines/{pipeline_id}"
        f"/mappings/{requests.utils.quote(event_type, safe='')}"
    )

    for attempt in range(max_retries + 1):
        response = requests.get(url, headers=headers)

        if response.status_code == 404:
            return None
        if response.status_code == 429:
            wait = 2 ** attempt  # 1, 2, 4, 8, 16 seconds
            time.sleep(wait)
            continue
        response.raise_for_status()
        body = response.json()
        return body.get("data", body) if isinstance(body, dict) else body

    # Exhausted retries
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    # --- Credentials ---
    api_key = os.environ.get("HEVO_API_KEY")
    api_secret = os.environ.get("HEVO_API_SECRET")

    if not api_key or not api_secret:
        sys.exit(
            "[ERROR] Please set the HEVO_API_KEY and HEVO_API_SECRET "
            "environment variables.\n"
            "  export HEVO_API_KEY='your_api_key'\n"
            "  export HEVO_API_SECRET='your_api_secret'"
        )

    region = os.environ.get("HEVO_REGION", "us")
    base_url = get_base_url(region)
    headers = get_auth_header(api_key, api_secret)

    pipeline_id = PIPELINE_ID
    print(f"Fetching objects for Pipeline ID {pipeline_id} "
          f"(region: {region}) ...")

    # 1. Retrieve all objects for the pipeline.
    objects = get_all_pipeline_objects(base_url, headers, pipeline_id)
    print(f"Found {len(objects)} object(s).\n")

    if not objects:
        print("No objects found for this pipeline.")
        return

    # 2. For each object, retrieve the schema mapping (concurrently).
    print("Fetching schema mappings for all objects (concurrent) ...")
    results = [None] * len(objects)  # preserve order

    def fetch_mapping(index: int, obj: dict) -> tuple:
        obj_name = obj.get("name", "unknown")
        mapping = get_schema_mapping(base_url, headers, pipeline_id, obj_name)
        return index, {"object": obj, "schema_mapping": mapping}

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(fetch_mapping, i, obj): i
            for i, obj in enumerate(objects)
        }
        done_count = 0
        for future in as_completed(futures):
            idx, result = future.result()
            results[idx] = result
            done_count += 1
            if done_count % 50 == 0 or done_count == len(objects):
                print(f"  {done_count}/{len(objects)} mappings fetched")

    # 3. Print the combined output.
    print("\n" + "=" * 72)
    print(f"  Pipeline {pipeline_id} -- Objects and Schema Mappings")
    print("=" * 72)

    for entry in results:
        obj = entry["object"]
        mapping = entry["schema_mapping"]

        print(f"\nObject: {obj.get('name')}")
        print(f"  Status      : {obj.get('status')}")
        print(f"  Last Run TS : {obj.get('last_run_ts')}")

        if mapping:
            print(f"  Dest. Table : {mapping.get('destination_table')}")
            print(f"  Auto-Mapping: {mapping.get('auto_mapping')}")
            print(f"  Map. Status : {mapping.get('mapping_status')}")

            field_mappings = mapping.get("field_mappings", [])
            if field_mappings:
                print(f"  Fields ({len(field_mappings)}):")
                for fm in field_mappings:
                    ignored = " [IGNORED]" if fm.get("ignored") else ""
                    incompat = " [INCOMPATIBLE]" if fm.get("incompatible") else ""
                    print(
                        f"    {fm.get('source_field')} "
                        f"({fm.get('source_field_type')}) -> "
                        f"{fm.get('destination_field')} "
                        f"({fm.get('destination_field_type')})"
                        f"{ignored}{incompat}"
                    )
            else:
                print("  Fields      : (none)")
        else:
            print("  Schema Mapping: not available")

        print("-" * 72)

    # 4. Optionally dump the full JSON for programmatic use.
    output_file = "pipeline_100_objects_and_mappings.json"
    with open(output_file, "w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=2, default=str)
    print(f"\nFull JSON output written to {output_file}")


if __name__ == "__main__":
    main()
