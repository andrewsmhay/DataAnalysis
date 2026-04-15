# Hevo Pipeline Objects and Schema Mappings

A Python utility that retrieves all objects and their associated schema mappings for a given [Hevo Data](https://hevodata.com/) pipeline using the [Hevo Public API v2.0](https://api-docs.hevodata.com/reference/introduction).

## Features

- Fetches all pipeline objects with automatic cursor-based pagination
- Retrieves schema mappings for each object concurrently
- Handles API rate limiting (HTTP 429) with exponential backoff
- Outputs a formatted summary to the console
- Exports full results to JSON for programmatic use
- Supports all Hevo regions (US, US2, US-GCP, EU, India, Asia, Australia)

## Prerequisites

- Python 3.10+
- A Hevo account with an API key and secret (requires the `owner` role)

## Installation

1. Clone the repository:

   ```bash
   git clone <repo-url>
   cd <repo-directory>
   ```

2. Install dependencies:

   ```bash
   pip install requests
   ```

## Configuration

The script is configured via environment variables and an in-file constant.

### Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `HEVO_API_KEY` | Yes | | Your Hevo API key |
| `HEVO_API_SECRET` | Yes | | Your Hevo API secret |
| `HEVO_REGION` | No | `us` | Hevo region identifier (see below) |

### Supported Regions

| Region | Base URL |
|---|---|
| `us` | `https://us.hevodata.com` |
| `us2` | `https://us2.hevodata.com` |
| `us-gcp` | `https://us-gcp.hevodata.com` |
| `eu` | `https://eu.hevodata.com` |
| `in` | `https://in.hevodata.com` |
| `asia` | `https://asia.hevodata.com` |
| `au` | `https://au.hevodata.com` |

### Pipeline ID

The target pipeline ID is set as the `PIPELINE_ID` constant near the top of `hevo_pipeline_objects.py`. Update this value to target a different pipeline.

## Usage

```bash
export HEVO_API_KEY='your_api_key'
export HEVO_API_SECRET='your_api_secret'
export HEVO_REGION='eu'

python hevo_pipeline_objects.py
```

## Output

### Console

The script prints a formatted summary for each object, including its status, last run timestamp, destination table, auto-mapping status, and field-level mappings where available.

```
========================================================================
  Pipeline 100 -- Objects and Schema Mappings
========================================================================

Object: Opportunity
  Status      : ACTIVE
  Last Run TS : 1776271583000
  Dest. Table : opportunity
  Auto-Mapping: ENABLED
  Map. Status : MAPPED
  Fields (286):
    Id (STRING) -> id (VARCHAR)
    IsDeleted (BOOLEAN) -> isdeleted (BOOLEAN)
    ...
------------------------------------------------------------------------
```

### JSON

A full JSON export is written to `pipeline_100_objects_and_mappings.json` in the working directory. Each entry contains the raw object metadata and its corresponding schema mapping (or `null` if unavailable).

## API Endpoints Used

| Endpoint | Method | Description |
|---|---|---|
| `/api/public/v2.0/pipelines/{id}/objects` | `GET` | List all objects for a pipeline |
| `/api/public/v2.0/pipelines/{id}/mappings/{event_type}` | `GET` | Get schema mapping for a specific object |

## Rate Limiting

The Hevo API enforces rate limits. This script uses 10 concurrent workers and retries rate-limited requests (HTTP 429) with exponential backoff (up to 5 retries per request).

## Licence

This project is provided as-is with no warranty. Use at your own risk.
