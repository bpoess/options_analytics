# Options Analytics Scripts

This package provides three user-facing scripts:

- `fetch_data`: Pull E*Trade API data into a local JSON cache file without transformation.
- `get_transactions`: Summarize transactions for use in options tracking.
- `update_spreadsheet`: List and categorize transactions and update your Google Sheet tracker.

## Prerequisites

- Python `>=3.14`
- `uv` installed (https://docs.astral.sh/uv/)
- E*Trade developer API key + secret (https://developer.etrade.com/home)
- (For `update_spreadsheet`) Google OAuth client credentials for Sheets API

### Obtaining credentials for Google OAuth

There are different ways to accomplish this. The following will give the script access to any google sheet under the user's account. The user can control which sheets get accessed through the sheet ID parameter.

1. Go to Google Cloud Console and create/select a project.
1. Enable Google Sheets API for that project.
1. Configure OAuth consent screen:
   Audience: External, Publishing status: Testing, and add yourself as a test user.
1. Go to APIs & Services -> Credentials -> Create Credentials -> OAuth client ID -> Desktop app.
1. Download the OAuth client file and save it as `credentials.json`. The scripts expect `credentials.json` in the current working directory when invoked.


### Create `config.ini`

The scripts read `config.ini` from the current working directory.

Create it from the example:

```bash
cp config.ini.example config.ini
```

Set at least:

- `CONSUMER_KEY`
- `CONSUMER_SECRET`
- `ACCOUNT_LIST`

Example:

```ini
[DEFAULT]
CONSUMER_KEY = your_consumer_key
CONSUMER_SECRET = your_consumer_secret
ACCOUNT_LIST = [{"id":"12345678","name":"Main"},{"id":"87654321","name":"IRA"}]
```

## Script Usage

Use the `--help` option to obtain documentation on arguments for each script.

### Date Format

All date flags use `MMDDYYYY`, for example:

- `03012026` (March 1, 2026)


### Logging

- `--loglevel DEBUG|INFO|WARNING|ERROR`
  Sets the log level for logging to standard out.
- `--logfile <path>`
  In addition to standard out logs will be written to the log file at DEBUG level.

### Caching vs Live Etrade APIs

By default the scripts will use the etrade backend to fetch data. In order to avoid rate limits for frequent usage the scripts support operating on data from a data cache created via `fetch_data`.

`--fromcache path/to/data/cache`

### Filtering accounts

Allows a user to only operate on one of the accounts listed in config.ini

`--accounts` expects comma-separated account IDs.



## Development
### Repository Setup

From the repository root:

```bash
uv sync
```

### Running scripts
- `uv run <script> ...` (no activation needed), or
- `.venv/bin/<script> ...`
