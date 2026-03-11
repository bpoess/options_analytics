# Options Analytics Scripts

This package provides these user-facing scripts:

- `fetch_data`: Pull E*Trade API data into a local JSON cache file without transformation.
- `get_transactions`: Summarize transactions for use in options tracking.
- `update_spreadsheet`: List and categorize transactions and update your Google Sheet tracker.
- `setup_config`: Interactive setup for creating `config.toml`.

## Package Prefix
All scripts are packaged as `options_analytics.<script name>`

## Prerequisites

- Python 3
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


### Create `config.toml`

The analytics scripts read `config.toml` from the current working directory.

Packaged distribution:

```bash
options_analytics.setup_config
```

or for developers

```bash
uv run setup_config
```

`setup_config` will attempt to convert from old style config.ini files if found in the current working directory.

Manual alternative:

```bash
cp config.toml.example config.toml
```

Then edit values in `config.toml` before running the analytics scripts.

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

## Development
### Repository Setup

From the repository root:

```bash
uv sync
```

### Running scripts
- `uv run <script> ...` (no activation needed), or
- `.venv/bin/<script> ...`

### Creating a package for distribution

This repo uses Pyinstaller (https://pyinstaller.org/en/stable/) to create a package for distributing to users that don't want to create their own development environment.

```bash
pyinstaller options_analytics.spec
```

This will create a new package in `dist/`. The package will be completely self contained and will work on any compatible computer (i.e similar chip and OS version). 
