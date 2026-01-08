# entsoe-generation-timescale
Python pipeline that ingests ENTSO-E electricity generation by production type into a TimescaleDB time-series database for analysis and visualisation (e.g. Grafana).
- Runs automatically via GitHub Actions on a rolling update window
- Upserts data to handle revisions without duplication
- Credentials are managed via GitHub Secrets (no secrets in this repo)

This repository contains ingestion logic only. Database credentials and API keys are managed via GitHub Secrets and are not included.
