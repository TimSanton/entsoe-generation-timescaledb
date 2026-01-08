# entsoe-generation-timescale
Python pipeline that ingests ENTSO-E electricity generation by production type into a TimescaleDB time-series database for analysis and visualisation (e.g. Grafana). It runs automatically via GitHub Actions on a rolling update window and upserts data to handle revisions without duplication.

This repository contains ingestion logic only. Database credentials and API keys are managed via GitHub Secrets and are not included.
