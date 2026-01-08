# entsoe-generation-timescale
Python pipeline that ingests ENTSO-E electricity generation by fuel type into a TimescaleDB time-series database for analysis and visualisation (e.g. Grafana).
- Runs automatically via GitHub Actions on a rolling update window

Upserts data to handle revisions without duplication

Credentials are managed via GitHub Secrets (no secrets in this repo)

This repository contains ingestion logic only. The database is private and optimised for time-series queries; please avoid heavy or unaggregated full-history scans.
