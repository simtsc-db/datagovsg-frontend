# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Singapore "Palantir clone" — a webapp hosted on Databricks Apps showing a 3D map of Singapore with real-time data from data.gov.sg APIs. 3D objects on the map are clickable and filterable. Multiple views focus on different data domains (HDB housing, LTA traffic, NEA environment).

## Tech Stack

- **databricks-ai-dev-kit** and **apx** for app scaffolding and deployment
- **CesiumJS** for 3D map rendering
- Data sourced from **data.gov.sg** real-time APIs
- Use Lakebase for state management and data storage. Sync with Delta.

## Architecture

- `agent.md` — High-level app specification and feature list
- `config.md` — Workspace deployment ID and API keys (data.gov.sg, CesiumJS)
- `views/` — Per-domain view specifications defining use cases, UI behavior, and API endpoints:
  - `hdb_housing.md` — HDB-related data (carparks, etc.)
  - `lta_traffic.md` — Traffic cameras and taxi locations
  - (Planned) `nea_environmental.md` — Air quality, lightning observations

## Key Details

- The Databricks workspace (`serverless-sandbox-simtsc`) is used for backend/deployment; authenticate before creating assets.
- API keys for data.gov.sg and CesiumJS are in `config.md`.
- Views integrate with specific data.gov.sg dataset endpoints listed in each view file. APIs may need joining (e.g., camera real-time images joined with camera location metadata).
- 3D objects should be color-coded per view spec (e.g., red for cameras, blue for taxis) and support click interactions to show detail panels.
