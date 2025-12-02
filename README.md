# ReconLab (Multi-Project Edition)

## Executive Summary
ReconLab (Reconciliation Laboratory) is a local Python Web Application designed as a standalone tool to manage multiple data quality projects. For each project, the user ingests a primary dataset ("Target"), matches it against a secondary source ("Source"), and manually validates matches row-by-row to create a "Golden Record."

## Installation
No manual installation is required. The application is portable.
1. Ensure you have the folder on your Windows machine.
2. Double-click `run.bat`.
3. The application will automatically download `uv` (if missing), setup the environment, and launch the dashboard in your default browser.

## Tech Stack
- **Runtime:** `uv` (Python 3.12+)
- **UI:** NiceGUI (FastAPI)
- **Database:** SQLite (Metadata), DuckDB (Heavy Data)
- **API:** HTTPX (Sirene API)

## Usage
1. **Create Project**: Upload your Target CSV and select a Source (CSV or Sirene API).
2. **Map**: Define the Join Key and map fields.
3. **Validate**: Review matches in the "Fiche" view.
4. **Export**: Download the reconciled dataset.
