# AGENTS.md

## 🧠 Project Context
**Name:** ReconLab
**Goal:** Local ETL tool for data reconciliation (CSV/API).
**Architecture:** "Thick Client" Web App (NiceGUI) running locally via `uv`.

## 🛠 Tech Stack Rules
1. **Runtime:** Python 3.11+ via `uv`.
2. **Database:**
   - State/Metadata -> SQLite (`sqlmodel`).
   - Heavy Data -> DuckDB (Raw SQL queries).
3. **UI:** NiceGUI. **Do not** use HTML/JS unless absolutely necessary; use Python components.
4. **Async:** All I/O (API calls, DB reads) must be `async` to keep the UI responsive.

## 📝 Coding Standards
- **Type Hinting:** Strict `typing` required.
- **Path Handling:** Use `pathlib.Path`, never string concatenation.
- **SQL:** Use Parameterized Queries strictly to prevent injection/errors.
