# Expense Tracker

A lightweight, mobile-first expense tracker designed for single-user operation, built with FastAPI and optimized for running on a Raspberry Pi. It tracks income and expenses, supports recurring rules, offers monthly budgets, and can export CSV + PDF reports.

## Features

- **Transaction Management**: Income/expense ledger with categories, search, edit, and soft-delete (trash + restore)
- **Recurring Rules**: Automated posting via APScheduler with idempotent occurrences
- **Budgets**: Monthly overall + per-category budgets with “spent vs remaining”
- **Reporting**: CSV export and configurable PDF reports (simple modal + advanced builder)
- **Importing**: CSV importer (preview + commit) and legacy SQLite importer with category mapping
- **Mobile-First UI**: Server-rendered templates with htmx, plus a small React date-range picker
- **Fast Dashboard**: Monthly rollups updated on writes for quick KPI queries

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- SQLite (included with Python)

## Setup

1. **Install uv** (if not already installed):
   ```bash
   # Using pip
   pip install uv
   
   # Or follow installation instructions at https://docs.astral.sh/uv/
   ```

2. **Clone and setup the project**:
   ```bash
   # Clone the repository
   git clone <repository-url>
   cd expenses-web
   
   # Create a virtualenv and install locked dependencies
   uv sync
   ```

3. **Initialize the database**:
   ```bash
   # Run migrations to create tables
   uv run alembic upgrade head
   ```

4. **Run the application**:

   **Option A: Development (auto-reload):**
   ```bash
   uv run uvicorn main:app --reload --host 0.0.0.0 --port 8000
   ```

   **Option B: Production-ish (no reload):**
   ```bash
   uv run uvicorn main:app --host 0.0.0.0 --port 8000
   ```

   **Option C: Run the built-in `main()` (also no reload):**
   ```bash
   uv run python main.py
   ```

The application will be available at `http://localhost:8000`

## Configuration

The application can be configured using environment variables:

- `EXPENSES_DATA_DIR`: Data directory for the SQLite DB and imports (default: `./data`)
- `EXPENSES_DATABASE_URL`: Database connection string (default: `sqlite:///.../<EXPENSES_DATA_DIR>/expenses.db`)
- `EXPENSES_TIMEZONE`: Timezone for scheduling (default: `Europe/Berlin`)
- `EXPENSES_CSRF_SECRET`: Secret for CSRF protection (set a unique value for deployments)
- `EXPENSES_ENV`: Label shown on the Admin page (default: `Local`)

Notes:
- PDF export uses WeasyPrint and may require OS-level libraries (cairo/pango). If PDF generation fails, install WeasyPrint’s system dependencies for your OS.
- The UI loads Tailwind/htmx/React/Babel/Lucide from CDNs by default; offline deployments should vendor these assets.

## Development

- Run tests: `uv run pytest`
- Install dev tools (ruff): `uv sync --extra dev`
- Lint code: `uv run ruff check .`
- Database migrations: `uv run alembic revision --autogenerate -m "description"` then `uv run alembic upgrade head`

## Architecture Notes

- Money is stored as integers (cents) to avoid floating point errors
- Recurring transactions use idempotent posting with unique constraints
- All dates are handled in local time (no timezone conversion)
- Monthly rollups are updated in write-time for fast KPI queries
- The UI uses htmx for dynamic updates with a small React island for date picking
