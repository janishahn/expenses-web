# Expense Tracker

A lightweight, mobile-first expense tracker designed for single-user operation, built with FastAPI and optimized for running on a Raspberry Pi. The application tracks income and expenses, handles recurring transactions automatically, and generates detailed PDF reports.

## Features

- **Transaction Management**: Track income and expenses with categories
- **Recurring Transactions**: Automated posting with customizable rules
- **Adjustment Transactions**: Reconciliation entries that don't affect charts
- **Reporting**: Export to CSV or generate configurable PDF reports
- **Mobile-First UI**: Responsive design with htmx for smooth interactions
- **Performance Optimized**: Write-time rollups for fast dashboard loading

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
   
   # Create virtual environment and install dependencies
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   uv pip install -r pyproject.toml
   ```

3. **Initialize the database**:
   ```bash
   # Run migrations to create tables
   uv run python -m alembic upgrade head
   ```

4. **Run the application**:

   **Option A: Using uvicorn directly (recommended):**
   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8000
   ```

   **Option B: Using the Python main module:**
   ```bash
   python main.py
   ```

   For development with auto-reload:
   ```bash
   uvicorn main:app --reload --host 0.0.0.0 --port 8000
   ```

The application will be available at `http://localhost:8000`

## Configuration

The application can be configured using environment variables:

- `EXPENSES_DATABASE_URL`: Database connection string (default: `sqlite:///data/expenses.db`)
- `EXPENSES_TIMEZONE`: Timezone for scheduling (default: `Europe/Berlin`)
- `EXPENSES_CSRF_SECRET`: Secret for CSRF protection (default: `change-me`)

## Development

- Run with auto-reload: `uvicorn main:app --reload`
- Run tests: `uv run pytest`
- Lint code: `uv run ruff check`
- Database migrations: `uv run python -m alembic revision --autogenerate -m "description"` followed by `uv run python -m alembic upgrade head`</

## Architecture Notes

- Money is stored as integers (cents) to avoid floating point errors
- Recurring transactions use idempotent posting with unique constraints
- All dates are handled in local time (no timezone conversion)
- Monthly rollups are updated in write-time for fast KPI queries
- The UI uses htmx for dynamic updates with a React island for date picking