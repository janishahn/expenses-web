#!/bin/bash
PROJECT_DIR="~/Documents/expenses-web"
cd "$PROJECT_DIR"
uv run python -m uvicorn main:app --reload --port 8001 --host 0.0.0.0

