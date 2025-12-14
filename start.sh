#!/bin/bash
PROJECT_DIR="~/Documents/expenses-web"
cd "$PROJECT_DIR"
source .venv/bin/activate
uvicorn main:app --reload --port 8001 --host 0.0.0.0 --proxy-headers --forwarded-allow-ips 127.0.0.1

