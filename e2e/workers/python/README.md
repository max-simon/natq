# natq E2E Worker (Python)

E2E test worker implementation for the natq Python SDK.

## Setup

```bash
# Install dependencies
poetry install

# Or use pip
pip install -e ../../../natq-py
```

## Running

```bash
# Using poetry
poetry run python main.py

# Or directly
python main.py
```

## Tasks Implemented

| Task ID | Type | Purpose |
|---------|------|---------|
| `e2e-add` | sync | Add two numbers |
| `e2e-echo` | sync | Echo input back |
| `e2e-client-error` | sync | Return 400 error |
| `e2e-delay` | async | Wait for delay then return |
| `e2e-retry` | async | Fail N times then succeed |
| `e2e-async-client-error` | async | Return 400 error (no retry) |
| `e2e-drop-result` | async | Test dropResultOnSuccess |
