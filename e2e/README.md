# E2E Tests

End-to-end tests for natq SDK implementations. These tests verify that workers in any supported language correctly implement the natq protocol.

## Prerequisites

- NATS server running at `localhost:4222` (no authentication)
- Go 1.21+ (for the test runner)
- Language-specific requirements for each worker

## Test Architecture

```
┌─────────────────┐         ┌──────────────────┐         ┌─────────────────┐
│   Go Tester     │────────▶│      NATS        │◀────────│  SDK Worker     │
│   (producer)    │         │  localhost:4222  │         │  (any language) │
└─────────────────┘         └──────────────────┘         └─────────────────┘
```

The Go tester acts as a producer, sending task requests and verifying responses. Any SDK worker can be attached to handle the tasks.

## Test Sequence

The test sequence covers core natq features. All workers must implement these tasks with the exact task IDs and behavior specified.

### 1. Sync: Add Numbers (`e2e-add`)

**Type:** `sync`
**Purpose:** Verify basic sync request/reply with data processing

**Input:**
```json
{
  "a": 5,
  "b": 3
}
```

**Expected Output:**
```json
{
  "id": "<run_id>",
  "taskId": "e2e-add",
  "status": 200,
  "data": { "sum": 8 }
}
```

### 2. Sync: Echo (`e2e-echo`)

**Type:** `sync`
**Purpose:** Verify data serialization round-trip

**Input:**
```json
{
  "message": "hello world",
  "nested": { "foo": "bar" }
}
```

**Expected Output:**
```json
{
  "id": "<run_id>",
  "taskId": "e2e-echo",
  "status": 200,
  "data": {
    "message": "hello world",
    "nested": { "foo": "bar" }
  }
}
```

### 3. Sync: Client Error (`e2e-client-error`)

**Type:** `sync`
**Purpose:** Verify 4xx error handling

**Input:**
```json
{
  "shouldFail": true
}
```

**Expected Output:**
```json
{
  "id": "<run_id>",
  "taskId": "e2e-client-error",
  "status": 400,
  "error": "Client requested failure"
}
```

### 4. Async: Delayed Response (`e2e-delay`)

**Type:** `async`
**Purpose:** Verify async task lifecycle with KV status progression

**Input:**
```json
{
  "runId": "delay-test-001",
  "delayMs": 500
}
```

**Verification Steps:**
1. Publish job to `natq.job.e2e-delay`
2. Poll KV `natq_results` for key `e2e-delay.delay-test-001`
3. Verify initial status is `100` (Processing)
4. Wait for completion
5. Verify final status is `200` with data `{ "delayed": true }`

**Expected Final Output:**
```json
{
  "id": "delay-test-001",
  "taskId": "e2e-delay",
  "status": 200,
  "data": { "delayed": true }
}
```

### 5. Async: Server Error with Retry (`e2e-retry`)

**Type:** `async`
**Purpose:** Verify server error triggers retry, KV stays at 100 during retry

**Input:**
```json
{
  "runId": "retry-test-001",
  "failCount": 2
}
```

**Behavior:**
- Worker tracks delivery count
- Fails with status 500 for first `failCount` deliveries
- Succeeds on subsequent delivery
- KV entry should remain at status 100 during retries

**Expected Final Output:**
```json
{
  "id": "retry-test-001",
  "taskId": "e2e-retry",
  "status": 200,
  "data": { "attempts": 3 }
}
```

### 6. Async: Client Error (`e2e-async-client-error`)

**Type:** `async`
**Purpose:** Verify async 4xx errors update KV and don't retry

**Input:**
```json
{
  "runId": "async-error-001"
}
```

**Expected Output in KV:**
```json
{
  "id": "async-error-001",
  "taskId": "e2e-async-client-error",
  "status": 400,
  "error": "Async client error"
}
```

## Task Summary

| Task ID | Type | Purpose |
|---------|------|---------|
| `e2e-add` | sync | Basic arithmetic |
| `e2e-echo` | sync | Data serialization |
| `e2e-client-error` | sync | 4xx error handling |
| `e2e-delay` | async | Async lifecycle, KV status |
| `e2e-retry` | async | Server error, retry behavior |
| `e2e-async-client-error` | async | Async 4xx, no retry |

## Running Tests

### 1. Start NATS Server

```bash
nats-server
```

### 2. Start a Worker

**TypeScript:**
```bash
cd e2e/workers/typescript
npm install
npm start
```

### 3. Run the Tester

```bash
cd e2e/tester
go run .
```

## Directory Structure

```
e2e/
├── README.md              # This file
├── tester/                # Go-based test runner
│   ├── main.go
│   └── go.mod
└── workers/
    └── typescript/        # TypeScript worker implementation
        ├── package.json
        └── src/
            └── main.ts
```

## Adding a New Language Worker

1. Create a new directory under `e2e/workers/<language>/`
2. Implement all tasks from the test sequence above
3. Ensure task IDs and behavior match exactly
4. Run the Go tester to verify compliance
