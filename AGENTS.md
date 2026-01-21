# AGENTS.md

Guide for AI agents working on the natq project.

## Project Overview

natq is a **language-agnostic distributed task queue** built on NATS. It enables decoupled task execution where:
- **Producers** trigger tasks by publishing NATS messages (no task implementation needed)
- **Workers** register task handlers and process incoming messages
- **NATS** provides messaging infrastructure (request/reply, JetStream, KV)

The goal is to provide SDKs in multiple languages that interoperate seamlessly.

## Architecture

```
Producer (any language)
    │
    ├── Sync task ──▶ NATS Request/Reply ──▶ Worker handles, responds immediately
    │
    └── Async task ─▶ JetStream Stream ────▶ Worker creates PROCESSING entry in KV
                                             Worker executes handler
                                             Worker updates KV with final result
                                             Producer polls KV for result
```

### Key Components

| Component | NATS Feature | Purpose |
|-----------|--------------|---------|
| Task Registry | KV Bucket `natq_tasks` | Store TaskDefinitions for discovery |
| Results Store | KV Bucket `natq_results` | Store async task results (key: `<task_id>.<run_id>`) |
| Sync Tasks | Request/Reply on `natq.req.<task_id>` | Immediate response, caller blocks |
| Async Tasks | JetStream stream `natq_jobs` | Persistent queue, at-least-once delivery |
| Async Subjects | `natq.job.<task_id>` | Per-task subjects within the stream |

## Message Formats

All messages use **JSON encoding**. Response metadata is in NATS headers.

### Task Input (Producer → Worker)

```json
{
  "runId": "optional-custom-id",
  "dropResultOnSuccess": false,
  "...": "task-specific input fields"
}
```

- `runId`: Optional. If omitted, worker generates UUIDv7. Used for logging/tracing and KV key.
- `dropResultOnSuccess`: Optional. If `true`, the result is deleted from KV immediately after successful acknowledgment (async tasks only). Useful for fire-and-forget workloads.
- Other fields: Task-specific input conforming to `inputSchema`.

### Task Output (Worker → Producer / KV)

```json
{
  "id": "run-id",
  "taskId": "task-id",
  "status": 100,
  "data": { "result": "..." },
  "error": "error message if failed"
}
```

**Status codes:**
- `100`: Processing (async task in progress, initial state)
- `200`: Success
- `4xx`: Client error
- `5xx`: Server error

**Response Headers** (set by worker for sync tasks):
- `status`: HTTP-style status code as string
- `error`: Error message (if status indicates failure)

### TaskDefinition (stored in KV)

```json
{
  "id": "send-email",
  "type": "sync",
  "subject": "natq.req.send-email",
  "inputSchema": "{ \"type\": \"object\", ... }",
  "outputSchema": "{ \"type\": \"object\", ... }"
}
```

## Status Codes & Async Acknowledgment

The `status` field in task output determines how async messages are handled:

| Status | Meaning | Async Action | KV Update | Retry? |
|--------|---------|--------------|-----------|--------|
| 100 | Processing | N/A (initial state) | Created before handler | N/A |
| < 300 | Success | `msg.ack()` | Updated with result | No |
| 300-499 | Client error | `msg.term()` | Updated with error | No |
| ≥ 500 | Server error | `msg.nak(delay)` | Not updated (retry pending) | Yes |

**Backoff formula**: `min(2^deliveryCount * 1000ms, 60000ms)`

## Async Task Result Flow

1. Worker receives message from JetStream
2. Worker parses input, determines `runId` (from input or generate UUIDv7)
3. Worker creates KV entry at `<task_id>.<run_id>` with status `100` (PROCESSING)
4. Worker executes handler
5. On completion (success or client error): Worker updates KV entry with final status
6. On server error: Worker NAKs message (KV entry remains at status 100 until retry succeeds)

**KV Key Format**: `<task_id>.<run_id>` (e.g., `send-email.018f6b2c-1234-7890-abcd-ef1234567890`)

## Worker Lifecycle

### Startup Sequence

1. **Connect to NATS** - Establish connection with provided options
2. **Ensure KV bucket `natq_tasks` exists** - Create if missing
3. **Ensure KV bucket `natq_results` exists** - Create if missing (for async task results)
4. **Ensure JetStream stream exists** - Create `natq_jobs` if async tasks registered
5. **Register TaskDefinitions** - Write to KV for producer discovery
6. **Start sync listeners** - Subscribe to `natq.req.<task_id>` subjects
7. **Start async listeners** - Create durable consumers, start consuming
8. **Register signal handlers** - SIGINT/SIGTERM for graceful shutdown

### Shutdown Sequence

1. **Stop async consumers** - Prevent new message delivery
2. **Drain sync subscriptions** - Finish in-flight requests
3. **Wait for async tasks** - Let running handlers complete
4. **Close NATS connection** - Clean disconnect

### Worker State

```
CREATED ──▶ registerTask() ──▶ CONFIGURED ──▶ start() ──▶ RUNNING ──▶ stop() ──▶ STOPPED
                                    │                         │
                                    └── Cannot register ──────┘
                                        tasks after start
```

## Implementing a New Language SDK

### Required Components

1. **NatqWorker class** with:
   - Constructor accepting options and optional logger
   - `registerTask(id, type, handler, options?)` method
   - `start(natsOptions)` async method
   - `stop()` async method
   - Worker ID (UUIDv7, generated at construction)

2. **Types/Interfaces**:
   - `TaskType` enum: `SYNC`, `ASYNC`
   - `TaskDefinition`: id, type, subject, inputSchema?, outputSchema?
   - `TaskContext`: runId, task, workerId
   - `TaskRunInput`: JSON object with optional runId, dropResultOnSuccess
   - `TaskRunOutput`: id, taskId, status, data?, error?
   - `TaskHandler`: function(input, context) → output
   - `StatusCode` enum: PROCESSING=100, OK=200, BAD_REQUEST=400, INTERNAL_ERROR=500

3. **Configuration Options**:
   ```
   kvBucket: string = "natq_tasks"
   resultsBucket: string = "natq_results"
   streamName: string = "natq_jobs"
   syncSubjectPrefix: string = "natq.req."
   asyncSubjectPrefix: string = "natq.job."
   heartbeatIntervalMs: number = 10000
   logLevel: string = "info"
   ```

### NATS Operations Checklist

#### KV Bucket Setup (Tasks)
```
bucket name: natq_tasks (or configured kvBucket)
history: 1
```

#### KV Bucket Setup (Results)
```
bucket name: natq_results (or configured resultsBucket)
history: 1
```

#### JetStream Stream Setup
```
name: natq_jobs (or configured streamName)
subjects: ["natq.job.>"] (or asyncSubjectPrefix + ">")
retention: workqueue
discard: new
```

#### Consumer Setup (per async task)
```
name: natq_worker_<task_id>
durable: yes
filter_subject: natq.job.<task_id>
ack_policy: explicit
deliver_policy: all
```

### Async Task Handler Flow

```
1. Receive JsMsg from consumer
2. Start heartbeat interval (msg.working() every heartbeatIntervalMs)
3. Parse JSON input
   - On parse error: return status 406, skip KV creation
4. Determine runId (input.runId || uuidv7())
5. Create KV entry: key = "<taskId>.<runId>", value = { id, taskId, status: 100 }
6. Build TaskContext { runId, task, workerId }
7. Execute handler
   - On success: update KV with result, msg.ack(), if dropResultOnSuccess delete KV entry
   - On client error (300-499): update KV with error, msg.term()
   - On server error (500+): msg.nak(delay), DO NOT update KV
   - On exception: msg.nak(5000), DO NOT update KV
8. Clear heartbeat interval
```

### Fire-and-Forget Mode (dropResultOnSuccess)

When `input.dropResultOnSuccess` is `true` for async tasks:
1. Result is written to KV (so watchers see the final status)
2. Message is acknowledged
3. Result is immediately deleted from KV

This keeps the results bucket clean for high-volume fire-and-forget workloads.

### Error Handling Requirements

1. **Invalid JSON input**: Return status 406, error "Invalid JSON input", do not create KV entry
2. **Handler exception**: Catch, return status 500, error "Unhandled exception: <message>"
3. **Async heartbeat**: Call `msg.working()` periodically to prevent timeout
4. **Graceful degradation**: Log errors, don't crash the worker
5. **KV write failures**: Log error, continue processing (don't crash)

### Logging Requirements

Include in all log messages:
- `workerId`: Worker instance ID
- `taskId`: Task being processed (when applicable)
- `runId`: Execution run ID (when applicable)

Log levels: `debug`, `info`, `warn`, `error`

## Code Organization (Reference: TypeScript SDK)

```
natq-ts/
├── src/
│   ├── index.ts          # Public exports
│   ├── worker.ts         # NatqWorker class, types, all logic
│   ├── logger.ts         # Logger interface and default implementation
│   ├── worker.test.ts    # Unit tests
│   └── logger.test.ts    # Unit tests
```

Key sections in `worker.ts`:
1. Configuration interfaces and defaults
2. Task types and definitions
3. Input/output types
4. Task handler type
5. NatqWorker class with public API
6. Private startup helpers
7. Task execution (shared logic)
8. Sync task handling
9. Async task handling (with KV operations)

## Testing Guidelines

### Unit Tests (no NATS required)

Test these with mocked NATS objects:

1. **registerTask**: Verify definition created, subject computed correctly
2. **executeTaskCallback**:
   - Valid input → handler called, output returned
   - Invalid JSON → status 406
   - Handler throws → status 500
3. **handleSyncMessage**:
   - Success → respond with data and status header
   - Error → respond with error header
4. **handleAsyncMessage**:
   - Status < 300 → KV updated, `msg.ack()` called
   - Status 300-499 → KV updated, `msg.term()` called
   - Status ≥ 500 → KV NOT updated, `msg.nak(delay)` called with backoff
   - Heartbeat sent during processing
   - Initial KV entry created with status 100

### Integration Tests (requires NATS)

1. Start worker, trigger sync task, verify response
2. Start worker, publish async job, verify KV entry created with status 100
3. Verify KV entry updated to final status after processing
4. Async task failure and retry behavior (KV stays at 100 during retries)
5. Multiple workers consuming same queue (load distribution)
6. Graceful shutdown with in-flight tasks

### E2E Tests

The `e2e/` directory contains end-to-end tests that verify SDK implementations. When implementing a new language SDK, you must pass all E2E tests.

**E2E Test Tasks:**

| Task ID | Type | Purpose |
|---------|------|---------|
| `e2e-add` | sync | Add two numbers, return `{ sum: a + b }` |
| `e2e-echo` | sync | Echo input back (excluding `runId`) |
| `e2e-client-error` | sync | Return status 400 with error message |
| `e2e-delay` | async | Wait `delayMs` milliseconds, return `{ delayed: true }` |
| `e2e-retry` | async | Fail with 500 for first N attempts, succeed after |
| `e2e-async-client-error` | async | Return status 400 (should not retry) |
| `e2e-drop-result` | async | Test `dropResultOnSuccess` (result deleted after success) |

**Running E2E tests:**
```bash
# Start NATS with JetStream
nats-server -js

# Start your worker implementation
cd e2e/workers/<language> && <start command>

# Run the Go tester
cd e2e/tester && go run .
```

See `e2e/README.md` for detailed task specifications and expected behavior.

## Common Implementation Pitfalls

1. **Forgetting heartbeats**: Long async tasks will timeout without `msg.working()`
2. **Not handling shutdown signals**: Worker won't stop gracefully on SIGINT/SIGTERM
3. **Blocking on async handlers**: Use fire-and-forget pattern, don't await in message loop
4. **Missing error boundaries**: One bad message shouldn't crash the worker
5. **Incorrect subject patterns**: Sync uses `natq.req.`, async uses `natq.job.`
6. **Not draining connections**: Always drain before close for clean shutdown
7. **Updating KV on server errors**: Don't update KV when NAKing - the task will retry
8. **Creating KV entry on parse errors**: Skip KV creation if input JSON is invalid

## Quick Reference

### Subject Patterns
- Sync task: `natq.req.<task_id>`
- Async task: `natq.job.<task_id>`
- Stream filter: `natq.job.>`

### KV Keys
- Task definition: `<task_id>` in `natq_tasks`
- Async result: `<task_id>.<run_id>` in `natq_results`

### Default Values
- Tasks KV bucket: `natq_tasks`
- Results KV bucket: `natq_results`
- Stream name: `natq_jobs`
- Heartbeat: 10000ms
- Max backoff: 60000ms
- Unexpected error retry: 5000ms

### Response Header Keys (sync tasks only)
- `status`: HTTP status code (string)
- `error`: Error message (string, optional)

### Status Codes
- `100`: Processing (async initial state)
- `200`: OK
- `400`: Bad Request
- `406`: Not Acceptable (invalid JSON)
- `500`: Internal Error

## File Index

| File | Purpose |
|------|---------|
| `README.md` | Project overview, architecture, specifications |
| `AGENTS.md` | This file - AI agent guide |
| `initial.md` | Original design document |
| `natq-ts/README.md` | TypeScript SDK documentation |
| `natq-ts/src/worker.ts` | Reference implementation |
| `natq-ts/src/logger.ts` | Logger interface |
| `natq-ts/src/worker.test.ts` | Unit test examples |
| `e2e/README.md` | E2E test documentation and task specifications |
| `e2e/tester/main.go` | Go-based E2E test runner (producer) |
| `e2e/workers/typescript/` | TypeScript E2E worker reference implementation |
