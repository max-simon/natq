# natq

A language-agnostic distributed task queue framework built on NATS.

## Overview

natq enables decoupled task execution where producers only need to publish messages—no task implementation required. Workers register tasks and handle execution, while NATS provides the messaging infrastructure.

## Architecture

```
┌──────────────┐         ┌─────────────────────────────────────────────┐
│   Producer   │         │                    NATS                     │
│              │         │  ┌─────────────────────────────────────┐    │
│  (any lang)  │────────▶│  │  KV: natq_tasks                     │    │
│              │         │  │  (task definitions for discovery)   │    │
└──────────────┘         │  └─────────────────────────────────────┘    │
      │                  │                                             │
      │                  │  ┌─────────────────────────────────────┐    │
      │                  │  │  KV: natq_results                   │    │
      │                  │  │  (async task results)               │    │
      │                  │  │  Key: <task_id>.<run_id>            │    │
      ▼                  │  └─────────────────────────────────────┘    │
┌──────────────┐         │                                             │
│ Poll results │◀────────│  ┌─────────────────────────────────────┐    │
│ from KV      │         │  │  Sync Tasks: Request/Reply          │    │
└──────────────┘         │  │  Subject: natq.req.<task_id>        │    │
                         │  └─────────────────────────────────────┘    │
                         │                                             │
                         │  ┌─────────────────────────────────────┐    │
                         │  │  Async Tasks: JetStream             │    │
                         │  │  Stream: natq_jobs                  │    │
                         │  │  Subject: natq.job.<task_id>        │    │
                         │  └─────────────────────────────────────┘    │
                         └─────────────────────────────────────────────┘
                                            │
                                            ▼
                         ┌─────────────────────────────────────────────┐
                         │                  Worker                     │
                         │  - Registers TaskDefinitions in KV          │
                         │  - Handles sync tasks via request/reply     │
                         │  - Consumes async jobs from JetStream       │
                         │  - Stores async results in KV               │
                         └─────────────────────────────────────────────┘
```

### Task Types

| Type  | Pattern | Use Case |
|-------|---------|----------|
| `sync` | Request/Reply | Short-lived operations where the caller waits for the result |
| `async` | JetStream work queue | Long-running jobs with guaranteed delivery, results stored in KV |

## Specification

### TaskDefinition

Stored in NATS KV bucket `natq_tasks` with the task ID as key.

```typescript
interface TaskDefinition {
  id: string;                        // Unique task identifier
  type: "sync" | "async";            // Execution model
  subject: string;                   // NATS subject for triggering
  inputSchema?: string;              // Optional JSON Schema for input validation/discovery
  outputSchema?: string;             // Optional JSON Schema for output validation/discovery
}
```

### TaskRunOutput

Result of task execution. For async tasks, stored in KV bucket `natq_results` under key `<task_id>.<run_id>`.

```typescript
interface TaskRunOutput {
  id: string;                        // Run ID (from input runId or generated UUIDv7)
  taskId: string;                    // Reference to TaskDefinition.id
  status: number;                    // HTTP-style status code (100 = processing, 200 = success, 4xx/5xx = error)
  data?: any;                        // Result payload (conforms to outputSchema)
  error?: string;                    // Error message if status indicates failure
}
```

### Task Input Payload

The message payload sent to trigger a task.

```typescript
interface TaskRunInput {
  [key: string]: any;                // Input data (conforms to inputSchema)
  runId?: string;                    // Optional: custom run ID for tracking
}
```

## NATS Components

| Component | Name | Purpose |
|-----------|------|---------|
| **KV Bucket** | `natq_tasks` | Stores TaskDefinition records for task discovery |
| **KV Bucket** | `natq_results` | Stores async task results (key: `<task_id>.<run_id>`) |
| **Request/Reply** | `natq.req.<task_id>` | Handles synchronous task execution with immediate response |
| **JetStream Stream** | `natq_jobs` | Persists async job messages with delivery guarantees |
| **JetStream Consumer** | Work queue per task | Distributes jobs across worker instances |
| **Subject (sync)** | `natq.req.<task_id>` | Producers publish here to trigger sync tasks |
| **Subject (async)** | `natq.job.<task_id>` | Producers publish here to enqueue async jobs |

## Usage

### Worker

Each language SDK provides a `NatqWorker` class:

```
worker = NatqWorker(options)
worker.register_task("my-task", "sync", handler_function)
worker.start(nats_connection_options)
```

The worker does not connect to NATS before calling `worker.start()`.

#### Startup Sequence

1. Establishes NATS connection
2. Ensures NATS objects exist (KV buckets, JetStream stream)
3. Registers TaskDefinitions in the KV bucket
4. Starts sync listeners (request/reply subscriptions)
5. Starts async listeners (JetStream consumers)
6. Registers signal handlers (SIGINT, SIGTERM) for graceful shutdown

#### Shutdown

Call `worker.stop()` to gracefully shutdown:
1. Stops async consumers
2. Drains sync subscriptions
3. Waits for in-flight tasks to complete
4. Closes NATS connection

### Worker Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `kvBucket` | `natq_tasks` | KV bucket name for task definitions |
| `resultsBucket` | `natq_results` | KV bucket name for async task results |
| `streamName` | `natq_jobs` | JetStream stream name for async jobs |
| `logLevel` | `info` | Log level (`debug`, `info`, `warn`, `error`) |
| `syncSubjectPrefix` | `natq.req.` | Subject prefix for sync tasks |
| `asyncSubjectPrefix` | `natq.job.` | Subject prefix for async tasks |
| `heartbeatIntervalMs` | `10000` | Heartbeat interval for async task processing (ms) |

### Task Handler

Task handlers receive input and context, and return a result:

```typescript
async function handler(input: TaskRunInput, context: TaskContext): Promise<TaskRunOutput> {
  // context.runId - unique run identifier
  // context.task - task definition
  // context.workerId - worker instance ID

  return {
    id: context.runId,
    taskId: context.task.id,
    status: 200,
    data: { result: "success" }
  };
}
```

### Async Task Lifecycle

1. **Producer publishes job** to `natq.job.<task_id>` with optional `runId`
2. **Worker creates initial result** in KV with status `100` (Processing)
3. **Worker executes handler** and updates KV with final result
4. **Producer polls KV** for result using key `<task_id>.<run_id>`

### Async Task Error Handling

Async tasks use message acknowledgment based on status codes:

| Status | Action | Behavior |
|--------|--------|----------|
| < 300 | ACK | Success, message removed from queue, result stored in KV |
| 300-499 | TERM | Client error, message terminated (no retry), result stored in KV |
| >= 500 | NAK | Server error, message requeued with exponential backoff (max 60s) |

### Producer

Producers trigger tasks by publishing to the appropriate subject:

```
# Sync task - wait for reply
result = nats.request("natq.req.my-task", payload)

# Async task - fire and forget
nats.jetstream.publish("natq.job.my-task", payload)

# Async task - with custom run ID for tracking
nats.jetstream.publish("natq.job.my-task", { ...payload, runId: "my-run-123" })

# Poll for async result
result = nats.kv("natq_results").get("my-task.my-run-123")
```

## Status Codes

TaskRunOutput uses HTTP-style status codes:

| Code | Meaning |
|------|---------|
| 100 | Processing (async task in progress) |
| 200 | Success |
| 400 | Bad request (invalid input) |
| 406 | Not acceptable (invalid JSON input) |
| 500 | Internal error |

## SDK Implementations

| Language | Package | Documentation |
|----------|---------|---------------|
| TypeScript | `natq-ts` | [natq-ts/README.md](natq-ts/README.md) |

## E2E Tests

End-to-end tests verify that SDK implementations correctly implement the natq protocol. The test suite uses a Go-based tester that acts as a producer, allowing any language worker to be tested.

**Test coverage:**
- Sync task execution (request/reply)
- Async task lifecycle (KV status progression)
- Error handling (4xx client errors, 5xx server errors)
- Retry behavior with exponential backoff

**Quick start:**
```bash
# Terminal 1: Start NATS with JetStream
nats-server -js

# Terminal 2: Start a worker (e.g., TypeScript)
cd e2e/workers/typescript && yarn install && yarn start

# Terminal 3: Run tests
cd e2e/tester && go run .
```

See [e2e/README.md](e2e/README.md) for full documentation.
