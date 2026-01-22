# natq-ts

TypeScript implementation of the natq distributed task queue framework.

## Installation

```bash
yarn add natq
# or
npm install natq
```

## Quick Start

```typescript
import { NatqWorker, TaskType, TaskContext, TaskRunInput, TaskRunOutput } from "natq";

// Create worker with optional configuration
const worker = new NatqWorker({
  logLevel: "info",
});

// Register a sync task (request/reply)
worker.registerTask("greet", TaskType.SYNC, async (input: TaskRunInput, context: TaskContext): Promise<TaskRunOutput> => {
  const name = (input as any).name || "World";
  return {
    id: context.runId,
    taskId: context.task.id,
    status: 200,
    data: { message: `Hello, ${name}!` },
  };
});

// Register an async task (job queue)
worker.registerTask("process-data", TaskType.ASYNC, async (input: TaskRunInput, context: TaskContext): Promise<TaskRunOutput> => {
  // Long-running processing...
  return {
    id: context.runId,
    taskId: context.task.id,
    status: 200,
    data: { processed: true },
  };
});

// Start the worker
await worker.start({ servers: "localhost:4222" });
```

## API Reference

### NatqWorker

#### Constructor

```typescript
const worker = new NatqWorker(options?: Partial<NatqWorkerOptions>, logger?: Logger);
```

#### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `kvBucket` | `string` | `"natq_tasks"` | KV bucket name for task definitions |
| `streamName` | `string` | `"natq_jobs"` | JetStream stream name for async jobs |
| `logLevel` | `LogLevel` | `LogLevel.INFO` | Log level for default logger |
| `syncSubjectPrefix` | `string` | `"natq.req."` | Subject prefix for sync tasks |
| `asyncSubjectPrefix` | `string` | `"natq.job."` | Subject prefix for async tasks |
| `heartbeatIntervalMs` | `number` | `10000` | Heartbeat interval for async tasks (ms) |

#### Methods

##### `registerTask(id, type, handler, options?)`

Register a task handler before starting the worker.

```typescript
worker.registerTask(
  "my-task",           // Task ID
  TaskType.SYNC,       // Task type: SYNC or ASYNC
  handler,             // Task handler function
  {                    // Optional configuration
    inputSchema: "...",         // JSON Schema for input
    outputSchema: "...",        // JSON Schema for output
  }
);
```

The NATS subject is computed automatically from the task type and id:
- Sync tasks: `natq.req.<task_id>`
- Async tasks: `natq.job.<task_id>`

##### `start(connectionOptions)`

Connect to NATS and start processing tasks.

```typescript
await worker.start({
  servers: "localhost:4222",
  // Additional NATS connection options...
});
```

##### `stop()`

Gracefully shutdown the worker.

```typescript
await worker.stop();
```

### Task Handler

```typescript
type TaskHandler = (input: TaskRunInput, context: TaskContext) => Promise<TaskRunOutput>;
```

#### TaskContext

| Property | Type | Description |
|----------|------|-------------|
| `runId` | `string` | Unique run identifier (from input or generated UUID) |
| `task` | `TaskDefinition` | The task definition |
| `workerId` | `string` | Worker instance ID |

#### TaskRunOutput

| Property | Type | Description |
|----------|------|-------------|
| `id` | `string` | Run ID |
| `taskId` | `string` | Task ID |
| `status` | `number` | HTTP-style status code |
| `data?` | `JsonSerializable` | Result data |
| `error?` | `string` | Error message (for non-2xx status) |

### Custom Logger

Provide a custom logger implementing the `Logger` interface:

```typescript
import { NatqWorker, Logger } from "natq";

const customLogger: Logger = {
  debug: (...args) => console.debug("[DEBUG]", ...args),
  info: (...args) => console.info("[INFO]", ...args),
  warn: (...args) => console.warn("[WARN]", ...args),
  error: (...args) => console.error("[ERROR]", ...args),
};

const worker = new NatqWorker({}, customLogger);
```

## Error Handling

### Sync Tasks

Errors are returned in the response headers:
- `status`: HTTP-style status code
- `error`: Error message (if applicable)

### Async Tasks

The worker automatically handles message acknowledgment:

| Handler Result | Action | Behavior |
|----------------|--------|----------|
| Status < 300 | ACK | Message removed from queue |
| Status 300-499 | TERM | Message terminated, no retry |
| Status >= 500 | NAK | Requeued with exponential backoff |
| Unhandled exception | NAK | Requeued after 5 seconds |

The heartbeat mechanism (`msg.working()`) keeps messages alive during long-running tasks.

## Development

### Prerequisites

- Node.js >= 18.0.0
- Yarn
- NATS server (for integration testing)

### Setup

```bash
cd natq-ts
yarn install
```

### Build

```bash
yarn build
```

### Test

```bash
yarn test
```

### Project Structure

```
natq-ts/
├── src/
│   ├── index.ts        # Public exports
│   ├── worker.ts       # NatqWorker implementation
│   ├── logger.ts       # Logger interface and default implementation
│   ├── worker.test.ts  # Worker unit tests
│   └── logger.test.ts  # Logger unit tests
├── dist/               # Compiled output
├── package.json
├── tsconfig.json
└── jest.config.js
```

## Example: Producer

Trigger tasks from any NATS client:

```typescript
import { connect, JSONCodec } from "nats";

const nc = await connect({ servers: "localhost:4222" });
const jc = JSONCodec();

// Sync task - request/reply
const response = await nc.request(
  "natq.req.greet",
  jc.encode({ name: "Alice" }),
  { timeout: 5000 }
);
console.log(jc.decode(response.data));

// Async task - fire and forget
const js = nc.jetstream();
await js.publish("natq.job.process-data", jc.encode({ items: [1, 2, 3] }));

// Async task - with reply subject
await js.publish(
  "natq.job.process-data",
  jc.encode({
    items: [1, 2, 3],
    runId: "custom-run-123",
    replySubject: "my.results",
  })
);
```
