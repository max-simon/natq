# natq-py

Python SDK for natq - a language-agnostic distributed task queue framework built on NATS.

## Installation

```bash
# Using poetry
poetry add natq

# Or install from source
cd natq-py
poetry install
```

## Quick Start

```python
import asyncio
from natq import NatqWorker, TaskType, TaskRunInput, TaskRunOutput, TaskContext

# Define a task handler
async def echo_handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
    return TaskRunOutput(
        id=ctx.run_id,
        task_id=ctx.task.id,
        status=200,
        data=input.data,
    )

async def main():
    # Create worker
    worker = NatqWorker()

    # Register tasks
    worker.register_task("echo", TaskType.SYNC, echo_handler)

    # Start processing
    await worker.start("nats://localhost:4222")

    # Keep running until interrupted
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        await worker.stop()

if __name__ == "__main__":
    asyncio.run(main())
```

## Task Types

### Sync Tasks (Request/Reply)

```python
async def add_handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
    a = input.data.get("a", 0)
    b = input.data.get("b", 0)
    return TaskRunOutput(
        id=ctx.run_id,
        task_id=ctx.task.id,
        status=200,
        data={"sum": a + b},
    )

worker.register_task("add", TaskType.SYNC, add_handler)
```

### Async Tasks (Job Queue)

```python
async def process_order_handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
    order_id = input.data.get("orderId")
    # Long-running processing...
    await process_order(order_id)
    return TaskRunOutput(
        id=ctx.run_id,
        task_id=ctx.task.id,
        status=200,
        data={"processed": True},
    )

worker.register_task("process-order", TaskType.ASYNC, process_order_handler)
```

## Configuration

```python
from natq import NatqWorker, NatqWorkerOptions, LogLevel

options = NatqWorkerOptions(
    kv_bucket="natq_tasks",           # KV bucket for task definitions
    results_bucket="natq_results",    # KV bucket for async results
    stream_name="natq_jobs",          # JetStream stream name
    log_level=LogLevel.DEBUG,         # Log level
    sync_subject_prefix="natq.req.",  # Sync task subject prefix
    async_subject_prefix="natq.job.", # Async task subject prefix
    heartbeat_interval_ms=10000,      # Heartbeat interval for async tasks
)

worker = NatqWorker(options)
```

## Custom Logger

```python
from natq import NatqWorker, Logger

class MyLogger(Logger):
    def debug(self, message: str, **kwargs) -> None:
        print(f"DEBUG: {message}", kwargs)

    def info(self, message: str, **kwargs) -> None:
        print(f"INFO: {message}", kwargs)

    def warn(self, message: str, **kwargs) -> None:
        print(f"WARN: {message}", kwargs)

    def error(self, message: str, **kwargs) -> None:
        print(f"ERROR: {message}", kwargs)

worker = NatqWorker(logger=MyLogger())
```

## Error Handling

Return appropriate status codes to control message handling:

```python
async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
    if not input.data.get("required_field"):
        # Client error (400-499): Message terminated, no retry
        return TaskRunOutput(
            id=ctx.run_id,
            task_id=ctx.task.id,
            status=400,
            error="Missing required_field",
        )

    try:
        result = await do_work(input.data)
        # Success (< 300): Message acknowledged
        return TaskRunOutput(
            id=ctx.run_id,
            task_id=ctx.task.id,
            status=200,
            data=result,
        )
    except TemporaryError:
        # Server error (>= 500): Message NAKed, will retry with backoff
        return TaskRunOutput(
            id=ctx.run_id,
            task_id=ctx.task.id,
            status=500,
            error="Temporary failure, please retry",
        )
```

## Fire-and-Forget Tasks

For async tasks where results don't need to be persisted, clients can set `dropResultOnSuccess: true` in the input. The worker will delete the result from KV immediately after successful processing.

## Development

```bash
# Install dependencies
poetry install

# Run tests
poetry run pytest

# Type checking
poetry run mypy src

# Linting
poetry run ruff check src tests
```

## API Reference

### Types

- `TaskType`: Enum with `SYNC` and `ASYNC` values
- `TaskDefinition`: Task metadata (id, type, subject, schemas)
- `TaskContext`: Execution context (run_id, task, worker_id)
- `TaskRunInput`: Input payload with `data`, `run_id`, `drop_result_on_success`
- `TaskRunOutput`: Output with `id`, `task_id`, `status`, `data`, `error`
- `StatusCode`: Common status codes (PROCESSING=100, OK=200, etc.)
- `TaskHandler`: Async function signature for handlers

### NatqWorker

- `register_task(task_id, task_type, handler, **options)`: Register a task handler
- `start(nats_url)`: Connect and start processing
- `stop()`: Gracefully shutdown
- `registry`: Dict of registered tasks
- `worker_id`: Unique worker instance ID
