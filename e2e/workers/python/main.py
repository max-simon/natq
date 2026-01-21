"""
E2E test worker for natq Python SDK.

Implements all required E2E test tasks:
- e2e-add (sync): Add two numbers
- e2e-echo (sync): Echo input back
- e2e-client-error (sync): Return 400 error
- e2e-delay (async): Wait for delay then return
- e2e-retry (async): Fail N times then succeed
- e2e-async-client-error (async): Return 400 error
- e2e-drop-result (async): Test dropResultOnSuccess
"""

import asyncio

from natq import (
    LogLevel,
    NatqWorker,
    NatqWorkerOptions,
    TaskContext,
    TaskRunInput,
    TaskRunOutput,
    TaskType,
)

# Track retry attempts per run_id for e2e-retry task
retry_attempts: dict[str, int] = {}


# =============================================================================
# Sync Task Handlers
# =============================================================================


async def add_handler(input: TaskRunInput, context: TaskContext) -> TaskRunOutput:
    """e2e-add: Add two numbers."""
    a = input.data.get("a", 0)
    b = input.data.get("b", 0)

    return TaskRunOutput(
        id=context.run_id,
        task_id=context.task.id,
        status=200,
        data={"sum": a + b},
    )


async def echo_handler(input: TaskRunInput, context: TaskContext) -> TaskRunOutput:
    """e2e-echo: Echo back the input (excluding runId)."""
    # Return the data as-is (runId is already extracted by TaskRunInput.from_dict)
    return TaskRunOutput(
        id=context.run_id,
        task_id=context.task.id,
        status=200,
        data=input.data,
    )


async def client_error_handler(
    input: TaskRunInput, context: TaskContext
) -> TaskRunOutput:
    """e2e-client-error: Return a 400 error."""
    return TaskRunOutput(
        id=context.run_id,
        task_id=context.task.id,
        status=400,
        error="Client requested failure",
    )


# =============================================================================
# Async Task Handlers
# =============================================================================


async def delay_handler(input: TaskRunInput, context: TaskContext) -> TaskRunOutput:
    """e2e-delay: Wait for specified delay then return."""
    delay_ms = input.data.get("delayMs", 1000)

    await asyncio.sleep(delay_ms / 1000.0)

    return TaskRunOutput(
        id=context.run_id,
        task_id=context.task.id,
        status=200,
        data={"delayed": True},
    )


async def retry_handler(input: TaskRunInput, context: TaskContext) -> TaskRunOutput:
    """e2e-retry: Fail N times then succeed."""
    fail_count = input.data.get("failCount", 1)
    run_id = context.run_id

    # Get current attempt count
    attempts = retry_attempts.get(run_id, 0) + 1
    retry_attempts[run_id] = attempts

    print(f"[e2e-retry] runId={run_id} attempt={attempts} failCount={fail_count}")

    if attempts <= fail_count:
        # Return server error to trigger retry
        return TaskRunOutput(
            id=context.run_id,
            task_id=context.task.id,
            status=500,
            error=f"Intentional failure {attempts}/{fail_count}",
        )

    # Success after enough retries
    # Clean up tracking
    del retry_attempts[run_id]

    return TaskRunOutput(
        id=context.run_id,
        task_id=context.task.id,
        status=200,
        data={"attempts": attempts},
    )


async def async_client_error_handler(
    input: TaskRunInput, context: TaskContext
) -> TaskRunOutput:
    """e2e-async-client-error: Return 400 error (should not retry)."""
    return TaskRunOutput(
        id=context.run_id,
        task_id=context.task.id,
        status=400,
        error="Async client error",
    )


async def drop_result_handler(
    input: TaskRunInput, context: TaskContext
) -> TaskRunOutput:
    """e2e-drop-result: Test fire-and-forget with dropResultOnSuccess."""
    # Simulate some work
    await asyncio.sleep(0.5)

    return TaskRunOutput(
        id=context.run_id,
        task_id=context.task.id,
        status=200,
        data={"dropped": True},
    )


# =============================================================================
# Main
# =============================================================================


async def main() -> None:
    """Start the E2E worker."""
    print("Starting natq E2E worker (Python)...")

    worker = NatqWorker(NatqWorkerOptions(log_level=LogLevel.INFO))

    # Register sync tasks
    worker.register_task("e2e-add", TaskType.SYNC, add_handler)
    worker.register_task("e2e-echo", TaskType.SYNC, echo_handler)
    worker.register_task("e2e-client-error", TaskType.SYNC, client_error_handler)

    # Register async tasks
    worker.register_task("e2e-delay", TaskType.ASYNC, delay_handler)
    worker.register_task("e2e-retry", TaskType.ASYNC, retry_handler)
    worker.register_task("e2e-async-client-error", TaskType.ASYNC, async_client_error_handler)
    worker.register_task("e2e-drop-result", TaskType.ASYNC, drop_result_handler)

    await worker.start("nats://localhost:4222")

    print("E2E worker started. Press Ctrl+C to stop.")

    # Keep running until interrupted
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        await worker.stop()


def main_sync() -> None:
    """Synchronous entry point for poetry scripts."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down...")


if __name__ == "__main__":
    main_sync()
