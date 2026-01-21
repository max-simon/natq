"""
NatqWorker - Distributed task queue worker built on NATS.

This module provides the main NatqWorker class and all associated types
for building distributed task queue workers.
"""

from __future__ import annotations

import asyncio
import json
import signal
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Any, Awaitable, Callable, TypeAlias

import nats
from nats.aio.client import Client as NatsClient
from nats.aio.subscription import Subscription
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import (
    AckPolicy,
    ConsumerConfig,
    DeliverPolicy,
    DiscardPolicy,
    RetentionPolicy,
    StreamConfig,
)
from nats.js.kv import KeyValue
from uuid_utils import uuid7

from .logger import DefaultLogger, Logger, LogLevel

# ============================================================================
# Configuration
# ============================================================================


@dataclass
class NatqWorkerOptions:
    """
    Configuration options for the NatqWorker.

    All options have sensible defaults. Override only what you need.

    Example:
        >>> options = NatqWorkerOptions(log_level=LogLevel.DEBUG, heartbeat_interval_ms=5000)
        >>> worker = NatqWorker(options)
    """

    kv_bucket: str = "natq_tasks"
    """NATS KV bucket for storing task definitions. Default: "natq_tasks" """

    results_bucket: str = "natq_results"
    """NATS KV bucket for storing async task results. Default: "natq_results" """

    stream_name: str = "natq_jobs"
    """JetStream stream for async job messages. Default: "natq_jobs" """

    log_level: LogLevel = LogLevel.INFO
    """Minimum log level for the default logger. Default: LogLevel.INFO"""

    sync_subject_prefix: str = "natq.req."
    """Subject prefix for sync (request/reply) tasks. Default: "natq.req." """

    async_subject_prefix: str = "natq.job."
    """Subject prefix for async (job queue) tasks. Default: "natq.job." """

    heartbeat_interval_ms: int = 10000
    """
    Interval (ms) for sending heartbeats during async task processing.
    Heartbeats prevent message timeout while long-running tasks execute.
    Default: 10000 (10 seconds)
    """


# ============================================================================
# Task Types & Definitions
# ============================================================================


class TaskType(str, Enum):
    """
    Execution model for a task.

    - SYNC: Request/reply pattern. Caller blocks waiting for response.
    - ASYNC: Job queue pattern. Message persisted in JetStream, processed when a worker is available.
    """

    SYNC = "sync"
    ASYNC = "async"


@dataclass
class TaskDefinition:
    """
    Metadata describing a registered task. Stored in NATS KV for discovery.

    Producers can read task definitions from KV to discover available tasks
    and their expected input/output schemas.
    """

    id: str
    """Unique identifier for this task (e.g., "send-email", "process-order")"""

    type: TaskType
    """How this task is executed: sync (request/reply) or async (job queue)"""

    subject: str
    """NATS subject where this task receives messages"""

    input_schema: str | None = None
    """Optional JSON Schema describing expected input structure"""

    output_schema: str | None = None
    """Optional JSON Schema describing output structure"""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result: dict[str, Any] = {
            "id": self.id,
            "type": self.type.value,
            "subject": self.subject,
        }
        if self.input_schema is not None:
            result["inputSchema"] = self.input_schema
        if self.output_schema is not None:
            result["outputSchema"] = self.output_schema
        return result


@dataclass
class TaskContext:
    """
    Execution context provided to task handlers.

    Use this to access run metadata and correlate logs across distributed systems.
    """

    run_id: str
    """Unique identifier for this execution. Use for logging and tracing."""

    task: TaskDefinition
    """The task definition being executed"""

    worker_id: str
    """ID of the worker instance processing this task"""


# ============================================================================
# Input/Output Types
# ============================================================================

JsonSerializable: TypeAlias = (
    str | int | float | bool | None | dict[str, Any] | list[Any]
)
"""Types that can be serialized to JSON for task communication."""


@dataclass
class TaskRunInput:
    """
    Input payload received by task handlers.

    Reserved fields:
    - run_id: Custom run ID for tracking. If omitted, a UUID is generated.
    - drop_result_on_success: For async tasks only. If true, the result is deleted
      from KV immediately after acknowledgment.
    """

    data: dict[str, Any] = field(default_factory=dict)
    """The input data payload"""

    run_id: str | None = None
    """Custom run ID for tracking. If omitted, a UUID is generated."""

    drop_result_on_success: bool = False
    """For async tasks: if true, delete result from KV after success."""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TaskRunInput:
        """Create TaskRunInput from a dictionary."""
        run_id = data.pop("runId", None)
        drop_result = data.pop("dropResultOnSuccess", False)
        return cls(data=data, run_id=run_id, drop_result_on_success=drop_result)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary (returns the original data dict)."""
        return self.data


@dataclass
class TaskRunOutput:
    """
    Output structure returned by task handlers.

    The status field determines how async messages are acknowledged:
    - < 300: Success. Message is ACKed and removed from queue.
    - 300-499: Client error. Message is TERMinated (no retry).
    - >= 500: Server error. Message is NAKed and retried with backoff.

    Example:
        >>> # Success response
        >>> return TaskRunOutput(id=context.run_id, task_id="my-task", status=200, data={"result": "ok"})
        >>> # Error response
        >>> return TaskRunOutput(id=context.run_id, task_id="my-task", status=400, error="Invalid input")
    """

    id: str
    """Run ID matching the execution context"""

    task_id: str
    """Task ID that produced this output"""

    status: int
    """HTTP-style status code indicating success (2xx) or failure (4xx/5xx)"""

    data: JsonSerializable | None = None
    """Result data on success"""

    error: str | None = None
    """Error message on failure"""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result: dict[str, Any] = {
            "id": self.id,
            "taskId": self.task_id,
            "status": self.status,
        }
        if self.data is not None:
            result["data"] = self.data
        if self.error is not None:
            result["error"] = self.error
        return result


class StatusCode(IntEnum):
    """Common HTTP-style status codes for task responses."""

    PROCESSING = 100
    OK = 200
    BAD_REQUEST = 400
    NOT_ACCEPTABLE = 406
    INTERNAL_ERROR = 500


# ============================================================================
# Task Handler
# ============================================================================

TaskHandler: TypeAlias = Callable[[TaskRunInput, TaskContext], Awaitable[TaskRunOutput]]
"""
Function signature for task handlers.

Handlers receive the input payload and execution context, and must return
a TaskRunOutput. Unhandled exceptions are caught and converted to 500 errors.

Example:
    >>> async def handler(input: TaskRunInput, context: TaskContext) -> TaskRunOutput:
    ...     name = input.data.get("name", "World")
    ...     return TaskRunOutput(
    ...         id=context.run_id,
    ...         task_id=context.task.id,
    ...         status=200,
    ...         data={"greeting": f"Hello, {name}!"},
    ...     )
"""


@dataclass
class RegisteredTask:
    """Internal structure holding a task's definition and handler function."""

    definition: TaskDefinition
    handler: TaskHandler


# ============================================================================
# NatqWorker
# ============================================================================


class NatqWorker:
    """
    Distributed task queue worker that processes tasks via NATS.

    ## Lifecycle

    1. Create worker: `NatqWorker(options)`
    2. Register tasks: `worker.register_task(id, type, handler)`
    3. Start processing: `await worker.start(nats_url)`
    4. Graceful shutdown: `await worker.stop()`

    ## Task Types

    - **Sync tasks** use NATS request/reply. The producer waits for an immediate response.
    - **Async tasks** use JetStream. Messages are persisted and processed with at-least-once delivery.

    ## Error Handling

    - Handler exceptions are caught and returned as 500 errors
    - Async tasks with status >= 500 are retried with exponential backoff
    - Async tasks with status 300-499 are terminated (no retry)

    Example:
        >>> worker = NatqWorker(NatqWorkerOptions(log_level=LogLevel.DEBUG))
        >>>
        >>> async def echo_handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
        ...     return TaskRunOutput(id=ctx.run_id, task_id=ctx.task.id, status=200, data=input.data)
        >>>
        >>> worker.register_task("echo", TaskType.SYNC, echo_handler)
        >>> await worker.start("nats://localhost:4222")
    """

    def __init__(
        self,
        options: NatqWorkerOptions | None = None,
        logger: Logger | None = None,
    ) -> None:
        """
        Create a new worker instance.

        The worker does not connect to NATS until `start()` is called.

        Args:
            options: Configuration overrides (merged with defaults)
            logger: Custom logger implementation (optional)
        """
        self.options = options or NatqWorkerOptions()
        self.worker_id = str(uuid7())
        self.logger = logger or DefaultLogger(self.options.log_level)
        self.registry: dict[str, RegisteredTask] = {}

        # NATS connections and clients
        self._connection: NatsClient | None = None
        self._js: JetStreamContext | None = None
        self._kv: KeyValue | None = None
        self._results_kv: KeyValue | None = None

        # Active subscriptions and consumers for cleanup
        self._sync_subscriptions: list[Subscription] = []
        self._async_tasks: list[asyncio.Task[None]] = []
        self._is_running = False
        self._shutdown_event = asyncio.Event()

    def register_task(
        self,
        task_id: str,
        task_type: TaskType,
        handler: TaskHandler,
        *,
        subject: str | None = None,
        input_schema: str | None = None,
        output_schema: str | None = None,
    ) -> None:
        """
        Register a task handler.

        Must be called before `start()`. Each task ID must be unique.

        Args:
            task_id: Unique task identifier
            task_type: TaskType.SYNC for request/reply, TaskType.ASYNC for job queue
            handler: Async function that processes the task
            subject: Optional custom subject (default: computed from prefix + id)
            input_schema: Optional JSON Schema for input validation
            output_schema: Optional JSON Schema for output validation

        Raises:
            RuntimeError: If called after worker has started

        Example:
            >>> worker.register_task("process-order", TaskType.ASYNC, process_order_handler)
        """
        if self._is_running:
            raise RuntimeError("Cannot register tasks after worker has started")

        if subject is None:
            if task_type == TaskType.SYNC:
                subject = f"{self.options.sync_subject_prefix}{task_id}"
            else:
                subject = f"{self.options.async_subject_prefix}{task_id}"

        definition = TaskDefinition(
            id=task_id,
            type=task_type,
            subject=subject,
            input_schema=input_schema,
            output_schema=output_schema,
        )

        self.registry[task_id] = RegisteredTask(definition=definition, handler=handler)

        self.logger.info(
            f"Registered task: {task_id}",
            type=task_type.value,
            subject=subject,
            workerId=self.worker_id,
        )

    async def start(self, nats_url: str = "nats://localhost:4222") -> None:
        """
        Connect to NATS and start processing tasks.

        Startup sequence:
        1. Connect to NATS server
        2. Create KV bucket and JetStream stream if they don't exist
        3. Publish task definitions to KV for discovery
        4. Subscribe to sync task subjects
        5. Create JetStream consumers for async tasks
        6. Register signal handlers for graceful shutdown

        Args:
            nats_url: NATS server URL (default: "nats://localhost:4222")

        Raises:
            RuntimeError: If already running or connection fails
        """
        if self._is_running:
            raise RuntimeError("Worker is already running")

        self.logger.info("Starting worker...")

        try:
            await self._connect_to_nats(nats_url)
            self.logger.info("NATS connection established", workerId=self.worker_id)

            await self._ensure_nats_objects()
            self.logger.info("NATS objects ensured", workerId=self.worker_id)

            await self._register_task_definitions()
            self.logger.info(
                "Task definitions registered in KV", workerId=self.worker_id
            )

            await self._start_sync_listeners()
            self.logger.info("Sync listeners started", workerId=self.worker_id)

            await self._start_async_listeners()
            self.logger.info("Async listeners started", workerId=self.worker_id)

            self._register_signal_handlers()

            self._is_running = True
            self.logger.info(
                "Worker started successfully",
                taskCount=len(self.registry),
                workerId=self.worker_id,
                tasks=list(self.registry.keys()),
            )
        except Exception as err:
            self.logger.error(
                "Failed to start worker", error=str(err), workerId=self.worker_id
            )
            await self.stop()
            raise

    async def stop(self) -> None:
        """
        Gracefully stop the worker.

        Shutdown sequence:
        1. Signal async tasks to stop
        2. Drain sync subscriptions (finish in-flight requests)
        3. Wait for async tasks to complete
        4. Close NATS connection

        Safe to call multiple times or when not running.
        """
        if not self._is_running:
            return

        self.logger.info("Stopping worker...", workerId=self.worker_id)

        # Signal shutdown
        self._shutdown_event.set()

        # Cancel async tasks
        for task in self._async_tasks:
            task.cancel()

        # Wait for async tasks to complete
        if self._async_tasks:
            await asyncio.gather(*self._async_tasks, return_exceptions=True)
        self._async_tasks = []
        self.logger.info("Async processing finished", workerId=self.worker_id)

        # Drain sync subscriptions
        for sub in self._sync_subscriptions:
            try:
                await sub.drain()
            except Exception as err:
                self.logger.warn(
                    "Error draining sync subscription",
                    error=str(err),
                    workerId=self.worker_id,
                )
        self._sync_subscriptions = []

        # Close NATS connection
        if self._connection:
            try:
                await self._connection.drain()
                await self._connection.close()
            except Exception as err:
                self.logger.warn(
                    "Error closing NATS connection",
                    error=str(err),
                    workerId=self.worker_id,
                )
            self._connection = None

        self._js = None
        self._kv = None
        self._results_kv = None
        self._is_running = False

        self.logger.info("Worker stopped", workerId=self.worker_id)

    # ==========================================================================
    # Startup Helpers
    # ==========================================================================

    async def _connect_to_nats(self, nats_url: str) -> None:
        """Connect to NATS server."""
        self.logger.info("Connecting to NATS...", workerId=self.worker_id)
        self._connection = await nats.connect(nats_url)
        self.logger.info(
            "Connected to NATS",
            server=str(self._connection.connected_url),
            workerId=self.worker_id,
        )

    async def _ensure_nats_objects(self) -> None:
        """
        Ensure required NATS infrastructure exists (KV buckets, JetStream stream).
        Creates them if missing; uses existing if present.
        """
        if not self._connection:
            raise RuntimeError("NATS connection not established")

        self._js = self._connection.jetstream()

        # KV bucket stores task definitions for producer discovery
        try:
            self._kv = await self._js.key_value(self.options.kv_bucket)
            self.logger.debug(
                f'KV bucket "{self.options.kv_bucket}" already exists',
                workerId=self.worker_id,
            )
        except Exception:
            self._kv = await self._js.create_key_value(
                bucket=self.options.kv_bucket, history=1
            )
            self.logger.info(
                f"Created KV bucket: {self.options.kv_bucket}", workerId=self.worker_id
            )

        # JetStream stream and results KV are only needed if we have async tasks
        has_async_tasks = any(
            t.definition.type == TaskType.ASYNC for t in self.registry.values()
        )

        if has_async_tasks:
            # Results KV bucket stores async task results
            try:
                self._results_kv = await self._js.key_value(self.options.results_bucket)
                self.logger.debug(
                    f'KV bucket "{self.options.results_bucket}" already exists',
                    workerId=self.worker_id,
                )
            except Exception:
                self._results_kv = await self._js.create_key_value(
                    bucket=self.options.results_bucket, history=1
                )
                self.logger.info(
                    f"Created KV bucket: {self.options.results_bucket}",
                    workerId=self.worker_id,
                )

            # JetStream stream for async job messages
            stream_subjects = [f"{self.options.async_subject_prefix}>"]

            try:
                await self._js.stream_info(self.options.stream_name)
                self.logger.debug(
                    f'JetStream stream "{self.options.stream_name}" already exists',
                    workerId=self.worker_id,
                )
            except Exception:
                await self._js.add_stream(
                    config=StreamConfig(
                        name=self.options.stream_name,
                        subjects=stream_subjects,
                        retention=RetentionPolicy.WORK_QUEUE,
                        discard=DiscardPolicy.NEW,
                    )
                )
                self.logger.info(
                    f"Created JetStream stream: {self.options.stream_name}",
                    workerId=self.worker_id,
                )

    async def _register_task_definitions(self) -> None:
        """Publish task definitions to KV bucket for producer discovery."""
        if not self._kv:
            raise RuntimeError("KV bucket not initialized")

        for task in self.registry.values():
            await self._kv.put(
                task.definition.id,
                json.dumps(task.definition.to_dict()).encode(),
            )
            self.logger.debug(
                f"Task definition {task.definition.id} registered",
                workerId=self.worker_id,
            )

    def _register_signal_handlers(self) -> None:
        """Register signal handlers for graceful shutdown."""

        def shutdown_handler(sig: int, frame: Any) -> None:
            self.logger.info("Received shutdown signal")
            asyncio.create_task(self.stop())

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

    # ==========================================================================
    # Task Execution
    # ==========================================================================

    async def _execute_task_callback(
        self, msg_data: bytes, task: RegisteredTask
    ) -> tuple[TaskRunInput, TaskRunOutput]:
        """
        Execute a task handler with proper error handling.

        - Parses JSON input (returns 406 on invalid JSON)
        - Creates execution context with run_id
        - Catches handler exceptions (returns 500)

        Returns:
            Both the parsed input and the handler output for response handling
        """
        # Parse input - return 406 if invalid JSON
        try:
            raw_input = json.loads(msg_data.decode())
            input_data = TaskRunInput.from_dict(raw_input)
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            self.logger.warn(
                "Failed to parse task input",
                workerId=self.worker_id,
                taskId=task.definition.id,
                error=str(error),
            )
            return (
                TaskRunInput(),
                TaskRunOutput(
                    id="<unknown>",
                    task_id=task.definition.id,
                    status=StatusCode.NOT_ACCEPTABLE,
                    error="Invalid JSON input",
                ),
            )

        # Build execution context
        context = TaskContext(
            run_id=input_data.run_id or str(uuid7()),
            task=task.definition,
            worker_id=self.worker_id,
        )

        self.logger.debug(
            "Executing task",
            workerId=context.worker_id,
            taskId=context.task.id,
            runId=context.run_id,
        )

        # Execute handler - catch exceptions and return 500
        try:
            output = await task.handler(input_data, context)
            self.logger.debug(
                "Task completed",
                workerId=context.worker_id,
                taskId=context.task.id,
                runId=context.run_id,
                status=output.status,
            )
            return input_data, output
        except Exception as error:
            error_message = str(error)
            self.logger.error(
                "Task execution failed",
                workerId=context.worker_id,
                taskId=context.task.id,
                runId=context.run_id,
                error=error_message,
            )
            return (
                input_data,
                TaskRunOutput(
                    id=context.run_id,
                    task_id=context.task.id,
                    status=StatusCode.INTERNAL_ERROR,
                    error=f"Unhandled exception: {error_message}",
                ),
            )

    # ==========================================================================
    # Sync Task Handling (Request/Reply)
    # ==========================================================================

    def _create_sync_handler(self, task: RegisteredTask) -> Callable[[Msg], Awaitable[None]]:
        """Create an async callback handler for a sync task."""

        async def handler(msg: Msg) -> None:
            await self._handle_sync_message(task, msg)

        return handler

    async def _start_sync_listeners(self) -> None:
        """
        Start NATS subscriptions for all sync tasks.
        Each subscription handles request/reply for one task.
        """
        if not self._connection:
            raise RuntimeError("NATS connection not established")

        sync_tasks = [
            t for t in self.registry.values() if t.definition.type == TaskType.SYNC
        ]

        if not sync_tasks:
            self.logger.debug("No sync tasks registered", workerId=self.worker_id)
            return

        for task in sync_tasks:
            handler = self._create_sync_handler(task)
            sub = await self._connection.subscribe(
                task.definition.subject,
                cb=handler,
            )
            self._sync_subscriptions.append(sub)
            self.logger.debug(
                f'Listening on subject "{task.definition.subject}"',
                workerId=self.worker_id,
                taskId=task.definition.id,
            )

    async def _handle_sync_message(self, task: RegisteredTask, msg: Msg) -> None:
        """
        Handle a sync (request/reply) message.
        Executes the task and sends the response back to the requester.
        """
        try:
            _, output = await self._execute_task_callback(msg.data, task)

            headers = {
                "status": str(output.status),
            }
            if output.error:
                headers["error"] = output.error

            # Only encode data if present
            response_data = (
                json.dumps(output.data).encode() if output.data is not None else b""
            )
            msg.headers = headers
            await msg.respond(response_data)

        except Exception as error:
            # Fallback error response if something goes wrong outside _execute_task_callback
            self.logger.error(
                "Failed to process sync request",
                workerId=self.worker_id,
                taskId=task.definition.id,
                error=str(error),
            )

            headers = {
                "status": "500",
                "error": "Internal server error",
            }
            msg.headers = headers
            await msg.respond(b"")

    # ==========================================================================
    # Async Task Handling (JetStream)
    # ==========================================================================

    async def _start_async_listeners(self) -> None:
        """
        Start JetStream consumers for all async tasks.
        Each consumer pulls messages from the stream for one task.
        """
        if not self._js:
            has_async_tasks = any(
                t.definition.type == TaskType.ASYNC for t in self.registry.values()
            )
            if not has_async_tasks:
                self.logger.debug("No async tasks registered", workerId=self.worker_id)
                return
            raise RuntimeError("JetStream not initialized")

        async_tasks = [
            t for t in self.registry.values() if t.definition.type == TaskType.ASYNC
        ]

        for task in async_tasks:
            consumer_name = f"natq_worker_{task.definition.id}"

            # Create durable consumer if it doesn't exist
            try:
                await self._js.consumer_info(self.options.stream_name, consumer_name)
                self.logger.debug(
                    f'Consumer "{consumer_name}" already exists',
                    workerId=self.worker_id,
                    taskId=task.definition.id,
                )
            except Exception:
                await self._js.add_consumer(
                    self.options.stream_name,
                    config=ConsumerConfig(
                        durable_name=consumer_name,
                        filter_subject=task.definition.subject,
                        ack_policy=AckPolicy.EXPLICIT,
                        deliver_policy=DeliverPolicy.ALL,
                    ),
                )
                self.logger.info(
                    f"Created consumer: {consumer_name}",
                    workerId=self.worker_id,
                    taskId=task.definition.id,
                )

            # Start consuming messages in background
            processing_task = asyncio.create_task(
                self._process_async_messages(task, consumer_name)
            )
            self._async_tasks.append(processing_task)

            self.logger.info(
                f"Consuming from subject: {task.definition.subject}",
                workerId=self.worker_id,
                taskId=task.definition.id,
            )

    async def _process_async_messages(
        self, task: RegisteredTask, consumer_name: str
    ) -> None:
        """
        Message processing loop for async tasks.
        Runs until shutdown is signaled.
        """
        if not self._js:
            return

        try:
            sub = await self._js.pull_subscribe(
                task.definition.subject,
                durable=consumer_name,
                stream=self.options.stream_name,
            )

            while not self._shutdown_event.is_set():
                try:
                    messages = await sub.fetch(batch=1, timeout=1.0)
                    for msg in messages:
                        try:
                            await self._handle_async_message(task, msg)
                        except Exception as err:
                            self.logger.error(
                                "Unhandled error in async message handler",
                                error=str(err),
                                workerId=self.worker_id,
                                taskId=task.definition.id,
                            )
                except asyncio.TimeoutError:
                    # No messages available, continue polling
                    continue
                except Exception as err:
                    if not self._shutdown_event.is_set():
                        self.logger.error(
                            "Error fetching messages",
                            error=str(err),
                            workerId=self.worker_id,
                            taskId=task.definition.id,
                        )
                    break
        except asyncio.CancelledError:
            pass
        except Exception as err:
            self.logger.error(
                "Fatal error in async message processor",
                error=str(err),
                workerId=self.worker_id,
                taskId=task.definition.id,
            )

    async def _put_result_to_kv(
        self, task: RegisteredTask, key: str, output: TaskRunOutput
    ) -> None:
        """Write a task result to the results KV bucket."""
        if not self._results_kv:
            self.logger.error(
                "Results KV bucket not initialized",
                workerId=self.worker_id,
                taskId=task.definition.id,
            )
            return

        try:
            await self._results_kv.put(key, json.dumps(output.to_dict()).encode())

            self.logger.debug(
                "Result written to KV",
                key=key,
                status=output.status,
                workerId=self.worker_id,
                taskId=task.definition.id,
            )
        except Exception as error:
            # Log but don't throw - KV failure shouldn't crash the worker
            self.logger.error(
                "Failed to write result to KV",
                key=key,
                error=str(error),
                workerId=self.worker_id,
                taskId=task.definition.id,
            )

    async def _delete_result_from_kv(self, task: RegisteredTask, key: str) -> None:
        """
        Delete a task result from the results KV bucket.
        Used for fire-and-forget tasks with drop_result_on_success.
        """
        try:
            await self._results_kv.delete(key)  # type: ignore[union-attr]

            self.logger.debug(
                "Result deleted from KV",
                key=key,
                workerId=self.worker_id,
                taskId=task.definition.id,
            )
        except Exception as error:
            # Log but don't throw - KV failure shouldn't crash the worker
            self.logger.error(
                "Failed to delete result from KV",
                key=key,
                error=str(error),
                workerId=self.worker_id,
                taskId=task.definition.id,
            )

    async def _handle_async_message(self, task: RegisteredTask, msg: Msg) -> None:
        """
        Handle a single async (JetStream) message.

        Flow:
        1. Parse input to get run_id
        2. Create PROCESSING entry in KV (status 100)
        3. Execute handler
        4. Update KV with final result (success/client error) or NAK (server error)

        Acknowledgment strategy based on status code:
        - < 300: ACK - success, update KV with result
        - 300-499: TERM - client error, update KV with error
        - >= 500: NAK - server error, do NOT update KV (stays at PROCESSING)
        """
        # Send periodic heartbeats to prevent message timeout during long tasks
        heartbeat_task: asyncio.Task[None] | None = None

        async def send_heartbeats() -> None:
            interval = self.options.heartbeat_interval_ms / 1000.0
            while True:
                await asyncio.sleep(interval)
                try:
                    await msg.in_progress()
                except Exception:
                    # Message may have been acked/nacked already - ignore
                    pass

        run_id: str | None = None
        input_data: TaskRunInput | None = None

        try:
            # Step 1: Parse input to get run_id
            try:
                raw_input = json.loads(msg.data.decode())
                input_data = TaskRunInput.from_dict(raw_input)
            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                self.logger.warn(
                    "Failed to parse async task input",
                    workerId=self.worker_id,
                    taskId=task.definition.id,
                    error=str(error),
                )
                # Invalid JSON - terminate without KV entry
                await msg.term()
                return

            # Step 2: Determine run_id and KV key
            run_id = input_data.run_id or str(uuid7())
            kv_key = f"{task.definition.id}.{run_id}"

            # Step 3: Create PROCESSING entry in KV before handler execution
            await self._put_result_to_kv(
                task,
                kv_key,
                TaskRunOutput(
                    id=run_id,
                    task_id=task.definition.id,
                    status=StatusCode.PROCESSING,
                ),
            )

            # Start heartbeat
            heartbeat_task = asyncio.create_task(send_heartbeats())

            # Step 4: Build context and execute handler
            context = TaskContext(
                run_id=run_id,
                task=task.definition,
                worker_id=self.worker_id,
            )

            self.logger.debug(
                "Executing async task",
                workerId=context.worker_id,
                taskId=context.task.id,
                runId=context.run_id,
            )

            try:
                output = await task.handler(input_data, context)
            except Exception as error:
                error_message = str(error)
                self.logger.error(
                    "Async task execution failed",
                    workerId=context.worker_id,
                    taskId=context.task.id,
                    runId=context.run_id,
                    error=error_message,
                )
                output = TaskRunOutput(
                    id=run_id,
                    task_id=task.definition.id,
                    status=StatusCode.INTERNAL_ERROR,
                    error=f"Unhandled exception: {error_message}",
                )

            # Stop heartbeat
            if heartbeat_task:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass

            # Step 5: Handle based on status
            if output.status < 300:
                # Success - update KV and ACK
                await self._put_result_to_kv(task, kv_key, output)
                self.logger.debug(
                    "Async task succeeded",
                    taskId=task.definition.id,
                    status=output.status,
                    runId=output.id,
                    workerId=self.worker_id,
                )
                await msg.ack()

                # Delete result if fire-and-forget mode
                if input_data.drop_result_on_success:
                    await self._delete_result_from_kv(task, kv_key)

            elif output.status < 500:
                # Client error - update KV and TERM
                await self._put_result_to_kv(task, kv_key, output)
                self.logger.warn(
                    "Async task client error",
                    taskId=task.definition.id,
                    status=output.status,
                    runId=output.id,
                    workerId=self.worker_id,
                    error=output.error,
                )
                await msg.term()

            else:
                # Server error - NAK, do NOT update KV (stays at PROCESSING until retry succeeds)
                metadata = msg.metadata
                delivery_count = metadata.num_delivered if metadata else 1
                delay = min(2**delivery_count, 60)
                self.logger.warn(
                    "Async task server error, will retry",
                    taskId=task.definition.id,
                    status=output.status,
                    error=output.error,
                    retryDelay=delay,
                    deliveryCount=delivery_count,
                    runId=run_id,
                    workerId=self.worker_id,
                )
                await msg.nak(delay=delay)

        except Exception as error:
            # Unexpected error (e.g., KV write failed) - retry after 5s
            if heartbeat_task:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass
            self.logger.error(
                "Unexpected error processing async task",
                taskId=task.definition.id,
                workerId=self.worker_id,
                runId=run_id,
                error=str(error),
            )
            await msg.nak(delay=5)
