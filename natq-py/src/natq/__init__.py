"""
natq - A language-agnostic distributed task queue framework built on NATS.

This package provides the NatqWorker class for building distributed task queue
workers that communicate via NATS messaging.

Example:
    >>> from natq import NatqWorker, TaskType, TaskRunInput, TaskRunOutput, TaskContext
    >>>
    >>> async def echo_handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
    ...     return TaskRunOutput(id=ctx.run_id, task_id=ctx.task.id, status=200, data=input.data)
    >>>
    >>> worker = NatqWorker()
    >>> worker.register_task("echo", TaskType.SYNC, echo_handler)
    >>> await worker.start("nats://localhost:4222")
"""

from .logger import DefaultLogger, Logger, LogLevel
from .worker import (
    JsonSerializable,
    NatqWorker,
    NatqWorkerOptions,
    RegisteredTask,
    StatusCode,
    TaskContext,
    TaskDefinition,
    TaskHandler,
    TaskRunInput,
    TaskRunOutput,
    TaskType,
)

__all__ = [
    # Logger
    "Logger",
    "DefaultLogger",
    "LogLevel",
    # Worker
    "NatqWorker",
    "NatqWorkerOptions",
    "TaskType",
    "TaskDefinition",
    "TaskContext",
    "TaskRunInput",
    "TaskRunOutput",
    "TaskHandler",
    "RegisteredTask",
    "StatusCode",
    "JsonSerializable",
]

__version__ = "0.1.0"
