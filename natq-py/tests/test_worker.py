"""Tests for the worker module."""

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from natq import (
    LogLevel,
    NatqWorker,
    NatqWorkerOptions,
    StatusCode,
    TaskContext,
    TaskDefinition,
    TaskRunInput,
    TaskRunOutput,
    TaskType,
)
from natq.logger import Logger


def create_mock_logger() -> Logger:
    """Create a silent mock logger."""
    logger = MagicMock(spec=Logger)
    logger.debug = MagicMock()
    logger.info = MagicMock()
    logger.warn = MagicMock()
    logger.error = MagicMock()
    return logger


class TestTaskRunInput:
    """Tests for TaskRunInput dataclass."""

    def test_from_dict_basic(self) -> None:
        """Should parse basic input data."""
        data = {"foo": "bar", "num": 42}
        input_data = TaskRunInput.from_dict(data.copy())

        assert input_data.data == {"foo": "bar", "num": 42}
        assert input_data.run_id is None
        assert input_data.drop_result_on_success is False

    def test_from_dict_with_run_id(self) -> None:
        """Should extract runId from input."""
        data = {"foo": "bar", "runId": "custom-id-123"}
        input_data = TaskRunInput.from_dict(data.copy())

        assert input_data.data == {"foo": "bar"}
        assert input_data.run_id == "custom-id-123"

    def test_from_dict_with_drop_result_on_success(self) -> None:
        """Should extract dropResultOnSuccess from input."""
        data = {"foo": "bar", "dropResultOnSuccess": True}
        input_data = TaskRunInput.from_dict(data.copy())

        assert input_data.data == {"foo": "bar"}
        assert input_data.drop_result_on_success is True

    def test_from_dict_with_all_reserved_fields(self) -> None:
        """Should extract all reserved fields."""
        data = {"foo": "bar", "runId": "my-run", "dropResultOnSuccess": True}
        input_data = TaskRunInput.from_dict(data.copy())

        assert input_data.data == {"foo": "bar"}
        assert input_data.run_id == "my-run"
        assert input_data.drop_result_on_success is True

    def test_to_dict(self) -> None:
        """Should return the data dictionary."""
        input_data = TaskRunInput(data={"foo": "bar"}, run_id="test")
        assert input_data.to_dict() == {"foo": "bar"}


class TestTaskRunOutput:
    """Tests for TaskRunOutput dataclass."""

    def test_to_dict_success(self) -> None:
        """Should serialize success output correctly."""
        output = TaskRunOutput(
            id="run-123",
            task_id="my-task",
            status=200,
            data={"result": "success"},
        )
        result = output.to_dict()

        assert result == {
            "id": "run-123",
            "taskId": "my-task",
            "status": 200,
            "data": {"result": "success"},
        }

    def test_to_dict_error(self) -> None:
        """Should serialize error output correctly."""
        output = TaskRunOutput(
            id="run-123",
            task_id="my-task",
            status=400,
            error="Bad request",
        )
        result = output.to_dict()

        assert result == {
            "id": "run-123",
            "taskId": "my-task",
            "status": 400,
            "error": "Bad request",
        }

    def test_to_dict_minimal(self) -> None:
        """Should serialize minimal output correctly."""
        output = TaskRunOutput(
            id="run-123",
            task_id="my-task",
            status=204,
        )
        result = output.to_dict()

        assert result == {
            "id": "run-123",
            "taskId": "my-task",
            "status": 204,
        }


class TestTaskDefinition:
    """Tests for TaskDefinition dataclass."""

    def test_to_dict_minimal(self) -> None:
        """Should serialize minimal definition."""
        definition = TaskDefinition(
            id="my-task",
            type=TaskType.SYNC,
            subject="natq.req.my-task",
        )
        result = definition.to_dict()

        assert result == {
            "id": "my-task",
            "type": "sync",
            "subject": "natq.req.my-task",
        }

    def test_to_dict_with_schemas(self) -> None:
        """Should include schemas when present."""
        definition = TaskDefinition(
            id="my-task",
            type=TaskType.ASYNC,
            subject="natq.job.my-task",
            input_schema='{"type": "object"}',
            output_schema='{"type": "string"}',
        )
        result = definition.to_dict()

        assert result == {
            "id": "my-task",
            "type": "async",
            "subject": "natq.job.my-task",
            "inputSchema": '{"type": "object"}',
            "outputSchema": '{"type": "string"}',
        }


class TestNatqWorkerRegisterTask:
    """Tests for NatqWorker.register_task method."""

    def test_register_sync_task_with_default_subject(self) -> None:
        """Should register a sync task with computed subject."""
        logger = create_mock_logger()
        worker = NatqWorker(logger=logger)

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            return TaskRunOutput(id="test", task_id="my-task", status=200)

        worker.register_task("my-task", TaskType.SYNC, handler)

        assert "my-task" in worker.registry
        assert worker.registry["my-task"].definition.id == "my-task"
        assert worker.registry["my-task"].definition.type == TaskType.SYNC
        assert worker.registry["my-task"].definition.subject == "natq.req.my-task"

    def test_register_async_task_with_default_subject(self) -> None:
        """Should register an async task with computed subject."""
        logger = create_mock_logger()
        worker = NatqWorker(logger=logger)

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            return TaskRunOutput(id="test", task_id="my-job", status=200)

        worker.register_task("my-job", TaskType.ASYNC, handler)

        assert worker.registry["my-job"].definition.subject == "natq.job.my-job"

    def test_register_task_with_custom_subject(self) -> None:
        """Should allow custom subject override."""
        logger = create_mock_logger()
        worker = NatqWorker(logger=logger)

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            return TaskRunOutput(id="test", task_id="custom", status=200)

        worker.register_task("custom", TaskType.SYNC, handler, subject="custom.subject")

        assert worker.registry["custom"].definition.subject == "custom.subject"

    def test_register_task_with_schemas(self) -> None:
        """Should store schemas when provided."""
        logger = create_mock_logger()
        worker = NatqWorker(logger=logger)

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            return TaskRunOutput(id="test", task_id="with-schema", status=200)

        worker.register_task(
            "with-schema",
            TaskType.SYNC,
            handler,
            input_schema='{"type": "object"}',
            output_schema='{"type": "string"}',
        )

        assert worker.registry["with-schema"].definition.input_schema == '{"type": "object"}'
        assert worker.registry["with-schema"].definition.output_schema == '{"type": "string"}'


class TestNatqWorkerExecuteTaskCallback:
    """Tests for NatqWorker._execute_task_callback method."""

    @pytest.mark.asyncio
    async def test_execute_handler_successfully(self) -> None:
        """Should execute handler and return output."""
        logger = create_mock_logger()
        worker = NatqWorker(logger=logger)

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            return TaskRunOutput(
                id=ctx.run_id,
                task_id=ctx.task.id,
                status=200,
                data={"received": input.data},
            )

        worker.register_task("test-task", TaskType.SYNC, handler)
        task = worker.registry["test-task"]

        input_dict = {"foo": "bar", "runId": "custom-run-id"}
        msg_data = json.dumps(input_dict).encode()

        input_result, output = await worker._execute_task_callback(msg_data, task)

        assert input_result.data == {"foo": "bar"}
        assert output.status == 200
        assert output.id == "custom-run-id"
        assert output.data == {"received": {"foo": "bar"}}

    @pytest.mark.asyncio
    async def test_generate_run_id_if_not_provided(self) -> None:
        """Should generate runId when not in input."""
        logger = create_mock_logger()
        worker = NatqWorker(logger=logger)

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            return TaskRunOutput(
                id=ctx.run_id,
                task_id=ctx.task.id,
                status=200,
            )

        worker.register_task("test-task", TaskType.SYNC, handler)
        task = worker.registry["test-task"]

        msg_data = json.dumps({"foo": "bar"}).encode()

        _, output = await worker._execute_task_callback(msg_data, task)

        # UUID v7 format check
        assert len(output.id) == 36
        assert output.id.count("-") == 4

    @pytest.mark.asyncio
    async def test_return_406_for_invalid_json(self) -> None:
        """Should return 406 status for invalid JSON input."""
        logger = create_mock_logger()
        worker = NatqWorker(logger=logger)

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            return TaskRunOutput(id="test", task_id="test-task", status=200)

        worker.register_task("test-task", TaskType.SYNC, handler)
        task = worker.registry["test-task"]

        invalid_data = b"not valid json {{{"

        _, output = await worker._execute_task_callback(invalid_data, task)

        assert output.status == 406
        assert output.error == "Invalid JSON input"
        assert output.id == "<unknown>"

    @pytest.mark.asyncio
    async def test_return_500_when_handler_raises(self) -> None:
        """Should return 500 status when handler throws."""
        logger = create_mock_logger()
        worker = NatqWorker(logger=logger)

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            raise ValueError("Handler exploded")

        worker.register_task("test-task", TaskType.SYNC, handler)
        task = worker.registry["test-task"]

        msg_data = json.dumps({"runId": "error-run"}).encode()

        _, output = await worker._execute_task_callback(msg_data, task)

        assert output.status == 500
        assert output.error == "Unhandled exception: Handler exploded"
        assert output.id == "error-run"


class TestNatqWorkerHandleSyncMessage:
    """Tests for NatqWorker._handle_sync_message method."""

    @pytest.mark.asyncio
    async def test_respond_with_success_data(self) -> None:
        """Should respond with data and status header on success."""
        logger = create_mock_logger()
        worker = NatqWorker(logger=logger)

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            return TaskRunOutput(
                id=ctx.run_id,
                task_id=ctx.task.id,
                status=200,
                data={"result": "success"},
            )

        worker.register_task("sync-task", TaskType.SYNC, handler)
        task = worker.registry["sync-task"]

        mock_msg = MagicMock()
        mock_msg.data = json.dumps({"runId": "sync-run-1"}).encode()
        mock_msg.respond = AsyncMock()

        await worker._handle_sync_message(task, mock_msg)

        mock_msg.respond.assert_called_once()
        call_args = mock_msg.respond.call_args
        response_data = json.loads(call_args[0][0].decode())
        headers = mock_msg.headers

        assert response_data == {"result": "success"}
        assert headers["status"] == "200"

    @pytest.mark.asyncio
    async def test_respond_with_error_headers(self) -> None:
        """Should respond with error header on handler failure."""
        logger = create_mock_logger()
        worker = NatqWorker(logger=logger)

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            raise Exception("Sync handler failed")

        worker.register_task("sync-task", TaskType.SYNC, handler)
        task = worker.registry["sync-task"]

        mock_msg = MagicMock()
        mock_msg.data = json.dumps({}).encode()
        mock_msg.respond = AsyncMock()

        await worker._handle_sync_message(task, mock_msg)

        mock_msg.respond.assert_called_once()
        headers = mock_msg.headers

        assert headers["status"] == "500"
        assert "Unhandled exception" in headers["error"]

    @pytest.mark.asyncio
    async def test_respond_with_empty_data_when_no_data(self) -> None:
        """Should respond with empty body when output has no data."""
        logger = create_mock_logger()
        worker = NatqWorker(logger=logger)

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            return TaskRunOutput(
                id=ctx.run_id,
                task_id=ctx.task.id,
                status=204,
            )

        worker.register_task("sync-task", TaskType.SYNC, handler)
        task = worker.registry["sync-task"]

        mock_msg = MagicMock()
        mock_msg.data = json.dumps({}).encode()
        mock_msg.respond = AsyncMock()

        await worker._handle_sync_message(task, mock_msg)

        response_data = mock_msg.respond.call_args[0][0]
        headers = mock_msg.headers

        assert response_data == b""
        assert headers["status"] == "204"


class TestNatqWorkerHandleAsyncMessage:
    """Tests for NatqWorker._handle_async_message method."""

    def create_mock_results_kv(self) -> MagicMock:
        """Create a mock results KV."""
        kv = MagicMock()
        kv.put = AsyncMock(return_value=1)
        kv.delete = AsyncMock()
        return kv

    @pytest.mark.asyncio
    async def test_ack_message_on_success(self) -> None:
        """Should ACK message on success (status < 300)."""
        logger = create_mock_logger()
        worker = NatqWorker(
            NatqWorkerOptions(heartbeat_interval_ms=100000),
            logger=logger,
        )
        worker._results_kv = self.create_mock_results_kv()

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            return TaskRunOutput(
                id=ctx.run_id,
                task_id=ctx.task.id,
                status=200,
                data={"done": True},
            )

        worker.register_task("async-task", TaskType.ASYNC, handler)
        task = worker.registry["async-task"]

        mock_msg = MagicMock()
        mock_msg.data = json.dumps({"runId": "async-run-1"}).encode()
        mock_msg.ack = AsyncMock()
        mock_msg.nak = AsyncMock()
        mock_msg.term = AsyncMock()
        mock_msg.in_progress = AsyncMock()
        mock_msg.metadata = MagicMock(num_delivered=1)

        await worker._handle_async_message(task, mock_msg)

        mock_msg.ack.assert_called_once()
        mock_msg.nak.assert_not_called()
        mock_msg.term.assert_not_called()

    @pytest.mark.asyncio
    async def test_term_message_on_client_error(self) -> None:
        """Should TERM message on client error (300-499)."""
        logger = create_mock_logger()
        worker = NatqWorker(
            NatqWorkerOptions(heartbeat_interval_ms=100000),
            logger=logger,
        )
        worker._results_kv = self.create_mock_results_kv()

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            return TaskRunOutput(
                id=ctx.run_id,
                task_id=ctx.task.id,
                status=400,
                error="Bad request",
            )

        worker.register_task("async-task", TaskType.ASYNC, handler)
        task = worker.registry["async-task"]

        mock_msg = MagicMock()
        mock_msg.data = json.dumps({}).encode()
        mock_msg.ack = AsyncMock()
        mock_msg.nak = AsyncMock()
        mock_msg.term = AsyncMock()
        mock_msg.in_progress = AsyncMock()
        mock_msg.metadata = MagicMock(num_delivered=1)

        await worker._handle_async_message(task, mock_msg)

        mock_msg.term.assert_called_once()
        mock_msg.ack.assert_not_called()
        mock_msg.nak.assert_not_called()

    @pytest.mark.asyncio
    async def test_nak_message_on_server_error(self) -> None:
        """Should NAK message with backoff on server error (500+)."""
        logger = create_mock_logger()
        worker = NatqWorker(
            NatqWorkerOptions(heartbeat_interval_ms=100000),
            logger=logger,
        )
        worker._results_kv = self.create_mock_results_kv()

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            return TaskRunOutput(
                id=ctx.run_id,
                task_id=ctx.task.id,
                status=500,
                error="Server error",
            )

        worker.register_task("async-task", TaskType.ASYNC, handler)
        task = worker.registry["async-task"]

        mock_msg = MagicMock()
        mock_msg.data = json.dumps({}).encode()
        mock_msg.ack = AsyncMock()
        mock_msg.nak = AsyncMock()
        mock_msg.term = AsyncMock()
        mock_msg.in_progress = AsyncMock()
        mock_msg.metadata = MagicMock(num_delivered=3)

        await worker._handle_async_message(task, mock_msg)

        mock_msg.nak.assert_called_once()
        # Backoff: 2^3 = 8 seconds
        mock_msg.nak.assert_called_with(delay=8)
        mock_msg.ack.assert_not_called()
        mock_msg.term.assert_not_called()

    @pytest.mark.asyncio
    async def test_cap_backoff_at_60_seconds(self) -> None:
        """Should cap backoff delay at 60 seconds."""
        logger = create_mock_logger()
        worker = NatqWorker(
            NatqWorkerOptions(heartbeat_interval_ms=100000),
            logger=logger,
        )
        worker._results_kv = self.create_mock_results_kv()

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            return TaskRunOutput(
                id=ctx.run_id,
                task_id=ctx.task.id,
                status=500,
                error="Server error",
            )

        worker.register_task("async-task", TaskType.ASYNC, handler)
        task = worker.registry["async-task"]

        mock_msg = MagicMock()
        mock_msg.data = json.dumps({}).encode()
        mock_msg.ack = AsyncMock()
        mock_msg.nak = AsyncMock()
        mock_msg.term = AsyncMock()
        mock_msg.in_progress = AsyncMock()
        mock_msg.metadata = MagicMock(num_delivered=10)  # 2^10 = 1024, should cap at 60

        await worker._handle_async_message(task, mock_msg)

        mock_msg.nak.assert_called_with(delay=60)

    @pytest.mark.asyncio
    async def test_write_processing_status_before_handler(self) -> None:
        """Should write PROCESSING status to KV before handler execution."""
        logger = create_mock_logger()
        worker = NatqWorker(
            NatqWorkerOptions(heartbeat_interval_ms=100000),
            logger=logger,
        )

        kv_put_calls: list[dict[str, Any]] = []

        async def mock_put(key: str, data: bytes) -> int:
            kv_put_calls.append({"key": key, "data": json.loads(data.decode())})
            return 1

        mock_kv = MagicMock()
        mock_kv.put = mock_put
        worker._results_kv = mock_kv

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            return TaskRunOutput(
                id=ctx.run_id,
                task_id=ctx.task.id,
                status=200,
                data={"result": "success"},
            )

        worker.register_task("async-task", TaskType.ASYNC, handler)
        task = worker.registry["async-task"]

        mock_msg = MagicMock()
        mock_msg.data = json.dumps({"runId": "kv-run-1"}).encode()
        mock_msg.ack = AsyncMock()
        mock_msg.nak = AsyncMock()
        mock_msg.term = AsyncMock()
        mock_msg.in_progress = AsyncMock()
        mock_msg.metadata = MagicMock(num_delivered=1)

        await worker._handle_async_message(task, mock_msg)

        # Should have two KV puts: PROCESSING (100) then final status (200)
        assert len(kv_put_calls) == 2
        assert kv_put_calls[0]["key"] == "async-task.kv-run-1"
        assert kv_put_calls[0]["data"]["status"] == 100
        assert kv_put_calls[1]["key"] == "async-task.kv-run-1"
        assert kv_put_calls[1]["data"]["status"] == 200
        assert kv_put_calls[1]["data"]["data"] == {"result": "success"}

    @pytest.mark.asyncio
    async def test_not_update_kv_on_server_error(self) -> None:
        """Should NOT update KV on server error (5xx)."""
        logger = create_mock_logger()
        worker = NatqWorker(
            NatqWorkerOptions(heartbeat_interval_ms=100000),
            logger=logger,
        )

        kv_put_calls: list[dict[str, Any]] = []

        async def mock_put(key: str, data: bytes) -> int:
            kv_put_calls.append({"key": key, "data": json.loads(data.decode())})
            return 1

        mock_kv = MagicMock()
        mock_kv.put = mock_put
        worker._results_kv = mock_kv

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            return TaskRunOutput(
                id=ctx.run_id,
                task_id=ctx.task.id,
                status=500,
                error="Server error",
            )

        worker.register_task("async-task", TaskType.ASYNC, handler)
        task = worker.registry["async-task"]

        mock_msg = MagicMock()
        mock_msg.data = json.dumps({"runId": "server-error-run"}).encode()
        mock_msg.ack = AsyncMock()
        mock_msg.nak = AsyncMock()
        mock_msg.term = AsyncMock()
        mock_msg.in_progress = AsyncMock()
        mock_msg.metadata = MagicMock(num_delivered=1)

        await worker._handle_async_message(task, mock_msg)

        # Should only have ONE KV put: PROCESSING (100), NO update after server error
        assert len(kv_put_calls) == 1
        assert kv_put_calls[0]["data"]["status"] == 100
        mock_msg.nak.assert_called_once()

    @pytest.mark.asyncio
    async def test_skip_kv_creation_on_invalid_json(self) -> None:
        """Should skip KV creation on invalid JSON input."""
        logger = create_mock_logger()
        worker = NatqWorker(
            NatqWorkerOptions(heartbeat_interval_ms=100000),
            logger=logger,
        )

        mock_kv = MagicMock()
        mock_kv.put = AsyncMock()
        worker._results_kv = mock_kv

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            return TaskRunOutput(
                id=ctx.run_id,
                task_id=ctx.task.id,
                status=200,
            )

        worker.register_task("async-task", TaskType.ASYNC, handler)
        task = worker.registry["async-task"]

        mock_msg = MagicMock()
        mock_msg.data = b"not valid json {{{"
        mock_msg.ack = AsyncMock()
        mock_msg.nak = AsyncMock()
        mock_msg.term = AsyncMock()
        mock_msg.in_progress = AsyncMock()
        mock_msg.metadata = MagicMock(num_delivered=1)

        await worker._handle_async_message(task, mock_msg)

        # Should NOT call KV put for invalid JSON
        mock_kv.put.assert_not_called()
        mock_msg.term.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_result_when_drop_result_on_success(self) -> None:
        """Should delete result from KV when dropResultOnSuccess is true."""
        logger = create_mock_logger()
        worker = NatqWorker(
            NatqWorkerOptions(heartbeat_interval_ms=100000),
            logger=logger,
        )

        kv_put_calls: list[dict[str, Any]] = []
        kv_delete_calls: list[str] = []

        async def mock_put(key: str, data: bytes) -> int:
            kv_put_calls.append({"key": key, "data": json.loads(data.decode())})
            return 1

        async def mock_delete(key: str) -> None:
            kv_delete_calls.append(key)

        mock_kv = MagicMock()
        mock_kv.put = mock_put
        mock_kv.delete = mock_delete
        worker._results_kv = mock_kv

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            return TaskRunOutput(
                id=ctx.run_id,
                task_id=ctx.task.id,
                status=200,
                data={"result": "success"},
            )

        worker.register_task("async-task", TaskType.ASYNC, handler)
        task = worker.registry["async-task"]

        mock_msg = MagicMock()
        mock_msg.data = json.dumps({"runId": "drop-run-1", "dropResultOnSuccess": True}).encode()
        mock_msg.ack = AsyncMock()
        mock_msg.nak = AsyncMock()
        mock_msg.term = AsyncMock()
        mock_msg.in_progress = AsyncMock()
        mock_msg.metadata = MagicMock(num_delivered=1)

        await worker._handle_async_message(task, mock_msg)

        # Should have two KV puts: PROCESSING (100) then final status (200)
        assert len(kv_put_calls) == 2
        assert kv_put_calls[0]["data"]["status"] == 100
        assert kv_put_calls[1]["data"]["status"] == 200

        # Should have deleted the result after ack
        assert len(kv_delete_calls) == 1
        assert kv_delete_calls[0] == "async-task.drop-run-1"

        mock_msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_not_delete_result_when_drop_result_false(self) -> None:
        """Should NOT delete result when dropResultOnSuccess is false or not set."""
        logger = create_mock_logger()
        worker = NatqWorker(
            NatqWorkerOptions(heartbeat_interval_ms=100000),
            logger=logger,
        )

        mock_kv = MagicMock()
        mock_kv.put = AsyncMock(return_value=1)
        mock_kv.delete = AsyncMock()
        worker._results_kv = mock_kv

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            return TaskRunOutput(
                id=ctx.run_id,
                task_id=ctx.task.id,
                status=200,
                data={"result": "success"},
            )

        worker.register_task("async-task", TaskType.ASYNC, handler)
        task = worker.registry["async-task"]

        # Test with dropResultOnSuccess: false
        mock_msg1 = MagicMock()
        mock_msg1.data = json.dumps({"runId": "keep-run-1", "dropResultOnSuccess": False}).encode()
        mock_msg1.ack = AsyncMock()
        mock_msg1.nak = AsyncMock()
        mock_msg1.term = AsyncMock()
        mock_msg1.in_progress = AsyncMock()
        mock_msg1.metadata = MagicMock(num_delivered=1)

        await worker._handle_async_message(task, mock_msg1)
        mock_kv.delete.assert_not_called()

        # Test with dropResultOnSuccess not set
        mock_msg2 = MagicMock()
        mock_msg2.data = json.dumps({"runId": "keep-run-2"}).encode()
        mock_msg2.ack = AsyncMock()
        mock_msg2.nak = AsyncMock()
        mock_msg2.term = AsyncMock()
        mock_msg2.in_progress = AsyncMock()
        mock_msg2.metadata = MagicMock(num_delivered=1)

        await worker._handle_async_message(task, mock_msg2)
        mock_kv.delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_not_delete_result_on_client_error(self) -> None:
        """Should NOT delete result on client error even with dropResultOnSuccess."""
        logger = create_mock_logger()
        worker = NatqWorker(
            NatqWorkerOptions(heartbeat_interval_ms=100000),
            logger=logger,
        )

        mock_kv = MagicMock()
        mock_kv.put = AsyncMock(return_value=1)
        mock_kv.delete = AsyncMock()
        worker._results_kv = mock_kv

        async def handler(input: TaskRunInput, ctx: TaskContext) -> TaskRunOutput:
            return TaskRunOutput(
                id=ctx.run_id,
                task_id=ctx.task.id,
                status=400,
                error="Client error",
            )

        worker.register_task("async-task", TaskType.ASYNC, handler)
        task = worker.registry["async-task"]

        mock_msg = MagicMock()
        mock_msg.data = json.dumps({"runId": "error-run", "dropResultOnSuccess": True}).encode()
        mock_msg.ack = AsyncMock()
        mock_msg.nak = AsyncMock()
        mock_msg.term = AsyncMock()
        mock_msg.in_progress = AsyncMock()
        mock_msg.metadata = MagicMock(num_delivered=1)

        await worker._handle_async_message(task, mock_msg)

        # Should NOT delete on client error (only on success)
        mock_kv.delete.assert_not_called()
        mock_msg.term.assert_called_once()


class TestStatusCode:
    """Tests for StatusCode enum."""

    def test_status_code_values(self) -> None:
        """Status codes should have correct values."""
        assert StatusCode.PROCESSING == 100
        assert StatusCode.OK == 200
        assert StatusCode.BAD_REQUEST == 400
        assert StatusCode.NOT_ACCEPTABLE == 406
        assert StatusCode.INTERNAL_ERROR == 500
