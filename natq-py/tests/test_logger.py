"""Tests for the logger module."""

import json
from io import StringIO
from unittest.mock import patch

import pytest

from natq import DefaultLogger, LogLevel


class TestLogLevel:
    """Tests for LogLevel enum."""

    def test_log_levels_ordered(self) -> None:
        """Log levels should be ordered from least to most severe."""
        assert LogLevel.DEBUG < LogLevel.INFO < LogLevel.WARN < LogLevel.ERROR

    def test_log_level_values(self) -> None:
        """Log level values should match expected integers."""
        assert LogLevel.DEBUG == 10
        assert LogLevel.INFO == 20
        assert LogLevel.WARN == 30
        assert LogLevel.ERROR == 40


class TestDefaultLogger:
    """Tests for DefaultLogger class."""

    def test_default_level_is_info(self) -> None:
        """Default log level should be INFO."""
        logger = DefaultLogger()
        assert logger.level == LogLevel.INFO

    def test_custom_log_level(self) -> None:
        """Logger should accept custom log level."""
        logger = DefaultLogger(LogLevel.DEBUG)
        assert logger.level == LogLevel.DEBUG

    def test_info_message_logged(self) -> None:
        """INFO messages should be logged when level is INFO."""
        logger = DefaultLogger(LogLevel.INFO)
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            logger.info("Test message", key="value")
            output = mock_stdout.getvalue()

        log_entry = json.loads(output.strip())
        assert log_entry["level"] == "INFO"
        assert log_entry["message"] == "Test message"
        assert log_entry["key"] == "value"
        assert "timestamp" in log_entry

    def test_debug_message_filtered_at_info_level(self) -> None:
        """DEBUG messages should be filtered when level is INFO."""
        logger = DefaultLogger(LogLevel.INFO)
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            logger.debug("Debug message")
            output = mock_stdout.getvalue()

        assert output == ""

    def test_debug_message_logged_at_debug_level(self) -> None:
        """DEBUG messages should be logged when level is DEBUG."""
        logger = DefaultLogger(LogLevel.DEBUG)
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            logger.debug("Debug message")
            output = mock_stdout.getvalue()

        log_entry = json.loads(output.strip())
        assert log_entry["level"] == "DEBUG"
        assert log_entry["message"] == "Debug message"

    def test_warn_message_logged(self) -> None:
        """WARN messages should be logged."""
        logger = DefaultLogger(LogLevel.INFO)
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            logger.warn("Warning message")
            output = mock_stdout.getvalue()

        log_entry = json.loads(output.strip())
        assert log_entry["level"] == "WARN"
        assert log_entry["message"] == "Warning message"

    def test_error_message_logged(self) -> None:
        """ERROR messages should be logged."""
        logger = DefaultLogger(LogLevel.INFO)
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            logger.error("Error message", error="details")
            output = mock_stdout.getvalue()

        log_entry = json.loads(output.strip())
        assert log_entry["level"] == "ERROR"
        assert log_entry["message"] == "Error message"
        assert log_entry["error"] == "details"

    def test_error_level_filters_lower_levels(self) -> None:
        """Only ERROR messages should be logged when level is ERROR."""
        logger = DefaultLogger(LogLevel.ERROR)
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            logger.debug("Debug")
            logger.info("Info")
            logger.warn("Warn")
            logger.error("Error")
            output = mock_stdout.getvalue()

        lines = [line for line in output.strip().split("\n") if line]
        assert len(lines) == 1
        log_entry = json.loads(lines[0])
        assert log_entry["level"] == "ERROR"

    def test_multiple_kwargs_logged(self) -> None:
        """Multiple kwargs should be included in log output."""
        logger = DefaultLogger(LogLevel.INFO)
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            logger.info("Message", key1="value1", key2=123, key3=True)
            output = mock_stdout.getvalue()

        log_entry = json.loads(output.strip())
        assert log_entry["key1"] == "value1"
        assert log_entry["key2"] == 123
        assert log_entry["key3"] is True
