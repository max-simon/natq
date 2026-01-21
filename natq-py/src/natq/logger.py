"""Logger interface and default implementation for natq."""

from abc import ABC, abstractmethod
from enum import IntEnum
from typing import Any
import json
import sys
from datetime import datetime, timezone


class LogLevel(IntEnum):
    """Log levels for the natq logger."""

    DEBUG = 10
    INFO = 20
    WARN = 30
    ERROR = 40


class Logger(ABC):
    """
    Abstract logger interface.

    Implement this interface to use a custom logging solution with NatqWorker.
    """

    @abstractmethod
    def debug(self, message: str, **kwargs: Any) -> None:
        """Log a debug message."""
        pass

    @abstractmethod
    def info(self, message: str, **kwargs: Any) -> None:
        """Log an info message."""
        pass

    @abstractmethod
    def warn(self, message: str, **kwargs: Any) -> None:
        """Log a warning message."""
        pass

    @abstractmethod
    def error(self, message: str, **kwargs: Any) -> None:
        """Log an error message."""
        pass


class DefaultLogger(Logger):
    """
    Default JSON logger implementation.

    Outputs structured JSON logs to stdout. Each log entry includes:
    - timestamp: ISO 8601 format
    - level: Log level name
    - message: Log message
    - Additional context fields from kwargs
    """

    def __init__(self, level: LogLevel = LogLevel.INFO) -> None:
        """
        Create a new DefaultLogger.

        Args:
            level: Minimum log level to output. Messages below this level are ignored.
        """
        self.level = level

    def _log(self, level: LogLevel, message: str, **kwargs: Any) -> None:
        """Internal logging method."""
        if level < self.level:
            return

        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level.name,
            "message": message,
            **kwargs,
        }
        print(json.dumps(entry), file=sys.stdout, flush=True)

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log a debug message."""
        self._log(LogLevel.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        """Log an info message."""
        self._log(LogLevel.INFO, message, **kwargs)

    def warn(self, message: str, **kwargs: Any) -> None:
        """Log a warning message."""
        self._log(LogLevel.WARN, message, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        """Log an error message."""
        self._log(LogLevel.ERROR, message, **kwargs)
