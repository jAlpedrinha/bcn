"""
Logging configuration for BCN
"""
import logging
import sys
from typing import Optional


class BCNLogger:
    """Centralized logging setup for BCN"""

    _loggers = {}
    _configured = False

    @staticmethod
    def setup_logging(
        level: str = "INFO",
        log_file: Optional[str] = None,
        format_string: Optional[str] = None
    ):
        """
        Configure logging for BCN

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR)
            log_file: Optional file path for logs
            format_string: Custom format string
        """
        if format_string is None:
            format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

        # Configure root logger
        root_logger = logging.getLogger("bcn")
        root_logger.setLevel(getattr(logging, level.upper()))

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, level.upper()))
        console_handler.setFormatter(logging.Formatter(format_string))
        root_logger.addHandler(console_handler)

        # File handler if specified
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(getattr(logging, level.upper()))
            file_handler.setFormatter(logging.Formatter(format_string))
            root_logger.addHandler(file_handler)

        BCNLogger._configured = True

    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """Get a logger for a module"""
        if not BCNLogger._configured:
            BCNLogger.setup_logging()

        if name not in BCNLogger._loggers:
            BCNLogger._loggers[name] = logging.getLogger(f"bcn.{name}")

        return BCNLogger._loggers[name]
