"""
Unit tests for logging configuration
"""
import logging
import tempfile
from pathlib import Path

import pytest

from bcn.logging_config import BCNLogger


class TestBCNLoggerSetup:
    """Test BCNLogger configuration"""

    def setup_method(self):
        """Reset logger state before each test"""
        BCNLogger._configured = False
        BCNLogger._loggers = {}
        # Remove all handlers from bcn logger
        bcn_logger = logging.getLogger("bcn")
        for handler in bcn_logger.handlers[:]:
            bcn_logger.removeHandler(handler)

    def test_logger_setup_default_level(self):
        """Test logger configuration with default INFO level"""
        BCNLogger.setup_logging()
        logger = BCNLogger.get_logger("test")
        assert logger.name == "bcn.test"
        assert logger.level == logging.NOTSET  # Logger uses parent level

    def test_logger_setup_debug_level(self):
        """Test logger configuration with DEBUG level"""
        BCNLogger.setup_logging(level="DEBUG")
        bcn_logger = logging.getLogger("bcn")
        assert bcn_logger.level == logging.DEBUG

    def test_logger_setup_error_level(self):
        """Test logger configuration with ERROR level"""
        BCNLogger.setup_logging(level="ERROR")
        bcn_logger = logging.getLogger("bcn")
        assert bcn_logger.level == logging.ERROR

    def test_logger_get_logger_caches(self):
        """Test that get_logger caches loggers"""
        BCNLogger.setup_logging()
        logger1 = BCNLogger.get_logger("module1")
        logger2 = BCNLogger.get_logger("module1")
        assert logger1 is logger2

    def test_logger_get_different_loggers(self):
        """Test that different modules get different loggers"""
        BCNLogger.setup_logging()
        logger1 = BCNLogger.get_logger("module1")
        logger2 = BCNLogger.get_logger("module2")
        assert logger1 is not logger2
        assert logger1.name == "bcn.module1"
        assert logger2.name == "bcn.module2"

    def test_logger_setup_with_file(self):
        """Test logger configuration with file output"""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "test.log"
            BCNLogger.setup_logging(level="INFO", log_file=str(log_file))
            logger = BCNLogger.get_logger("test")

            logger.info("Test message")

            # Verify log file was created and contains the message
            assert log_file.exists()
            content = log_file.read_text()
            assert "Test message" in content
            assert "bcn.test" in content

    def test_logger_setup_custom_format(self):
        """Test logger configuration with custom format"""
        custom_format = "%(levelname)s: %(message)s"
        BCNLogger.setup_logging(level="INFO", format_string=custom_format)

        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "test.log"
            # Re-setup with the custom format and file
            BCNLogger._configured = False
            BCNLogger._loggers = {}
            bcn_logger = logging.getLogger("bcn")
            for handler in bcn_logger.handlers[:]:
                bcn_logger.removeHandler(handler)

            BCNLogger.setup_logging(
                level="INFO", log_file=str(log_file), format_string=custom_format
            )
            logger = BCNLogger.get_logger("test")
            logger.info("Test message")

            content = log_file.read_text()
            assert "INFO: Test message" in content

    def test_logger_auto_setup(self):
        """Test that get_logger auto-initializes if not configured"""
        logger = BCNLogger.get_logger("test")
        assert BCNLogger._configured
        assert logger is not None

    def test_logger_output_to_console(self, caplog):
        """Test logger output is captured in caplog"""
        BCNLogger.setup_logging(level="DEBUG")
        logger = BCNLogger.get_logger("test")

        with caplog.at_level(logging.DEBUG, logger="bcn.test"):
            logger.debug("Debug message")
            logger.info("Info message")
            logger.error("Error message")

        assert "Debug message" in caplog.text
        assert "Info message" in caplog.text
        assert "Error message" in caplog.text

    def test_logger_levels(self, caplog):
        """Test different log levels work correctly"""
        BCNLogger.setup_logging(level="WARNING")
        logger = BCNLogger.get_logger("test")

        with caplog.at_level(logging.WARNING, logger="bcn.test"):
            logger.debug("Debug message")
            logger.info("Info message")
            logger.warning("Warning message")
            logger.error("Error message")

        # Only WARNING and ERROR should be captured at WARNING level
        assert "Debug message" not in caplog.text
        assert "Info message" not in caplog.text
        assert "Warning message" in caplog.text
        assert "Error message" in caplog.text
