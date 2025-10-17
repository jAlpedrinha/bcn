"""
Custom exceptions for BCN (Backup/Restore for Iceberg)
"""


class BCNError(Exception):
    """Base exception for BCN operations"""
    pass


class BCNCriticalError(BCNError):
    """Critical error that should stop the operation"""
    pass


class BCNWarning(BCNError):
    """Non-critical error that can be logged and skipped"""
    pass
