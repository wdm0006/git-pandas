import logging
from typing import Any

# Setup library logging
logger = logging.getLogger("gitpandas")  # Use a consistent name
logger.addHandler(logging.NullHandler())


def get_logger(name: str | None = None) -> logging.Logger:
    """Get a logger instance for the specified name.

    Args:
        name: The name of the logger to get. If None, returns the main gitpandas logger.
              If specified, returns a child logger of the main gitpandas logger.

    Returns:
        logging.Logger: The requested logger instance.
    """
    if name is None:
        return logger
    return logger.getChild(name)


def set_log_level(level: int | str) -> None:
    """Set the logging level for the gitpandas library.

    Args:
        level: The logging level to set. Can be either a string (e.g., 'INFO')
               or an integer (e.g., logging.INFO).
    """
    logger.setLevel(level)


def add_stream_handler(
    level: int | str = logging.INFO,
    format_string: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    **handler_kwargs: Any,
) -> None:
    """Add a stream handler to the gitpandas logger.

    Args:
        level: The logging level for the handler. Defaults to INFO.
        format_string: The format string for log messages.
        **handler_kwargs: Additional keyword arguments to pass to StreamHandler.
    """
    # Avoid adding duplicate handlers
    if any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        logger.warning("StreamHandler already exists for gitpandas logger.")
        return

    handler = logging.StreamHandler(**handler_kwargs)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def add_file_handler(
    filename: str,
    level: int | str = logging.INFO,
    format_string: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    **handler_kwargs: Any,
) -> None:
    """Add a file handler to the gitpandas logger.

    Args:
        filename: The name of the file to log to.
        level: The logging level for the handler. Defaults to INFO.
        format_string: The format string for log messages.
        **handler_kwargs: Additional keyword arguments to pass to FileHandler.
    """
    # Avoid adding duplicate file handlers for the same file
    if any(isinstance(h, logging.FileHandler) and h.baseFilename == filename for h in logger.handlers):
        logger.warning(f"FileHandler for {filename} already exists for gitpandas logger.")
        return

    handler = logging.FileHandler(filename, **handler_kwargs)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def remove_all_handlers() -> None:
    """Remove all handlers from the gitpandas logger (except the default NullHandler)."""
    for handler in logger.handlers[:]:
        if not isinstance(handler, logging.NullHandler):
            logger.removeHandler(handler)


__all__ = [
    "logger",
    "get_logger",
    "set_log_level",
    "add_stream_handler",
    "add_file_handler",
    "remove_all_handlers",
]
