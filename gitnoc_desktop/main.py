import sys
import os
from pathlib import Path
from gitpandas.cache import DiskCache
import logging
import logging.handlers

# Configuration paths
LOG_DIR = Path.home() / ".gitnoc_desktop"
LOG_FILE = LOG_DIR / "app.log"
CACHE_DIR = Path.home() / ".gitnoc_desktop"
CACHE_FILE = CACHE_DIR / "cache.json.gz"

def setup_logging():
    """Configure application logging with console and file handlers."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_level = logging.DEBUG
    log_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Console Handler - INFO level and above
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    # File Handler - DEBUG level and above, rotates at 1MB with 3 backups
    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=1024*1024, backupCount=3
    )
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)

    # Configure gitpandas logger
    gitpandas_logger = logging.getLogger('gitpandas')
    gitpandas_logger.setLevel(logging.DEBUG)

    logging.info("Logging configured.")

setup_logging()

# Ensure module discoverability when running directly
if __package__ is None and not hasattr(sys, 'frozen'):
    script_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(script_dir))
    print(f"Adjusted sys.path for direct execution: {script_dir}")

from PySide6.QtWidgets import QApplication
from PySide6.QtCore import Qt
from qt_material import apply_stylesheet
from ui.main_window import MainWindow

if __name__ == "__main__":
    # Enable high DPI scaling
    QApplication.setAttribute(Qt.AA_EnableHighDpiScaling)

    # Initialize disk cache
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        disk_cache = DiskCache(filepath=str(CACHE_FILE), max_keys=500)
        logging.info(f"DiskCache initialized. Path: {CACHE_FILE}")
    except Exception as e:
        logging.error(f"Failed to initialize DiskCache: {e}")
        disk_cache = None

    app = QApplication(sys.argv)

    # Apply material design theme
    apply_stylesheet(app, theme='dark_blue.xml')

    # Create and show main window
    try:
        window = MainWindow(cache_backend=disk_cache)
        window.show()
    except Exception as e:
        logging.error(f"Failed to create or show MainWindow: {e}")
        sys.exit(1)

    # Save cache on application exit
    if disk_cache:
        app.aboutToQuit.connect(disk_cache.save)
        logging.info("Connected disk_cache.save to app.aboutToQuit signal.")

    sys.exit(app.exec()) 