import sys
import os
from pathlib import Path
from gitpandas.cache import DiskCache # Import DiskCache
import logging
import logging.handlers

# --- Logging Setup --- #
LOG_DIR = Path.home() / ".gitnoc_desktop"
LOG_FILE = LOG_DIR / "app.log"

# --- Cache Setup --- #
CACHE_DIR = Path.home() / ".gitnoc_desktop"
CACHE_FILE = CACHE_DIR / "cache.json.gz"

def setup_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_level = logging.DEBUG # More verbose level
    log_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger = logging.getLogger() # Get root logger
    logger.setLevel(log_level)

    # Console Handler
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(logging.INFO) # Show INFO and above on console
    logger.addHandler(console_handler)

    # File Handler (Rotating)
    # Rotate logs after 1MB, keep 3 backup files
    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=1024*1024, backupCount=3
    )
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.DEBUG) # Log DEBUG and above to file
    logger.addHandler(file_handler)

    # Set gitpandas logger to DEBUG level specifically for file output
    gitpandas_logger = logging.getLogger('gitpandas')
    gitpandas_logger.setLevel(logging.DEBUG)
    # Note: We don't add handlers here, it uses the root logger's handlers.
    # If we wanted separate file/console levels for gitpandas, we'd add handlers here
    # and set gitpandas_logger.propagate = False

    logging.info("Logging configured.")

setup_logging()
# --- End Logging Setup --- #

# Ensure the project root (gitnoc_desktop) is discoverable if running script directly
if __package__ is None and not hasattr(sys, 'frozen'):
    script_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(script_dir))
    # If modules are in subdirs like 'ui', Python might need the parent in path too
    # sys.path.insert(0, str(script_dir.parent))
    # Attempt to set package context if needed, though imports below should work with adjusted path
    # __package__ = script_dir.name
    print(f"Adjusted sys.path for direct execution: {script_dir}")

from PySide6.QtWidgets import QApplication
from PySide6.QtCore import Qt # Import Qt for attribute setting
from qt_material import apply_stylesheet

# Import the main window from its new location
# Assuming main.py is in gitnoc_desktop, ui is a subfolder
from ui.main_window import MainWindow

# --- Main Application Execution --- #
if __name__ == "__main__":
    # Set environment variable for Qt to handle high DPI scaling (alternative)
    # os.environ["QT_AUTO_SCREEN_SCALE_FACTOR"] = "1"
    # Use Application attribute (preferred)
    QApplication.setAttribute(Qt.AA_EnableHighDpiScaling)

    # --- Initialize Disk Cache --- #
    try:
        # Ensure cache directory exists
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        disk_cache = DiskCache(filepath=str(CACHE_FILE), max_keys=500) # Adjust max_keys if needed
        logging.info(f"DiskCache initialized. Path: {CACHE_FILE}")
        # TODO: Pass 'disk_cache' to wherever gitpandas.Repository instances are created.
        # Example: window = MainWindow(cache=disk_cache)
        #          or data_manager = DataManager(cache=disk_cache)
    except Exception as e:
        logging.error(f"Failed to initialize DiskCache: {e}")
        disk_cache = None # Fallback or handle error appropriately

    app = QApplication(sys.argv)

    # Setup stylesheet - use qt-material
    apply_stylesheet(app, theme='dark_blue.xml')

    # Create and show the main window
    try:
        # Pass the initialized disk_cache to the MainWindow
        window = MainWindow(cache_backend=disk_cache)
        window.show()
    except Exception as e:
        logging.error(f"Failed to create or show MainWindow: {e}")
        sys.exit(1)

    # --- Connect Cache Save on Exit --- #
    if disk_cache:
        app.aboutToQuit.connect(disk_cache.save)
        logging.info("Connected disk_cache.save to app.aboutToQuit signal.")

    # Start the Qt event loop
    sys.exit(app.exec()) 