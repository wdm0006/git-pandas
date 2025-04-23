import logging
import json
from pathlib import Path

logger = logging.getLogger(__name__)

# Constants for configuration file path
CONFIG_DIR = Path.home() / ".gitnoc_desktop"
CONFIG_FILE = CONFIG_DIR / "repos.json"

def load_repositories():
    """Load saved repositories from config file."""
    logger.debug(f"Attempting to load repositories from {CONFIG_FILE}")
    repositories = {}
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r") as f:
                loaded_data = json.load(f)
            
            # Convert old format (dict[name, path]) to new format (dict[name, dict[path, default_branch]])
            for name, value in loaded_data.items():
                if isinstance(value, str): # Old format detected
                    repositories[name] = {'path': value, 'default_branch': None}
                    logger.info(f"Converted old format repo entry: {name}")
                elif isinstance(value, dict) and 'path' in value: # New format
                    repositories[name] = {
                        'path': value['path'],
                        'default_branch': value.get('default_branch') # None if missing
                    }
                else:
                    logger.warning(f"Skipping invalid repository entry in config: {name} = {value}")

            logger.info(f"Loaded {len(repositories)} repositories from config.")
            return repositories
        except json.JSONDecodeError:
            logger.warning(f"Config file {CONFIG_FILE} is corrupted. Starting with empty list.")
            return {}
        except Exception as e:
            logger.exception(f"Unexpected error loading config file {CONFIG_FILE}")
            return {}
    else:
        logger.info("Config file not found. Starting with empty repository list.")
        return {}

def save_repositories(repositories):
    """Save repositories to config file."""
    logger.debug(f"Saving {len(repositories)} repositories to {CONFIG_FILE}")
    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        # Ensure data saved is in the new format {name: {path: ..., default_branch: ...}}
        data_to_save = {}
        for name, info in repositories.items():
            if isinstance(info, dict) and 'path' in info:
                data_to_save[name] = {
                    'path': info['path'],
                    'default_branch': info.get('default_branch')
                }
            else:
                 logger.warning(f"Skipping save for malformed repo info: {name} = {info}")

        with open(CONFIG_FILE, "w") as f:
            json.dump(data_to_save, f, indent=4)
        logger.info("Repositories saved successfully.")
    except Exception as e:
        logger.exception(f"Failed to save repositories to {CONFIG_FILE}") 