import os
import logging
from pathlib import Path

from .utils import setup_logging, authenticate, setup_data_path


logger = logging.getLogger("pipeline")

config = {
    "log_level": "DEBUG",
    "data_path": Path(os.getcwd()) / "data",
    "api_key": None,
}

def initialize():
    """Initialize Pylanet project"""
    setup_logging(level=config["log_level"])
    authenticate(api_key=config["api_key"])
    setup_data_path(path=config["data_path"])


def deactivate() -> int:
    """Deletes the api key from the environment.

    parameters:
        None
    returns:
        int: 0 if successful, 1 if error deleteing `PL_API_KEY`
    raises:
        KeyError: If PL_API_KEY is not in environment.
        PermissionError: If the user does not have permission to pop 
                         enivornment variables.
        OSError: If there was trouble reading or popping environment variables.
    """
    try:
        os.environ.pop("PL_API_KEY")
        logger.debug(f"Removed PL_API_KEY from environment variables")
    except KeyError as e:
        logger.warning(f"PL_API_KEY not found in environment. Failed to remove: {e}")
        return 1
    except PermissionError as e:
        logger.warning(f"You do not have permision to remove PL_API_KEY from environment: {e}")
    except OSError as e:
        logger.warning(f"Error removing PL_API_KEY from environment. Environment table may be corrupted. {e}")
        return 1
    return 0
