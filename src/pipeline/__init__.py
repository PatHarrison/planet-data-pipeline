import logging
from pathlib import Path
from typing import Any, Dict

from .utils import setup_data_paths, setup_logging


logger = logging.getLogger("pipeline")

config = {
        "log_level": "DEBUG",
        "log_path": Path("logs"),
        "data_path": Path("data"),
        "api_key": None
}

def initialize(config: Dict[str, Any]=config):
    """
    Initialize the pipeline.

    Args:
        config (Dict[str, Any]): General configuration of the pipeline.

    Returns:
        None

    Raises:
        None
    """
    setup_logging(config["log_path"], level=config["log_level"])
    setup_data_paths(path=config["data_path"])

    return True
