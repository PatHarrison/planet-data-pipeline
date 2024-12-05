import logging
from pathlib import Path
from typing import Any, Dict

from .utils import setup_data_paths, setup_logging


logger = logging.getLogger("pipeline")

config = {
        "log_level": "DEBUG",
        "log_path": Path("logs"),
        "data_path": Path("data"),
        "image_dir_name": "imagedir",
        "search_dir_name": "search_results",
}

def initialize(new_config: Dict[str, Any]=config):
    """
    Initialize the pipeline.

    Args:
        config (Dict[str, Any]): General configuration of the pipeline.

    Returns:
        None

    Raises:
        None
    """
    global config
    if new_config:
        config.update(new_config)

    setup_logging(new_config["log_path"], level=new_config["log_level"])
    setup_data_paths(path=new_config["data_path"],
                     images_dir_name=new_config["image_dir_name"],
                     search_dir_name=new_config["search_dir_name"]
                     )


    return True
