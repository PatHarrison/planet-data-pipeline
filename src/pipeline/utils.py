"""
Title: Utils
Project: planet-data-pipeline
Authors:
    - Patrick Harrison
    - Lee-Anna Gibson
Purpose:
    Utility Module for the pipeline. Contains useful functions
    for facilitating the datapipeline. Functions in this module
    should be used across the pipeline and in scripts running the
    pipeline.

    Functions:
        - setup_logging
        - indent
        - read_geojson
"""
import os
import json
import logging
import logging.config
from pathlib import Path
from typing import Any, Dict, List
from datetime import datetime

import geopandas as gpd


logger = logging.getLogger("pipeline")


def setup_logging(log_directory: Path=Path("logs"), level: str="DEBUG"):
    """Sets up logging for the pipeline application.
    This function configures logging for the application including
    creating a log directory (if it does not exist) and generates a
    time stamped log file name. Log levels can also be passed here.

    parameters:
        log_directory (Path): The directory where the log files should be
                              stored. The default is 'logs/'
        level (str): The log level string representation for the logging
                     Currently, only one log level is supported for all
                     handlers
    returns:
        None
    raises:
        FileNotFoundError: If the logging configuration file cannot be found.
        json.JSONDecodeError: If the logging configuration file is not a valid
                              JSON.
        KeyError: If the expected keys are not present in the logging config
                  file.
        OSError: If there is an issue creating the directory
    """
    try:
        log_directory.mkdir(exist_ok=True) # log directory
    except OSError as e:
        print(f"Error creating the logging directory. Storing logs in root.")
        log_directory = Path(os.getcwd())

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_directory / f"pylanet_{timestamp}.log"

    # read logging_conf.json
    config_path = Path(os.getcwd()) / "logging_config.json"

    try:
        with open(config_path, "r") as f:
            config = json.load(f)
            config["handlers"]["fileHandler"]["filename"] = log_file
            config["loggers"]["pipeline"]["level"] = level
            config["handlers"]["fileHandler"]["level"] = level
            config["handlers"]["consoleHandler"]["level"] = level
            logging.config.dictConfig(config)

    except FileNotFoundError:
        print(f"The logging configuration file '{config_path}' was not found")
        raise
    except json.JSONDecodeError:
        print(f"Failed to decode JSON in {config_path}")
        raise
    except KeyError as e:
        print(f"Error: Missing expected key in logging config: {e}")
        raise


def indent(data: Dict[Any, Any]):
    """Pretty printing for dictionary data.
    Prints the dictionary to stdout with and indent level for
    human readablilty. Uses json.dumps method to convert the
    dictionary to valid JSON

    parameters:
        data (Dict): Data dictionary to be printed nicely.
    returns:
        None
    Raises:
        TypeError: If the provided data is not a dict. 
        json.JSONDecodeError: If the data cannot be converted into
                              a dictionary
    """
    try:
        print(json.dumps(data, indent=2))
    except TypeError as e:
        logging.warning(f"Unable to print {data} in JSON form. {e}")
    except json.JSONDecodeError as e:
        logging.error(f"JSON encoding error: {e}")


def authenticate(api_key: str) -> str:
    """Stores API in environment variable.
    
    parameter:
        api_key (str): Planet API key for authentication.
    returns
        str: Planet API key for authentication from environment vars.
    """
    if "PL_API_KEY" in os.environ:
        auth = os.environ["PL_API_KEY"]
    else:
        os.environ["PL_API_KEY"] = api_key
        auth = os.environ["PL_API_KEY"]
    return auth




def read_geojson(file_path: Path) -> Dict[str, Any]:
    """Reads a GeoJSON file and returns its contents as a dictionary.
    Checks if 'features' and 'geometry' keys are present in the file
    before attempting returning dictionary. An empty dictionary is
    returned if the file is unreachable or invalid.

    Parameters:
        file_path (Path): Path to the GeoJSON file to read.
    Returns:
        Dict[str, Any]: dictionary representation of the geometry of the first
                        feature in the GeoJSON file. Returns an empty dictionay
                        if an error occurs or if the geometry data is missing.
    Raises:
        FileNotFoundError: If the specified file cannot be found.
        json.JSONDecodeError: If the file contains invlaud JSON.
        KeyError: if the expected 'features' or 'geometry' is missing.
        Exception: If an unexpected error occurs.
    """
    try:
        with open(file_path, 'r', encoding="utf-8") as geojson_file:
            geojson_data = json.load(geojson_file)
            logger.info(f"Successfully read {file_path} geojson")
        
        # Check if "features" and "geometry" keys are present
        if "features" in geojson_data and geojson_data["features"]:
            geometry = geojson_data["features"][0].get("geometry")
            if geometry is not None:
                return geometry
            else:
                logger.error(f"Geometry data not found in the first feature of {file_path}")
                return {}
        else:
            logger.error(f"Features data not found in {file_path}")
            return {}

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return {}
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON in file: {file_path}")
        return {}
    except KeyError as e:
        logger.error(f"Missing key in GeoJSON data: {e}")
        return {}
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return {}


def overlap_percent(aoi: List[Dict[str, Any]],
                    footprints: List[Dict[str, Any]],
                    epsg: str|int
                    ) -> float:
    """Determines the overlap percentage of the study area.
    Uses geopandas to merge, clip and intersect the footprints
    with the study area feature to get the percentage overlap.

    parameters:
        feature (Path): study area geojson
        footprints (geojson features): geosjon features of the footprints.
        espg (str|int): ESPG identifier number. `3005` for ESPG:3005.
    returns:
        (float): coverage percentage of the found footprints with the 
            study area.
    """
    logger.debug("Determining overlap percentage of footprints")
    footprints_df = gpd.GeoDataFrame.from_features(footprints)
    aoi_df = gpd.GeoDataFrame.from_features(aoi, crs="EPSG:4326")

    if footprints_df.crs is None:
        # Sets footprints to WGS84 crs as these will come from api
        footprints_df = footprints_df.set_crs("EPSG:4326")
        logger.debug(f"Setting footprints to WGS84 crs")

    # Ensure both GeoDataFrames are in the same CRS
    footprints_df = footprints_df.to_crs(f"EPSG:{epsg}")
    aoi_df = aoi_df.to_crs(f"EPSG:{epsg}")
    logger.debug(f"Projected footprints and study area to EPSG:{epsg}")

    # Merge all footprints into a single polygon
    merged_footprint = gpd.GeoSeries([footprints_df.unary_union])
    merged_footprint = merged_footprint.set_crs(f"EPSG:{epsg}")

    # Clip the merged footprint polygon to the target area (features)
    clipped_footprint = aoi_df.intersection(merged_footprint).iloc[0]

    # Calculate the area of the target feature and the clipped footprint
    aoi_area = aoi_df.area.iloc[0]
    clipped_area = clipped_footprint.area if not clipped_footprint.is_empty else 0

    # Calculate the coverage percentage
    coverage_percentage = (clipped_area / aoi_area) * 100

    return coverage_percentage
    
