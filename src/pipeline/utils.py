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
import logging
import os
import json
from datetime import datetime as dt
from typing import Any, Dict, List
import logging.config
from pathlib import Path

import geopandas as gpd
import pandas as pd

import pipeline


logger = logging.getLogger("pipeline")


def setup_data_paths(path: Path, images_dir_name: str, search_dir_name: str) -> Path:
    """
    Setups up a directory to store data with sub directories for image orders
    and search results. All parent directories of data path will be
    created if they do not exist.

    Args:
        path (Path): Path to the data directory

    Returns:
        None
    
    Raises:
        FileExistsError: If logging directory exists as a file.
        OSError: Error from OS when trying to create directory
    """
    path.mkdir(exist_ok=True, parents=True) 

    # Fill out data sub directories
    search_results_dir = path / search_dir_name
    search_results_dir.mkdir(exist_ok=True, parents=True)

    images_dir = path / images_dir_name
    images_dir.mkdir(exist_ok=True, parents=True)

    return path


def create_log_path(log_directory: Path) -> Path:
    """
    Creates a logging directory to store pipeline logs.
    The function will create all non-existent parent directories
    if the do not exist yet. If the logging directory exists it
    will pass creating a copy or overwritting existing logs.
    
    Args:
        log_directory (Path): Path to the logging directory

    Returns:
        (Path): Path to the created logging directory.

    Raises:
        FileExistsError: If logging directory exists as a file.
        OSError: Error from OS when trying to create directory
    """
    log_directory.mkdir(exist_ok=True, parents=True) 

    return log_directory


def setup_logging(log_dir: Path, level: str="DEBUG"):
    """
    Sets up logging for the pipeline application.
    This function configures logging for the application including
    creating a log directory (if it does not exist) and generates a
    time stamped log file name. Log levels can also be passed here.
    If creating the log store fails, then the cwd will be used.

    Args:
        log_directory (Path): The directory where the log files should be
            stored. The default is 'logs/'
        level (str): The log level string representation for the logging
             Currently, only one log level is supported for all handlers.

    Returns:
        None

    Raises:
        KeyError: If the expected keys are not present in the logging config 
            file.
    """
    log_directory = Path(os.getcwd())
    try:
        log_directory = create_log_path(log_dir)
    except Exception as e:
        print(f"Error creating logging config. defaulting to cwd. {e}")

    timestamp = dt.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_directory / f"pylanet_{timestamp}.log"

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s [%(name)-8.8s] [%(levelname)-7.7s]   %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S"
            },
            "detailed": {
                "format": "%(asctime)s [%(name)-8.8s] [%(levelname)-7.7s]  %(module)s.%(funcName)s | %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S"
            }
        },
        "handlers": {
            "consoleHandler": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
                "formatter": "standard"
            },
            "fileHandler": {
                "class": "logging.FileHandler",
                "level": "DEBUG",
                "formatter": "detailed",
                "filename": "pylanet.log"
            }
        },
        "loggers": {
            "pipeline": {
                "level": "DEBUG",
                "handlers": ["fileHandler"],
                "propagate": False
            }
        }
    }

    config["handlers"]["fileHandler"]["filename"] = log_file
    config["loggers"]["pipeline"]["level"] = level
    config["handlers"]["fileHandler"]["level"] = level
    config["handlers"]["consoleHandler"]["level"] = level
    logging.config.dictConfig(config)


def read_geojson(file_path: Path) -> Dict[str, Any]:
    """
    Reads a GeoJSON file and returns the first feature.
    Checks if 'features' and 'geometry' keys are present in the file before 
    attempting returning dictionary.  

    Args:
        file_path (Path): Path to the GeoJSON file to read.

    Returns:
        Dict[str, Any]: dictionary representation of the geometry of the first
            feature in the GeoJSON file. 

    Raises:
        FileNotFoundError: If the specified file cannot be found.
        json.JSONDecodeError: If the file contains invlaud JSON.
        KeyError: if the expected 'features' or 'geometry' is missing.
    """
    try:
        with open(file_path, 'r', encoding="utf-8") as geojson_file:
            geojson_data = json.load(geojson_file)
            logger.info(f"Successfully read {file_path} geojson")
        
        # Check if "features" and "geometry" keys are present
        if "features" in geojson_data and geojson_data["features"]:
            geometry = geojson_data["features"][0].get("geometry")
            if geometry is not None:
                return geojson_data["features"][0]
            else:
                logger.error(f"Geometry data not found in the first feature of {file_path}")
                raise KeyError
        else:
            logger.error(f"Features data not found in {file_path}")
            raise KeyError("No features found in gson")

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON in file: {file_path}")
        raise


def overlap_percent(aoi: List[Dict[str, Any]],
                    footprints: List[Dict[str, Any]],
                    epsg: str|int
                    ) -> float:
    """
    Determines the overlap percentage of the study area.
    Uses geopandas to merge, clip and intersect the footprint with the study 
    area feature to get the percentage overlap.

    Args:
        feature (Path): study area geojson
        footprints (geojson features): geosjon features of the footprints.
        espg (str|int): ESPG identifier number. `3005` for ESPG:3005.

    Returns:
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


def group_images_by_date(results: List[Dict[str, Any]], 
                         aoi: Path,
                         crs: str|int) -> pd.DataFrame:
    """
    Puts the results from a search into a geodataframe.
    This functions makes an intial dataframe of id, date, coverage and geometry
    for all the images, then groups by images taken on the same day. and 
    recalculates the coverage percent of all the images over the aoi.

    Args:
        results (Dict[str, Any]): The search results
        crs (str|int): The coordinate reference system for the dataframe.

    Returns:
        gpd.GeoDataFrame: Dataframe for the search results

    Raises:
        None
    """
    # Initialize the dataframe
    images_df = gpd.GeoDataFrame(columns=["id", "date", "coverage", "geometry"],
                                 crs="EPSG:4326", 
                                 geometry="geometry"
                                 )
    # Add image data
    for image in results:
        row = {"id": image["id"],
               "date": dt.strptime(image["properties"]["acquired"], 
                                   "%Y-%m-%dT%H:%M:%S.%fZ"
                                   ).strftime("%Y-%m-%d"),
               "geometry": image["geometry"],
        }
        images_df = images_df._append(row,ignore_index=True)

    # Group by date
    images_df = images_df.groupby("date")["id"].apply(list).reset_index()
    images_df = images_df.rename(columns={"id": "ids"})

    # Calculate coverage by date
    images_df["coverage"] = images_df["ids"].apply(
            lambda ids: overlap_percent([read_geojson(aoi)], 
                                        [image for image in results if image["id"] in ids],
                                        epsg=crs)
    ) 

    # Show number of images
    images_df["num_images"] = images_df["ids"].apply(len)

    return images_df


def write_results(results: List[Dict[str, Any]], search_name: str) -> List[Dict[str, Any]]:
    """
    Write the results of an API call to a file.
    Writes the JSON response from the API call to a file specified by the user.
    This function uses the json library.

    Args:
        results (List[Dict[str, Any]]): results to write to the file.
        file_path (Path): path to where the output file should be written.

    Returns:
        List[Dict[str, Any]: The results passed to the function.

    Raises:
        FileNotFoundError: If the path to the file does not exist.
    """
    search_time = dt.now().strftime("%y%m%d%H%M%S")
    search_out_dir = f"{pipeline.config["data_path"]}/{pipeline.config["search_dir_name"]}/{search_name}_{search_time}.json"
    try:
        with open(search_out_dir, "w") as f:
            json.dump(results, f, indent=4)
        logger.info(f"Wrote search results to {search_out_dir}")
    except TypeError as e:
        logger.error(f"Failed to write results to {search_out_dir}: {e}")
        raise
    except IOError as e:
        logger.error(f"Failed to write results to {search_out_dir}: {e}")
        raise

    return results
