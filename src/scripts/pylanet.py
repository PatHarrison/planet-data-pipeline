"""
Title: Planet Data Pipeline
Project: planet-data-pipeline
Authors:
    - Patrick Harrison
    - Lee-Anna Gibson
Purpose:
    This module provides the entry point for data searching Planet
    data.
"""
import asyncio
import argparse
import configparser
import os
from argparse import Namespace
from datetime import datetime as dt
from typing import Any, Dict, Tuple, Literal
from pathlib import Path

from planet import Auth

import pipeline
from pipeline.utils import read_geojson, write_results, group_images_by_date
from pipeline.extract.filters import FilterBuilder
from pipeline.extract.search import SearchHandler
from pipeline.extract.order import concurrent_planet_order, run_concurrent_image_fetch


def parse_configuration(config_file: Path) -> Dict[str, Any]:
    """
    Parses the configuration file for the pipeline.
    Reads a config.ini file provided in the root directory of the repository.
    Splits the configuration into the different sections and returns in a 
    dicionary of the different config dictionares for the sections.
    
    Args:
        config_file (Path): path to the configuration file.
    
    Returns:
        Dict[str, Any]: Dictionary of configuration settings.

    Raises:
        RuntimeError: Raised when the configuration file is invalid
    """
    config = configparser.ConfigParser()

    try:
        config.read(config_file)
    except configparser.Error as e:
        raise RuntimeError(f"Error reading the configuration file: {e}")

    def get_option(section, option, default=None, type_cast=None):
        if config.has_section(section) and config.has_option(section, option):
            value = config.get(section, option)
            if type_cast:
                try:
                    return type_cast(value)
                except ValueError as e:
                    raise ValueError(f"Invalid value for '{section}.{option}': {value}. {e}")
            return value
        elif default is not None:
            return default
        else:
            raise KeyError(f"Missing required configuration: '{section}.{option}'")  


    pipeline_config = {
            "log_level": get_option("pipeline", "loglevel", default="DEBUG"),
            "log_path": Path(get_option("pipeline", "logpath", default="logs")), 
            "data_path": Path(get_option("pipeline", "outdatadir", default="data")),
            "image_dir_name": get_option("pipeline", "outimagedirname", default="images"),
            "search_dir_name": get_option("pipeline", "outsearchresults", default="search_results")
    }

    delivery_config = {
            "crs": get_option("pylanet.delivery", "outcrs", type_cast=int),
            "order_name": get_option("pylanet.delivery", "ordername")
    }

    filters_config = {
        "item_types": eval(get_option("pylanet.filters", "itemtypes", default="[]")),  # use eval to safely get list
        "start_date": get_option("pylanet.filters", "startdate"),
        "end_date": get_option("pylanet.filters", "enddate"),
        "aoi": get_option("pylanet.filters", "aoi", default=None),
        "min_cloud_cover": get_option("pylanet.filters", "mincloudcover", default=0.0, type_cast=float),
        "max_cloud_cover": get_option("pylanet.filters", "maxcloudcover", default=1.0, type_cast=float),
        "min_clear_percent": get_option("pylanet.filters", "minclearpercent", default=0.0, type_cast=float),
        "max_clear_percent": get_option("pylanet.filters", "maxclearpercent", default=1.0, type_cast=float),
        "permission_filter": get_option("pylanet.filters", "permissionfilter", default=True, type_cast=bool),
        "std_quality_filter": get_option("pylanet.filters", "stdqualityfilter", default=True, type_cast=bool),
        "instruments": eval(get_option("pylanet.filters", "instruments", default=None)),  # use eval to safely get list
        "assests": eval(get_option("pylanet.filters", "assests", default=None)),  # use eval to safely get list
        "required_coverage": get_option("pylanet.filters", "requiredcoverage", type_cast=float)
    }

    return {"pipeline": pipeline_config,
            "delivery": delivery_config, 
            "filters": filters_config}

def parse_arguments() -> str:
    """
    Parse Command-line arguments for the search stats script.
    This parser sets up a script description as well as a checks the 
    validity of arguments. The script will exit with an error after 
    printing a slighlty helpful message.

    Args:
        None

    Returns:
        str: apikey for planet.com api

    Raises:
        None: Will exit with argparse.error
    """
    class Args(Namespace):
        """Ensures Namespace of argparser is expected"""
        apikey: str

    parser = argparse.ArgumentParser(
                description=("Finder for planet images")

             )
    parser.add_argument(
        str("apikey"), type=str, default=os.getenv("PL_API_KEY"),
        help="Your API key for accessing the Planet API."
    )
    args = parser.parse_args(namespace=Args)
    apikey: str = args.apikey

    if not apikey:
        parser.error("An API key is required for the planet pipeline.")

    return args.apikey


def main():
    """
    Entry point for pylanet script.
    Runs the search and the orders from the planet API.
    """
    config = parse_configuration(Path("config.ini"))
    api_key = parse_arguments()
    auth = Auth.from_key(api_key)
    pipeline.initialize(config["pipeline"])

    filters_config = config["filters"]
    item_types = filters_config["item_types"]
    filter_dict = FilterBuilder("And", filters_config).build()

    search_handler = SearchHandler(auth=auth)
    search_name = (f"SiteCFilling_"
                   f"{filters_config["start_date"].replace("-","")}_"
                   f"{filters_config["end_date"].replace("-","")}"
                )
    search = asyncio.run(search_handler.make_search(name=search_name, 
                                               search_filter=filter_dict,
                                               item_types=item_types)
                          )
    images = asyncio.run(search_handler.perform_search(search["id"]))
    write_results(images, search_name)
    
    results = group_images_by_date(images, 
                                   filters_config["aoi"], 
                                   config["delivery"]["crs"])

    full_cov = results[results["coverage"] >= filters_config["required_coverage"]]
    print(f"Number of days with full AOI coverage: {len(full_cov)}")
    print(f"Details:\n {full_cov.head(99)}")


    order_flag = input("Continue to daily composite order? [y/n] ")

    if order_flag.upper() == "Y":
        tasks = [
                concurrent_planet_order(row, filters_config["crs"], aoi_feature, auth)
                for row in full_cov[["ids", "date"]].itertuples(index=False)
        ]

        asyncio.run(run_concurrent_image_fetch(tasks))

if __name__ == "__main__":
    main()

