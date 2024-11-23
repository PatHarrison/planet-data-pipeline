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
import os
import sys
from datetime import datetime as dt
from typing import Any, Dict, Tuple, Literal
from pathlib import Path

from planet import Auth

import pipeline
from pipeline.utils import read_geojson, write_results, group_images_by_date
from pipeline.extract.filters import FilterBuilder
from pipeline.extract.search import SearchHandler
from pipeline.extract.order import concurrent_planet_order, run_concurrent_image_fetch


# Type Definitions
LogLevelType = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def parse_arguments() -> Tuple[str, Path, LogLevelType, Tuple[dt, dt], str]:
    """
    Parse Command-line arguments for the search stats script.
    This parser sets up a script description as well as a checks the 
    validity of arguments. The script will exit with an error after 
    printing a slighlty helpful message.

    Args:
        None

    Returns:
        Tuple[str, Path, LogLevelType, Tuple[dt, dt], str]: arguments for the
            script as apikey, aoi, loglevel, start date, end date.

    Raises:
        None: Will exit with argparse.error
    """
    parser = argparse.ArgumentParser(
                description=("Finder for planet images in a certain date range"
                             " and other filters. Search results are written"
                             " to a json file. Date filters are the first two"
                             " positional arguments for the script in YYYY-MM"
                             "-DD format. The script will find images with 0"
                             "-15 percent cloud cover, Standard quality and"
                             " intersecting the aoi.")

             )
    parser.add_argument(
        "startdate",
        type=str,
        help="Start date in the form YYYY-MM-DD"
    )
    parser.add_argument(
        "enddate",
        type=str,
        help="End date in the form YYYY-MM-DD"
    )
    parser.add_argument(
        "--apikey", type=str, default=os.getenv("PL_API_KEY"),
        help="Your API key for accessing the Planet API."
    )
    parser.add_argument(
        "--aoi", type=str, required=True,
        help="GeoJSON file path for your area of interest in WGS84"
    )
    parser.add_argument(
        "--crs", type=str, required=True,
        help="CRS to determine overlap percentage in."
    )
    parser.add_argument(
        "--loglevel", default="DEBUG",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level for the script."
    )

    args = parser.parse_args()

    try:
        study_area: Path = Path(args.aoi)
        if not study_area.exists():
            print(f"Cannot find {study_area}")
            sys.exit(1)
        elif not study_area.is_file():
            print(f"The path {study_area} is a directory, not a file")
            sys.exit(1)
    except (FileNotFoundError, IsADirectoryError) as e:
        parser.error(f"Error setting study area: {e}")
    except ValueError as e:
        parser.error(f"Invalid Path to AOI {e}")

    if not args.apikey:
        parser.error("An API Key is required for Planet. Please provide as a"
                     "system environment variable `PLANET_API_KEY` or as a "
                     "parameter to the script.")
    try:
        date_range = (dt.strptime(args.startdate, "%Y-%m-%d"),
                      dt.strptime(args.enddate, "%Y-%m-%d")
                      )
    except ValueError:
        parser.error(f"Invlaid dates passed `{args.startdate}`,"
                     f"`{args.enddate}` use YYYY-MM-DD Format"
                     )

    return args.apikey, study_area, args.loglevel, date_range, args.crs

def main():
    """
    Entry point for pylanet script.
    Runs the search and the orders from the planet API.
    """

    api_key, aoi, log_level, date_range, crs = parse_arguments()

    pipeline.config["log_level"] = log_level
    pipeline.config["api_key"] = api_key
    pipeline.config["data_path"] = Path(os.getcwd()) / "data"
    pipeline.initialize()

    aoi_feature = read_geojson(aoi)
    aoi = [{"geometry": aoi_feature, "properties": None}]

    auth = Auth.from_key(api_key)


    item_types = ["PSScene"]
    filter_builder = FilterBuilder("And")
    filter_dict = (filter_builder
                   .add_acquired_date_filter((date_range[0], date_range[1]))
                   .add_cloud_cover_filter((0,0.15))
                   .add_std_quality_filter()
                   .add_geometry_filter(aoi_feature)
                   .add_permission_filter()
                   ).build()

    searches = SearchHandler(auth=auth)
    search_name = (f"SiteCFilling_"
                   f"{date_range[0].strftime("%Y%m%d_%H%M%S")}_"
                   f"{date_range[1].strftime("%Y%m%d_%H%M%S")}"
                )
    request = asyncio.run(searches.make_search(name=search_name, 
                                               search_filter=filter_dict,
                                               item_types=item_types)
                          )
    images = asyncio.run(searches.perform_search(request["id"]))
    
    write_results(images, search_name)
    
    results = group_images_by_date(images, aoi, crs)

    full_cov = results[results["coverage"] >= 100.00]
    print(f"Number of days with full AOI coverage: {len(full_cov)}")
    print(f"Details:\n {full_cov.head(99)}")


    order_flag = input("Continue to daily composite order? [y/n] ")

    if order_flag.upper() == "Y":
        tasks = [
                concurrent_planet_order(row, crs, aoi_feature, auth)
                for row in full_cov[["ids", "date"]].itertuples(index=False)
        ]

        asyncio.run(run_concurrent_image_fetch(tasks))

if __name__ == "__main__":
    main()

