"""
Title: Data
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
import logging
import os
import sys
from datetime import datetime as dt
from pathlib import Path
from typing import Tuple, Literal

import pipeline
from pipeline.utils import read_geojson, write_results, group_images_by_date
from pipeline.extract.filters import FilterBuilder
from pipeline.extract.search import SearchHandler
from pipeline.extract.order import concurrent_planet_order, run_concurrent_image_fetch


logger = logging.getLogger("pipeline")

LogLevelType = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def parse_arguments() -> Tuple[str, Path, LogLevelType, Tuple[dt, dt], str]:
    """Parse Command-line arguments for the search stats script."""
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
            raise FileNotFoundError(f"Cannot find {study_area}")
        elif not study_area.is_file():
            raise IsADirectoryError(f"The path {study_area} is a directory, not a file")
    except (FileNotFoundError, IsADirectoryError) as e:
        logger.error(f"Error setting study area: {e}")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"Invalid Path to AOI {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occured setting up AOI with {study_area}. {e}")
        sys.exit(1)

    if not args.apikey:
        parser.error("An API Key is required for Planet. Please provide as a"
                     "system environment variable `PLANET_API_KEY` or as a "
                     "parameter to the script.")
    try:
        date_range = (dt.strptime(args.startdate, "%Y-%m-%d"),
                      dt.strptime(args.enddate, "%Y-%m-%d")
                      )
    except ValueError:
        logger.error(f"Invlaid dates passed `{args.startdate}`,"
                     f"`{args.enddate}` use YYYY-MM-DD Format"
                     )
        sys.exit(1)

    return args.apikey, study_area, args.loglevel, date_range, args.crs


def main():
    api_key, aoi, log_level, date_range, crs = parse_arguments()

    # Overwrite logging levels with user specficed
    pipeline.config["log_level"] = log_level
    pipeline.config["api_key"] = api_key
    pipeline.config["data_path"] = Path(os.getcwd()) / "data"
    pipeline.initialize()
    logger.info(f"starting {__file__} with: API key {api_key}")

    # Setup the session
    logger.info("Setting up the session with the API Key")

    aoi_feature = read_geojson(aoi)
    aoi = [{"geometry": aoi_feature, "properties": None}]

    item_types = ["PSScene"]
    filter_builder = FilterBuilder("And")
    filter_dict = (filter_builder
                   .add_acquired_date_filter((date_range[0], date_range[1]))
                   .add_cloud_cover_filter((0,0.15))
                   .add_std_quality_filter()
                   .add_geometry_filter(aoi_feature)
                   ).build()

    searches = SearchHandler()
    # del_searches = asyncio.run(
    #         searches.delete_search(asyncio.run(searches._get_searches())))
    # print(del_searches)
    request = asyncio.run(searches.make_search(name="SiteCFilling", 
                                               search_filter=filter_dict,
                                               item_types=item_types)
                          )
    images = asyncio.run(searches.perform_search(request["id"]))

    search_time = dt.now().strftime("%y%m%d%H%M%S")
    search_name = f"{request["name"]}_{request["id"]}_{search_time}.geojson"
    search_out_dir = f"{pipeline.config["data_path"]}/search_results/{search_name}"
    write_results(images, Path(os.getcwd()) / search_out_dir)
    
    results = group_images_by_date(images, aoi, crs)

    full_cov = results[results["coverage"] >= 100.00]
    print(f"Number of days with full AOI coverage: {len(full_cov)}")
    print(f"Details:\n {full_cov.head(99)}")


    order_flag = input("Continue to daily composite order? [y/n] ")

    if order_flag.upper() == "Y":
        logger.info(f"Continuing to Ordering")
        tasks = [
                concurrent_planet_order(row, crs, aoi_feature)
                for row in full_cov[["ids", "date"]].itertuples(index=False)
        ]

        asyncio.run(run_concurrent_image_fetch(tasks))
        # for row in full_cov[["ids", "date"]].itertuples(index=False):
        #     request = (OrderBuilder("SiteCFilling")
        #                .add_product(row.ids,
        #                             "analytic_8b_sr_udm2",
        #                             "PSScene")
        #                .add_reproject_tool(crs)
        #                .add_clip_tool(aoi_feature)
        #                .add_composite_tool()
        #                .add_delivery_config(archive_type="zip",
        #                                     single_archive=True,
        #                                     archive_filename="{}.zip".format(row.date.replace("-",""))
        #                                     )
        #             )
        #     request_json = request.build()
        #
        #     order = OrderHandler(request_json).create_poll_and_download()
        #     logger.info(f"Downloaded order to {order}")

    sys.exit(pipeline.deactivate())


if __name__ == "__main__":
    main()
