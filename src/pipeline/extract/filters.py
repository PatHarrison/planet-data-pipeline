"""
Title: Filters
Authors:
    - Patrick Harrison
    - Lee-Anna Gibson
Purpose:
    This module contains the filter builder class to create planet api filter 
    requests segments. Not all available filters are implemented in the filter 
    builder however enough are for our purposes.

    Classes
        - FilterBuilder
"""
from __future__ import annotations
import logging
from datetime import datetime as dt
from typing import Any, Tuple, Dict, List
from pathlib import Path

from planet import data_filter

from pipeline.utils import read_geojson


logger = logging.getLogger("pipeline")

DATEFORMAT = "%Y-%m-%d"

class FilterBuilder:
    """
    Builder pattern class for building filters for the planet API.
    """
    def __init__(self, filter_type: str="And", config: Dict[str, Any] | None=None):
        """
        Filter Builder Constructor.
        Setups up a list of filters to add to the filter type.

        Args:
            filter_type (str): Type of filter builder [And|Or].

        Returns:
            None
        """
        self.filter_type = filter_type
        self.filters = []
        if config is not None:
            self._from_config(config)

    def _from_config(self, config: Dict[str, Any]) -> None:
        """
        Adds filters from a config file set up for the pipeline.
        This function is very picky about the structure of the config dict.
        
        Args:
            config (Dict[str, Any]): configuration file from the pipeline.

        Returns:
            None

        Raises:
            None
        """

        if "aoi" in config:
            aoi = read_geojson(Path(config["aoi"]))
            aoi_feature = aoi["geometry"]

            self.add_geometry_filter(aoi_feature)

        if "start_date" in config and "end_date" in config:
            date_range = (dt.strptime(config["start_date"], DATEFORMAT),
                          dt.strptime(config["end_date"], DATEFORMAT))

            self.add_acquired_date_filter((date_range[0], date_range[1]))

        if "min_cloud_cover" in config and "max_cloud_cover" in config:
            self.add_cloud_cover_filter(
                    (config["min_cloud_cover"], 
                     config["max_cloud_cover"])
            )

        if "min_clear_percent" in config and "max_clear_percent" in config:
            self.add_clear_percent_filter(
                    (config["min_clear_percent"], 
                     config["max_clear_percent"])
            )

        if config["std_quality_filter"]:
            self.add_std_quality_filter()

        if config["permission_filter"]:
            self.add_permission_filter()

        if config["instruments"]:
            self.add_instrument_filter(config["instruments"])

        if config["assests"]:
            self.add_asset_filter(config["assests"])

    
    def build(self) -> Dict:
        """Build the final filter dictionary in Planet API format."""
        return {
            "type": f"{self.filter_type}Filter",
            "config": self.filters
        }

    def add_acquired_date_filter(self,
            date_range: Tuple[dt, dt], inclusive: bool=True
    ) -> FilterBuilder:
        """
        Builds a datetime filter from user given bounds.
        date time objects must be in RFC 3339 format according to the Planet
        API documentation. This should be handled by the planet SDK by just
        passing the datetime objects.

        Args:
            date_range (Tuple[datetime, datetime]): time range for aquisition
                of the imagery (start, end).
            inclusive (bool): Specifies if the bounds are inclusive or exclusive.

        Returns:
            FilterBuilder: Instance of the class
        """
        if inclusive:
            self.filters.append(
                data_filter.date_range_filter(
                    "acquired",
                    gte=date_range[0],
                    lte=date_range[1]
                )
            )
        else:
            self.filters.append(
                data_filter.date_range_filter(
                    "acquired",
                    gt=date_range[0],
                    lt=date_range[1]
                )
            )
        return self

    def add_geometry_filter(self, aoi: Dict) -> FilterBuilder:
        """
        Build a geometry filter from user specified feature.
        The filter support Point, MultiPoint, LineString, MultiLineString,
        Polygon, and MultiPolygon in GeoJSON format. The API will attempt
        to correct geometry if GeoJSON is invalid.

        Args:
            geojson_feature (Dict): geojson format of the feature to use.

        Returns:
            FilterBuilder: Instance of the class
        """
        self.filters.append(data_filter.geometry_filter(aoi))
        return self

    def add_cloud_cover_filter(self,
            clear_percent_range: Tuple[float, float], inclusive: bool=True
    ) -> FilterBuilder:
        """
        Builds a cloud cover filter from user given bounds.
        Uses the data_filter.range_filter function to build the filter.
        
        Args:
            cloud_cover_range (Tuple[float, float]): cloud cover bounds (minimum, maximum)
                between 0 and 1.
            inclusive (bool): Specifies if the bounds are inclusive or exclusive.

        Returns:
            FilterBuilder: Instance of the class
        """
        if inclusive:
            self.filters.append(
                data_filter.range_filter(
                    "cloud_cover",
                    gte=clear_percent_range[0], 
                    lte=clear_percent_range[1]
                )
            )
        else:
            self.filters.append(
                data_filter.range_filter(
                    "clear_percent",
                    gt=clear_percent_range[0], 
                    lt=clear_percent_range[1]
                )
            )

        return self

    def add_clear_percent_filter(self,
            cloud_cover_range: Tuple[float, float], inclusive: bool=True
    ) -> FilterBuilder:
        """
        Builds a cloud cover filter from user given bounds.
        Uses the data_filter.range_filter function to build the filter.
        
        Args:
            cloud_cover_range (Tuple[float, float]): cloud cover bounds (minimum, maximum)
                between 0 and 1.
            inclusive (bool): Specifies if the bounds are inclusive or exclusive.

        Returns:
            FilterBuilder: Instance of the class
        """
        if inclusive:
            self.filters.append(
                data_filter.range_filter(
                    "clear_percent",
                    gte=cloud_cover_range[0], 
                    lte=cloud_cover_range[1]
                )
            )
        else:
            self.filters.append(
                data_filter.range_filter(
                    "cloud_cover",
                    gt=cloud_cover_range[0], 
                    lt=cloud_cover_range[1]
                )
            )

        return self

    def add_view_angle_filter(self, 
            view_angle_range: Tuple[float, float], inclusive: bool=True
    ) -> FilterBuilder:
        """
        Builds a view angle filter from user given bounds.
        Uses the data_filter.range_filter function to build the filter.
        
        Args:
            view_angle_range (Tuple[float, float]): view angle bounds (minimum, maximum)
                between -60 and 60.
            inclusive (bool): Specifies if the bounds are inclusive or exclusive.

        Returns:
            FilterBuilder: Instance of the class
        """
        if inclusive:
            self.filters.append(
                data_filter.range_filter(
                    "view_angle",
                    gte=view_angle_range[0], 
                    lte=view_angle_range[1]
                )
            )
        else:
            self.filters.append(
                data_filter.range_filter(
                    "view_angle",
                    gt=view_angle_range[0], 
                    lt=view_angle_range[1]
                )
            )

        return self

    def add_std_quality_filter(self) -> FilterBuilder:
        """
        Builds a quality filter to only get standard quality images.

        Args:
            qualities (List[str]): qualities of products to retreive

        Returns:
            FilterBuilder: Instance of the class
        """
        self.filters.append(data_filter.std_quality_filter())
        return self

    def add_instrument_filter(self, instruments: List[str]) -> FilterBuilder:
        """
        Builds an instrumnet filter.

        Args:
            intruments (List[str]): Instruments to retrieve data from.

        Returns:
            FilterBuilder: Instance of the class
        """
        self.filters.append(
            data_filter.string_in_filter(
                "instrument",
                instruments
            )
        )
        return self

    def add_permission_filter(self) -> FilterBuilder:
        """
        Builds a permission filter

        Args:
            None

        Returns:
            FilterBuilder: Instance of the class
        """
        self.filters.append(
                data_filter.permission_filter()
        )
        return self

    def add_asset_filter(self, assests: List[str]) -> FilterBuilder:
        """
        Builds a asset availabiliy filter.

        Args:
            assests (List[str]): Assest types to include in results

        Returns:
            FilterBuilder: Instance of the class
        """
        self.filters.append(
                data_filter.asset_filter(assests)
        )
        return self
