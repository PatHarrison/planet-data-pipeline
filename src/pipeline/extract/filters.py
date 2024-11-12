"""
Title: Filters
Project: planet-data-pipeline
Authors:
    - Patrick Harrison
    - Lee-Anna Gibson
Purpose:
    This module contains the filter builder class
    to create planet api filter requests segments.

    Classes
        - FilterBuilder
"""
from __future__ import annotations
import logging
from datetime import datetime as dt
from typing import Tuple, Dict, List

from planet import data_filter


logger = logging.getLogger("pipeline")


class FilterBuilder:
    """Builder pattern class for building filters"""
    def __init__(self, filter_type: str="And"):
        """Filter Builder Constructor.
        Setups up a list of filters to add to the filter type.

        parameters:
            filter_type (str): Type of filter builder [And|Or].
        returns:
            None
        raises:
            None
        """
        self.filter_type = filter_type
        self.filters = []
    
    def build(self) -> Dict:
        """Build the final filter dictionary in Planet API format."""
        return {
            "type": f"{self.filter_type}Filter",
            "config": self.filters
        }

    def add_acquired_date_filter(self,
            date_range: Tuple[dt, dt], inclusive: bool=True
    ) -> FilterBuilder:
        """Builds a datetime filter from user given bounds.
        date time objects must be in RFC 3339 format according to the Planet
        API documentation. This should be handled by the planet SDK by just
        passing the datetime objects.

        parameters:
            date_range (Tuple[datetime, datetime]): time range for aquisition
                of the imagery (start, end).
            inclusive (bool): Specifies if the bounds are inclusive or exclusive.
        returns:
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
        """Build a geometry filter from user specified feature.
        The filter support Point, MultiPoint, LineString, MultiLineString,
        Polygon, and MultiPolygon in GeoJSON format. The API will attempt
        to correct geometry if GeoJSON is invalid.

        parameters:
            geojson_feature (Dict): geojson format of the feature to use.
        returns:
            FilterBuilder: Instance of the class
        """
        self.filters.append(data_filter.geometry_filter(aoi))
        return self

    def add_cloud_cover_filter(self,
            cloud_cover_range: Tuple[float, float], inclusive: bool=True
    ) -> FilterBuilder:
        """Builds a cloud cover filter from user given bounds.
        Uses the data_filter.range_filter function to build the filter.
        
        parameters:
            cloud_cover_range (Tuple[float, float]): cloud cover bounds (minimum, maximum)
                between 0 and 1.
            inclusive (bool): Specifies if the bounds are inclusive or exclusive.
        returns:
            FilterBuilder: Instance of the class
        """
        if inclusive:
            self.filters.append(
                data_filter.range_filter(
                    "cloud_cover",
                    gte=cloud_cover_range[0], 
                    lte=cloud_cover_range[1]
                )
            )
        else:
            self.filters.append(
                data_filter.range_filter(
                    "cloud_cover",
                    gte=cloud_cover_range[0], 
                    lte=cloud_cover_range[1]
                )
            )

        return self

    def add_view_angle_filter(self, 
            view_angle_range: Tuple[float, float], inclusive: bool=True
    ) -> FilterBuilder:
        """Builds a view angle filter from user given bounds.
        Uses the data_filter.range_filter function to build the filter.
        
        parameters:
            view_angle_range (Tuple[float, float]): view angle bounds (minimum, maximum)
                between -60 and 60.
            inclusive (bool): Specifies if the bounds are inclusive or exclusive.
        returns:
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
                    gte=view_angle_range[0], 
                    lte=view_angle_range[1]
                )
            )

        return self

    def add_permission_filter(self) -> FilterBuilder:
        """Builds a permission filter so downloadable images are returned.

        parameters:
            None
        returns:
            FilterBuilder: Instance of the class
        """
        self.filters.append(data_filter.permission_filter())
        return self

    def add_std_quality_filter(self) -> FilterBuilder:
        """Builds a quality filter to only get standard quality images.

        parameters:
            qualities (List[str]): qualities of products to retreive
        returns:
            FilterBuilder: Instance of the class
        """
        self.filters.append(data_filter.std_quality_filter())
        return self

    def add_instrument_filter(self, instruments: List[str]) -> FilterBuilder:
        """Builds an instrumnet filter.

        parameters:
            intruments (List[str]): Instruments to retrieve data from.
        returns:
            FilterBuilder: Instance of the class
        """
        self.filters.append(
            data_filter.string_in_filter(
                "instrument",
                instruments
            )
        )
        return self

