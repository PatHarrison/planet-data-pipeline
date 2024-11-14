import pytest
from datetime import datetime as dt

from pipeline.extract.filters import FilterBuilder


class TestFilterBuilder():
    """Tests the filter builder"""
    def test_empty_filters(self):
        """Tests if filters are empty"""
        expected_and_filter_seg = {"type": "AndFilter", "config": []}
        expected_or_filter_seg = {"type": "AndFilter", "config": []}

        and_filters = FilterBuilder("And").build()
        or_filters = FilterBuilder("And").build()

        assert and_filters == expected_and_filter_seg
        assert or_filters == expected_or_filter_seg

    def test_all_filter_types(self):
        expected_filter = {
         'type': 'AndFilter', 
         'config': [
             {'type': 'DateRangeFilter', 
              'field_name': 'acquired', 
              'config': {'gte': '2024-08-25T00:00:00Z', 'lte': '2024-08-30T00:00:00Z'}}, 
             {'type': 'RangeFilter', 
              'field_name': 'cloud_cover', 
              'config': {'gte': 0, 'lte': 0.15}}, 
             {'type': 'StringInFilter', 
              'field_name': 'quality_category', 
              'config': ['standard']}, 
             {'type': 'GeometryFilter', 
              'field_name': 'geometry', 
              'config': {
                  'type': 'Polygon',
                  'coordinates': [[[0,0],[0,1],[1,1],[1,0],[0,0]]]
                }
              }
            ]
        }

        filter_builder = FilterBuilder("And")
        filter_dict = (filter_builder
                       .add_acquired_date_filter(
                           (dt.strptime("2024-08-25", "%Y-%m-%d"), dt.strptime("2024-08-30", "%Y-%m-%d"))
                        )
                       .add_cloud_cover_filter((0,0.15))
                       .add_std_quality_filter()
                       .add_geometry_filter({"type": "Polygon", "coordinates": [[[0,0],[0,1],[1,1],[1,0],[0,0]]]})
                       ).build()

        assert filter_dict == expected_filter

        

