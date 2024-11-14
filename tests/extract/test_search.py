import pytest
from datetime import datetime as dt

from planet import Session

from pipeline.extract.filters import FilterBuilder


class TestFilterBuilder():
    """Tests the filter builder"""
    def test_empty_filters(self):
        """Tests if filters are empty"""
        expected_and_filter_seg = {"type": "AndFilter", "config": []}
        expected_or_filter_seg = {"type": "OrFilter", "config": []}

        and_filters = FilterBuilder("And").build()
        or_filters = FilterBuilder("Or").build()

        assert and_filters == expected_and_filter_seg
        assert or_filters == expected_or_filter_seg

    def test_all_filter_types(self, aoi_feature):
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
              'config': aoi_feature
              },
             {'type': 'RangeFilter',
              'field_name': 'view_angle',
              'config': {'gte': -90.0, 'lte': 90.0},
              },
             {'type': 'StringInFilter',
              'field_name': 'instrument',
              'config': ['PSB.SD', 'PS2.SD', 'PS2'],
              },
            ]
        }

        filter_builder = FilterBuilder("And")
        filter_dict = (filter_builder
                       .add_acquired_date_filter(
                           (dt.strptime("2024-08-25", "%Y-%m-%d"), dt.strptime("2024-08-30", "%Y-%m-%d"))
                        )
                       .add_cloud_cover_filter((0,0.15))
                       .add_std_quality_filter()
                       .add_geometry_filter(aoi_feature)
                       .add_view_angle_filter((-90.0,90.0))
                       .add_instrument_filter(["PSB.SD", "PS2.SD", "PS2"])
                       ).build()

        assert filter_dict == expected_filter

    def test_all_filter_exclusive(self, aoi_feature):
        expected_filter = {
         'type': 'AndFilter', 
         'config': [
             {'type': 'DateRangeFilter', 
              'field_name': 'acquired', 
              'config': {'gt': '2024-08-25T00:00:00Z', 'lt': '2024-08-30T00:00:00Z'}}, 
             {'type': 'RangeFilter', 
              'field_name': 'cloud_cover', 
              'config': {'gt': 0, 'lt': 0.15}}, 
             {'type': 'StringInFilter', 
              'field_name': 'quality_category', 
              'config': ['standard']}, 
             {'type': 'GeometryFilter', 
              'field_name': 'geometry', 
              'config': aoi_feature
              },
             {'type': 'RangeFilter',
              'field_name': 'view_angle',
              'config': {'gt': -90.0, 'lt': 90.0},
              },
             {'type': 'StringInFilter',
              'field_name': 'instrument',
              'config': ['PSB.SD', 'PS2.SD', 'PS2'],
              },
            ]
        }

        filter_builder = FilterBuilder("And")
        filter_dict = (filter_builder
                       .add_acquired_date_filter(
                           (dt.strptime("2024-08-25", "%Y-%m-%d"), dt.strptime("2024-08-30", "%Y-%m-%d")),
                           inclusive=False
                        )
                       .add_cloud_cover_filter((0,0.15), inclusive=False)
                       .add_std_quality_filter()
                       .add_geometry_filter(aoi_feature)
                       .add_view_angle_filter((-90.0,90.0), inclusive=False)
                       .add_instrument_filter(["PSB.SD", "PS2.SD", "PS2"])
                       ).build()

        assert filter_dict == expected_filter


# class TestSearchHandler():
#     """Tests the search handler"""
#     def test_search(self, mocker):
#         mock_client = mocker.MagicMock(spec=Session.client("data"))
#
#         mock_response = [
#                 {
#                   "__daily_email_enabled": False,
#                   "_links": {
#                     "_self": "string",
#                     "thumbnail": "string"
#                   },
#                   "created": "2019-08-24T14:15:22Z",
#                   "filter": {
#                     "type": "string"
#                   },
#                   "id": "string",
#                   "last_executed": "2019-08-24T14:15:22Z",
#                   "name": "string",
#                   "updated": "2019-08-24T14:15:22Z"
#                 }
#         ]
#         mock_client.quick_search.return_value.items_iter.return_value = mock_response
#

