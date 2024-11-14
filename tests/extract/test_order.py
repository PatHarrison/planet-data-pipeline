from pipeline.extract.order import OrderBuilder
from pipeline.utils import indent


class TestOrderBuilder():
    """Tests the filter builder"""
    def test_empty_order(self):
        """Tests if filters are empty"""
        expected_order_seg = {"name": "Test_Name", "products": []}

        order_seg = OrderBuilder("Test_Name").build()

        assert order_seg == expected_order_seg

    def test_all_order_seg(self, aoi_feature):
        expected_order_seg = {
              "name": "Test_Name",
              "products": [
                {
                  "item_ids": ["TEST_IMAGE1", "TEST_IMAGE2"],
                  "item_type": "PSScene",
                  "product_bundle": "analytic_8b_sr_udm2"
                }
              ],
              "delivery": {
                "archive_type": "zip",
                "archive_filename": "{{order_id}}.zip",
                "single_archive": True # Python True instead of json true
              },
              "tools": [
                {
                  "reproject": {
                    "projection": "EPSG:3005",
                    "kernel": "cubic"
                  }
                },
                {
                  "clip": {
                    "aoi": aoi_feature
                  }
                },
                {
                  "composite": {}
                }
              ]
            } 


        order_seg = (OrderBuilder("Test_Name")
                   .add_product(["TEST_IMAGE1", "TEST_IMAGE2"], 
                                "analytic_8b_sr_udm2",
                                "PSScene")
                   .add_reproject_tool(3005)
                   .add_clip_tool(aoi_feature)
                   .add_composite_tool()
                   .add_delivery_config(archive_type="zip",
                                        single_archive=True,
                                        archive_filename="{{order_id}}.zip"
                                        )
                     ).build()
        indent(order_seg)

        assert order_seg == expected_order_seg
