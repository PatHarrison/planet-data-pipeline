"""
Title: Order
Project: planet-data-pipeline
Authors:
    - Patrick Harrison
    - Lee-Anna Gibson
Purpose:
    This module provides the search handler class
    data from the Planet API using the Planet SDK.

    Classes:
        - OrderHandler
"""
from __future__ import annotations
import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, List

from planet import Auth, Session, OrdersClient
from planet.order_request import (
        build_request, product, composite_tool, reproject_tool, clip_tool, delivery
)
from planet import reporting
from planet.exceptions import APIError, ClientError

import pipeline


logger = logging.getLogger("pipeline")


class OrderBuilder():
    """Builder for order request"""
    def __init__(self, name: str):
        """"""
        self.tools = []
        self.products = []
        self.delivery = {}
        self.name = name

    def build(self) -> Dict[str, Any]:
        """Builds the order to a dictionary for requesting.
        uses the build_request method from the Planet SDK. and pulls
        from the state of the class for the products, tools, and delivery
        specifications, etc.

        parameters:
            None
        returns:
            Dict[str, Any]: Order request JSON
        raises:
            planet.specs.SpecificationException: If order_type is not a valid
                order type.
        """
        return build_request(name=self.name, products=self.products, 
                             tools=self.tools, delivery=self.delivery
                             )

    def add_product(self, 
                    image_ids: List[str], 
                    product_bundle: str,
                    item_type: str
                    ) -> OrderBuilder:
        """Adds a product to the order.
        Uses the product method from the Planet SDK.

        parameters:
            image_ids (List[str]): IDs of the catalog items to include in the order.
            product_bundle (str): Set of asset types for the catalog items.
            item_type (str): The class of spacecraft and processing characteristics
                             for the catalog items.
        returns:
            OrderBuilder: self instance
        raises:
            planet.specs.SpecificationException: If bundle or fallback bundle
                are not valid bundles or if item_type is not valid for the given
                bundle or fallback bundle.
        """
        self.products.append(
            product(item_ids=image_ids, product_bundle=product_bundle,
                    item_type=item_type, fallback_bundle=None)
        )
        return self
    
    def add_reproject_tool(self, 
                           projection: int|str,
                           resolution: float|None=None,
                           kernel="cubic"
                           ) -> OrderBuilder:
        """Adds a reprojection tool to the order request.
        Uses the reproject_tool method from the planet SDK to build the 
        tool JSON.

        parameters:
            projection: A coordinate system in the form 
                        EPSG:<projection>. (ex. 4326 for WGS84)
            resolution: The pixel width and height in the output file. 
                        The API default is the resolution of the input 
                        item. This value will be in meters unless the 
                        coordinate system is geographic.
            kernel: The resampling kernel used. The API default is 
                    "near". This parameter also supports "bilinear", 
                    "cubic", "cubicspline", "lanczos", "average" and 
                    "mode".
        returns:
            OrderBuilder: self instance
        """
        self.tools.append(
                reproject_tool(projection=f"EPSG:{projection}",
                               resolution=resolution,
                               kernel=kernel)
        )
        return self

    def add_clip_tool(self, aoi: Dict[str, Any]) -> OrderBuilder:
        """Adds a clipping tool to the order.
        Uses the clip_tool method from the planet SDK to build the
        tool JSON

        parameters:
            aoi (Dict[str, Any]: clip GeoJSON, either Polygon or Multipolygon.
        returns:
            OrderBuilder: self instance
        raises:
            planet.exceptions.ClientError: If GeoJSON is not a valid polygon or
                multipolygon.
        """
        self.tools.append(
            clip_tool(aoi)
        )
        return self

    def add_composite_tool(self) -> OrderBuilder:
        """Adds a composite tool to the order.
        This tool combines multiple images into one image, similar to
        mosaicing. The input images must have the same band config.
        Uses the planet SDK method composite_tool() however this method
        does not seem to support grouping by strip instead only uses
        the default `order` group by. This means all images in the order
        will get composited together.

        parameters:
            None
        return:
            OrderBuilder: self instance
        raises:
            planet.exceptions.ClientError: If GeoJSON is not a valid polygon or
                multipolygon.
        """
        self.tools.append(
            composite_tool()
        )
        return self
    
    def add_delivery_config(self, 
                            archive_type: str, 
                            single_archive: bool,
                            archive_filename: str="{{order_id}}.zip"
                            ) -> OrderBuilder:
        """Adds the delivery configuration segment of the request.
        uses the delivery() method from the planet SDK.

        parameters:
            archive_type: Archive order files. Only supports 'zip'.
            single_archive: Archive all bundles together in a single file.
            archive_filename: Custom naming convention to use to name the
                archive file that is received. Uses the template variables
                {{name}} and {{order_id}}. e.g. "{{name}}_{{order_id}}.zip".
            cloud_config: Cloud delivery configuration.
        raises:
            planet.specs.SpecificationException: If archive_type is not valid.
        """
        self.delivery = delivery(archive_type, single_archive, 
                                 archive_filename)
        return self

class OrderHandler():
    """Order Handler for Planet data"""
    def __init__(self, request: Dict[str, Any], auth: Auth):
        """Constructor will get a list of saved orders.

        parameters:
            None
        returns:
            List[str]: A list of search ids
        raises:
            None
        """
        self.request = request
        self.auth = auth
        self.bar = reporting.StateBar(state="initialized")

    async def get_order(self, order_id: str) -> Dict[str, Any]:
        """Gets the order request details given the order id.
        
        parameters:
            order_id: The order id to retreive the order details from
        returns:
            Dict[str, Any]: Order details.
        raises:
            APIError: If an error is encounted with the API.
            ClientError: Invalid request sent to Planet.
            Exception: Unexpected error setting up the client
        """
        try:
            async with Session(auth=self.auth) as sess:
                cl = OrdersClient(sess)
                try:
                    order_details = await cl.get_order(order_id=order_id)
                    logger.debug(f"Found the following order details for {order_id}: {order_details}")


                except APIError as e:
                    logger.error(f"Error accessing Planet API: {e}")
                    return {}
                except ClientError as e:
                    logger.error(f"Error with request to Planet API: {e}")
                    return {}
        except Exception as e:
            logger.error(f"An Unexpected error occured: {e}")
            raise

    async def _create_order(self, request: Dict[str,Any]) -> Dict[str, Any]:
        """Gets the order request details given the order id.
        
        parameters:
            order_id: The order id to retreive the order details from
        returns:
            Dict[str, Any]: Order details.
        raises:
            APIError: If an error is encounted with the API.
            ClientError: Invalid request sent to Planet.
            Exception: Unexpected error setting up the client
        """
        try:
            async with Session(auth=self.auth) as sess:
                cl = OrdersClient(sess)
                try:
                    with self.bar as bar:
                        bar.update(state="creating")
                        order_details = await cl.create_order(request=request)
                        bar.update(state="created", order_id=order_details["id"])
                        logger.info(f"Created order: {order_details["id"]}")

                        await cl.wait(order_details["id"], 
                                      callback=bar.update_state,
                                      delay=11,
                                      max_attempts=0) # default 200 usually times out before getting to download and script will fail. 0=no limit

                    return order_details

                except APIError as e:
                    logger.error(f"Error accessing Planet API: {e}")
                    return {}
                except ClientError as e:
                    logger.error(f"Error with request to Planet API: {e}")
                    return {}
        except Exception as e:
            logger.error(f"An Unexpected error occured: {e}")
            raise

    async def _download_order(self,
                             order_id,
                             directory: Path|None=None,
                             overwrite: bool=False,
                             progress_bar=True
                             ) -> List[Path]:
        """Downloads the order based on order_id.
        This method will download all assest in an order useing the 
        download_order() method from the Planet SDK.

        parameters:
            order_id: Order id to download assests from.
            directory: Base directory for file download. Must already exist.
            overwrite: Overwrite files if they already exist.
            progress_bar: Show progress bar during download.
        returns:
            List[Path]: Paths to downloaded files.
        raises:
            APIError: If an error is encounted with the API.
            ClientError: Invalid request sent to Planet.
            Exception: Unexpected error setting up the client
        """
        dir = directory if directory is not None else pipeline.config["data_path"] / "images"

        try:
            async with Session(auth=self.auth) as sess:
                cl = OrdersClient(sess)
                try:
                    files = await cl.download_order(order_id=order_id,
                                                    directory=dir,
                                                    overwrite=overwrite,
                                                    progress_bar=progress_bar
                                                    )
                    logger.debug(f"downloaded order: {order_id}")
                    return files

                except APIError as e:
                    logger.error(f"Error accessing Planet API: {e}")
                    return []
                except ClientError as e:
                    logger.error(f"Error with request to Planet API: {e}")
                    return []
        except Exception as e:
            logger.error(f"An Unexpected error occured: {e}")
            raise

    async def create_poll_and_download(self):
        """Full create poll and download of an order.
        """
        order = await self._create_order(self.request)
        logger.info(f"Found order: {order}")
        download_paths = await self._download_order(order["id"])

        return download_paths


async def concurrent_planet_order(row, crs, aoi_feature, auth):
    request = (OrderBuilder("SiteCFilling")
               .add_product(row.ids,
                            "analytic_8b_sr_udm2",
                            "PSScene")
               .add_reproject_tool(crs)
               .add_clip_tool(aoi_feature)
               .add_composite_tool()
               .add_delivery_config(archive_type="zip",
                                    single_archive=True,
                                    archive_filename="{}.zip".format(row.date.replace("-",""))
                                    )
            )
    request_json = request.build()

    order = await OrderHandler(request_json, auth=auth).create_poll_and_download()
    logger.info(f"Downloaded order to {order}")

async def run_concurrent_image_fetch(tasks):
    await asyncio.gather(*tasks)
