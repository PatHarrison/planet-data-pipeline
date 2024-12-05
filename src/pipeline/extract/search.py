"""
Title: Search
Project: planet-data-pipeline
Authors:
    - Patrick Harrison
    - Lee-Anna Gibson
Purpose:
    This module provides the search handler class
    data from the Planet API using the Planet SDK.

    Classes:
        - SearchHandle
"""
import logging
from typing import Any, Dict, List

from planet import Session
from planet.exceptions import APIError, ClientError


logger = logging.getLogger("pipeline")


class SearchHandler():
    """Search Handler for planet API searches."""
    def __init__(self, auth):
        """
        Constructor will get a list of saved searches.

        Args:
            auth (Auth): Planet Auth object

        Returns:
            None

        Raises:
            None
        """
        self.auth = auth

    async def make_search(self, name: str, search_filter: Dict[Any, Any],
                          item_types: List[str], enable_email=False
                         ) -> Dict[str, Any]:
        """
        Makes a new saved search and returns the search JSON.
        Uses the create_search method from the data client to make a new saved
        search with Planet. The function will return the search JSON request.

        Args:
            name (str): Name for the search.
            item_types (List[str]): Item types for the search.
            search_filter (dict[str, Any]): Filters for the search.
            enable_email (bool): Whether to send email notifications for the
                search.
        Returns:
            Dict[str, Any]: JSON search request

        Raises:
            APIError: If an error is encounted with the API.
            ClientError: Invalid request sent to Planet.
            Exception: Unexpected error setting up the Session.
        """
        logger.info(f"Making a new search: {name}")
        try:
            async with Session(auth=self.auth) as sess:
                try:
                    logger.info(f"Starting Session to make search")
                    cl = sess.client("data")
                    request = await cl.create_search(name=name, 
                                                     search_filter=search_filter,
                                                     item_types=item_types, 
                                                     enable_email=enable_email
                                                     )
                    logger.info(f"Search Created {request['name']}")
                    logger.debug(request)
                    return request
                except APIError as e:
                    logger.error(f"Error accessing Planet API: {e}")
                    raise
                except ClientError as e:
                    logger.error(f"Error with request to Planet API: {e}")
                    raise
        except Exception as e:
            logger.error(f"An Unexpected error occured: {e}")
            raise


    async def perform_search(self, search_id=str) -> List[Dict[str, Any]]:
        """
        Performs the search matching the search_id.
        Uses the run_search method from the data client from Planet.

        Args:
            search_id (str): The search id for the search to perform

        Returns:
            List[Dict[str, Any]]: List of items found by the search.

        Raises:
            APIError: If an error is encounted with the API.
            ClientError: Invalid request sent to Planet.
            Exception: Unexpected error setting up the client
        """
        logger.info(f"Starting Search {search_id}")
        try:
            async with Session(auth=self.auth) as sess:
                try:
                    cl = sess.client("data")
                    items = cl.run_search(search_id=search_id, limit=0)
                    item_list = [i async for i in items]
                    logger.info(f"Search done found {len(item_list)} images")
                    logger.debug(item_list)
                    return item_list
                except APIError as e:
                    logger.error(f"Error accessing Planet API: {e}")
                    raise
                except ClientError as e:
                    logger.error(f"Error with request to Planet API: {e}")

        except Exception as e:
            logger.error(f"An Unexpected error occured: {e}")
            raise

