"""
title: conftest
Authors:
    - Patrick Harrison
    - Lee-Anna Gibson
Purpose:
    This module store global pytest fixtures.
"""
import pytest

@pytest.fixture(scope="session")
def api_key():
    return "PLFAKEAPIKEY123456"

@pytest.fixture(scope="session")
def aoi_feature():
    return {"type": "Polygon",
            "coordinates": [[[0,0],[0,1],[1,1],[1,0],[0,0]]]
            }
