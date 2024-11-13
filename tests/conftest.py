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
