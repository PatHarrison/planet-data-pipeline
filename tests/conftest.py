"""
title: conftest
Authors:
    - Patrick Harrison
    - Lee-Anna Gibson
Purpose:
    This module store global pytest fixtures.
"""
import os
import json
import pytest
from datetime import datetime as dt

@pytest.fixture(scope="session")
def delete_log_file():
    """
    Deletes the last log file in cwd.
    Need a scope session so the logger releases the lock on the log
    before we can delete it.
    """
    now = dt.now()
    yield

    # Cleanup
    for filename in os.listdir(os.getcwd()):
        file_modified_time = dt.fromtimestamp(os.path.getmtime(filename))
        if filename.endswith(".log") and file_modified_time > now:
            os.remove(filename)

    
@pytest.fixture
def test_geojson(tmp_path):
    """
    Fixture to write a fake geojson file
    """
    geojson_data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [102.0, 0.0],
                            [103.0, 0.0],
                            [103.0, 1.0],
                            [102.0, 1.0],
                            [102.0, 0.0]
                        ]
                    ]
                },
                "properties": {
                    "name": "Polygon A"
                }
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [102.0, 0.5]
                },
                "properties": {
                    "name": "Point A"
                }
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [102.0, 0.0],
                        [103.0, 1.0],
                        [104.0, 0.0]
                    ]
                },
                "properties": {
                    "name": "Line A"
                }
            }
        ]
    }

    # Create the GeoJSON file in the temporary directory
    geojson_file_path = tmp_path / "fake_geojson_file.geojson"
    
    # Write the GeoJSON data to the file
    with open(geojson_file_path, "w") as f:
        json.dump(geojson_data, f, indent=4)

    # Return the file path to the test
    yield geojson_file_path, geojson_data


@pytest.fixture
def test_bad_geojson(tmp_path):
    """
    Fixture to write a fake geojson file with invalid geojosn but
    correct json
    """
    geojson_data = {
        "type": "FeatureCollection",
        "featuress": [
            {
                "type": "Featureee",
                "geometry": {
                    "coordinates": [
                        [
                            [102.0, 0.0],
                            [103.0, 0.0],
                            [103.0, 1.0],
                            [102.0, 1.0],
                        ]
                    ]
                },
                "properties": {
                    "name": "Polygon A"
                }
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [102.0, 0.5]
                },
                "properties": {
                    "name": "Point A"
                }
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [102.0, 0.0],
                        [103.0, 1.0],
                        [104.0, 0.0]
                    ]
                },
                "properties": {
                    "name": "Line A"
                }
            }
        ]
    }

    # Create the GeoJSON file in the temporary directory
    geojson_file_path = tmp_path / "fake_geojson_file.geojson"
    
    # Write the GeoJSON data to the file
    with open(geojson_file_path, "w") as f:
        json.dump(geojson_data, f, indent=4)

    # Return the file path to the test
    yield geojson_file_path, geojson_data


@pytest.fixture
def test_bad_geojson_geometry(tmp_path):
    """
    Fixture to write a fake geojson file with no geometry field.
    """
    geojson_data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometryy": {
                    "coordinates": [
                        [
                            [102.0, 0.0],
                            [103.0, 0.0],
                            [103.0, 1.0],
                            [102.0, 1.0],
                        ]
                    ]
                },
                "properties": {
                    "name": "Polygon A"
                }
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [102.0, 0.5]
                },
                "properties": {
                    "name": "Point A"
                }
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [102.0, 0.0],
                        [103.0, 1.0],
                        [104.0, 0.0]
                    ]
                },
                "properties": {
                    "name": "Line A"
                }
            }
        ]
    }

    # Create the GeoJSON file in the temporary directory
    geojson_file_path = tmp_path / "fake_geojson_file.geojson"
    
    # Write the GeoJSON data to the file
    with open(geojson_file_path, "w") as f:
        json.dump(geojson_data, f, indent=4)

    # Return the file path to the test
    yield geojson_file_path, geojson_data


@pytest.fixture
def test_badjson_geojson(tmp_path):
    """
    Fixture to write a fake geojson file with invalid json.
    """
    geojson_data = {
        "type": "FeatureCollection",
        "featuress": [
            {
                "type": "Featureee",
                "geometry": {
                    "coordinates": [
                        [
                            [102.0, 0.0],
                            [103.0, 0.0],
                            [103.0, 1.0],
                            [102.0, 1.0],
                        ]
                    ]
                },
                "properties": {
                    "name": "Polygon A"
                }
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [102.0, 0.5]
                },
                "properties": {
                    "name": "Point A"
                }
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [102.0, 0.0],
                        [103.0, 1.0],
                        [104.0, 0.0]
                    ]
                },
                "properties": {
                    "name": "Line A"
                }
            }
        ]
    }

    # Create the GeoJSON file in the temporary directory
    geojson_file_path = tmp_path / "fake_geojson_file.geojson"
    
    # Write the GeoJSON data to the file
    with open(geojson_file_path, "w") as f:
        json.dump(geojson_data, f, indent=4)
        f.write("}")

    # Return the file path to the test
    yield geojson_file_path, geojson_data


