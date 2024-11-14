"""
Title: Test Pylanet Script
Authors:
    - Patrick Harrison
    - Lee-Anna Gibson
Purpose:
    Test the pylanet script.
"""
import pytest
import subprocess


class TestPylanetArgs():
    def test_no_params_passed(self):
        """"""
        results = subprocess.run(
                ["pylanet"],
                capture_output=True, 
                text=True
        )
        # assertions
        assert results.returncode != 0
        assert ("error: the following arguments are required: startdate, "
                "enddate, --aoi, --crs") in results.stderr

    def test_missing_api_key(self):
        """"""
        results = subprocess.run(
                ["pylanet",
                 "2025-13-00",
                 "2025-",
                 "--aoi", "T083_R019_W6.geojson", 
                 "--crs", "3005"
                 ],
                capture_output=True, 
                text=True
        )
        assert results.returncode != 0
        assert ("An API Key is required for Planet. Please provide as a"
                 "system environment variable `PLANET_API_KEY` or as a "
                 "parameter to the script.") in results.stderr

    def test_no_invalid_dates_passed(self, api_key):
        """"""
        results = subprocess.run(
                ["pylanet",
                 "2025-13-00",
                 "2025-",
                 "--apikey", api_key,
                 "--aoi", "T083_R019_W6.geojson", 
                 "--crs", "3005"
                 ],
                capture_output=True, 
                text=True
        )
        # assertions
        assert results.returncode != 0
        assert ("Invlaid dates passed `2025-13-00`,`2025-` use YYYY-MM-DD "
                "Format") in results.stderr


