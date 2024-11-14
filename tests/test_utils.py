import pytest
import planet
import json
from pathlib import Path

import pipeline
from pipeline.utils import *


class TestSetupDataPath():
    """Tests utils.setup_data_path function."""
    def test_valid(self, tmp_path):
        """Tests valid path.
        tmpdir is a built in fixture for testing directory creation in
        scripts.
        """
        temp_dir = tmp_path / "data_test"

        # ensure dir does not already exist for test
        assert os.path.exists(temp_dir) is False

        setup_data_path(temp_dir)

        assert os.path.exists(temp_dir) is True
        assert os.path.isdir(temp_dir) is True

        assert os.path.exists(temp_dir / "images") is True
        assert os.path.isdir(temp_dir / "images") is True

        assert os.path.exists(temp_dir / "search_results") is True
        assert os.path.isdir(temp_dir / "search_results") is True
    
    def test_invalid(self):
        """Tests valid path.
        tmpdir is a built in fixture for testing directory creation in
        scripts.
        """
        with pytest.raises(NotADirectoryError):
            setup_data_path(Path("test_:.data"))


class TestIndent():
    """Test the indent function"""
    def test_valid_json(self, capsys):
        """Tests valid indent"""
        data = {"a": 1.0, "b": 2, "c": [1,2,3]}
        expected_out = "{\n  \"a\": 1.0,\n  \"b\": 2,\n  \"c\": [\n    1,\n    2,\n    3\n  ]\n}\n"

        indent(data)

        captured = capsys.readouterr()

        assert captured.out == expected_out


class TestAuthenticate():
    """Test the authenticate util method."""
    def test_no_initial_key(self, api_key):
        """Tests setting the api key to an environment var"""

        # Test that no key is set
        with pytest.raises(KeyError):
            os.environ["PL_API_KEY"]

        authenticate(api_key)

        assert os.environ["PL_API_KEY"] == api_key

    def test_no_overwrite_if_exists(self, api_key):
        """Tests setting the api key to an environment var"""

        # Test that no key is set
        os.environ["PL_API_KEY"] = "Testing"

        authenticate(api_key)

        assert os.environ["PL_API_KEY"] == "Testing"


class TestSetupLogging():
    """Tests the logging setup from config file"""
    def test_valid_dir(self, tmp_path):
        temp_dir = tmp_path / "logs_test"

        # ensure dir does not already exist for test
        assert os.path.exists(temp_dir) is False

        setup_logging(temp_dir)

        assert os.path.exists(temp_dir) is True
        assert os.path.isdir(temp_dir) is True

    def test_invalid_dir(self, tmp_path, capsys):
        """Should print message that logs will be stored in root."""
        temp_dir = tmp_path / "logs_:.test"

        setup_logging(temp_dir)

        results = capsys.readouterr()

        assert results.out == "Error creating the logging directory. Storing logs in root.\n"
        assert results.err == ""


class TestReadGeoJson():
    """Smoke test the read geojson function"""
    def test_valid_geojson(self):
        geometry = read_geojson(Path("T083_R019_W6.geojson"))

        assert isinstance(geometry, dict)


class TestPipelineInit():
    """Tests pipeline init"""
    def test_deactivate(self, api_key):        
        # test setup
        os.environ["PL_API_KEY"] = api_key

        result = pipeline.deactivate()

        assert result == 0

    def test_already_deactivated(self):
        result = pipeline.deactivate()
        assert result != 0


    def test_initialize(self, api_key, tmp_path):
        temp_logs = tmp_path / "logs_test"
        temp_data = tmp_path / "data"
        config = {
            "log_level": "DEBUG",
            "log_path": temp_logs,
            "data_path": temp_data,
            "api_key": api_key
        }

        pipeline.initialize(config=config)

        pipeline.deactivate()

        assert os.path.exists(temp_data)
        assert os.path.exists(temp_data / "images")
        assert os.path.exists(temp_data / "search_results")

        assert os.path.exists(temp_logs)

