import os
import json
import pytest
from datetime import datetime as dt
from pathlib import Path

from pipeline import initialize
from pipeline.utils import setup_data_paths, setup_logging, read_geojson


class Test_setup_data_paths():
    """
    Tests for setup_data_paths.
    """
    @pytest.mark.smoke
    def test_valid_directory(self, tmp_path):
        """
        Test for ensuring a a valid directory is created from
        setup_data_paths. The built-in tmp path fixture is used
        to automatically tear down the created directory after
        the test is complete.

        Given:
            - No directory or file with the same name exists in the path
            - the argument is a valid path object
        Verifies:
            - A directory is created at the specified location
            - The returned path is a valid directory
        """
        dir_name = tmp_path / "data_test"

        path = setup_data_paths(dir_name)

        assert path.exists()
        assert path.is_dir()

        assert os.path.exists(path / "images")
        assert os.path.exists(path / "search_results")

        assert os.path.isdir(path / "images")
        assert os.path.isdir(path / "search_results")

    @pytest.mark.parametrize("data_path", [
        ("bad_path"),
        (100),
        (1.0),
        (True),
    ])
    def test_bad_arguments(self, data_path):
        """
        Test for ensuring that a path object must be given
        or a Value Error will be thrown.

        Given:
            - No directory or file with the same name exists in the path
            - the argument is not a valid path object
        Verifies:
            - Value error is thrown
        """
        dir_name = data_path
        
        with pytest.raises(ValueError):
            setup_data_paths(dir_name)

    def test_directory_already_exists(self, tmp_path):
        """
        Test for ensuring that if a directory already exists, the function
        will skip creation.

        Given:
            - A valid data directory exists already in the path
            - The argument is a valid path object
        Verifies:
            - The data will not be over written in the data folder
            - The returned path is a valid directory
        """
        dir_name = tmp_path / "data_test"
        test_file = Path(dir_name) / "test_file"

        os.mkdir(dir_name)
        test_file.touch()

        path = setup_data_paths(dir_name)

        assert path.exists()
        assert path.is_dir()

        assert os.path.exists(path / "images")
        assert os.path.exists(path / "search_results")

        assert os.path.isdir(path / "images")
        assert os.path.isdir(path / "search_results")

        assert os.path.isfile(test_file)

    def test_data_directory_to_overwrite_file(self, tmp_path):
        """
        Test for ensuring that if a directory already exists as a file,
        an exception will be raised

        Given:
            - A valid path is passed but already is a file.
            - The argument is a valid path object
        Verifies:
            - An Exception is thrown
        """
        test_file = Path(tmp_path) / "test_file"
        test_file.touch()

        with pytest.raises(FileExistsError):
            path = setup_data_paths(test_file)


class Test_setup_logging():
    """
    Tests for the setup logging function
    """
    @pytest.mark.smoke
    def test_valid_config(self, tmp_path):
        """
        Ensures that the log path is setup correctly and loggers are
        returned.

        Given:
            - A valid logging_config.json file
            - A valid logging level string
        Verifies:
            - A directory is created at the specified location
        """
        dir_name = tmp_path / "logs_test"

        setup_logging(dir_name)

        assert os.path.exists(dir_name)
        assert os.path.isdir(dir_name)

    @pytest.mark.parametrize("data_path", [
        ("bad_path"),
        (100),
        (1.0),
        (True),
    ])
    def test_bad_arguments(self, data_path, capsys, delete_log_file):
        """
        Test for ensuring that a path object must be given
        or a Value Error will be thrown.

        Given:
            - No directory or file with the same name exists in the path
            - the argument is not a valid path object
        Verifies:
            - Value error is thrown
        """
        dir_name = data_path
        
        setup_logging(dir_name)

        captured = capsys.readouterr()

        assert captured.out == f"Error creating logging config. defaulting to cwd. Data path given must be of type Path not {dir_name}\n"


class Test_initialize():
    """
    Tests for the pipeline initialization
    """
    @pytest.mark.smoke
    def test_valid_initialize(self):
        assert initialize()


class TestReadGeoJson():
    """Smoke test the read geojson function"""
    @pytest.mark.smoke
    def test_valid_geojson(self, test_geojson):
        path, data = test_geojson
        geometry = read_geojson(path)

        assert geometry == data["features"][0]["geometry"]

    def test_invlaid_geojson(self, test_bad_geojson):
        path, data = test_bad_geojson

        with pytest.raises(KeyError):
            geometry = read_geojson(path)

    def test_invlaid_geojson_geometry(self, test_bad_geojson_geometry):
        path, data = test_bad_geojson_geometry

        with pytest.raises(KeyError):
            geometry = read_geojson(path)

    def test_invalid_json(self, test_badjson_geojson):
        path, data = test_badjson_geojson
        with pytest.raises(json.JSONDecodeError):
            geometry = read_geojson(path)
        






