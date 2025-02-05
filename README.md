# Planet Data Pipeline

## Overview
This python program enables user to authenticate and interact with the Planet
API to:
- Search for satellite imagery based on specified criteria
- Download imagery and metadata for further processing in ArcGIS Pro
or other GIS environments

## Requirements
- Python >=3.9
- Planet API Key (for authenticating with the planet API)


## Installation
1. Clone the Repository
```bash
git clone https://github.com/PatHarrison/planet-data-pipeline
cd planet-data-pipeline
```
2. Set Up Virtual Environment
```bash
python -m virtualenv venv
.\venv\Scripts\activate.ps1
```
3. Install the Package
```bash
pip install .
```
This will allow you to run the scripts. All dependencies are included
when installing the pipeline in step 3.

### Development Mode
To install the pipeline in development mode, use:
```bash
pip install -e .[dev]
```
This will install the pipline package and scripts in an editable environment
which allows you to make changes to the pipeline and run/test the changes.
This will also include development only packages like pytest and
pytest-asyncio.


## Configuration
Configuration for the download is pulled from the `config.ini` file.
This file specifies pipeline configuration like logging and output directories.
Different filters can easily be set from the configuration file.
An example configuration:

```ini
[pipeline]
loglevel = DEBUG
logpath = logs
outdatadir = data
; The follow will be put into `outdatadir` path
outimagedirname = images
outsearchresults = search_results

[pylanet.delivery]
outcrs = 3005
ordername = SiteCFilling


[pylanet.filters]
itemtypes = ["PSScene"]
startdate = 2024-11-04
enddate = 2024-11-11
aoi = T083_R019_W6.geojson
mincloudcover = 0
maxcloudcover = 1
maxclearpercent = 100
minclearpercent = 40
permissionfilter = True
stdqualityfilter = True
instruments = ["PSB.SD", "PS2.SD", "PS2"]
assests = ["ortho_analytic_8b_sr"]
requiredcoverage = 100.0
```

### API Key
To Access the Planet API, you will need to set up a planet account and API key.
The API key is availble under your profile settings on Planet.com

The API key can be passed to the scripts:
```bash
pylanet <api_key>
```

### Logging
logs will be created in a `logs/` directory at the root level of the repository.
The configuration is exposed to you for logging so be careful but have fun if you 
want logging to the console for example.
Logging levels can be passed as a parameter to the scripts as well.


## Usage
After installing the pipeline to the pip virtual environment, the endpoints
are exposed in your path. This means that you are able to use the finder script
like:
```bash
pylanet
```
to get more information of CLI options run:
```bash
<script> -h
```


### Running Tests
This pipeline uses pytest for unit and integration testing. To use pytest,
with a coverage report use:

```bash
pytest --cov
```
This will run all tests under the `tests/` directory.

