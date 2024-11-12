from setuptools import setup, find_packages

setup(
    name = "planet-data-pipeline",
    version="0.0.1",
    package=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "planet",
        "geopandas"
        
    ],
    entry_points={
        "console_scripts": {
            "finder = scripts.finder:main"
        }
    }
)


