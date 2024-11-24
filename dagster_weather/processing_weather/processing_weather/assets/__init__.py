from dagster import load_assets_from_package_module
from . import analysis_data


ANALYSIS_DATA = "analysis_data"
weather_analysis_data_assets = load_assets_from_package_module(
    package_module=analysis_data, group_name=ANALYSIS_DATA
)
