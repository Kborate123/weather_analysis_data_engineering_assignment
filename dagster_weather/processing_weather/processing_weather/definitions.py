from dagster import Definitions, load_assets_from_modules

from processing_weather.utils.dagster import load_jobs_from_modules

from . import assets, jobs
from .jobs import (
    daily_weather_data_analysis_job_schedule,
)

all_assets = load_assets_from_modules([assets])
all_jobs = load_jobs_from_modules([jobs])
all_schedules = [
    daily_weather_data_analysis_job_schedule,
]

defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
)
