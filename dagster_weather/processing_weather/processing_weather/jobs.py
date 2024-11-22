from dagster import (
    build_schedule_from_partitioned_job,
    define_asset_job,
)

from .partitions import (
    daily_partitions,
)

daily_weather_data_analysis_job_schedule = build_schedule_from_partitioned_job(
    define_asset_job(
        "daily_weather_data_analysis_job",
        partitions_def=daily_partitions,
    )
)
