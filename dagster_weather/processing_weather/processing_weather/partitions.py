from dagster import DailyPartitionsDefinition

from processing_weather.utils import timezone

daily_partitions = DailyPartitionsDefinition(
    start_date="2024-11-22", timezone=timezone.IST.zone, fmt="%Y-%m-%d"
)
"""Daily partitions for T-1 day partition."""
