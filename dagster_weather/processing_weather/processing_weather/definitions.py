from dagster import Definitions, load_assets_from_modules

from processing_weather import assets  # noqa: TID252

all_assets = load_assets_from_modules([assets])
# all_jobs = load_assets_from_modules([jobs])

defs = Definitions(
    assets=all_assets,
    # jobs=all_jobs,
)
