from dagster import define_asset_job, AssetSelection
from processing_weather.assets import (
    run_scrapy_spider,
    load_mongo_data,
    cleaned_df,
    transform_to_csv,
    save_to_delta_lake,
    query_previous_version_of_data,
    analyze_data,
    generate_visualizations,
)

# Define the weather data job by selecting all relevant assets
weather_data_job = define_asset_job(
    name="weather_data_job",
    selection=AssetSelection.assets(
        run_scrapy_spider,
        load_mongo_data,
        cleaned_df,
        transform_to_csv,
        save_to_delta_lake,
        query_previous_version_of_data,
        analyze_data,
        generate_visualizations,
    ),
)
