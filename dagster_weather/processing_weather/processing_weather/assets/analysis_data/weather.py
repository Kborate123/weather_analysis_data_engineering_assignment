import pandas as pd
from datetime import datetime
import sys
import subprocess
from dagster import asset
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns
import os
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    LongType,
)
from pymongo import MongoClient  # Import MongoDB client

# Now you can import settings from the Scrapy project
import settings  # Assuming the settings.py file has the MongoDB settings

cleaneddata_bucket = os.getenv("CLEANED_DATA_BUCKET")
# Add Scrapy project to Python path
sys.path.append("D:/Assignment_weather_data/weather/weather")


# MongoDB connection setup using settings from settings.py
client = MongoClient(settings.MONGO_URI)  # MongoDB URI from settings
db = client[settings.MONGO_DATABASE]  # Database name from settings
collection = db[settings.MONGO_COLLECTION]  # Collection name from settings


# Function to initialize Spark session with Delta support
def get_spark_session():
    spark = (
        SparkSession.builder.appName("Delta Lake Integration")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "")
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    sc._jsc.hadoopConfiguration().set(
        "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    return spark


@asset
def run_scrapy_spider(context):
    scrapy_command = ["scrapy", "crawl", "crawl_weather_data"]
    try:
        # Run the Scrapy spider command
        result = subprocess.run(
            scrapy_command,
            cwd="D:/Assignment_weather/weather",  # Path to the Scrapy project
            text=True,  # Captures output as text (instead of bytes)
            capture_output=True,  # Captures both stdout and stderr
            check=True,  # Raises CalledProcessError on non-zero exit code
        )

        # Log output if the process was successful
        context.log.info("Scrapy spider completed successfully.")
        context.log.info(f"Scrapy output: {result.stdout}")  # Log stdout

    except subprocess.CalledProcessError as e:
        # Log the error if subprocess fails
        context.log.error(f"Error running Scrapy spider: {e.stderr}")  # Log stderr
        raise  # Re-raise the exception to propagate it further

    except Exception as e:
        # Handle any other exceptions
        context.log.error(f"An unexpected error occurred: {str(e)}")
        raise  # Re-raise the exception to propagate it further


# Asset 1: Load data from MongoDB
@asset
def load_mongo_data(context, run_scrapy_spider):
    # Fetch data from MongoDB
    todays_date = datetime.now().strftime("%Y-%m-%d")
    cursor = collection.find({"date": todays_date})
    data = list(cursor)

    # Convert the data into a pandas DataFrame
    df = pd.DataFrame(data)
    context.log.info(f"Loaded {len(df)} rows from MongoDB.")

    return df


# Asset 2: Transform data (convert temperatures from Kelvin to Celsius) and handle missing data
@asset
def cleaned_df(context, load_mongo_data):
    df = load_mongo_data
    try:
        # Clean the data by converting temperatures from Kelvin to Celsius
        df["temperature_celsius"] = df["temperature_kelvin"] - 273.15
        df["temp_min_celsius"] = df["temp_min_kelvin"] - 273.15
        df["temp_max_celsius"] = df["temp_max_kelvin"] - 273.15

        context.log.info("Data cleaned: Converted temperatures to Celsius.")

        # Handle missing values: fill missing temperature values with the mean of the column
        df["temperature_celsius"].fillna(df["temperature_celsius"].mean(), inplace=True)
        df["temp_min_celsius"].fillna(df["temp_min_celsius"].mean(), inplace=True)
        df["temp_max_celsius"].fillna(df["temp_max_celsius"].mean(), inplace=True)

        # Remove rows where temperature_celsius is anomalous (e.g., below absolute zero)
        df = df[df["temperature_celsius"] >= -273.15]
        df = df[df["temp_min_celsius"] >= -273.15]
        df = df[
            df["temp_max_celsius"] >= -273.15
        ]  # Ensure no temperatures below absolute zero

        # Handle other anomalies or missing values as needed (e.g., dropping rows with missing essential columns)
        df.dropna(
            subset=["temperature_celsius", "temp_min_celsius", "temp_max_celsius"],
            inplace=True,
        )
        context.log.info("Missing values handled")
        csv_path = "weather_data.csv"

        # Save DataFrame to CSV
        df.to_csv(csv_path, index=False)
        context.log.info(f"Data saved to CSV at: {csv_path}.")
    except Exception as e:
        print(e)

    return df


# Asset 3: Save data to Delta Lake (Parquet format)
@asset
def save_to_delta_lake(context, cleaned_df):
    """
    This asset takes the cleaned data (in pandas DataFrame),
    converts it to a Spark DataFrame, and writes it to Delta Lake.
    """
    context.log.info("Starting to process data...")
    cleaned_df = cleaned_df
    # Step 1: Clean the pandas DataFrame before conversion to Spark DataFrame
    # Ensure _id is consistently typed (cast to string or integer as needed)
    cleaned_df["_id"] = cleaned_df["_id"].astype(
        str
    )  # Cast to string or any other type as needed

    # Step 2: Initialize Spark session
    spark = get_spark_session()
    context.log.info("Spark session initialized.")

    # Step 3: Define the explicit schema for the Spark DataFrame
    schema = StructType(
        [
            StructField("_id", StringType(), False),  # Unique identifier as String
            StructField("city", StringType(), False),  # City name as String
            StructField("date", StringType(), False),  # Date as DateType
            StructField(
                "temperature_kelvin", FloatType(), False
            ),  # Temperature in Kelvin as Float
            StructField("humidity", IntegerType(), False),  # Humidity as Integer
            StructField(
                "weather", StringType(), False
            ),  # Weather description as String
            StructField("longitude", FloatType(), False),  # Longitude as Float
            StructField("latitude", FloatType(), False),  # Latitude as Float
            StructField(
                "temp_min_kelvin", FloatType(), False
            ),  # Min Temperature in Kelvin as Float
            StructField(
                "temp_max_kelvin", FloatType(), False
            ),  # Max Temperature in Kelvin as Float
            StructField(
                "sunrise", LongType(), False
            ),  # Sunrise as Unix timestamp (Long)
            StructField("sunset", LongType(), False),  # Sunset as Unix timestamp (Long)
            StructField("wind_speed", FloatType(), False),  # Wind speed as Float
            StructField(
                "temperature_celsius", FloatType(), False
            ),  # Temperature in Celsius as Float
            StructField(
                "temp_min_celsius", FloatType(), False
            ),  # Min Temperature in Celsius as Float
            StructField("temp_max_celsius", FloatType(), False),
        ]
    )

    # Step 4: Convert the pandas DataFrame to a Spark DataFrame using the schema
    try:
        df = spark.createDataFrame(cleaned_df, schema=schema)
        context.log.info("DataFrame successfully converted to Spark DataFrame.")
    except Exception as e:
        context.log.error(f"Error while creating Spark DataFrame: {str(e)}")
        raise

    # Define Delta table path
    delta_table_path = f"s3a://{cleaneddata_bucket}/delta_weather_data"

    # Write to Delta if the table doesn't exist
    try:
        df.write.format("delta").mode("overwrite").save(delta_table_path)
        context.log.info(f"Data written to Delta table at {delta_table_path}")
    except Exception as e:
        context.log.error(f"Error writing to Delta table: {str(e)}")
        raise  # Returning path to be used by other assets
    # Return Delta table path for further processing
    return delta_table_path


# Asset 4: Query a previous version of the Delta table
@asset
def query_previous_version_of_data(context, save_to_delta_lake):
    """
    This asset queries a specific version of the Delta table using the 'versionAsOf' option.
    """
    # Initialize Spark session
    spark = get_spark_session()

    # Retrieve the path where the Delta table is saved (from the save_to_delta_lake asset)
    delta_path = save_to_delta_lake

    # Load the data from a specific version (e.g., version 2)
    version_to_query = 2
    delta_table = (
        spark.read.format("delta")
        .option("versionAsOf", version_to_query)
        .load(delta_path)
    )

    # Show the versioned data (for example, the first 5 rows)
    delta_table.show(5)

    # Log the number of rows in the queried version
    row_count = delta_table.count()
    context.log.info(
        f"Rows in version {version_to_query} of the Delta table: {row_count}"
    )

    return delta_table


# Asset 5: Analyze the data (average temperature and humidity)
@asset
def analyze_data(context, cleaned_df):
    df = cleaned_df

    # Perform basic analysis (calculating means, checking missing values, etc.)
    avg_temperature = df["temperature_celsius"].mean()
    avg_humidity = df["humidity"].mean()

    context.log.info(f"Average Temperature: {avg_temperature:.2f} °C")
    context.log.info(f"Average Humidity: {avg_humidity:.2f}%")

    return {"avg_temperature": avg_temperature, "avg_humidity": avg_humidity, "df": df}


# Asset 6: Generate Visualizations (Line chart, Bar chart, Pie chart)
@asset
def generate_visualizations(context, analyze_data):
    analysis_results = analyze_data
    df = analysis_results["df"]

    # Visualization 1: Line chart for temperature trend over 7 days
    plt.figure(figsize=(10, 6))
    date = datetime.now().strftime("%Y-%m-%d")  # Assuming there's a 'date' column
    plt.plot(
        df["city"], df["temperature_celsius"], marker="o", linestyle="-", color="b"
    )
    plt.title(f"Temperature Trend citiwise today's date{date}")
    plt.xlabel("city")
    plt.ylabel("Temperature (°C)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("temperature_trend.png")
    plt.close()
    context.log.info("Temperature trend chart saved as 'temperature_trend.png'.")

    # Visualization 2: Bar chart comparing average humidity levels across cities
    avg_humidity_per_city = df.groupby("city")[
        "humidity"
    ].mean()  # Assuming there's a 'city' column
    plt.figure(figsize=(10, 6))
    avg_humidity_per_city.plot(kind="bar", color="skyblue")
    plt.title("Average Humidity Levels Across Cities")
    plt.xlabel("City")
    plt.ylabel("Average Humidity (%)")
    plt.tight_layout()
    plt.savefig("humidity_bar_chart.png")
    plt.close()
    context.log.info("Humidity bar chart saved as 'humidity_bar_chart.png'.")

    # Visualization 3: Pie chart for weather condition distribution
    weather_condition_counts = df[
        "weather"
    ].value_counts()  # Assuming there's a 'weather_condition' column
    plt.figure(figsize=(8, 8))
    weather_condition_counts.plot(
        kind="pie",
        autopct="%1.1f%%",
        colors=sns.color_palette("Set3", len(weather_condition_counts)),
    )
    plt.title("Weather Condition Distribution")
    plt.ylabel("")  # Remove the y-label for pie chart
    plt.tight_layout()
    plt.savefig("weather_condition_pie_chart.png")
    plt.close()
    context.log.info(
        "Weather condition pie chart saved as 'weather_condition_pie_chart.png'."
    )

    return {
        "temperature_trend_chart": "temperature_trend.png",
        "humidity_bar_chart": "humidity_bar_chart.png",
        "weather_condition_pie_chart": "weather_condition_pie_chart.png",
    }
