import json
from datetime import datetime
import scrapy
from weather.items import weatherItem
import pymongo


class CrawlWeatherDataSpider(scrapy.Spider):
    name = "crawl_weather_data"
    allowed_domains = ["openweathermap.org"]
    custom_settings = {
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "CONCURRENT_REQUESTS": 1,
    }

    # Predefined list of cities
    cities = ["Tokyo", "Mumbai", "Delhi", "London", "Bangkok"]

    # The OpenWeatherMap API Key
    API_KEY = "9ae5753315e69ce502ee91d16ef3edef"

    weather_data_list = []

    # Removed the __init__ constructor since we will access settings elsewhere
    def start_requests(self):
        # Access MongoDB settings directly from Scrapy settings
        mongo_uri = self.settings.get("MONGO_URI")
        mongo_database = self.settings.get("MONGO_DATABASE")
        mongo_collection = self.settings.get("MONGO_COLLECTION")

        # MongoDB client setup using the settings from settings.py
        self.client = pymongo.MongoClient(mongo_uri)
        self.db = self.client[mongo_database]  # Database name from settings
        self.collection = self.db[mongo_collection]  # Collection name from settings

        # Loop through cities and make an API request for each
        for city in self.cities:
            # Prepare the URL for the API request
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={self.API_KEY}"

            # Log the request being made
            self.logger.info(f"Fetching weather data for {city} for current day.")

            # Send the request to the OpenWeatherMap API
            yield scrapy.Request(url=url, callback=self.parse, meta={"city": city})

    # Parse method - handles the response from the API
    def parse(self, response):
        city = response.meta["city"]

        # Parse JSON data from the API response
        data = json.loads(response.text)

        # Extract weather data
        if response.status == 200:
            # Extract relevant data from the API response
            temperature_kelvin = data["main"]["temp"]
            weather_description = data["weather"][0]["description"]
            humidity = data["main"]["humidity"]
            Longitude = data["coord"]["lon"]
            Latitude = data["coord"]["lat"]
            temp_min = data["main"]["temp_min"]
            temp_max = data["main"]["temp_max"]
            sunrise = data["sys"]["sunrise"]
            sunset = data["sys"]["sunset"]
            wind_speed = data["wind"]["speed"]

            # Log the successful data extraction
            self.logger.info(f"Weather data extracted for {city}")

            # Store the extracted data in the list
            item = weatherItem(
                city=city,
                date=datetime.now().strftime("%Y-%m-%d"),
                temperature_kelvin=temperature_kelvin,
                humidity=humidity,
                weather=weather_description,
                longitude=Longitude,
                latitude=Latitude,
                temp_min_kelvin=temp_min,
                temp_max_kelvin=temp_max,
                sunrise=sunrise,
                sunset=sunset,
                wind_speed=wind_speed,
            )
            # Store the weather data in MongoDB
            self.collection.insert_one(dict(item))

            # Log the successful data insertion
            self.logger.info(f"Weather data for {city} stored in MongoDB.")
        else:
            self.logger.error(
                f"Error fetching data for {city} - Status Code: {response.status}"
            )
