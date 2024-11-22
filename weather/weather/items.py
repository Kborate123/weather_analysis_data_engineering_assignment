# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class weatherItem(scrapy.Item):
    # define the fields for your item here like:
    city = scrapy.Field()
    date = scrapy.Field()
    temperature_kelvin = scrapy.Field()
    humidity = scrapy.Field()
    weather = scrapy.Field()
    longitude = scrapy.Field()
    latitude = scrapy.Field()
    temp_min_kelvin = scrapy.Field()
    temp_max_kelvin = scrapy.Field()
    sunrise = scrapy.Field()
    sunset = scrapy.Field()
    wind_speed = scrapy.Field()
