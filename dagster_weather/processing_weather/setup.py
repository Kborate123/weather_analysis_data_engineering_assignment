from setuptools import find_packages, setup

setup(
    name="processing_weather",
    packages=find_packages(exclude=["processing_weather_tests"]),
    install_requires=["dagster", "dagster-cloud"],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
