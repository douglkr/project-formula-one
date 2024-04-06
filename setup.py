from setuptools import find_packages, setup

setup(
    name="analytics",
    packages=find_packages(exclude=["analytics_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-databricks",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
