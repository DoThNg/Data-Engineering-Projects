from setuptools import find_packages, setup

setup(
    name="etl_project",
    packages=find_packages(exclude=["etl_project_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas==2.1.0",
        "psycopg2==2.9.7",
        "python-dotenv==1.0.0",
        "pyarrow==14.0.1"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
