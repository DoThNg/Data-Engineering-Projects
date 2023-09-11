from etl_project.extract_taxi_data import extract_data_from_source
from etl_project.transform_taxi_data import transform_taxi_data
from etl_project.load_data_to_postgresql import load_taxi_data
import pandas as pd

from dagster import asset

# Task 1: Extracting data (*parquet files*) from online source (website) to a local machine.
@asset
def extract_data() -> pd.DataFrame:
    return extract_data_from_source()
    
# Task 2: Transforming this data, using Pandas library.
@asset
def transform_data(extract_data: pd.DataFrame) -> pd.DataFrame:
    return transform_taxi_data(extract_data)

# Task 3: Loading transformed data into a local PostgreSQL database.
@asset
def load_data(transform_data: pd.DataFrame):
    load_taxi_data(transform_data)
