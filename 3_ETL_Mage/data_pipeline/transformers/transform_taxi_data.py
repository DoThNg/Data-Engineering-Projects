import pandas as pd
from pandas import DataFrame

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data: DataFrame, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    data_rm_na = data.loc[:, ~data.columns.isin(["PULocationID", "DOLocationID", "Airport_fee", "store_and_fwd_flag", "VendorID"])].dropna()

    data_rm_na["pu_hour"] = data_rm_na["tpep_pickup_datetime"].dt.hour
    data_rm_na["do_hour"] = data_rm_na["tpep_dropoff_datetime"].dt.hour
    data_rm_na["travel_day"] = data_rm_na["tpep_pickup_datetime"].dt.dayofweek
    data_rm_na["fee_per_mile"] = round(data_rm_na["total_amount"] / data_rm_na["trip_distance"], 2)

    data_rm_na.replace({'travel_day':
                                {
                                    0: "Monday",
                                    1: "Tuesday",
                                    2: "Wednesday",
                                    3: "Thursday",
                                    4: "Friday",
                                    5: "Saturday",
                                    6: "Sunday"
                                }
                        }, inplace = True)

    data_rm_na.replace({"RatecodeID":
                                {
                                    1: "Standard rate",
                                    2: "JFK",
                                    3: "Newark",
                                    4: "Nassau or Westchester",
                                    5: "Negotiated fare",
                                    6: "Group ride",
                                    99: "Unknown"
                                }}, inplace = True)
    
    data_rm_na.replace({"payment_type":
                                {
                                    1: "Credit card",
                                    2: "Cash",
                                    3: "No charge",
                                    4: "Dispute",
                                    5: "Unknown",
                                    6: "Voided trip"
                                }}, inplace = True)

    data_rm_na.rename(columns={
                                "RatecodeID": "rate_code_des", 
                                "payment_type": "pmt_type_des"

                                }, inplace = True)

    return data_rm_na


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
