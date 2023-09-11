import pandas as pd

def transform_taxi_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Template code to transform data loaded from online source.

    Args:
        data: The output from the data extraction step

    Returns:
        a data frame
    """

    data_rm_na = data.loc[:, ~data.columns.isin(["PULocationID", "DOLocationID", "store_and_fwd_flag", "ehail_fee", "VendorID"])].dropna()

    data_rm_na["pu_hour"] = data_rm_na["lpep_pickup_datetime"].dt.hour
    data_rm_na["do_hour"] = data_rm_na["lpep_dropoff_datetime"].dt.hour
    data_rm_na["travel_day"] = data_rm_na["lpep_pickup_datetime"].dt.dayofweek
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

    data_rm_na.replace({"trip_type":
                                {
                                    1: "Street-hail",
                                    2: "Dispatch"
                                }}, inplace = True)    

    data_rm_na.rename(columns={
                                "RatecodeID": "rate_code_des", 
                                "payment_type": "pmt_type_des"
                                }, inplace = True)

    return data_rm_na
