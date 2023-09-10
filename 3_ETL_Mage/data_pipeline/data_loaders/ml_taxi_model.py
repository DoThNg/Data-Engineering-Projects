from sklearn.tree import DecisionTreeRegressor
from sklearn.model_selection import train_test_split
from sklearn.decomposition import PCA
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import GridSearchCV
import numpy as np
import pandas as pd
from pandas import DataFrame
from dotenv import load_dotenv
import psycopg2
import os

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here

    load_dotenv()

    conn = None

    # Load data
    try:
        # Set up connect to database
        conn = psycopg2.connect(database=os.getenv('DB_NAME'),
                                user=os.getenv('DB_USER'),
                                password=os.getenv('DB_PASS'),
                                host=os.getenv('DB_HOST'),
                                port=os.getenv('DB_PORT'))
        
        print("Database connected successfully")

        # Create a cursor
        cur = conn.cursor()

        # Select data from 'yellowtaxi' table
        cur.execute("""SELECT passenger_count,
                            trip_distance,
                            rate_code_des,
                            pmt_type_des,
                            pu_hour,
                            do_hour,
                            travel_day,
                            tip_amount,
                            total_amount
                        FROM yellowtaxi
                        LIMIT 100000;
                    """)

        rows = cur.fetchall()

        ml_data_rm_na = pd.DataFrame(rows, columns = ["passenger_count", "trip_distance", "rate_code_des",
                                                        "pmt_type_des", "pu_hour", "do_hour", 
                                                        "travel_day", "tip_amount", "total_amount"])

        ml_data_rm_na["total_amount_excl_tip"] = ml_data_rm_na["total_amount"] - ml_data_rm_na["tip_amount"]
        ml_data_rm_na = ml_data_rm_na.loc[:, ~ml_data_rm_na.columns.isin(["tip_amount", "total_amount"])]

        cur.close()

    except Exception as error:

        print(error)

    finally:    
        # Close communication with the database
        if conn is not None:
            conn.close()
    
    # Encoding categorical variables
    ml_data_rm_na_encoded = pd.get_dummies(ml_data_rm_na, columns=["rate_code_des", "pmt_type_des", "travel_day"])

    # Load the dataset
    taxi_X = ml_data_rm_na_encoded.loc[:, ~ml_data_rm_na_encoded.columns.isin(["total_amount_excl_tip"])]
    taxi_y = ml_data_rm_na_encoded["total_amount_excl_tip"]

    #Convert data to numpy
    taxi_X = taxi_X.to_numpy()
    taxi_y = taxi_y.to_numpy()

    # Split dataset into training and test data
    taxi_X_train, taxi_X_test, taxi_y_train, taxi_y_test = train_test_split(taxi_X, taxi_y, test_size=0.3, random_state=12)

    # Apply PCA
    pca = PCA(n_components = 0.99)
    taxi_X_train_pca = pca.fit_transform(taxi_X_train)
    taxi_X_test_pca = pca.transform(taxi_X_test)

    taxi_regressor = DecisionTreeRegressor(random_state=0)

    # Set up params grid
    param_grid = {
                    "max_depth": [10, 15],
                    "min_samples_leaf": [5]
                }
    # Create a GridSearch:
    grid_search_mod = GridSearchCV(estimator = taxi_regressor,
                              param_grid = param_grid,
                              cv = 5,
                              scoring = "neg_mean_squared_error",
                              refit = True,
                              n_jobs = -1,
                              return_train_score = True
                    )

    grid_search_mod.fit(taxi_X_train_pca, taxi_y_train)

    # Print the best score
    best_score = grid_search_mod.best_score_
    print("The best cross-validation score: {:.2f}".format(best_score))

    # Print the parameters from the best score:
    best_params = grid_search_mod.best_params_
    print("The best parameters: {}".format(best_params))

    # Print the best estimators:
    print("The best estimator is as follows:\n{}".format(grid_search_mod.best_estimator_)) 

    return best_params

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
