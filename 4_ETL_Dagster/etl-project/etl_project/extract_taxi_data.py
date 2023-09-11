import os
import requests
import pandas as pd
import re
import glob

def extract_data_from_source():
    """
    Template for extracting taxi data from online source (website)

    """

    DATA_DIR =  ".\dataset"
 
    # Specify page url
    page_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

    # Set the string containing yellow taxi data
    string = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_"

    # Specify the cutoff for url in the response in text
    cutoff_url = 78
    
    # Specify the cutoff for file name (parquet file)
    cutoff_file_name = 48
    
    # Specify the number of data months to be downloaded
    # In this practice, only data of the first 2 months of 2023 is downloaded
    n_months_to_download = 2


    # checking if the directory "./dataset" exist or not.
    if not os.path.isdir("./dataset"):
        # Create a folder to store dataset
        os.makedirs("./dataset")

    # The following steps are implemented to download parquet file(s) to local machine
    response = requests.get(page_url)

    response_text = response.text

    # Find locations of url in the text.
    file_list_pos = [file_pos.start() for file_pos in re.finditer(string, response_text)]

    # Create a list containing list of urls for data downloading (Each url represents a download link for a data file) 
    file_url_list = {}

    for i in range(len(file_list_pos)):

        file_url = response_text[file_list_pos[i] : file_list_pos[i] + cutoff_url]
        file_name = response_text[file_list_pos[i] + cutoff_file_name : file_list_pos[i] + cutoff_url]
        added_file = [(file_name, file_url)]
        file_url_list.update(added_file)

    for n_month in range(n_months_to_download):
        select_file_name = list(file_url_list.keys())[n_month]
        select_file_url = list(file_url_list.values())[n_month]

        saved_file_path = os.path.join(DATA_DIR, select_file_name)

        data = requests.get(select_file_url)
        
        with open(saved_file_path, 'wb') as file:
            file.write(data.content)

    data.close()
    response.close()

    # Read the downloaded parquet file(s)
    data_files = glob.glob(os.path.join(DATA_DIR, '*.parquet'))

    file_list = []

    for datafile in data_files:

        datafile_path = os.path.abspath(datafile)

        read_file = pd.read_parquet(datafile_path, engine='pyarrow')
        file_list.append(read_file)

    combined_file = pd.concat(file_list)
    
    return combined_file