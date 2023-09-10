## Build a data pipeline quickly with Mage
---

### Introduction
The objective of this practice is building a data pipeline using **Mage** - an open-source data pipeline tool. The workflow includes:
1. **Task 1**: Extracting data (*parquet files*) from online source (website) to local machine.
2. **Task 2**: Transforming this data.
3. **Task 3**: Loading transformed data into a local PostgreSQL database.
4. **Side Task** [Optional]: Buidling a simple ML model to predict the fee (excluding tip amount) paid for a taxi trip based on data loaded to the local PostgreSQL database in Task 3.
**Note**: The focus of this practice is about building a data pipline (Task 1, Task 2 and Task 3) with Mage. The side task of building a Machine Learning (ML) model is optional, however if it is implemented, this task can be easily integrated into the data pipeline.  

The above workflow will be developed with **Mage** (The workflow is composed of buidling blocks where each block represents a task). Further info on Mage can be found in the following: https://www.mage.ai/. The overview of the above workflow on Mage UI is as follows:

  ![ml_data_pipeline](https://github.com/DoThNg/Data-Engineering-Projects/blob/main/3_ETL_Mage/docs/ml_data_pipeline.png)

The dataset used in this practice is TLC Trip Record Data for yellow taxi (format: *parquet file*). For a quick experiment, the dataset only consists of the taxi trips in the first month of 2023. (*yellow_tripdata_2023-01.parquet*, Dataset - Retrieved September 7, 2023, from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

The Dataset and Data Dictionary used in this practice can be found and downloaded in the following:
1. Dataset: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
2. Data Dictionary: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

Tech stack:
- Python 3.10
- PostgreSQL 10
- mage-ai (v0.9.18)

---
### Workflow Overview in this practice

  ![workflow](https://github.com/DoThNg/Data-Engineering-Projects/blob/main/3_ETL_Mage/docs/workflow.png)

---

### Steps to run the data pipeline with Mage:
**Step 1:** Set up the virtual environment
- Run command: `python -m venv {virtualenv name}`
- Create a folder named `dataset` where the virtual env is created (The dataset will be downloaded and saved in this folder)

**Step 2:** Run command: 
`pip install -r requirements.txt` (This will install all relevant python packages for this practice)

**Step 3:** Set up a local PostgreSQL database (PostgreSQL 10 is used in this practice)

**Step 4:** Store credentials to create a database connection in a .env file (Reference: [env-template](https://github.com/DoThNg/Data-Engineering-Projects/blob/main/3_ETL_Mage/env-template))

**Step 5:** Set up Mage project (For a quick setup of Mage project, reference this [docs](https://docs.mage.ai/getting-started/setup)).

Run the following commands to set up a project:
- Project setup:
  `mage start data_pipeline` (In this Practice, Project Name is **data_pipeline**)
- Pipeline setup: Create a new pipeline named `etl_workflow` on Mage UI (To create a new pipeline (**Standard (batch)**) on Mage UI, reference this [docs](https://docs.mage.ai/design/data-pipeline-management)).
- For database connection, Mage enables connection to PostgreSQL database (For further info on setup, reference this [docs](https://docs.mage.ai/getting-started/setup)). Another method is to create a **Generic** block (no template) for a database connection - This method will be applied in this practice.

- The following files are used for data pipeline with Mage (These files are placed in the folder directories according to their tasks):
   - Task 1 [Extracting Data]: [data_pipeline/data_loaders](https://github.com/DoThNg/Data-Engineering-Projects/blob/main/3_ETL_Mage/data_pipeline/data_loaders/extract_taxi_data.py)
   - Task 2 [Transforming Data]: [data_pipeline/transformer](https://github.com/DoThNg/Data-Engineering-Projects/blob/main/3_ETL_Mage/data_pipeline/transformers/transform_taxi_data.py)
   - Task 3 [Loading Data]: [data_pipeline/data_exporters](https://github.com/DoThNg/Data-Engineering-Projects/blob/main/3_ETL_Mage/data_pipeline/data_exporters/load_data_to_postgresql.py)
   - ML model Task [Optional]: [data_pipeline/data_loaders](https://github.com/DoThNg/Data-Engineering-Projects/blob/main/3_ETL_Mage/data_pipeline/data_loaders/ml_taxi_model.py)

Each task represents a building block of the data pipeline. All blocks created in this practice are **Generic (no template)**, illustrated as follows: 

![Generic_Block](https://github.com/DoThNg/Data-Engineering-Projects/blob/main/3_ETL_Mage/docs/code_block.png)

The overall view of data pipeline folder is as follows:

  ![data_pipeline](https://github.com/DoThNg/Data-Engineering-Projects/blob/main/3_ETL_Mage/docs/data_pipeline_project_structure.png)

**Step 6:** Run the entire data pipeline: 

`mage run data_pipeline etl_workflow`

  ![data_pipeline_run](https://github.com/DoThNg/Data-Engineering-Projects/blob/main/3_ETL_Mage/docs/data_pipeline.png)
