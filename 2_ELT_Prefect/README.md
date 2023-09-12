## Quick ELT Practice with PostgreSQL, dbt and Prefect
---

### Introduction
This is a follow-up of this [practice](https://github.com/DoThNg/Data-Engineering-Projects/tree/main/1_PostgreSQL_ETL). The objective is creating a more effective ELT workflow orchestration, using Prefect for the following tasks:
1. Task 1: Loading files (*.parquet*) into a local PostgreSQL database.
2. Task 2: Transforming dataset with dbt. The transformed dataset can be used to build BI Dashboards later.

Prefect provides a pythonic way to orchestrate all tasks of the workflow. Further info on *Prefect* can be found in the following: https://www.prefect.io/


The dataset used in this practice include:
1. TLC Trip Record Data for green taxi (format: *parquet files*).
2. Taxi Zone Maps and Lookup data (format: *CSV file*)

(Note: The ELT process is already implemented in this [practice](https://github.com/DoThNg/Data-Engineering-Projects/tree/main/1_PostgreSQL_ETL))

The Dataset and Data Dictionary used in this practice can be found and downloaded in the folllowing: 
1. Dataset: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
2. Data Dictionary: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf

Note: In this practice, for a quick implementation, the dataset consists of data for taxi zone (csv file) and taxi trips (parquet files) only for the first 2 months of 2023:
- taxi_zone_lookup.csv
- green_tripdata_2023-01.parquet
- green_tripdata_2023-02.parquet 

Dataset - Retrieved August 20, 2023, from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Tech stack:
- Python 3.10
- PostgreSQL 10
- dbt-core (1.6.0)
- dbt-postgres (1.6.0)
- prefect (2.11.5)
- prefect-dbt[cli]

---
### Workflow Overview in this practice

  ![workflow](https://github.com/DoThNg/Data-Engineering-Projects/blob/main/2_ELT_Prefect/docs/project_workflow.png)

---

### Steps to run this ELT process with Prefect:
**Step 1:** Set up the virtual environment
- Run commnad: `python -m venv {virtualenv name}` 
- Create a folder named `dataset` where the virtual env is created. 
- Save the data files (*parquet files*) in this folder. 

**Step 2:** Run command: `pip install -r requirements.txt` (This will install all relevant python packages for this practice)

**Step 3:** Set up a local PostgreSQL database (PostgreSQL 10 is used in this practice)

**Step 4:** Store credentials to create a database connection in a .env file (Reference: [env-template](https://github.com/DoThNg/Data-Engineering-Projects/blob/main/2_ELT_Prefect/env-template))

**Step 5:** Set up dbt project (For a quick setup of dbt project, reference this [docs](https://docs.getdbt.com/quickstarts/manual-install?step=2)). 

Run the following commands to set up dbt project: 
- Project setup: `dbt init analytics` (In this Practice, Project Name is **analytics**)
- Create a YAML file - `profiles.yml` in the **./analytics/** directory (Reference: [profiles.yml](https://github.com/DoThNg/Data-Engineering-Projects/blob/main/2_ELT_Prefect/analytics/profiles.yml)). (**Note**: *profiles.yml* is generally placed outside of dbt project to avoid sensitive credentials being checked in to version control, however in this practice, for just one-time local implementation, this file and *dbt_project.yml* will be in the same directory)
- Provide info (e.g., host, port, user, etc.) related to PostgreSQL database in this YAML file - `profiles.yml`. For this practice, set 'dev' to 'target' (target: dev))
- Create a sub-folder 'seeds' in project folder and add the file to the seeds directory, with a .csv file extension (In this practice, `taxi_zone_lookup.csv` file will be used)
- Create sub-folders `staging` and `mart` in folder `models`.
- Save `.sql` files in sub-folders `mart` and `staging`
Reference:
 - `.sql` files in sub-folder `mart`: [models/mart](https://github.com/DoThNg/Data-Engineering-Projects/tree/main/2_ELT_Prefect/analytics/models/mart)
 - `.sql` files in sub-folder `staging`: [models/staging](https://github.com/DoThNg/Data-Engineering-Projects/tree/main/2_ELT_Prefect/analytics/models/staging)

The overall dbt project structure is as follows:

  ![dbt project structure](https://github.com/DoThNg/Data-Engineering-Projects/blob/main/2_ELT_Prefect/docs/dbt_project_structure.png)

**Step 6:** Run command: `python elt.py`

Note: This command will run the workflow from loading local files to local PostgreSQL database and having this data transformed with dbt.

---

### dbt - Directed Acyclic Graph (DAG) 
The DAG of how data flows after transformation step with dbt is as follows:

![dbt project dag](https://github.com/DoThNg/Data-Engineering-Projects/blob/main/1_PostgreSQL_ETL/docs/dbt-dag.png)
