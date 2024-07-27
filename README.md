# Metaflow ETL Workflow for Airbnb Data

This repo contains an ETL project which is based on Metaflow-based workflow to perform Extract, Transform, Load (`ETL`) operations on Airbnb data using Python and PostgreSQL. The workflow is split into separate scripts for data ingestion (`data_ingestion.py`) and data transformation (`data_transformation.py`), orchestrated by Metaflow (`etl_flow.py`).


## Setup

**Prerequisites**

   * Python 3.x installed
   * psycopg2-binary library installed
   * PostgreSQL database setup with credentials and schema as defined in `db_connection.py`
    * Metaflow library installed

## Installation

1. Clone the repository

```
git clone <repository_url>
cd <repository_directory>/etl_code
```
2. Install the required dependencies using requirements.txt file. Please use the below command to install the required packaged which is required as part of the project
```
pip install -r requirements.txt
```
3. Setup environment variables:
please check the .env file and add the connection parameters information
```
DB_NAME=<your_db_name>
DB_USER=<your_db_user>
DB_PASSWORD=<your_db_password>
DB_HOST=<your_db_host>
DB_PORT=<your_db_port>
```


## Command to run the project
There is a files called ```etl_flow.py```. Please run this file using below command which will perform various operations like **data ingestion, data transformation, and data loading**
```
python3 etl_flow.py run
```

**Note**

* Ensure PostgreSQL is running and accessible with the correct credentials specified in .env file.
