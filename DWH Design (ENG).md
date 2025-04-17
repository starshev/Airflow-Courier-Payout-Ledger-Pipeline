### Project Repository Contents

readme.md - project overview \
«DWH Design».md - ETL design \
Documentation.pdf - ETL schema \
«sql» folder - SQL scripts  \
«modules» folder - Python modules  \
«dags» folder - pipeline  

### New Data Source - Courier Service API

#### 1. Method "GET / couriers"

Sorted by courier name in ascending order.  
Batch size: 50 records per request.  

Response = JSON array with fields:

* "_id" - courier ID (string)
* "name" - full name of the courier (string)

#### 2. Method "GET / deliveries"

Sorted by delivery date in ascending order.  
Batch size: 50 records per request.  

Response = JSON array with fields:

* "order_id" - order ID (string)
* "order_ts" - order timestamp
* "delivery_id" - delivery ID (string)
* "courier_id" - courier ID (string)
* "address" - delivery address (string)
* "delivery_ts" - actual delivery timestamp
* "rate" - delivery rating (integer 1-5)
* "sum" - order total (large decimal number)
* "tip_sum" - tip amount (large decimal number)

##### Notes

* The response of the "GET / couriers" method lacks an incremental surrogate key and entity update date, preventing support for SCD2 and incremental loading. This dimension will be fully overwritten with the latest state in each DAG run, updating only the single attribute (full name) using SCD1.

### Implementation of Python Modules for Incremental Loading from Source to STG

* */modules/load_couriers.py* - loads courier data
* */modules/load_deliveries.py* - loads delivery data

### ETL Design

#### 1. **CDM**

The **cdm.dm_courier_ledger** data mart according to requirements.  
Data types are based on source analysis and existing DWH data.  

Constraints:
- Surrogate PK with auto-increment
- Monetary and quantity values must be >=0 with a default of 0
- All fields are NOT NULL
- Rating between 0 and 5 inclusive
- Month between 1 and 12 inclusive
- Year between 2022 and 2100 inclusive
- UNIQUE constraint on the combination: courier business key + report year + report month

Implementation of the data mart: */sql/DDL_cdm.dm_courier_ledger.sql*  
Implementation of the update script: */sql/courier_ledger_update.sql*  

#### 2. **DDS**

Snowflake model.

Facts:
* **dds.fct_deliveries** (deliveries completed by couriers)

Dimensions:
* Couriers **dds.dm_couriers** (new)
* Orders **dds.dm_orders** (already in DWH, shared with another snowflake model, no additions)
* Timestamps **dds.dm_timestamps** (already in DWH, supplemented with new delivery dates)

Data types are aligned with further usage for **cdm.dm_courier_ledger**.

Attributes in the courier dimension table:
- Courier business key
- Courier full name

Dimensions in the fact table:
- Delivery timestamp
- Order ID
- Order total
- Courier ID

Metrics in the fact table:
- Delivery rating
- Tip amount

Technical attributes in the fact table:
- Delivery business key

Constraints:
- Surrogate PKs with auto-increment in all tables
- Monetary and quantity values must be >= 0
- All fields are NOT NULL
- FKs in the fact table (deliveries) reference surrogate keys of couriers, orders, and timestamps
- UNIQUE on courier business key (to prevent duplicates) in the courier table - relevant as SCD1
- UNIQUE on delivery business key (to prevent duplicates) in the fact table - relevant as SCD0

Implementation of the fact table: */sql/DDL_dds.fct_deliveries.sql*  
Implementation of the courier dimension table: */sql/DDL_dds.dm_couriers.sql*  

Implementation of the incremental delivery loading script: */sql/deliveries_stg_to_dds.sql*  
Implementation of the incremental courier loading script: */sql/couriers_stg_to_dds.sql*  
Implementation of the incremental timestamp loading script: */sql/timestamps_stg_to_dds.sql*  

##### Notes

Incremental loading of facts and dimensions (couriers, timestamps) is achieved by recording the last processed delivery date (from **stg.deliverysystem_deliveries**) in **dds.srv_wf_settings** with the workflow key "deliveries_stg_to_dds_workflow".

Loading into the "couriers" dimension follows SCD1 = insert new / overwrite existing attributes on business key conflict. \
Loading into the "timestamps" dimension follows SCD0 = insert new / do nothing on UNIQUE key "ts" conflict. \
Loading into the fact table follows SCD0 = insert new / do nothing on business key conflict.

#### 3. **STG**

Table **stg.deliverysystem_couriers** for incremental loads via API using the "GET / couriers" method:
- Surrogate PK with auto-increment
- Full JSON response stored as text
- Courier business key extracted from JSON response

Constraints:
- UNIQUE on courier business key (to prevent duplicates)

Full reload of all couriers in each run, inserting new couriers into STG / updating existing ones using SCD1.

Table **stg.deliverysystem_deliveries** for incremental loads via API using the "GET / deliveries" method:
- Surrogate PK with auto-increment
- Full JSON response stored as text
- Delivery business key extracted from JSON response
- Delivery timestamp extracted from JSON response

Constraints:
- UNIQUE on delivery business key (to prevent duplicates)

Implementation: */sql/DDL_stg.deliverysystem_deliveries.sql*

Incremental loading is ensured by recording the last processed delivery date in **stg.srv_wf_settings** with the workflow key "deliverysystem_origin_to_stg_workflow".

### General Notes

The default delivery rating is set to 0. When calculating the average rating in the data mart, only ratings greater than 0 are considered (i.e., filtering out deliveries where a rating might not have been given).

All layer movements are implemented using PostgresOperator, as all layers reside in a Postgres database.

The pipeline is scheduled to run daily. This allows viewing the current payout status up to the current month with a maximum delay of one day.
Scheduled execution is at 00:15, ensuring completeness of the report for the past day while allowing other DWH-loading DAGs, running every 15 minutes, to complete.
Optionally, a sensor can be added to wait for the completion of other DWH-loading DAGs.

According to the requirements, the first data load occurs for the last 7 days - the first extraction sets **last_loaded_ts** in the service table to 7 days before the DAG run date.

Since there are no timezone specifications yet, the DAG has not been calibrated for timezone differences between the API and Airflow. In test mode, some data may arrive slightly outside the "from" and "to" range.