# TEAM 3 - MSBA 405 Final Project README 🚀
## NYC Taxi Demand Pipeline

---

## 1. Project Overview 📌

This project builds an end-to-end data pipeline that integrates multiple New York City public datasets, including taxi trip data, 311 service request data, weather data, and geographic data. The goal is to transform raw datasets into a unified, analysis-ready dataset for visualization and insights.

The final outputs include processed tables, a master dataset, and a Tableau dashboard that presents taxi demand patterns, borough-level trends, weather conditions, 311 activity, and geographic distributions across NYC.

The project uses cloud-based tools and data engineering workflows, including BigQuery, Google Cloud Storage (GCS), Dataproc, PySpark, Snowflake, SQL, and Tableau.

---

## 2. Project Goal 🎯

The goal of this project is to analyze how taxi demand varies across New York City and identify key factors that may influence these patterns, including weather conditions and 311 complaint activity. Multiple public datasets are integrated into a unified data pipeline to support efficient analysis and clear visualization.

The project aims to provide actionable insights for city planners and transportation agencies, enabling better resource allocation, improved transportation planning, and a more efficient travel experience across the city.


---

## 3. Data Sources 📂

This project uses publicly available datasets from official sources.

### 3.1 NYC Taxi Trip Data  
**Source:** NYC Taxi and Limousine Commission (TLC)  
**Link:** https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page  

This dataset provides raw taxi trip records used to construct hourly taxi activity metrics, including trip counts, trip duration, distance, and fare-related variables.

---

### 3.2 NYC 311 Service Request Data  
**Source:** NYC Open Data  
**Link:** https://opendata.cityofnewyork.us  

This dataset contains 311 complaint and service request records, which are used to construct hourly and borough-level complaint measures.

---

### 3.3 Weather Data  
**Source:** NOAA National Centers for Environmental Information (NCEI), Integrated Surface Database  
**Link:** https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database  

This dataset provides hourly weather information such as temperature, precipitation, wind, and other variables used to analyze the relationship between weather conditions and taxi demand.

---

### 3.4 Geographic Data  
**Source:** Taxi Zone Shapefile  
**Link:** https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip  

In this repository, the taxi zone shapefile components (`.shp`, `.shx`, `.dbf`, and related files) are included to support Tableau geographic mapping and zone-level spatial analysis. These files are used to draw taxi zone boundaries and connect processed taxi activity data to map-based visualizations.

---

## 4. Repository Structure 🗂️

This repository contains the scripts, SQL files, notebooks, and documentation used in the project.

### Main pipeline files
- `pipeline.sh`  Early ingestion pipeline script used to download raw source data.

- `run_nyc_pipeline_final.sh`  Main team cloud pipeline for weather, 311, taxi, borough bridge, and master table creation.

- `run_full_pipeline_gcs_snowflake.sh`  Extended pipeline that links the cloud pipeline, taxi zone processing, Snowflake loading, and validation.

### Main Snowflake SQL files
- `data import to snowflake.sql`  Imports the borough-level processed output into Snowflake and performs the main transformation and cleaning steps.

- `data connect to tableau.sql`  Creates a Tableau-ready borough-level view.

- `(location) data connect to tableau.sql`  Creates a Tableau-ready zone-level view for detailed location analysis.

### Main notebooks
- `time_series.ipynb`  Notebook for time series analysis.

- `data_processed.ipynb`  Notebook used for processed data inspection and validation.

### Main processing scripts in `scripts/`
- `311_hourly_borough_final.py`  Aggregates 311 complaint data to hourly borough-level totals.

- `clean_taxi_zone_pyspark.py`  Cleans and aggregates taxi trip data into hourly zone-level outputs.

- `etl_taxi_hourly_zone_final.py`  Processes monthly taxi parquet data into hourly zone-level aggregates.

- `etl_taxi_hourly_borough_final.py`  Maps taxi zones to boroughs for borough-level integration.

- `build_master_table_v2_final.py`  Joins weather, taxi, and 311 data into the final master table.

- `download_311_2023_2025.sh`  Downloads 311 data.

- `download_taxi_2023_2025.sh`  Downloads taxi data.

- `download_weather_2023_2025.sh` / `download_weather_2023_2025.py`  Downloads weather data.

- `download_zone_lookup.sh`  Downloads taxi zone lookup data.
  
- `taxi_zones.shp` and related shapefile components  Geographic boundary files used for Tableau map-based zone visualization.

---

## 5. Pipeline Workflow ⚙️

The pipeline is built across multiple cloud tools:

- **Google Cloud Storage (GCS):** stores raw and processed outputs  
- **BigQuery:** used for weather reprocessing  
- **Dataproc + PySpark:** used for 311 aggregation, taxi processing, borough mapping, and final table generation  
- **Snowflake:** used for loading processed outputs, further transformation, and Tableau-ready views  
- **Tableau:** used for dashboard visualization  

The project produces two major analytical outputs:

- a **borough-level master dataset** integrating taxi, weather, and 311 data  
- a **zone-level taxi dataset** for more detailed geographic and spatial analysis  

---

## 6. How to Run the Pipeline ▶️

This project can be run in two stages:

1. Run the Google Cloud / GCS pipeline  
2. Run the Snowflake SQL workflow and connect the outputs to Tableau  

This two-stage process is the most stable and recommended workflow.

### 6.1 Google Cloud Setup

Before running the pipeline, make sure you have:

- access to a Google Cloud project  
- access to Google Cloud Storage  
- access to Dataproc and BigQuery  
- `gcloud` and `bq` installed, or access to Google Cloud Shell  

Our project bucket is:

```bash
gs://msba405-nyc-project/raw/
gs://msba405-nyc-project/code/
gs://msba405-nyc-project/processed/
gs://msba405-nyc-project/Processed/
gs://msba405-nyc-project/processed/taxi_hourly_zone/nyc_hourly_zone_final/
gs://msba405-nyc-project/Processed/master_hourly_borough_v2/
```
Note: path capitalization matters because both processed/ and Processed/ are used in this project.

### 6.2 Recommended Pipeline Execution

The recommended way to run the project is:

```bash
bash run_full_pipeline_gcs_snowflake.sh
```
This script is the full workflow entry point. It links the cloud pipeline and the Snowflake loading process, so the project can be executed in a more complete end-to-end sequence.

The script performs the following tasks:
	1.	downloads required support files from GCS
	2.	runs the main cloud pipeline
	3.	runs the taxi zone-level parquet generation step
	4.	runs Snowflake SQL loading scripts
	5.	performs validation queries

The main team cloud pipeline is handled by:

```bash
bash run_nyc_pipeline_final.sh
```
This script is the core Google Cloud pipeline and is responsible for:
	1.	reprocessing weather data in BigQuery
	2.	exporting weather data to GCS parquet
	3.	aggregating 311 complaint data in Dataproc
	4.	processing taxi trip data into hourly outputs
	5.	mapping taxi zones to boroughs
	6.	building the final borough-level master table

In other words, run_full_pipeline_gcs_snowflake.sh uses run_nyc_pipeline_final.sh as the main cloud-processing component and then continues with Snowflake loading and validation.

This workflow assumes:
	•	gcloud is configured
	•	Snowflake CLI is installed
	•	a Snowflake connection named main already exists

### 6.3 Validation

After running the full pipeline and Snowflake loading steps, the outputs can be validated with:

```bash
snow sql -c main -q "SELECT COUNT(*) FROM SNOWFLAKE_LEARNING_DB.PUBLIC.NYC_HOURLY_ZONE_FINAL_GCS;"
snow sql -c main -q "SELECT MIN(hour_ts), MAX(hour_ts) FROM SNOWFLAKE_LEARNING_DB.PUBLIC.NYC_HOURLY_ZONE_FINAL_GCS;"
```
In our project environment, the zone-level table covered the time range from 2023-01-01 00:00:00 to 2025-11-30 23:00:00.

---

## 7. Snowflake ☁️

Snowflake was used to ingest, transform, and clean processed data before connecting final outputs to Tableau. In addition, time series analysis was performed in Python to capture temporal patterns in taxi demand.

### 7.1 Account Setup

Create a Snowflake account at: https://www.snowflake.com/  

Then create your own warehouse, database, and schema for the project.

---

### 7.2 Data Import to Snowflake 
We used [data import to snowflake.sql](./data%20import%20to%20snowflake.sql) connected Snowflake to Google Cloud Storage (GCS) using an external stage and Parquet file format:


- **Final Data Cleaning and Output Table**  
  We removed duplicated columns and filtered out invalid records to ensure a clean and consistent dataset for downstream analysis and visualization.  
  **Output:** `nyc_hourly_borough_final`

---

### 7.3 Time Series Analysis

We used [time_series.ipynb](./time_series.ipynb) to analyze trends and patterns in taxi demand over time.

Main tasks include:
- Time-based aggregation  
- Trend and seasonality analysis  
- Visualization of temporal patterns  

---

### 7.4 Tableau-Ready View

We use [data connect to tableau.sql](./data%20connect%20to%20tableau.sql).

This script creates a Tableau-ready view by standardizing data types and selecting the fields required for visualization.

---

### 7.5 Location-Level Tableau View

We use [(location) data connect to tableau.sql](./(location)%20data%20connect%20to%20tableau.sql).

This script creates a more detailed dataset for spatial analysis, preparing location-based features and ensuring compatibility with Tableau.

---

## 8. Tableau Dashboard 🖥️

The final Tableau dashboard presents key findings through clear and intuitive visualizations. It highlights taxi demand patterns across NYC, borough-level differences, the impact of weather conditions, and the relationship between 311 complaints and demand. Map-based visualizations enhance geographic insights.

### Dashboard Links

- **Main Dashboard**  
  **Link:** https://public.tableau.com/views/maindashboard_17740469250420/Dashboard1?:language=en-US&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link  

- **Weather Impact Dashboard**  
  **Link:** https://public.tableau.com/views/WeatherImpactDashboard/finaldashboardforweatherimpact?:language=en-US&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link  

- **311 Complaint Dashboard**  
  **Link:** https://public.tableau.com/views/311impactdashboard1/Dashboard?:language=en-US&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link  
