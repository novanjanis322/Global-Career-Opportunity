#!/usr/bin/env python


import os
import sys
from datetime import datetime
from google.cloud import bigquery
from dotenv import load_dotenv
load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
BQ_DATASET = os.getenv("BQ_DATASET", "worldbank_economics")
PROCESS_DATE = datetime.today().strftime("%Y-%m-%d")

if len(sys.argv) > 1:
    PROCESS_DATE = sys.argv[1]

TRANSFORMED_PATH = f"gs://{BUCKET_NAME}/worldbank/transformed/{PROCESS_DATE}"

client = bigquery.Client(project=PROJECT_ID)

def create_dataset_if_not_exists():
    """Create the BigQuery dataset if it doesn't exist"""
    print(f"Ensuring dataset {BQ_DATASET} exists...")
    
    dataset_id = f"{PROJECT_ID}.{BQ_DATASET}"
    
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"Created dataset {dataset_id}")

def load_parquet_to_bigquery(source_uri, table_name, schema=None, write_disposition='WRITE_TRUNCATE'):
    """Load parquet data from GCS to BigQuery"""
    print(f"Loading {source_uri} to {table_name}...")
    
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition
    )
    
    if schema:
        job_config.schema = schema
    
    load_job = client.load_table_from_uri(
        source_uri,
        table_id,
        job_config=job_config
    )
    
    load_job.result()
    
    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows to {table_id}")

def create_country_comparison_view():
    """Create a view for country comparison"""
    print("Creating country comparison view...")
    
    view_id = f"{PROJECT_ID}.{BQ_DATASET}.country_comparison_view"
    
    view_query = f"""
    SELECT 
        c.country_name,
        c.country_code,
        t.year,
        e.unemployment_rate,
        e.youth_unemployment,
        e.employment_ratio,
        ec.gdp,
        ec.gdp_per_capita,
        ec.gdp_per_capita_ppp,
        ec.inflation,
        ec.government_debt,
        ec.foreign_direct_investment,
        ec.trade_percent_gdp,
        edu.education_spending,
        edu.school_enrollment,
        oi.opportunity_index,
        oi.opportunity_rank
    FROM 
        `{PROJECT_ID}.{BQ_DATASET}.dim_countries` c
    JOIN 
        `{PROJECT_ID}.{BQ_DATASET}.dim_time` t ON 1=1
    LEFT JOIN 
        `{PROJECT_ID}.{BQ_DATASET}.fact_employment_metrics` e 
        ON c.country_code = e.country_code AND t.year = e.year
    LEFT JOIN 
        `{PROJECT_ID}.{BQ_DATASET}.fact_economic_indicators` ec 
        ON c.country_code = ec.country_code AND t.year = ec.year
    LEFT JOIN 
        `{PROJECT_ID}.{BQ_DATASET}.fact_education` edu 
        ON c.country_code = edu.country_code AND t.year = edu.year
    LEFT JOIN 
        `{PROJECT_ID}.{BQ_DATASET}.opportunity_index` oi 
        ON c.country_code = oi.country_code AND t.year = oi.year
    WHERE 
        t.year >= 2018
    """
    
    view = bigquery.Table(view_id)
    view.view_query = view_query
    
    try:
        client.delete_table(view_id)
        print(f"Deleted existing view {view_id}")
    except Exception:
        pass
    
    client.create_table(view)
    print(f"Created view {view_id}")

def create_top_countries_view():
    """Create a view for top countries for job opportunities"""
    print("Creating top countries view...")
    
    view_id = f"{PROJECT_ID}.{BQ_DATASET}.top_countries_for_grads_view"
    
    view_query = f"""
    SELECT 
        c.country_name,
        oi.year,
        oi.unemployment_rate,
        oi.youth_unemployment,
        oi.employment_ratio,
        oi.gdp_per_capita,
        oi.gdp_per_capita_ppp,
        oi.inflation,
        oi.government_debt,
        oi.school_enrollment,
        oi.education_spending,
        oi.opportunity_index,
        oi.opportunity_rank
    FROM 
        `{PROJECT_ID}.{BQ_DATASET}.opportunity_index` oi
    JOIN 
        `{PROJECT_ID}.{BQ_DATASET}.dim_countries` c ON oi.country_code = c.country_code
    WHERE 
        oi.year = (SELECT MAX(year) FROM `{PROJECT_ID}.{BQ_DATASET}.opportunity_index`)
        AND oi.opportunity_rank <= 30
    ORDER BY 
        oi.opportunity_rank ASC
    """
    
    view = bigquery.Table(view_id)
    view.view_query = view_query
    
    try:
        client.delete_table(view_id)
        print(f"Deleted existing view {view_id}")
    except Exception:
        pass
    
    client.create_table(view)
    print(f"Created view {view_id}")

def create_country_trends_view():
    """Create a view for analyzing trends over time"""
    print("Creating country trends view...")
    
    view_id = f"{PROJECT_ID}.{BQ_DATASET}.country_trends_view"
    
    view_query = f"""
    SELECT 
        c.country_name,
        t.year,
        t.decade,
        e.unemployment_rate,
        e.youth_unemployment,
        ec.gdp_per_capita,
        ec.inflation,
        edu.school_enrollment,
        oi.opportunity_rank
    FROM 
        `{PROJECT_ID}.{BQ_DATASET}.dim_countries` c
    CROSS JOIN 
        `{PROJECT_ID}.{BQ_DATASET}.dim_time` t
    LEFT JOIN 
        `{PROJECT_ID}.{BQ_DATASET}.fact_employment_metrics` e 
        ON c.country_code = e.country_code AND t.year = e.year
    LEFT JOIN 
        `{PROJECT_ID}.{BQ_DATASET}.fact_economic_indicators` ec 
        ON c.country_code = ec.country_code AND t.year = ec.year
    LEFT JOIN 
        `{PROJECT_ID}.{BQ_DATASET}.fact_education` edu 
        ON c.country_code = edu.country_code AND t.year = edu.year
    LEFT JOIN 
        `{PROJECT_ID}.{BQ_DATASET}.opportunity_index` oi 
        ON c.country_code = oi.country_code AND t.year = oi.year
    WHERE 
        t.year >= 2010
        AND c.country_code IN (
            -- Include top countries and Indonesia for comparison
            SELECT country_code FROM `{PROJECT_ID}.{BQ_DATASET}.opportunity_index`
            WHERE year = (SELECT MAX(year) FROM `{PROJECT_ID}.{BQ_DATASET}.opportunity_index`)
            AND opportunity_rank <= 15
            UNION ALL
            SELECT country_code FROM `{PROJECT_ID}.{BQ_DATASET}.dim_countries`
            WHERE country_name = 'Indonesia'
        )
    ORDER BY
        c.country_name, t.year
    """
    
    view = bigquery.Table(view_id)
    view.view_query = view_query
    
    try:
        client.delete_table(view_id)
        print(f"Deleted existing view {view_id}")
    except Exception:
        pass
    
    client.create_table(view)
    print(f"Created view {view_id}")

def main():
    """Main function to load all data into BigQuery"""
    print(f"Starting BigQuery loading process for data from {PROCESS_DATE}...")
    
    create_dataset_if_not_exists()
    
    load_parquet_to_bigquery(
        f"{TRANSFORMED_PATH}/dim_countries/*", 
        "dim_countries"
    )
    
    load_parquet_to_bigquery(
        f"{TRANSFORMED_PATH}/dim_time/*", 
        "dim_time"
    )
    
    load_parquet_to_bigquery(
        f"{TRANSFORMED_PATH}/fact_employment_metrics/*", 
        "fact_employment_metrics"
    )
    
    load_parquet_to_bigquery(
        f"{TRANSFORMED_PATH}/fact_economic_indicators/*", 
        "fact_economic_indicators"
    )
    
    load_parquet_to_bigquery(
        f"{TRANSFORMED_PATH}/fact_education/*", 
        "fact_education"
    )
    
    load_parquet_to_bigquery(
        f"{TRANSFORMED_PATH}/opportunity_index/*", 
        "opportunity_index"
    )
    
    create_country_comparison_view()
    create_top_countries_view()
    create_country_trends_view()
    
    print("✅ BigQuery loading complete!")
    print(f"You can now query these views for analysis:")
    print(f"- {PROJECT_ID}.{BQ_DATASET}.country_comparison_view")
    print(f"- {PROJECT_ID}.{BQ_DATASET}.top_countries_for_grads_view")
    print(f"- {PROJECT_ID}.{BQ_DATASET}.country_trends_view")

if __name__ == "__main__":
    main()