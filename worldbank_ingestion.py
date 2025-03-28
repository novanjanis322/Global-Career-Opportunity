#API Docs: https://datahelpdesk.worldbank.org/knowledgebase/articles/898581-api-basic-call-structure
#API Endpoint: http://api.worldbank.org/v2/country/all/indicator/
import os
from datetime import datetime
import requests
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
TODAY = datetime.today().strftime("%Y-%m-%d")
RAW_DATA_PATH = f"worldbank/raw/{TODAY}"

INDICATORS = {
    "SL.UEM.TOTL.ZS": "unemployment_rate",
    "SL.UEM.1524.ZS": "youth_unemployment",
    "SL.EMP.TOTL.SP.ZS": "employment_ratio",
    "NY.GDP.MKTP.CD": "gdp",
    "NY.GDP.PCAP.CD": "gdp_per_capita",
    "NY.GDP.PCAP.PP.KD": "gdp_per_capita_ppp",
    "BX.KLT.DINV.WD.GD.ZS": "foreign_direct_investment",
    "IC.BUS.EASE.XQ": "ease_of_business",
    "NE.TRD.GNFS.ZS": "trade_percent_gdp",
    "FP.CPI.TOTL.ZG": "inflation",
    "GC.DOD.TOTL.GD.ZS": "government_debt",
    "SE.XPD.TOTL.GD.ZS": "education_spending",
    "SE.TER.ENRR": "school_enrollment"
}

def extract_world_bank_data(indicator_id):
    """Extract data for a specific indicator from World Bank API"""
    url = "http://api.worldbank.org/v2/country/all/indicator/"
    params = {
        "format": "json",
        "per_page": 1000,
        "date": "2012:2025"
    }
    all_data = []
    page = 1

    while True:
        params["page"] = page
        response = requests.get(f"{url}{indicator_id}", params=params)

        if response.status_code != 200:
            print(f"Error fetching data: {response.status_code}")
            break
        
        data = response.json()

        if len(data) < 2 or len(data[1]) == 0:
            break
            
        all_data.extend(data[1])
        page += 1

        print(f"Fetched page {page-1} for indicator {indicator_id}")
        
    return all_data

def transform_to_dataframe(data, indicator_name):
    """Transform API response to a pandas DataFrame with basic structure"""
    records = []
    
    for item in data:
        record = {
            "country_code": item.get("countryiso3code", ""),
            "country_name": item.get("country", {}).get("value", ""),
            "indicator_id": item.get("indicator", {}).get("id", ""),
            "indicator_name": indicator_name,
            "year": item.get("date", ""),
            "value": item.get("value", None),
            "extraction_date": TODAY
        }
        records.append(record)
    
    return pd.DataFrame(records)

def upload_to_gcs(dataframe, indicator_name):
    """Upload the DataFrame as CSV and JSON to Google Cloud Storage."""
    if dataframe.empty:
        print(f"Warning: Empty dataframe for {indicator_name}, skipping upload.")
        return
        
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    
    csv_data = dataframe.to_csv(index=False)
    blob_name = f"{RAW_DATA_PATH}/{indicator_name}.csv"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(csv_data, content_type="text/csv")
    print(f"Uploaded {blob_name} to GCS")
    
    json_data = dataframe.to_json(orient="records")
    json_blob_name = f"{RAW_DATA_PATH}/{indicator_name}.json"
    json_blob = bucket.blob(json_blob_name)
    json_blob.upload_from_string(json_data, content_type="application/json")
    print(f"Uploaded {json_blob_name} to GCS")

def main():
    """Main function to extract and upload all indicators."""
    if not PROJECT_ID:
        raise ValueError("PROJECT_ID environment variable is not set")
    if not BUCKET_NAME:
        raise ValueError("BUCKET_NAME environment variable is not set")
        
    print(f"Starting World Bank data extraction to bucket: {BUCKET_NAME}")
    print(f"Data will be stored at path: {RAW_DATA_PATH}")
    
    for indicator_id, indicator_name in INDICATORS.items():
        print(f"Processing: {indicator_name} ({indicator_id})")
        
        raw_data = extract_world_bank_data(indicator_id)
        
        if not raw_data:
            print(f"No data found for {indicator_name}, skipping")
            continue
            
        df = transform_to_dataframe(raw_data, indicator_name)
        
        upload_to_gcs(df, indicator_name)
        
        print(f"Completed {indicator_name}\n")
    
    print("✅ Data extraction and upload complete!")

if __name__ == "__main__":
    main()