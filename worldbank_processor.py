#!/usr/bin/env python

import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.window import Window

PROCESS_DATE = datetime.today().strftime("%Y-%m-%d")
if len(sys.argv) > 1:
    PROCESS_DATE = sys.argv[1]

# Get project and bucket from command line
if len(sys.argv) > 3:
    PROJECT_ID = sys.argv[2]
    BUCKET_NAME = sys.argv[3]
else:
    print("ERROR: Missing required arguments")
    print("Usage: script.py [date] [project_id] [bucket_name]")
    sys.exit(1)

print(f"Using PROJECT_ID: {PROJECT_ID}")
print(f"Using BUCKET_NAME: {BUCKET_NAME}")
print(f"Using PROCESS_DATE: {PROCESS_DATE}")

RAW_DATA_PATH = f"gs://{BUCKET_NAME}/worldbank/raw/{PROCESS_DATE}"
PROCESSED_DATA_PATH = f"gs://{BUCKET_NAME}/worldbank/processed/{PROCESS_DATE}"
TRANSFORMED_PATH = f"gs://{BUCKET_NAME}/worldbank/transformed/{PROCESS_DATE}"

INDICATORS = [
    "unemployment_rate",       
    "youth_unemployment",      
    "employment_ratio",        
    "gdp",                     
    "gdp_per_capita",          
    "gdp_per_capita_ppp",      
    "foreign_direct_investment", 
    "ease_of_business",        
    "trade_percent_gdp",       
    "inflation",               
    "government_debt",         
    "education_spending",      
    "school_enrollment"        
]

def create_spark_session():
    """Create a Spark session"""
    return (SparkSession.builder
            .appName("WorldBankDataProcessor")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .getOrCreate())

def process_indicator_data(spark, indicator_name):
    """Process a single indicator dataset"""
    print(f"Processing {indicator_name}...")
    
    
    input_path = f"{RAW_DATA_PATH}/{indicator_name}.csv"
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    cleaned_df = (df
        
        .withColumn("year", F.col("year").cast(IntegerType()))
        .withColumn("value", F.col("value").cast(DoubleType()))
        
        .filter(F.col("value").isNotNull())
        
        .filter(F.col("country_code").isNotNull() & (F.col("country_code") != ""))
        
        .withColumn("process_date", F.lit(PROCESS_DATE))
    )
    
    output_path = f"{PROCESSED_DATA_PATH}/{indicator_name}"
    cleaned_df.write.mode("overwrite").parquet(output_path)
    
    return cleaned_df

def create_country_dimension(spark):
    """Create country dimension table from processed data"""
    print("Creating country dimension table...")
    
    countries_df = spark.read.parquet(f"{PROCESSED_DATA_PATH}/gdp")
    
    dim_countries = (countries_df
        .select("country_code", "country_name")
        .distinct()
        
        .withColumn("region", F.lit(None).cast(StringType()))
        .withColumn("income_group", F.lit(None).cast(StringType()))
    )
    
    dim_countries.write.mode("overwrite").parquet(f"{TRANSFORMED_PATH}/dim_countries")
    
    return dim_countries

def create_time_dimension(spark):
    """Create time dimension table"""
    print("Creating time dimension table...")
    
    years_df = spark.read.parquet(f"{PROCESSED_DATA_PATH}/gdp")
    
    years = years_df.select("year").distinct().collect()
    years_list = [row.year for row in years]
    
    data = [(year, 
             f"Y{year}", 
             year // 10 * 10, 
             f"{year}-01-01") for year in years_list]
    
    dim_time = spark.createDataFrame(data, 
        ["year", "year_label", "decade", "year_start_date"])
    
    dim_time.write.mode("overwrite").parquet(f"{TRANSFORMED_PATH}/dim_time")
    
    return dim_time

def create_employment_metrics_fact(spark):
    """Create employment metrics fact table"""
    print("Creating employment metrics fact table...")
    
    employment_indicators = [
        "unemployment_rate", 
        "youth_unemployment", 
        "employment_ratio"
    ]
    
    first_indicator = employment_indicators[0]
    fact_df = (spark.read.parquet(f"{PROCESSED_DATA_PATH}/{first_indicator}")
               .select("country_code", "year", "value")
               .withColumnRenamed("value", first_indicator))
    
    for indicator in employment_indicators[1:]:
        indicator_df = (spark.read.parquet(f"{PROCESSED_DATA_PATH}/{indicator}")
                       .select("country_code", "year", "value")
                       .withColumnRenamed("value", indicator))
        
        fact_df = fact_df.join(
            indicator_df, 
            on=["country_code", "year"], 
            how="outer"
        )
    
    fact_df.write.mode("overwrite").parquet(f"{TRANSFORMED_PATH}/fact_employment_metrics")
    
    return fact_df

def create_economic_indicators_fact(spark):
    """Create economic indicators fact table"""
    print("Creating economic indicators fact table...")
      
    economic_indicators = [
        "gdp", 
        "gdp_per_capita", 
        "gdp_per_capita_ppp", 
        "inflation", 
        "foreign_direct_investment",
        "trade_percent_gdp",
        "government_debt",
        "ease_of_business"
    ]
    
    first_indicator = economic_indicators[0]
    fact_df = (spark.read.parquet(f"{PROCESSED_DATA_PATH}/{first_indicator}")
               .select("country_code", "year", "value")
               .withColumnRenamed("value", first_indicator))
    
    for indicator in economic_indicators[1:]:
        try:
            indicator_df = (spark.read.parquet(f"{PROCESSED_DATA_PATH}/{indicator}")
                           .select("country_code", "year", "value")
                           .withColumnRenamed("value", indicator))
            
            fact_df = fact_df.join(
                indicator_df, 
                on=["country_code", "year"], 
                how="outer"
            )
        except:
            print(f"Warning: Could not process {indicator}, skipping")
    
    fact_df.write.mode("overwrite").parquet(f"{TRANSFORMED_PATH}/fact_economic_indicators")
    
    return fact_df

def create_education_fact(spark):
    """Create education fact table"""
    print("Creating education fact table...")
    
    education_indicators = [
        "education_spending",
        "school_enrollment"
    ]
    
    fact_df = None
    
    for indicator in education_indicators:
        try:
            indicator_path = f"{PROCESSED_DATA_PATH}/{indicator}"
            indicator_df = (spark.read.parquet(indicator_path)
                          .select("country_code", "year", "value")
                          .withColumnRenamed("value", indicator))
            
            if fact_df is None:
                fact_df = indicator_df
            else:
                fact_df = fact_df.join(
                    indicator_df, 
                    on=["country_code", "year"], 
                    how="outer"
                )
        except Exception as e:
            print(f"Warning: Could not process {indicator}, skipping. Error: {str(e)}")
    
    if fact_df is None:
        print("Warning: Could not create education fact table, creating empty table")
        fact_df = spark.createDataFrame(
            [], 
            "country_code STRING, year INT"
        )
    
    fact_df.write.mode("overwrite").parquet(f"{TRANSFORMED_PATH}/fact_education")
    
    return fact_df

def create_opportunity_index(spark):
    """Create an opportunity index for fresh graduates (combining multiple factors)"""
    print("Creating opportunity index...")
      
    try:
        employment_df = spark.read.parquet(f"{TRANSFORMED_PATH}/fact_employment_metrics")
        economic_df = spark.read.parquet(f"{TRANSFORMED_PATH}/fact_economic_indicators")
        education_df = spark.read.parquet(f"{TRANSFORMED_PATH}/fact_education")
        
        opportunity_df = (employment_df
            .join(economic_df, on=["country_code", "year"], how="inner")
            .join(education_df, on=["country_code", "year"], how="left")
        )
        
        
        opportunity_df = (opportunity_df
            
            .withColumn("emp_score", 
                        F.when(F.col("unemployment_rate").isNull(), 50)
                         .otherwise(100 - F.least(F.col("unemployment_rate"), F.lit(100))))
            
            .withColumn("gdp_score", 
                        F.when(F.col("gdp_per_capita").isNull(), 50)
                         .when(F.col("gdp_per_capita") > 50000, 100)
                         .when(F.col("gdp_per_capita") > 30000, 90)
                         .when(F.col("gdp_per_capita") > 20000, 80)
                         .when(F.col("gdp_per_capita") > 10000, 70)
                         .when(F.col("gdp_per_capita") > 5000, 60)
                         .otherwise(50))
            
            .withColumn("edu_score",
                        F.when(F.col("school_enrollment").isNull(), 50)
                         .when(F.col("school_enrollment") > 80, 100)
                         .when(F.col("school_enrollment") > 60, 80)
                         .when(F.col("school_enrollment") > 40, 60)
                         .when(F.col("school_enrollment") > 20, 40)
                         .otherwise(20))
            
            .withColumn("stability_score",
                        F.when(F.col("inflation").isNull() | F.col("government_debt").isNull(), 50)
                         .otherwise(
                             100 - F.least(F.col("inflation") * 2, F.lit(50)) -
                             F.least(F.col("government_debt") / 2, F.lit(50))
                         ))
            
            .withColumn("business_score",
                        F.when(F.col("ease_of_business").isNull(), 50)
                         .otherwise(100 - F.col("ease_of_business")))
        )
        
        opportunity_df = opportunity_df.withColumn("opportunity_index",
            (F.col("emp_score") * 0.45 +
             F.col("gdp_score") * 0.20 +
             F.col("edu_score") * 0.10 +
             F.col("stability_score") * 0.15 +
             F.col("business_score") * 0.1)
            .cast(DoubleType()))
        
        window_spec = Window.partitionBy("year").orderBy(F.desc("opportunity_index"))
        opportunity_df = opportunity_df.withColumn("opportunity_rank", F.rank().over(window_spec))
        
        opportunity_df = opportunity_df.select(
            "country_code", "year", 
            "unemployment_rate", "youth_unemployment", "employment_ratio",
            "gdp_per_capita", "gdp_per_capita_ppp", "inflation", "government_debt",
            "school_enrollment", "education_spending",
            "opportunity_index", "opportunity_rank"
        )
        
        opportunity_df.write.mode("overwrite").parquet(f"{TRANSFORMED_PATH}/opportunity_index")
        
        return opportunity_df
        
    except Exception as e:
        print(f"Error creating opportunity index: {str(e)}")
        return None

def main():
    """Main ETL process"""
    spark = create_spark_session()
    
    for indicator in INDICATORS:
        try:
            process_indicator_data(spark, indicator)
        except Exception as e:
            print(f"Error processing {indicator}: {str(e)}")
    
    create_country_dimension(spark)
    create_time_dimension(spark)
    create_employment_metrics_fact(spark)
    create_economic_indicators_fact(spark)
    create_education_fact(spark)
    
    create_opportunity_index(spark)
    
    print("✅ Data processing complete!")

if __name__ == "__main__":
    main()