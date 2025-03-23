# Global Career Opportunity

## Overview

The Global Career Opportunity project is a comprehensive data engineering pipeline that analyzes economic data from the World Bank to help individuals identify the most suitable countries for their career aspirations. As a fresh graduate, I created this project to assist myself and others in making data-driven decisions about where to live and work based on key economic indicators.

This project extracts, transforms, and loads data from the World Bank API, processes it using Google Cloud Platform services, and creates analytical views that rank countries based on their opportunity index - a composite score calculated from factors such as employment rates, economic stability, education levels, and business environment.

## Motivation

In today's globalized world, career opportunities span across countries, and making informed decisions about where to live and work can be challenging. This project aims to:

- Provide objective economic data to help people evaluate different countries
- Compare employment prospects, economic stability, and quality of life metrics
- Create a standardized "opportunity index" for straightforward comparison
- Analyze trends over time to identify improving or declining economies

As someone looking to start my own career journey, I wanted to create a tool that could provide insights not just for myself, but for anyone considering international opportunities.

## Features

- **Comprehensive Data Collection**: Extracts 13 key economic indicators from the World Bank API
- **Cloud-Based ETL Pipeline**: Utilizes GCP services (GCS, Dataproc, BigQuery)
- **Data Transformation**: Uses PySpark for efficient data processing
- **Dimensional Data Model**: Implements a star schema for analytics
- **Opportunity Index**: Custom-calculated metric combining employment, economic, and educational factors
- **Analytical Views**: Ready-to-use views for country comparison and trend analysis

## Technology Stack

- **Google Cloud Storage (GCS)**: Data lake for raw and processed data
- **Google Dataproc**: Managed Spark service for data processing
- **Apache PySpark**: Data transformation engine
- **Google BigQuery**: Data warehouse for analysis
- **Python**: Primary programming language
- **Bash**: Orchestration scripts

## Data Model

The project implements a dimensional model with:

### Dimension Tables
- `dim_countries`: Country details
- `dim_time`: Time dimension with year attributes

### Fact Tables
- `fact_employment_metrics`: Unemployment and employment ratios
- `fact_economic_indicators`: GDP, inflation, and other economic indicators
- `fact_education`: Education spending and enrollment data

### Analytics Tables
- `opportunity_index`: Derived composite score and country rankings

## Key Economic Indicators

The analysis includes these key indicators from the World Bank:

| Indicator | Description | Impact on Opportunity |
|-----------|-------------|----------------------|
| Unemployment Rate | % of labor force without work | Lower is better |
| Youth Unemployment | % of youth without work | Lower is better |
| GDP per Capita | Economic output per person | Higher is better |
| Inflation | Annual % change in prices | Lower is better |
| Government Debt | % of GDP | Lower is better |
| Education Enrollment | Tertiary education enrollment rate | Higher is better |
| Ease of Business | Regulatory environment ranking | Lower is better |

## Setup Instructions

### Prerequisites
- Google Cloud Platform account
- Python 3.7+
- `gcloud` CLI installed and configured

### Environment Setup

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/Global-Career-Opportunity.git
   cd Global-Career-Opportunity
   ```

2. Install required Python packages:
   ```
   pip install -r requirements.txt
   ```

3. Copy the example environment file and fill in your GCP details:
   ```
   cp .env.example .env
   # Edit .env with your GCP project ID, bucket name, etc.
   ```

### Running the Pipeline

Execute the full ETL pipeline:
```
./run_pipeline.sh
```

Or run individual steps:
```
# Extract data from World Bank API
python world_bank_extractor.py

# Process data with PySpark
./run_dataproc_job.sh

# Load data to BigQuery
python bigquery_loader.py
```

## Analysis & Insights

After running the pipeline, you can analyze the data using BigQuery or connect to visualization tools like Looker Studio. The project creates these analytical views:

- `country_comparison_view`: Compare all metrics across countries
- `top_countries_for_grads_view`: Shows the top 30 countries ranked by opportunity index
- `country_trends_view`: Analyzes trends over time for key countries

## Future Enhancements

With the current state of the project, i am currently working for further improvements, including:
- Add scheduling via Cloud Composer or Cloud Scheduler
- Create interactive web dashboard for exploration

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.