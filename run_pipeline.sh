#!/bin/bash

export PROJECT_ID=${PROJECT_ID}
export BUCKET_NAME=${BUCKET_NAME}
export BQ_DATASET=${BQ_DATASET}
export REGION=${REGION}

PROCESS_DATE=${1:-$(date +%Y-%m-%d)}
CLUSTER_NAME="worldbank-processor-${PROCESS_DATE//-/}"

echo "========================================================"
echo "World Bank Economics Data Pipeline"
echo "========================================================"
echo "Project ID: $PROJECT_ID"
echo "Bucket: $BUCKET_NAME"
echo "Dataset: $BQ_DATASET"
echo "Region: $REGION"
echo "Process Date: $PROCESS_DATE"
echo "========================================================"

echo "========================================================"
echo "STEP 1: Extracting World Bank data"
echo "========================================================"
python worldbank_ingestion.py
if [ $? -ne 0 ]; then
    echo "❌ Data extraction failed. Exiting."
    exit 1
fi
echo "✓ Data extraction completed successfully."

echo "========================================================"
echo "STEP 2: Processing data with PySpark"
echo "========================================================"

PYSPARK_SCRIPT="gs://${BUCKET_NAME}/scripts/worldbank_processor.py"

echo "Uploading PySpark script to GCS..."
gsutil cp worldbank_processor.py ${PYSPARK_SCRIPT}

echo "Creating/checking Dataproc cluster ${CLUSTER_NAME}..."
if ! gcloud dataproc clusters describe ${CLUSTER_NAME} --region=${REGION} --project=${PROJECT_ID} &>/dev/null; then
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region=${REGION} \
        --project=${PROJECT_ID} \
        --single-node \
        --master-machine-type=n1-standard-4 \
        --image-version=2.0-debian10 \
        --max-idle=30m
    
    echo "Waiting for cluster to be ready..."
    sleep 30
fi

echo "Submitting PySpark job to cluster..."
gcloud dataproc jobs submit pyspark ${PYSPARK_SCRIPT} \
    --region=${REGION} \
    --project=${PROJECT_ID} \
    --cluster=${CLUSTER_NAME} \
    --properties=spark.executor.memory=4g \
    -- ${PROCESS_DATE} ${PROJECT_ID} ${BUCKET_NAME}

if [ $? -ne 0 ]; then
    echo "❌ Data processing failed. Exiting."
    exit 1
fi
echo "✓ Data processing completed successfully."

echo "========================================================"
echo "STEP 3: Loading data to BigQuery"
echo "========================================================"
python bigquery_loader.py ${PROCESS_DATE} ${PROJECT_ID} ${BUCKET_NAME} ${BQ_DATASET}
if [ $? -ne 0 ]; then
    echo "❌ BigQuery loading failed. Exiting."
    exit 1
fi
echo "✓ BigQuery loading completed successfully."

echo "========================================================"
echo "✅ ETL Pipeline completed successfully!"
echo "========================================================"
echo "Results available in BigQuery dataset: $BQ_DATASET"
echo "Check out these views for analysis:"
echo "- $BQ_DATASET.country_comparison_view"
echo "- $BQ_DATASET.top_countries_for_grads_view"
echo "- $BQ_DATASET.country_trends_view"
echo ""
echo "To delete Dataproc cluster and save costs:"
echo "gcloud dataproc clusters delete ${CLUSTER_NAME} --region=${REGION} --quiet"