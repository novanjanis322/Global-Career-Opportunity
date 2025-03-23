#!/bin/bash

if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
else
    echo "❌ .env file not found! Exiting..."
    exit 1
fi

echo "✅ Environment variables loaded."
echo "PROJECT_ID: $PROJECT_ID"
echo "BUCKET_NAME: $BUCKET_NAME"
echo "REGION: $REGION"
PROCESS_DATE=${1:-$(date +%Y-%m-%d)}
CLUSTER_NAME="worldbank-processor-${PROCESS_DATE//-/}"

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

echo "Job submitted!"


echo "Job submitted! The cluster will automatically delete after 30 minutes of inactivity."
echo "To manually delete the cluster, run:"
echo "gcloud dataproc clusters delete ${CLUSTER_NAME} --region=${REGION} --quiet"
