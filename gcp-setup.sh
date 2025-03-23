#!/bin/bash
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
else
    echo "❌ .env file not found! Exiting..."
    exit 1
fi
ECHO "✅ Environment variables loaded."
ECHO "PROJECT_ID: $PROJECT_ID"
ECHO "BUCKET_NAME: $BUCKET_NAME"
ECHO "REGION: $REGION"
ECHO "DATASET: $BQ_DATASET"
gcloud config set project $PROJECT_ID

echo "Enabling required APIs..."
gcloud services enable storage-api.googleapis.com dataproc.googleapis.com bigquery.googleapis.com

echo "Creating GCS bucket..."
gsutil mb -l $REGION gs://$BUCKET_NAME

echo "Creating folder structure..."
touch empty.txt
gsutil cp empty.txt gs://$BUCKET_NAME/worldbank/raw/
gsutil cp empty.txt gs://$BUCKET_NAME/worldbank/processed/
gsutil cp empty.txt gs://$BUCKET_NAME/worldbank/transformed/
gsutil cp empty.txt gs://$BUCKET_NAME/logs/extraction_logs/
gsutil cp empty.txt gs://$BUCKET_NAME/logs/processing_logs/
rm empty.txt

cat > lifecycle.json << EOF
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {
          "age": 90,
          "matchesPrefix": ["worldbank/raw/"]
        }
      }
    ]
  }
}
EOF
gsutil lifecycle set lifecycle.json gs://$BUCKET_NAME
rm lifecycle.json

echo "Creating BigQuery dataset..."
bq --location=$REGION mk --dataset $PROJECT_ID:$BQ_DATASET

echo "Creating service account for data pipeline..."
SA_NAME="worldbank-pipeline-sa"
SA_EMAIL="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"

gcloud iam service-accounts create $SA_NAME \
    --display-name="World Bank Data Pipeline Service Account"

echo "Granting permissions to service account..."
gsutil iam ch serviceAccount:$SA_EMAIL:roles/storage.admin gs://$BUCKET_NAME

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/dataproc.adminis"

echo "Setup complete!"
echo "Next steps:"
echo "1. Create a key for the service account if running locally:"
echo "   gcloud iam service-accounts keys create key.json --iam-account=$SA_EMAIL"
echo "2. Set environment variables:"
echo "   source .env"
echo "3. Run the World Bank data extraction script"