#!/bin/bash

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
set -u  # Treat unset variables as an error
: "${INFLUXDB_V2_URL:?Environment variable INFLUXDB_V2_URL must be set and non-empty}"
: "${INFLUXDB_V2_ORG:?Environment variable INFLUXDB_V2_ORG must be set and non-empty}"
: "${INFLUXDB_V2_TOKEN:?Environment variable INFLUXDB_V2_TOKEN must be set and non-empty}"
: "${INFLUXDB_V2_BUCKET:?Environment variable INFLUXDB_V2_BUCKET must be set and non-empty}"
# Configuration
ID=0
GRAFANA_URL="http://localhost:3000"
ADMIN_USER="admin"
ADMIN_PASS="admin"
DASHBOARD_FILE="dashboard/influx_v2_dashboard.json"
SERVICE_ACCOUNT_NAME="influx-sa-${ID}"
SERVICE_ACCOUNT_TOKEN_NAME="sa-token-${ID}"
NEW_DATASOURCE_NAME="influx_v2-${ID}"
# Create service account
echo "Creating service account..."
SA_RESPONSE=$(curl -s -X POST ${GRAFANA_URL}/api/serviceaccounts \
  -u "${ADMIN_USER}:${ADMIN_PASS}" \
  -H "Content-Type: application/json" \
  -d '{"name":"'${SERVICE_ACCOUNT_NAME}'","role":"Admin","isDisabled":false}')
SA_ID=$(echo "$SA_RESPONSE" | jq -r '.id')
if [ "$SA_ID" = "null" ]; then
    echo "Failed to create service account"
    echo "$SA_RESPONSE"
    exit 1
fi
# Create token
echo "Creating token..."
TOKEN_RESPONSE=$(curl -s -X POST ${GRAFANA_URL}/api/serviceaccounts/${SA_ID}/tokens \
  -u "${ADMIN_USER}:${ADMIN_PASS}" \
  -H "Content-Type: application/json" \
  -d '{"name":"'${SERVICE_ACCOUNT_NAME}'","secondsToLive":3600}')
TOKEN_KEY=$(echo "$TOKEN_RESPONSE" | jq -r '.key')
if [ "$TOKEN_KEY" = "null" ]; then
    echo "Failed to create token"
    echo "$TOKEN_RESPONSE"
    exit 1
fi
echo "Service Account ID: $SA_ID"
echo "Token Key: $TOKEN_KEY"
# Create datasource
echo "Creating datasource..."
DS_RESPONSE=$(curl -s -X POST "${GRAFANA_URL}/api/datasources" \
  -H "Authorization: Bearer $TOKEN_KEY" \
  -H "Content-Type: application/json" \
  -d "{
    \"orgId\": 1,
    \"name\": \"${NEW_DATASOURCE_NAME}\",
    \"type\": \"influxdb\",
    \"access\": \"proxy\",
    \"url\": \"${INFLUXDB_V2_URL}\",
    \"basicAuth\": false,
    \"jsonData\": {
      \"defaultBucket\": \"${INFLUXDB_V2_BUCKET}\",
      \"organization\": \"${INFLUXDB_V2_ORG}\",
      \"httpMode\": \"POST\",
      \"version\": \"Flux\"
    },
    \"secureJsonData\": {
      \"token\": \"${INFLUXDB_V2_TOKEN}\"
    }
  }")
# Extract the new datasource UID
NEW_DS_UID=$(echo "$DS_RESPONSE" | jq -r '.datasource.uid')
if [ "$NEW_DS_UID" = "null" ]; then
    echo "Failed to create datasource"
    echo "$DS_RESPONSE"
    exit 1
fi
echo "New datasource UID: $NEW_DS_UID"
echo "Updating dashboard with new datasource UID and bucket name..."
UPDATED_DASHBOARD=$(cat "$DASHBOARD_FILE" | sed -e "s/my_dashboard_id/$NEW_DS_UID/g" -e "s/mybucket/${INFLUXDB_V2_BUCKET}/g")
# Create the dashboard payload
DASHBOARD_PAYLOAD=$(echo "$UPDATED_DASHBOARD" | jq '{
  dashboard: .,
  folderId: 0,
  overwrite: true,
  message: "Imported via API with updated datasource UID and bucket name"
}')
# Import the dashboard
echo "Importing dashboard..."
IMPORT_RESPONSE=$(curl -s -X POST ${GRAFANA_URL}/api/dashboards/db \
  -H "Authorization: Bearer $TOKEN_KEY" \
  -H "Content-Type: application/json" \
  -d "$DASHBOARD_PAYLOAD")
DASHBOARD_STATUS=$(echo "$IMPORT_RESPONSE" | jq -r '.status')
if [ "$DASHBOARD_STATUS" = "success" ]; then
    DASHBOARD_URL=$(echo "$IMPORT_RESPONSE" | jq -r '.url')
    echo "Dashboard imported successfully!"
    echo "Dashboard URL: ${GRAFANA_URL}${DASHBOARD_URL}"
else
    echo "Failed to import dashboard"
    echo "$IMPORT_RESPONSE"
    exit 1
fi
echo "Setup complete!"
