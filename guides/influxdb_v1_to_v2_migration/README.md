# InfluxDB v1 to v2 Read Replica Migration Guide

In order to migrate an InfluxDB v1 instance to a Timestream for InfluxDB v2 read replica cluster, you must export and ingest [line protocol](https://docs.influxdata.com/influxdb3/clustered/reference/syntax/line-protocol/). The Influx v2 CLI provides [backup](https://docs.influxdata.com/influxdb/v2/admin/backup-restore/backup/) and [restore](https://docs.influxdata.com/influxdb/v2/admin/backup-restore/restore/) commands, which are more efficient than ingesting line protocol. But read replica instances do not support restore operations.

To complete the migration between InfluxDB v1 and an InfluxDB v2 read replica cluster, you must complete the following steps:

1. Upgrade the InfluxDB v1 instance to InfluxDB v2 using the `influxd upgrade` command from the InfluxDB v2 daemon.

   **Note**: The influxd upgrade command copies the data to a new directory on the system leaving the original InfluxDB v1 instance intact.
2. Export the new InfluxDB v2 data to line protocol using the `influxd inspect export-lp` command from the InfluxDB v2 daemon.
3. Ingest the data into the read replica cluster using the Amazon Timestream `influxdb_ingestion.py` script.

## Upgrading InfluxDB v1 to InfluxDB v2

This guide uses an ARM64 Amazon Linux 2023 EC2 instance to perform the migration. You may need to alter the guide slightly if using a Debian or AMD64 system.

1. Download the InfluxDB 2 binaries from the InfluxData downloads page:
   ```shell
   mkdir ~/influxdb2-binary
   cd ~/influxdb2-binary

   wget https://download.influxdata.com/influxdb/releases/v2.8.0/influxdb2-2.8.0-2_linux_arm64.tar.gz && \
      echo "67118f0aad0b50fb1278bb982a02d65d8aaa64d23aa6678f8787e7ca754a5ec1 influxdb2-2.8.0-2_linux_arm64.tar.gz" | sha256sum -c - && \
      tar xvfz influxdb2-2.8.0-2_linux_arm64.tar.gz
   ```

2. Perform the upgrade:
   ```shell
   ~/influxdb2-binary/influxdb2-2.8.0/usr/bin/influxd upgrade
   ```

   **Note**: You will need to set the `--engine-path` flag, if the database engine directory is not in the default - (default `"/home/ec2-user/.influxdbv2/engine"`).

After the upgrade has been completed, you should a copy of your dataset in the `~/.influxdbv2/engine/data`.

## Exporting line protocol

Now that we have a dataset that the InfluxDB v2 daemon can work with, we can export the dataset into line protocol. The data for InfluxDB v2 is grouped per bucket; we can parallelize the workload to export each bucket concurrently. Each bucket ID can be found in the `engine/data` directory.

1. Make a new directory for the exported line protocol:

   ```shell
   mkdir ~/influxdb2-line-protocol-export
   cd ~/influxdb2-line-protocol-export
   ```

2. Start the InfluxDB v2 daemon in order to find the mapping between InfluxDB v2 buckets and IDs. You will need to spawn another terminal while the database server is running in order to run the script in the next step:

   ```shell
   ~/influxdb2-binary/influxdb2-2.8.0/usr/bin/influxd
   ```

   **Note**: If there is another program using the current port (such as InfluxDB v1), consider copying the data over to a new system at this point, or stop InfluxDB v1.

3. Export each bucket in parallel. This example script may require altering if the data directories are not in their default, expected locations. Save the following bash script named `export-influxdb2-buckets.sh`:

   ```shell
   #!/bin/bash

   # Usage: `./export-influxdb2-buckets.sh <org_name> <token> [influxdb_url]`

   ORG_NAME="$1"
   TOKEN="$2"
   INFLUX_URL="${3:-http://localhost:8086}"  # default

   if [ -z "$ORG_NAME" ] || [ -z "$TOKEN" ]; then
       echo "Usage: $0 <org_name> <token> [influxdb_url]"
       exit 1
   fi
   ```

## Getting buckets for the org

Paste the following into a new script, `export-influxdb2-buckets.sh`:
   
   ```shell
   BUCKETS_JSON=$(curl -s -X GET "$INFLUX_URL/api/v2/buckets?org=$ORG_NAME" \
       -H "Authorization: Token $TOKEN" \
       -H "Accept: application/json")

   mkdir -p buckets
   echo "$BUCKETS_JSON" | jq -r '.buckets[]? | select(.name | startswith("_") | not) | "\(.name | sub("/autogen$"; "")) \(.id)"' | while
   read -r BUCKET_NAME BUCKET_ID; do
       echo "Beginning export for bucket $BUCKET_NAME"
       mkdir buckets/$BUCKET_NAME
       ~/influxdb2-binary/influxdb2-2.8.0/usr/bin/influxd inspect export-lp \
           --bucket-id $BUCKET_ID \
           --engine-path ~/.influxdbv2/engine \
           --output-path buckets/$BUCKET_NAME/$BUCKET_ID.gz \
           --compress &

   done
   wait
   ```

1. Make the script executable:

   ```shell
   sudo chmod +x export-influxdb2-buckets.sh
   ```
2. Execute the export script:
   
   ```shell
   ./export-influxdb2-buckets.sh <org name> <token>
   ```
   - `<org name>` is the name of the organization you set when upgrading from v1 to v2. The token value can be found in the `~/.influxdbv2/configs` file under the default section.

   **Note**: Depending on the size and distribution of your dataset across buckets, this script may take a long time to finish.

## Ingesting data to the InfluxDB v2 read replica cluster

Timestream provides an open source ingestion script that can ingest gzip-compressed line protocol files to InfluxDB v2: https://github.com/awslabs/amazon-timestream-tools/tree/mainline/tools/python/liveanalytics_migration_scripts/targets/timestream_for_influxdb/ingestion#usage

**Note**: You can ignore the data preparation steps as we already have our line protocol dataset compressed and ready for ingestion.

1. Clone the Amazon Timestream Tools repository:

   ```shell
   git clone https://github.com/awslabs/amazon-timestream-tools.git ~/amazon-timestream-tools
   ```
2. Create a virtual environment for python:

   ```shell
   python3 -m venv venv
   ```
3. Activate the virtual environment:

   ```shell
   source venv/bin/activate
   ```
4. Install pip dependencies:

   ```shell
   pip install influxdb_client dotenv boto3
   ```
5. Set the following environment variables for the read replica cluster:

   ```shell 
   export INFLUXDB_V2_URL=<https://your-cluster-url:your-port>
   export INFLUXDB_V2_ORG=<your-read-replica-org>
   export INFLUXDB_V2_TOKEN=<your-read-replica-token>
   ```
6. Create an ingestion script named `influxdb_ingestion.sh` and make it executable:

   ```shell 
   touch influxdb_ingestion.sh`
   sudo chown +x influxdb_ingestion.sh
   ```
7. Paste the following into the script:

   ```shell
   #!/bin/bash

   set -euo pipefail
   : "${INFLUXDB_V2_URL:?INFLUXDB_V2_URL environment variable is required}"
   : "$INFLUXDB_V2_TOKEN:?INFLUXDB_V2_TOKEN environment variable is required}"
   : "$INFLUXDB_V2_ORG:?INFLUXDB_V2_ORG environment variable is required}"

   RESPONSE=$(curl -s -X GET "$INFLUXDB_V2_URL/api/v2/orgs?name=$INFLUXDB_V2_ORG" \
       -H "Authorization: Token $INFLUXDB_V2_TOKEN" \
       -H "Accept: application/json")

   ORG_ID=$(echo "$RESPONSE" | jq -r '.orgs[0].id // empty')

   if [ -z "$ORG_ID" ]; then
       echo "Error: Organization '$INFLUXDB_V2_ORG' not found or access denied." >&2
       return 1
   fi

   for dir in ./buckets/*; do
       DIR=${dir%/}
       BUCKET_NAME=${dir##*/}
       echo "Creating new bucket $BUCKET_NAME"

       HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
           -X POST "$INFLUXDB_V2_URL/api/v2/buckets" \
           -H "Authorization: Token $INFLUXDB_V2_TOKEN" \
           -H "Content-Type: application/json" \
           -d "{
               \"name\": \"$BUCKET_NAME\",
               \"orgID\": \"$ORG_ID\"
            }")

       if [[ "$HTTP_CODE" == "201" ]]; then
           echo "Created bucket '$BUCKET_NAME'"
       elif [[ "$HTTP_CODE" == "422" ]]; then
           echo "bucket '$BUCKET_NAME' already exists"
       else
           echo "ERROR: Failed to create bucket '$BUCKET_NAME' (HTTP $HTTP_CODE)" >&2
           exit 1
       fi

       echo "Beginning ingestion for file bucket $BUCKET_NAME"

       python3 ~/amazon-timestream-tools/tools/python/liveanalytics_migration_scripts/targets/timestream_for_influxdb/ingestion/influxdb_ingestion.py \
           $BUCKET_NAME $DIR \
           --skip-bucket-check
   done
   ```
8. Execute the script:

   ```shell
   ./influxdb_ingestion.sh
   ```

If all goes well, the script will create the necessary buckets in the read replica cluster and ingest the dataset one bucket at a time.
