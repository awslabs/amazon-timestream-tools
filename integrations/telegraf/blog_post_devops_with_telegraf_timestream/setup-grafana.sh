#!/bin/bash -xe

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

echo Running setup-grafana.sh

# Installing Grafana
cd /tmp
wget https://dl.grafana.com/oss/release/grafana-7.1.5-1.x86_64.rpm && \
    echo "1fe585db9e7d79cd93231f737ec7dd1bd7e53abca5ea808ecea6c4f697a1a883 grafana-7.1.5-1.x86_64.rpm" | sha256sum -c - && \
    sudo yum -y install grafana-7.1.5-1.x86_64.rpm

# Install Timestream datasource
grafana-cli plugins install grafana-timestream-datasource

# Configure Timestream datasource
wget https://raw.githubusercontent.com/awslabs/amazon-timestream-tools/master/integrations/telegraf/blog_post_devops_with_telegraf_timestream/grafana_timestream_datasource.yaml && \
    echo "a3e799998384d42f34d80e3f626ae0fff1410a7830038323d3937928f3b2118f grafana_timestream_datasource.yaml" | sha256sum -c -
STATUS=$?

if [ $STATUS -ne 0 ]; then
    echo "Failed to verify checksum for grafana_timestream_datasource.yaml"
    exit 1
fi

sudo mkdir -p /etc/grafana/provisioning/datasources
TIMESTREAM_REGION=$1
sudo sed "s/TIMESTREAM_REGION/${TIMESTREAM_REGION}/g" grafana_timestream_datasource.yaml | sudo tee /etc/grafana/provisioning/datasources/timestream.yaml

# Configure dashboard config
wget https://raw.githubusercontent.com/awslabs/amazon-timestream-tools/master/integrations/telegraf/blog_post_devops_with_telegraf_timestream/grafana_dashboard.yaml && \
    echo "7dca236d3a20c60d4d5cacbfbb6ba5d3f261abb6 grafana_dashboard.yaml" | sha256sum -c -
STATUS=$?

if [ $STATUS -ne 0 ]; then
    echo "Failed to verify checksum for grafana_dashboard.yaml"
    exit 1
fi

sudo mkdir -p /etc/grafana/provisioning/dashboards
sudo cp /tmp/grafana_dashboard.yaml /etc/grafana/provisioning/dashboards/default.yaml

sudo mkdir -p /var/lib/grafana/dashboards
cd /tmp

# Add sample dashboard
wget https://raw.githubusercontent.com/awslabs/amazon-timestream-tools/master/integrations/telegraf/blog_post_devops_with_telegraf_timestream/grafana_pi_estimation_dashboard.json && \
    echo "d695cc7c52d019735437ec42358a02268ab53b99fff1697f7e3795973fa32ae9 grafana_pi_estimation_dashboard.json" | sha256sum -c -
STATUS=$?

if [ $STATUS -ne 0 ]; then
    echo "Failed to verify checksum for grafana_pi_estimation_dashboard.json"
    exit 1
fi

TIMESTREAM_DATABASE=$2
sudo sed "s/YOUR_TIMESTREAM_DB_NAME_HERE/${TIMESTREAM_DATABASE}/g" grafana_pi_estimation_dashboard.json | sudo tee /var/lib/grafana/dashboards/grafana_pi_estimation_dashboard.json
sudo chown -R grafana:grafana /var/lib/grafana

# Configure Grafana as a service
sudo systemctl daemon-reload
sudo systemctl start grafana-server
sudo systemctl status grafana-server
sudo systemctl enable grafana-server

echo Executed setup-grafana.sh
