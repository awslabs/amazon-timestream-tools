#!/bin/bash -xe

echo Running setup-grafana.sh

# Installing Grafana
cd /tmp
wget https://dl.grafana.com/oss/release/grafana-7.1.5-1.x86_64.rpm
sudo yum -y install grafana-7.1.5-1.x86_64.rpm

# Install Timestream datasource
grafana-cli plugins install grafana-timestream-datasource

# Configure Timestream datasource
wget https://raw.githubusercontent.com/awslabs/amazon-timestream-tools/master/integrations/telegraf/blog_post_devops_with_telegraf_timestream/grafana_timestream_datasource.yaml
sudo mkdir -p /etc/grafana/provisioning/datasources
TIMESTREAM_REGION=$1
sudo sed "s/TIMESTREAM_REGION/${TIMESTREAM_REGION}/g" grafana_timestream_datasource.yaml | sudo tee /etc/grafana/provisioning/datasources/timestream.yaml

# Configure dashboard config
wget https://raw.githubusercontent.com/awslabs/amazon-timestream-tools/master/integrations/telegraf/blog_post_devops_with_telegraf_timestream/grafana_dashboard.yaml
sudo mkdir -p /etc/grafana/provisioning/dashboards
sudo cp /tmp/grafana_dashboard.yaml /etc/grafana/provisioning/dashboards/default.yaml

sudo mkdir -p /var/lib/grafana/dashboards
cd /tmp

# Add sample dashboard
wget https://raw.githubusercontent.com/awslabs/amazon-timestream-tools/master/integrations/telegraf/blog_post_devops_with_telegraf_timestream/grafana_pi_estimation_dashboard.json
TIMESTREAM_DATABASE=$2
sudo sed "s/YOUR_TIMESTREAM_DB_NAME_HERE/${TIMESTREAM_DATABASE}/g" grafana_pi_estimation_dashboard.json | sudo tee /var/lib/grafana/dashboards/grafana_pi_estimation_dashboard.json
sudo chown -R grafana:grafana /var/lib/grafana

# Configure Grafana as a service
sudo systemctl daemon-reload
sudo systemctl start grafana-server
sudo systemctl status grafana-server
sudo systemctl enable grafana-server

echo Executed setup-grafana.sh
