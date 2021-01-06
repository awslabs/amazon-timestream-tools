#!/bin/bash -xe

echo Running setup_sample_application.sh
yum update -y

# Install dependencies
## Install Python 3
yum install python3 -y

echo Creating Sample Application
## Create directory
useradd --no-create-home --shell /bin/false sample_app
mkdir -p /home/sample_app/
cd /home/sample_app/

## Create virtual environment
python3 -m venv env
source env/bin/activate

## Make sure that you have the latest pip module installed within your environment
pip install pip --upgrade

## Install InfluxDB client library
pip install influxdb

## Download the sample application code
wget https://raw.githubusercontent.com/awslabs/amazon-timestream-tools/master/integrations/telegraf/blog_post_devops_with_telegraf_timestream/app.py

# Configure sample_app as a service
wget https://raw.githubusercontent.com/awslabs/amazon-timestream-tools/master/integrations/telegraf/blog_post_devops_with_telegraf_timestream/sample_app.service
cp sample_app.service /etc/systemd/system/sample_app.service
chown sample_app:sample_app /etc/systemd/system/sample_app.service
chown -R sample_app:sample_app /home/sample_app/
systemctl daemon-reload
systemctl start sample_app
systemctl status sample_app
systemctl enable sample_app

echo Executed setup_sample_application.sh
