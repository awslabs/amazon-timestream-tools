#!/bin/bash -xe

echo Running setup_Telegraf_with_Timestream.sh

yum update -y

echo Installing Telegraf
cat <<EOF | sudo tee /etc/yum.repos.d/influxdb.repo
[influxdb]
name = InfluxDB Repository - RHEL \$releasever
baseurl = https://repos.influxdata.com/rhel/\$releasever/\$basearch/stable
enabled = 1
gpgcheck = 1
gpgkey = https://repos.influxdata.com/influxdb.key
EOF

# Fix yum repo definition for Amazon Linux 2
sudo sed -i "s/\$releasever/$(rpm -E %{rhel})/g" /etc/yum.repos.d/influxdb.repo

sudo yum install telegraf -y

echo Setting up Telegraf
cd /tmp
wget https://raw.githubusercontent.com/awslabs/amazon-timestream-tools/master/integrations/telegraf/blog_post_devops_with_telegraf_timestream/telegraf.conf
TIMESTREAM_DATABASE=$1
sudo sed "s/yourDatabaseNameHere/${TIMESTREAM_DATABASE}/g" telegraf.conf | sudo tee /etc/telegraf/telegraf.conf

# Configure Telegraf service
systemctl restart telegraf
systemctl status telegraf
systemctl enable telegraf

echo Executed setup_Telegraf_with_Timestream.sh
