# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

packer {
  required_plugins {
    amazon = {
      version = ">= 1.2.8"
      source  = "github.com/hashicorp/amazon"
    }
  }
}

source "amazon-ebs" "influxdb_v2_to_v3_migration_runner" {
  region               = "us-west-2"
  instance_type        = "t4g.large"
  ssh_username         = "ec2-user"

  source_ami_filter {
    filters = {
      name                = "al2023-ami-*-arm64"
      virtualization-type = "hvm"
      root-device-type    = "ebs"
    }
    owners      = ["amazon"]
    most_recent = true
  }

  ami_name = "influxdb-v2-to-v3-migration-runner-{{timestamp}}"
}

build {
  sources = ["source.amazon-ebs.influxdb_v2_to_v3_migration_runner"]

  # Install wget and Python 3.13.
  provisioner "shell" {
    inline = [
      "sudo dnf update",
      "sudo dnf install -y python3.13 wget python3.13-pip",
      "sudo update-alternatives --install /usr/bin/python python /usr/bin/python3.13 313",
      "sudo update-alternatives --install /usr/bin/pip pip /usr/bin/pip3.13 313",
      "sudo update-alternatives --set python /usr/bin/python3.13",
      "sudo update-alternatives --set pip /usr/bin/pip3.13",
    ]
  }

  # InfluxDB v2 CLI.
  provisioner "shell" {
    inline = [
      "cd /home/ec2-user",
      "mkdir -p influx_cli",
      "cd influx_cli",
      "wget https://dl.influxdata.com/influxdb/releases/influxdb2-client-2.7.5-linux-arm64.tar.gz",
      "echo \"867c3cbabd63a34a9b1ac643fd5c5d268b694acc98e3b75fa5a78d63037097dd influxdb2-client-2.7.5-linux-arm64.tar.gz\" | sha256sum -c -",
      "tar -xzf influxdb2-client-2.7.5-linux-arm64.tar.gz",
      "sudo mv influx /usr/local/bin/",
      "sudo chmod +x /usr/local/bin/influx",
      "cd /home/ec2-user",
      "rm -rf influx_cli"
    ]
  }

  # InfluxDB v2 daemon (influxd).
  provisioner "shell" {
    inline = [
      "cd /home/ec2-user",
      "wget https://download.influxdata.com/influxdb/releases/v2.8.0/influxdb2-2.8.0-2_linux_arm64.tar.gz",
      "echo \"67118f0aad0b50fb1278bb982a02d65d8aaa64d23aa6678f8787e7ca754a5ec1 influxdb2-2.8.0-2_linux_arm64.tar.gz\" | sha256sum -c -",
      "tar -xzf influxdb2-2.8.0-2_linux_arm64.tar.gz",
      "sudo mv influxdb2-2.8.0/usr/bin/influxd /usr/local/bin/",
      "sudo chmod +x /usr/local/bin/influxd",
      "rm -rf influxdb2-2.8.0",
      "rm influxdb2-2.8.0-2_linux_arm64.tar.gz"
    ]
  }

  # Creating the backup data directory.
  provisioner "shell" {
    inline = [
      "sudo mkdir -p /engine/data",
      "sudo chmod 777 /engine/data"
    ]
  }

  # Application files.
  provisioner "file" {
    source      = "../influxdb_v2_to_v3_migration.py"
    destination = "/home/ec2-user/influxdb_v2_to_v3_migration.py"
  }

  provisioner "file" {
    source      = "../influxdb_v3_ingestion.py"
    destination = "/home/ec2-user/influxdb_v3_ingestion.py"
  }

  provisioner "file" {
    source      = "../utils.py"
    destination = "/home/ec2-user/utils.py"
  }

  provisioner "file" {
    source      = "../pyproject.toml"
    destination = "/home/ec2-user/pyproject.toml"
  }

  # Install Python dependencies from pyproject.toml.
  provisioner "shell" {
    inline = [
      "cd /home/ec2-user",
      "sudo pip install --root-user-action=ignore ."
    ]
  }

  # Set ownership and permissions.
  provisioner "shell" {
    inline = [
      "sudo chmod +x /home/ec2-user/influxdb_v2_to_v3_migration.py"
    ]
  }

  post-processor "manifest" {
    output = "packer-manifest.json"
  }

  post-processor "shell-local" {
    inline = [
      "AMI_ID=$(jq -r '.builds[-1].artifact_id | split(\":\") | .[1]' packer-manifest.json)",
      "aws ssm put-parameter --name /amis/influxdb-v2-to-v3-migration-runner/latest --value \"$AMI_ID\" --type String --overwrite --region us-west-2"
    ]
  }
}
