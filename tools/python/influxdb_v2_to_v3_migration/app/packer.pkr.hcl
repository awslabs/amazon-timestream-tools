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

  # Install base OS packages.
  provisioner "shell" {
    inline = [
      "sudo dnf install -y git pkg-config",
      "sudo dnf install -y dnf-plugins-core",
      "sudo dnf builddep -y python3",
      "sudo dnf install -y wget make gcc gcc-c++ gdb lzma glibc-devel libstdc++-devel openssl-devel readline-devel zlib-devel libzstd-devel libffi-devel bzip2-devel xz-devel sqlite sqlite-devel sqlite-libs libuuid-devel gdbm-libs perf expat expat-devel mpdecimal python3-pip"
    ]
  }

  # Build Python 3.14.1 from source.
  provisioner "shell" {
    inline = [
      "cd /tmp",
      "wget https://www.python.org/ftp/python/3.14.1/Python-3.14.1.tgz",
      "tar -xzf Python-3.14.1.tgz",
      "cd Python-3.14.1",
      "./configure",
      "make -s -j $(nproc) build_all",
      "sudo make altinstall",
      "sudo ln -sf /usr/local/bin/python3.14 /usr/local/bin/python",
      "sudo ln -sf /usr/local/bin/pip3.14 /usr/local/bin/pip"
    ]
  }

  # InfluxDB v2 CLI.
  provisioner "shell" {
    inline = [
      "cd /home/ec2-user",
      "mkdir -p influx_cli",
      "cd influx_cli",
      "wget https://dl.influxdata.com/influxdb/releases/influxdb2-client-2.7.5-linux-arm64.tar.gz",
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
      "curl -LO https://download.influxdata.com/influxdb/releases/v2.7.12/influxdb2-2.7.12_linux_arm64.tar.gz",
      "tar -xzf influxdb2-2.7.12_linux_arm64.tar.gz",
      "sudo mv influxdb2-2.7.12/usr/bin/influxd /usr/local/bin/",
      "sudo chmod +x /usr/local/bin/influxd",
      "rm -rf influxdb2-2.7.12",
      "rm influxdb2-2.7.12_linux_arm64.tar.gz"
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
    source      = "influxdb_v2_to_v3_migration.py"
    destination = "/home/ec2-user/influxdb_v2_to_v3_migration.py"
  }

  provisioner "file" {
    source      = "influxdb_v3_ingestion.py"
    destination = "/home/ec2-user/influxdb_v3_ingestion.py"
  }

  provisioner "file" {
    source      = "utils.py"
    destination = "/home/ec2-user/utils.py"
  }

  provisioner "file" {
    source      = "pyproject.toml"
    destination = "/home/ec2-user/pyproject.toml"
  }

  # Install Python dependencies from pyproject.toml.
  provisioner "shell" {
    inline = [
      "cd /home/ec2-user",
      "sudo pip install ."
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
