terraform {
  required_version = ">= 1.2.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.96.0"
    }
  }
}

provider "aws" {
  region = "us-west-2"
}

resource "aws_vpc" "vpc" {
  cidr_block = "10.0.0.0/16"
}

# Private subnet used by the Timestream for InfluxDB instance.
resource "aws_subnet" "private_subnet" {
  vpc_id                  = aws_vpc.vpc.id
  cidr_block              = "10.0.1.0/24"
  map_public_ip_on_launch = false
}

# Public subnet used by the EC2 bastion host.
resource "aws_subnet" "public_subnet" {
  vpc_id                  = aws_vpc.vpc.id
  cidr_block              = "10.0.2.0/24"
  map_public_ip_on_launch = true
}

resource "aws_internet_gateway" "internet_gateway" {
  vpc_id = aws_vpc.vpc.id
}

resource "aws_route" "test_route" {
  route_table_id         = aws_vpc.vpc.main_route_table_id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.internet_gateway.id
}

resource "aws_route_table_association" "route_table_association" {
  subnet_id      = aws_subnet.public_subnet.id
  route_table_id = aws_vpc.vpc.main_route_table_id
}

resource "aws_security_group" "ec2_security_group" {
  name        = "bastion_sg"
  description = "Security group for the EC2 instance."
  vpc_id      = aws_vpc.vpc.id

  ingress {
    description = "Allows the client to connect via SSH."
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["${var.client_ip}/32"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "timestream_influxdb_security_group" {
  name        = "timestream_influxdb_sg"
  description = "Security group for the Timestream for InfluxDB instance."
  vpc_id      = aws_vpc.vpc.id

  ingress {
    description     = "Allows inbound traffic only from the bastion host."
    from_port       = var.port
    to_port         = var.port
    protocol        = "tcp"
    security_groups = [aws_security_group.ec2_security_group.id]
  }
}

data "aws_ami" "amzn-linux-2023-ami" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-arm64"]
  }
}

resource "aws_instance" "ec2_instance" {
  ami                         = data.aws_ami.amzn-linux-2023-ami.id
  instance_type               = var.ec2_instance_type
  subnet_id                   = aws_subnet.public_subnet.id
  vpc_security_group_ids      = [aws_security_group.ec2_security_group.id]
  key_name                    = var.ec2_key_name
  associate_public_ip_address = true

  root_block_device {
    volume_size = var.ec2_allocated_storage
    volume_type = "io1"
    iops        = 200
    encrypted   = true
    kms_key_id  = var.ec2_kms_key_id != "" ? var.ec2_kms_key_id : null
  }

  tags = {
    Name = var.ec2_instance_name
  }
}

resource "aws_timestreaminfluxdb_db_instance" "timestream_influxdb_instance" {
  allocated_storage      = 20
  db_instance_type       = "db.influx.medium"
  vpc_subnet_ids         = [aws_subnet.private_subnet.id]
  vpc_security_group_ids = [aws_security_group.timestream_influxdb_security_group.id]
  name                   = "test-db-instance"
  bucket                 = "test-bucket-name"
  username               = var.username
  password               = var.password
  port                   = 8086
  organization           = "organization"
  publicly_accessible    = false
}

output "instance_url" {
  value = "https://${aws_timestreaminfluxdb_db_instance.timestream_influxdb_instance.endpoint}:${var.port}"
}

output "ec2_public_ip" {
  value = aws_instance.ec2_instance.public_ip
}
