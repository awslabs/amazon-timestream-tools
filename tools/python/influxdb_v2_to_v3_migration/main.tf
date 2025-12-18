terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  required_version = ">= 1.2"
}

provider "aws" {
  region = "us-west-2"
}

data "aws_vpc" "main" {
  id = var.vpc_id
}

data "aws_subnet" "subnet" {
  id = var.subnet_id
}

resource "aws_security_group" "runner_security_group" {
  name   = "influxdb-v2-to-v3-migration-security-group"
  vpc_id = data.aws_vpc.main.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_access_ip]
    description = "User SSH access permission"
  }

  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Outbound HTTP traffic permission"
  }

  egress {
    from_port   = var.influxdb_v2_port
    to_port     = var.influxdb_v2_port
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Outbound InfluxDB v2 instance traffic permission"
  }

  egress {
    from_port   = var.influxdb_v3_port
    to_port     = var.influxdb_v3_port
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Outbound InfluxDB v3 instance traffic permission"
  }
}

data "aws_ssm_parameter" "influxdb_v2_to_v3_migration_runner_ami" {
  name = "/amis/influxdb-v2-to-v3-migration-runner/latest"
}

resource "aws_secretsmanager_secret" "migration_secret" {
  name                    = "influxdb_v2_to_v3_migration"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "migration_secret_version" {
  secret_id     = aws_secretsmanager_secret.migration_secret.id
  secret_string = jsonencode(var.tokens)
}

resource "aws_iam_role" "runner_role" {
  name = "influxdb_v2_to_v3_migration_runner_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "runner_policy" {
  name = "influxdb_v2_to_v3_migration_runner_policy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = ["secretsmanager:GetSecretValue"]
        Effect   = "Allow"
        Resource = "${aws_secretsmanager_secret.migration_secret.arn}"
      },
      {
        Action = [
          "ec2:DescribeSubnets",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeVpcs"
        ]
        Effect   = "Allow"
        Resource = "*"
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "runner_policy_attachment" {
  role       = aws_iam_role.runner_role.name
  policy_arn = aws_iam_policy.runner_policy.arn
}

resource "aws_iam_instance_profile" "runner_profile" {
  name = "influxdb_v2_to_v3_migration_runner_profile"
  role = aws_iam_role.runner_role.name
}

data "aws_region" "current" {}

resource "aws_instance" "influxdb_v2_to_v3_migration_runner" {
  ami                  = data.aws_ssm_parameter.influxdb_v2_to_v3_migration_runner_ami.value
  instance_type        = var.runner_type
  iam_instance_profile = aws_iam_instance_profile.runner_profile.name

  key_name = var.runner_ssh_key_name

  subnet_id              = var.subnet_id
  vpc_security_group_ids = [aws_security_group.runner_security_group.id]

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.runner_storage_amount
    delete_on_termination = true
  }

  user_data = <<-EOF
    #!/bin/bash
    echo "AWS_REGION=${data.aws_region.current.name}" >> /etc/environment
    echo "AWS_DEFAULT_REGION=${data.aws_region.current.name}" >> /etc/environment
  EOF

  tags = var.runner_tags
}

resource "aws_iam_role_policy_attachment" "ssm_policy_attachment" {
  role       = aws_iam_role.runner_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

output "runner_public_ip" {
  value = aws_instance.influxdb_v2_to_v3_migration_runner.public_ip
}

output "runner_id" {
  value = aws_instance.influxdb_v2_to_v3_migration_runner.id
}

output "secret_id" {
  value = aws_secretsmanager_secret.migration_secret.id
}
