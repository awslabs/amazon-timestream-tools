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

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
}

resource "aws_subnet" "subnet" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(aws_vpc.main.cidr_block, 8, 1)
  map_public_ip_on_launch = true
}

resource "aws_internet_gateway" "internet_gateway" {
  vpc_id = aws_vpc.main.id
}

resource "aws_route_table" "route_table" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.internet_gateway.id
  }
}

resource "aws_route_table_association" "subnet_route" {
  subnet_id      = aws_subnet.subnet.id
  route_table_id = aws_route_table.route_table.id
}

resource "aws_security_group" "security_group" {
  name   = "influxdb-v2-to-v3-migration-security-group"
  vpc_id = aws_vpc.main.id

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

  egress {
    from_port   = 2049
    to_port     = 2049
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
    description = "EFS access permission"
  }
}

resource "aws_ecr_repository" "migration_repository" {
  name                 = "influxdb_v2_to_v3_migration_ecr_repository"
  image_tag_mutability = "MUTABLE"
}

resource "aws_efs_file_system" "migration_file_system" {
  creation_token = "influxdb_v2_to_v3_migration"
}

resource "aws_cloudwatch_log_group" "migration_log_group" {
  name = "influxdb_v2_to_v3_migration_log_group"

  tags = {
    Environment = "production"
    Application = "influxdb_v2_to_v3_migration"
  }
}

resource "aws_efs_access_point" "migration_access_point" {
  file_system_id = aws_efs_file_system.migration_file_system.id
  posix_user {
    gid = "1000"
    uid = "1000"
  }

  root_directory {
    path = "/engine"
    creation_info {
      owner_gid   = "1000"
      owner_uid   = "1000"
      permissions = "777"
    }
  }
}

resource "aws_iam_role" "migration_execution_role" {
  name = "influxdb_v2_to_v3_migration_execution_role"

  managed_policy_arns = ["arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"]

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_secretsmanager_secret" "migration_secret" {
  name                    = "influxdb_v2_to_v3_migration"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "migration_secret_version" {
  secret_id     = aws_secretsmanager_secret.migration_secret.id
  secret_string = jsonencode(var.tokens)
}

resource "aws_iam_role" "migration_task_role" {
  name = "influxdb_v2_to_v3_migration_task_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })

  inline_policy {
    name = "influxdb_v2_to_v3_migration_inline_policy"

    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Action = [
            "elasticfilesystem:ClientMount",
            "elasticfilesystem:ClientWrite",
            "elasticfilesystem:ClientRootAccess"
          ]
          Effect   = "Allow"
          Resource = "${aws_efs_access_point.migration_access_point.arn}"
        },
        {
          Action   = ["secretsmanager:GetSecretValue"]
          Effect   = "Allow"
          Resource = "${aws_secretsmanager_secret.migration_secret.arn}"
        },
      ]
    })
  }
}

data "aws_region" "current_region" {}

resource "aws_ecs_task_definition" "migration_task_definition" {
  family                   = "influxdb_v2_to_v3_migration"
  network_mode             = "awsvpc"
  cpu                      = 1024
  memory                   = 2048
  requires_compatibilities = ["FARGATE"]
  execution_role_arn       = aws_iam_role.migration_execution_role.arn
  task_role_arn            = aws_iam_role.migration_task_role.arn

  container_definitions = jsonencode([
    {
      name      = "influxdb_v2_to_v3_migration_container"
      image     = "${aws_ecr_repository.migration_repository.repository_url}:latest"
      essential = true
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = "influxdb_v2_to_v3_migration_log_group"
          awslogs-region        = "${data.aws_region.current_region.region}"
          awslogs-stream-prefix = "ecs"
        }
        environment = {
          name  = "TOKEN_SECRET_NAME"
          value = "influxdb_v2_to_v3_migration_secret"
        }
      }
    }
  ])

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  volume {
    name = "service-storage"
    efs_volume_configuration {
      file_system_id          = aws_efs_file_system.migration_file_system.id
      root_directory          = "/"
      transit_encryption      = "ENABLED"
      transit_encryption_port = 2999
      authorization_config {
        access_point_id = aws_efs_access_point.migration_access_point.id
        iam             = "ENABLED"
      }
    }
  }
}

output "ecr_repository_url" {
  value       = aws_ecr_repository.migration_repository.repository_url
  description = "The URL of the ECR repository. Use this when pushing your Docker image."
}

output "ecs_task_definition_id" {
  value = aws_ecs_task_definition.migration_task_definition.id
}

output "vcp_id" {
  value = aws_vpc.main.id
}

output "secret_id" {
  value = aws_secretsmanager_secret.migration_secret.id
}
