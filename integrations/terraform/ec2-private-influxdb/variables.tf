# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "username" {
  type    = string
  default = "admin"
}

variable "password" {
  type      = string
  sensitive = true
}

variable "client_ip" {
  type = string
}

variable "ec2_key_name" {
  type = string
}

variable "port" {
  type    = number
  default = 8086
}

variable "influxdb_allocated_storage" {
  type        = number
  default     = 20
  description = "The amount of storage to allocate for your DB storage type in GiB."
}

variable "ec2_allocated_storage" {
  type        = number
  default     = 30
  description = "The amount of storage to allocate for your EC2 instance in GiB."
}

variable "ec2_instance_name" {
  type    = string
  default = "TimestreamInfluxDBBastionHost"
}

variable "ec2_instance_type" {
  type    = string
  default = "t4g.medium"
}

variable "ec2_kms_key_id" {
  type = string
}
