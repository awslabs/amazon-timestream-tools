variable "vpc_id" {
  type        = string
  default     = "replace me"
  description = "The ID of an existing VPC to use for all resources. This VPC will be used as a data source and will not be deleted."
}

variable "subnet_id" {
  type        = string
  default     = "replace me"
  description = "The ID of an existing subnet to use for all resources. This subnet will be used as a data source and will not be deleted."
}

variable "ssh_access_ip" {
  type        = string
  default     = "replace me"
  description = "The IP to grant access to the EC2 instance used to run the migration script. For example, '127.0.0.1/32'."
}

variable "tokens" {
  sensitive = true
  type      = map(string)
  default = {
    INFLUXDB_V2_TOKEN = "replace me"
    INFLUXDB_V3_TOKEN = "replace me"
  }
}

variable "runner_ssh_key_name" {
  type        = string
  default     = "replace me"
  description = "The name of the SSH key pair to use to SSH onto the EC2 instance used to run the migration script."
}

variable "influxdb_v2_port" {
  type    = number
  default = 8086
}

variable "influxdb_v3_port" {
  type    = number
  default = 8181
}

variable "runner_tags" {
  type = map(string)
  default = {
    ExampleTag = "raplce me"
  }
}

variable "runner_storage_amount" {
  type        = number
  default     = 200
  description = "The amount of storage to allocate for the EC2 instance used to run the migration script. This should be greater than all of your uncompressed source data."
}

variable "runner_type" {
  type        = string
  default     = "t4g.xlarge" # Must be a graviton instance, as AMI uses ARM64.
  description = "The EC2 instance size to use for the runner."
}
