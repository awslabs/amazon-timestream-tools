variable "influxdb_v2_port" {
  type    = number
  default = 8086
}

variable "influxdb_v3_port" {
  type    = number
  default = 8181
}

variable "tokens" {
  sensitive = true
  type      = map(string)
  default = {
    INFLUXDB_V2_TOKEN = "replace me"
    INFLUXDB_V3_TOKEN = "replace me"
  }
}
