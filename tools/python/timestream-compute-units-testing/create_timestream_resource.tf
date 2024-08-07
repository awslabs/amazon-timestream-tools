resource "aws_timestreamwrite_database" "tcu_testing" {
  database_name = "devops"
}


resource "aws_timestreamwrite_table" "tcu_testing" {
  database_name = aws_timestreamwrite_database.tcu_testing.database_name
  table_name    = "sample_devops"

  retention_properties {
    magnetic_store_retention_period_in_days = 365
    memory_store_retention_period_in_hours  = 24
  }
 
  schema {
    composite_partition_key {
      enforcement_in_record = "REQUIRED"
      name                  = "hostname"
      type                  = "DIMENSION"
    }
  }
}
