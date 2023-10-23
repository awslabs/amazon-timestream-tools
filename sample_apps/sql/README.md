# Getting started with Amazon Timestream SQL queries

These sample SQL queries provide how to do complex analysis in Amazon Timestream. 
Each example contains
1. Example query in SQL file `<example>.sql`
2. Example data in CSV file that can be loaded to apply SQL query `<example>.csv`
3. Data model JSON file used to map CSV file to Amazon Timestream attributes during data load. This model is used during Batch Load task.
`datamodel.json`

## Folder content

Folder | Description
-------|-------------
[utils](utils) | Utility python script that performs <br /> 1. Upload example data to S3 <br /> 2. Creates Batch Load task to load example data into Timestream Table
[last_value_fill_forward](last_value_fill_forward) | Create time series and fill gaps with LAST_VALUE() function |

