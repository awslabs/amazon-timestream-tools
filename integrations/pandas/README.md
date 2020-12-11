# Writing and Querying Amazon Timestream with Pandas

## What is Pandas?

[Pandas](https://pandas.pydata.org/) is a fast, powerful, flexible and easy to use open source data analysis and manipulation tool,
built on top of the Python programming language.

---

## What is AWS Data Wrangler?

An [open-source](https://github.com/awslabs/aws-data-wrangler) Python package that extends the power of [Pandas](https://github.com/pandas-dev/pandas) library to AWS connecting **DataFrames** and AWS data related services (**Amazon Redshift**, **AWS Glue**, **Amazon Athena**, **Amazon Timestream**, **Amazon EMR**, etc).

Built on top of other open-source projects like [Pandas](https://github.com/pandas-dev/pandas), [Apache Arrow](https://github.com/apache/arrow) and [Boto3](https://github.com/boto/boto3), it offers abstracted functions to execute usual ETL tasks like load/unload data from **Data Lakes**, **Data Warehouses** and **Databases**.

Check our [list of functionalities](https://aws-data-wrangler.readthedocs.io/en/stable/api.html).

---

## Examples

* [Writing and Querying basics](https://github.com/awslabs/amazon-timestream-tools/blob/master/integrations/pandas/1-writing-and-querying-basics.ipynb)
* [Writing and Querying advanced](https://github.com/awslabs/amazon-timestream-tools/blob/master/integrations/pandas/2-writing-and-querying-advanced.ipynb)

---

## AWS Data Wrangler API for Amazon Timestream

* [wr.timestream.create_database()](https://aws-data-wrangler.readthedocs.io/en/latest/stubs/awswrangler.timestream.create_database.html)
* [wr.timestream.create_table()](https://aws-data-wrangler.readthedocs.io/en/latest/stubs/awswrangler.timestream.create_table.html)
* [wr.timestream.delete_database()](https://aws-data-wrangler.readthedocs.io/en/latest/stubs/awswrangler.timestream.delete_database.html)
* [wr.timestream.delete_table()](https://aws-data-wrangler.readthedocs.io/en/latest/stubs/awswrangler.timestream.delete_table.html)
* [wr.timestream.write()](https://aws-data-wrangler.readthedocs.io/en/latest/stubs/awswrangler.timestream.write.html)
* [wr.timestream.query()](https://aws-data-wrangler.readthedocs.io/en/latest/stubs/awswrangler.timestream.query.html)
