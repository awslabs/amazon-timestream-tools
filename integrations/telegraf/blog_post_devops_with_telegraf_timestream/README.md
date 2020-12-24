# Collecting, storing, and analyzing your DevOps workloads with open-source Telegraf, Amazon Timestream, and Grafana

Resources related to [the blog post](https://aws.amazon.com/blogs/database/collecting-storing-and-analyzing-your-devops-workloads-with-open-source-telegraf-amazon-timestream-and-grafana/), which guides how to use [Timestream output plugin for Telegraf](https://github.com/influxdata/telegraf/tree/master/plugins/outputs/timestream).

## Files index

[The CloudFormation template](https://aws-database-blog.s3.amazonaws.com/artifacts/DevOps_workloads_Telegraf_Timestream_Grafana/DevOpsWithTelegrafAndTimestream.template.json) used in the blog post will fetch and use the following files:

 * [setup_Telegraf_with_Timestream.sh](setup_Telegraf_with_Timestream.sh) - script to setup Telegraf on Amazon Linux 2 EC2. It will fetch and transform the following file:
   * [telegraf.conf](telegraf.conf) - sample configuration for the Telegraf agent
 * [setup_sample_application.sh](setup_sample_application.sh) - script to setup sample python application. It will fetch the following files:
   * [app.py](app.py) - sample python application code
   * [sample_app.service](sample_app.service) - configuration file to run the python sample application as a Linux service
 * [setup-grafana.sh](setup-grafana.sh) - script to setup Grafana. It will fetch and transform the following files:
   * [grafana_timestream_datasource.yaml](grafana_timestream_datasource.yaml) - [provisioning data source](https://grafana.com/docs/grafana/latest/administration/provisioning/#data-sources) for Timestream to Grafana
   * [grafana_dashboard.yaml](grafana_dashboard.yaml) - configuration file for Grafana dashboard
   * [grafana_pi_estimation_dashboard.json](grafana_pi_estimation_dashboard.json) - the dashboard source