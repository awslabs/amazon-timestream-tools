# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from .data_generator import DataGenerator

class DevOpsDataGenerator(DataGenerator):
    def __init__(self):
        self.measure_templates = [
            {
                "name": "cpu_usage",
                "type": "DOUBLE",
                "max_variation": 1.2,
                "max": 100.0,
                "min": 0.0
            },
            {
                "name": "mem_usage",
                "type": "DOUBLE",
                "max_variation": 2.3,
                "max": 100.0,
                "min": 0.0
            },
            {
                "name": "disk_usage",
                "type": "DOUBLE",
                "max_variation": 0.5,
                "max": 100.0,
                "min": 0.0
            },
            {
                "name": "network_in",
                "type": "DOUBLE",
                "max_variation": 100,
                "max": 5000,
                "min": 0
            },
            {
                "name": "network_out",
                "type": "DOUBLE",
                "max_variation": 20,
                "max": 2000,
                "min": 0
            }
        ]
        self.dimension_templates = [
            {
                "name": "server_id",
                "value_length": 14
            },
            {
                "name": "region",
                "random_options": ["ca-central-1", "ca-west-1", "us-east-1", "us-east-2", "us-west-1", "us-west-2", "sa-east-1", "eu-central-1", "eu-west-1", "eu-west-2", "eu-south-1", "eu-west-3"]
            }
        ]
    
    def generate_dashboard(self, grafana_data_source_name, database_name, table_name) -> dict:
        return {
            "panels": [
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "defaults": {
                            "unit": "percent"
                        },
                    },
                    "gridPos": {
                        "h": 16,
                        "w": 9,
                        "x": 0,
                        "y": 0
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, server_id, cpu_usage AS \"CPU Usage\", mem_usage as \"Memory Usage\" FROM \"{database_name}\".\"{table_name}\" WHERE server_id = '$Server' ORDER BY time ASC",
                        }
                    ],
                    "title": "CPU / Memory Usage",
                    "type": "timeseries"
                },
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "defaults": {
                            "color": {
                                "mode": "continuous-BlPu"
                            },
                            "unit": "binBps"
                        },
                    },
                    "gridPos": {
                        "h": 16,
                        "w": 10,
                        "x": 9,
                        "y": 0
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, network_in AS \"Network In\", network_out AS \"Network Out\", server_id FROM \"{database_name}\".\"{table_name}\" WHERE server_id = '$Server' ORDER BY time ASC",
                        }
                    ],
                    "title": "Network In / Network Out",
                    "type": "timeseries"
                },
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "defaults": {
                            "unit": "percent"
                        },
                    },
                    "gridPos": {
                        "h": 8,
                        "w": 5,
                        "x": 0,
                        "y": 16
                    },
                    "options": {
                        "reduceOptions": {
                            "calcs": [
                                "mean"
                            ],
                        },
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, AVG(disk_usage) FROM \"{database_name}\".\"{table_name}\" WHERE server_id = '$Server' GROUP BY time",
                        }
                    ],
                    "title": "Average Disk Usage Percentage",
                    "type": "stat"
                }
            ],
            "templating": {
                "list": [
                    {
                        "definition": f"SELECT DISTINCT server_id FROM \"{database_name}\".\"{table_name}\"",
                        "name": "Server",
                        "query": f"SELECT DISTINCT server_id FROM \"{database_name}\".\"{table_name}\"",
                        "refresh": 1,
                        "sort": 1,
                        "type": "query"
                    }
                ]
            },
            "time": {
                "from": "now-15m",
                "to": "now"
            },
            "title": "Amazon Timestream for LiveAnalytics Sample Dashboard"
        }
