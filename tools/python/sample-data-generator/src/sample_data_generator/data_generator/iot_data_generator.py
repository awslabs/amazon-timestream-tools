# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from .data_generator import DataGenerator

class IoTDataGenerator(DataGenerator):
    def __init__(self):
        self.measure_templates = [
            {
                "name": "temperature_celsius",
                "type": "DOUBLE",
                "max_variation": 0.5,
                "max": 60,
                "min": -30
            },
            {
                "name": "relative_humidity",
                "type": "DOUBLE",
                "max_variation": 0.3,
                "max": 100.0,
                "min": 0.0
            },
            {
                "name": "battery_level",
                "type": "BIGINT",
                "max_variation": 1,
                "max": 100,
                "min": 1 # All devices have enough battery to report
            },
            {
                "name": "velocity",
                "type": "DOUBLE",
                "max_variation": 4.2,
                "max": 100.0,
                "min": 0.0
            }
        ]
        self.dimension_templates = [
            {
                "name": "device_id",
                "value_length": 14
            }
        ]

    def generate_dashboard(self, grafana_data_source_name, database_name, table_name) -> dict:
        return {
            "panels": [
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "defaults": {
                            "color": {
                                "mode": "thresholds"
                            },
                            "mappings": [],
                            "thresholds": {
                                "mode": "absolute",
                                "steps": [
                                    {
                                        "color": "red",
                                        "value": None
                                    },
                                    {
                                        "color": "yellow",
                                        "value": 15
                                    },
                                    {
                                        "color": "green",
                                        "value": 40
                                    }
                                ]
                            },
                            "unit": "percent"
                        },
                    },
                    "gridPos": {
                        "h": 8,
                        "w": 12,
                        "x": 0,
                        "y": 0
                    },
                    "options": {
                        "reduceOptions": {
                            "values": True
                        }
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT device_id, battery_level FROM \"{database_name}\".\"{table_name}\" WHERE time = (SELECT MAX(time) FROM \"{database_name}\".\"{table_name}\" AS sub WHERE sub.device_id = \"{database_name}\".\"{table_name}\".device_id)",
                        }
                    ],
                    "title": "Current Device Battery Level",
                    "type": "gauge"
                },
                {
                    "datasource": grafana_data_source_name,
                    "gridPos": {
                        "h": 8,
                        "w": 12,
                        "x": 12,
                        "y": 0
                    },
                    "options": {
                        "cellValues": {
                            "unit": "velocityms"
                        },
                        "color": {
                            "mode": "scheme",
                            "scheme": "RdYlBu",
                        },
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "format": 1,
                            "rawQuery": f"SELECT time, device_id, velocity FROM \"{database_name}\".\"{table_name}\" ORDER BY time ASC",
                        }
                    ],
                    "title": "Device Velocity",
                    "type": "heatmap"
                },
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "defaults": {
                            "thresholds": {
                                "mode": "absolute",
                                "steps": [
                                {
                                    "color": "green",
                                    "value": None
                                },
                                {
                                    "color": "#EAB839",
                                    "value": 50
                                },
                                {
                                    "color": "red",
                                    "value": 70
                                }
                                ]
                            },
                            "unit": "celsius"
                        },
                    },
                    "gridPos": {
                        "h": 8,
                        "w": 12,
                        "x": 0,
                        "y": 8
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "format": 1,
                            "rawQuery": f"SELECT time, device_id, temperature_celsius FROM \"{database_name}\".\"{table_name}\" ORDER BY time ASC",
                        }
                    ],
                    "title": "Device Temperature",
                    "type": "timeseries"
                },
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "defaults": {
                            "thresholds": {
                                "mode": "absolute",
                                "steps": [
                                    {
                                        "color": "green",
                                        "value": None
                                    },
                                    {
                                        "color": "red",
                                        "value": 80
                                    }
                                ]
                            },
                            "unit": "humidity"
                        },
                    },
                    "gridPos": {
                        "h": 8,
                        "w": 12,
                        "x": 12,
                        "y": 8
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "format": 1,
                            "rawQuery": f"SELECT time, device_id, relative_humidity FROM \"{database_name}\".\"{table_name}\" ORDER BY time ASC",
                        }
                    ],
                    "title": "Device Relative Humidity",
                    "type": "timeseries"
                }
            ],
            "time": {
                "from": "now-15m",
                "to": "now"
            },
            "title": "Amazon Timestream for LiveAnalytics Sample Dashboard"
        }
