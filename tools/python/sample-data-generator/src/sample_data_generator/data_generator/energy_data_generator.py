# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from .data_generator import DataGenerator

class EnergyDataGenerator(DataGenerator):
    def __init__(self):
        self.measure_templates = [
            {
                "name": "energy_usage_kWh",
                "type": "BIGINT",
                "max_variation": 50,
                "max": 300,
                "min": 10
            },
            {
                "name": "occupancy",
                "type": "BIGINT",
                "max_variation": 20,
                "max": 100,
                "min": 0
            },
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
            }
        ]
        self.dimension_templates = [
            {
                "name": "building_id",
                "value_length": 14
            }
        ]
    
    def generate_dashboard(self, grafana_data_source_name, database_name, table_name) -> dict:
        return {
            "panels": [
                {
                    "datasource": grafana_data_source_name,
                    "gridPos": {
                        "h": 9,
                        "w": 9,
                        "x": 0,
                        "y": 0
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "format": 1,
                            "rawQuery": f"SELECT time, \"occupancy\", \"building_id\" FROM \"{database_name}\".\"{table_name}\" ORDER BY time ASC",
                        }
                    ],
                    "title": "Building Occupancy",
                    "type": "stat"
                },
                {
                    "datasource": grafana_data_source_name,
                    "gridPos": {
                        "h": 22,
                        "w": 18,
                        "x": 0,
                        "y": 9
                    },
                    "options": {
                        "cellValues": {
                            "unit": "kwatth"
                        },
                        "color": {
                            "exponent": 0.5,
                            "fill": "dark-orange",
                            "mode": "scheme",
                            "reverse": False,
                            "scale": "exponential",
                            "scheme": "RdYlGn",
                            "steps": 64
                        },
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "format": 1,
                            "rawQuery": f"SELECT time, \"energy_usage_kWh\", \"building_id\" FROM \"{database_name}\".\"{table_name}\" ORDER BY time ASC",
                        }
                    ],
                    "title": "Energy Usage",
                    "type": "heatmap"
                },
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "defaults": {
                            "color": {
                                "mode": "thresholds"
                            },
                            "custom": {
                                "fillOpacity": 6
                            },
                            "thresholds": {
                                "mode": "absolute",
                                "steps": [
                                    {
                                        "color": "blue",
                                        "value": None
                                    },
                                    {
                                        "color": "green",
                                        "value": 20
                                    },
                                    {
                                        "color": "red",
                                        "value": 35
                                    }
                                ]
                            },
                            "unit": "celsius"
                        }
                    },
                    "gridPos": {
                        "h": 17,
                        "w": 18,
                        "x": 0,
                        "y": 31
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "format": 1,
                            "rawQuery": f"SELECT time, \"temperature_celsius\", \"building_id\" FROM \"{database_name}\".\"{table_name}\" ORDER BY time ASC"
                        }
                    ],
                    "title": "Building Temperature",
                    "type": "timeseries"
                },
                {
                    "datasource": grafana_data_source_name,
                    "gridPos": {
                        "h": 17,
                        "w": 18,
                        "x": 0,
                        "y": 48
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "format": 1,
                            "rawQuery": f"SELECT time, \"relative_humidity\", \"building_id\" FROM \"{database_name}\".\"{table_name}\" ORDER BY time ASC"
                        }
                    ],
                    "title": "Building Relative Humidity",
                    "type": "timeseries"
                }
            ],
            "time": {
                "from": "now-15m",
                "to": "now"
            },
            "title": "Amazon Timestream for LiveAnalytics Sample Dashboard"
        }
