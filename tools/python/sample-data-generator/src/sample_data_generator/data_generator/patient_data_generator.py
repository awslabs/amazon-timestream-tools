# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from .data_generator import DataGenerator

class PatientDataGenerator(DataGenerator):
    def __init__(self):
        self.measure_templates = [
            {
                "name": "heart_rate_bpm",
                "type": "BIGINT",
                "max_variation": 5.0,
                "max": 100,
                "min": 60
            },
            {
                "name": "oxygen_saturation_percentage",
                "type": "DOUBLE",
                "max_variation": 5.0,
                "max": 100.0,
                "min": 60.0
            },
            {
                "name": "temperature_celsius",
                "type": "DOUBLE",
                "max_variation": 2.0,
                "max": 40.0,
                "min": 30.0
            }
        ]
        self.dimension_templates = [
            {
                "name": "patient_id",
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
                            "thresholds": {
                                "mode": "absolute",
                                "steps": [
                                    {
                                        "color": "blue",
                                        "value": None
                                    },
                                    {
                                        "color": "green",
                                        "value": 36.5
                                    },
                                    {
                                        "color": "red",
                                        "value": 37.5
                                    }
                                ]
                            },
                            "unit": "celsius"
                        }
                    },
                    "gridPos": {
                        "h": 11,
                        "w": 8,
                        "x": 0,
                        "y": 0
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, \"temperature_celsius\", \"patient_id\" FROM \"{database_name}\".\"{table_name}\" WHERE patient_id = '$Patient' ORDER BY time ASC"
                        }
                    ],
                    "title": "Body Temperature",
                    "type": "gauge"
                },
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "defaults": {
                        "color": {
                            "mode": "thresholds"
                        },
                        "custom": {
                            "fillOpacity": 7
                        },
                        "displayName": "BPM",
                            "thresholds": {
                                "mode": "absolute",
                                "steps": [
                                    {
                                        "color": "green",
                                        "value": None
                                    },
                                    {
                                        "color": "#EAB839",
                                        "value": 60
                                    },
                                    {
                                        "color": "red",
                                        "value": 80
                                    }
                                ]
                            }
                        }
                    },
                    "gridPos": {
                        "h": 11,
                        "w": 9,
                        "x": 8,
                        "y": 0
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, \"heart_rate_bpm\", \"patient_id\" FROM \"{database_name}\".\"{table_name}\" WHERE patient_id = '$Patient' ORDER BY time ASC"
                        }
                    ],
                    "title": "Heart Rate",
                    "type": "timeseries"
                },
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "defaults": {
                            "color": {
                                "fixedColor": "blue",
                                "mode": "shades"
                            },
                            "unit": "percent"
                        }
                    },
                    "gridPos": {
                        "h": 13,
                        "w": 17,
                        "x": 0,
                        "y": 11
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, \"oxygen_saturation_percentage\", \"patient_id\" FROM \"{database_name}\".\"{table_name}\" WHERE patient_id = '$Patient' ORDER BY time ASC"
                        }
                    ],
                    "title": "Oxygen Saturation",
                    "type": "timeseries"
                }
            ],
            "templating": {
                "list": [
                    {
                        "datasource": grafana_data_source_name,
                        "definition": f"SELECT DISTINCT patient_id FROM \"{database_name}\".\"{table_name}\"",
                        "name": "Patient",
                        "query": f"SELECT DISTINCT patient_id FROM \"{database_name}\".\"{table_name}\"",
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
