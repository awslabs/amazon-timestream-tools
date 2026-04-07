# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from .data_generator import DataGenerator

class WeatherDataGenerator(DataGenerator):
    def __init__(self):
        self.measure_templates = [
            {
                "name": "temperature_celsius",
                "type": "DOUBLE",
                "max_variation": 1.5,
                "max": 65.0,
                "min": -30
            },
            {
                "name": "relative_humidity",
                "type": "DOUBLE",
                "max_variation": 0.5,
                "max": 100.0,
                "min": 0.0
            },
            {
                "name": "wind_speed_kph",
                "type": "DOUBLE",
                "max_variation": 5.5,
                "max": 407.164,
                "min": 0.0
            },
            {
                "name": "precipitation_mm",
                "type": "DOUBLE",
                "max_variation": 1.5,
                "max": 60.0,
                "min": 0.0
            },
            {
                "name": "cloud_percentage",
                "type": "DOUBLE",
                "max_variation": 10.0,
                "max": 100.0,
                "min": 0.0
            },
            {
                "name": "pressure_hpa",
                "type": "BIGINT",
                "max_variation": 10,
                "max": 1050,
                "min": 950
            },
            {
                "name": "visibility_km",
                "type": "BIGINT",
                "max_variation": 20,
                "max": 200,
                "min": 1
            }
        ]
        self.dimension_templates = [
            {
                "name": "location", 
                "unique_options": ["San Francisco, CA", "Chicago, IL", "New York, NY", "Miami, FL", "Dallas, TX", "Gary, IN", "Las Vegas, NV", "San Diego, CA", "Portland, OR", "Seattle, WA", "New Orleans, LA", "Fargo, ND", "Albuquerque, NM"]
            }
        ]

    def generate(self, start_date, end_date, reporting_frequency, num_entities, precision="MILLISECONDS", generate_unique_options_fallback=False) -> list:
        num_locations = len(self.dimension_templates[0]["unique_options"])
        if num_entities > num_locations and not generate_unique_options_fallback:
            raise Exception(f"num_entities ({num_entities}) was greater than the number of locations ({num_locations})")
        return super().generate(start_date, end_date, reporting_frequency, num_entities, precision, generate_unique_options_fallback)
    
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
                                        "color": "green",
                                        "value": None
                                    },
                                    {
                                        "color": "red",
                                        "value": 40
                                    }
                                ]
                            },
                            "unit": "velocitykmh"
                        }
                    },
                    "gridPos": {
                        "h": 10,
                        "w": 6,
                        "x": 0,
                        "y": 0
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, wind_speed_kph AS \"Wind Speed\", location FROM \"{database_name}\".\"{table_name}\" WHERE location = '$Location' ORDER BY time ASC",
                        }
                    ],
                    "title": "Wind Speed",
                    "type": "stat"
                },
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
                        }
                    },
                    "gridPos": {
                        "h": 10,
                        "w": 6,
                        "x": 6,
                        "y": 0
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, relative_humidity AS \"Relative Humidity\", location FROM \"{database_name}\".\"{table_name}\" WHERE location = '$Location' ORDER BY time ASC",
                        }
                    ],
                    "title": "Relative Humidity",
                    "type": "stat"
                },
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "defaults": {
                            "color": {
                                "fixedColor": "#00d7ff",
                                "mode": "fixed"
                            },
                            "unit": "pressurehpa"
                        }
                    },
                    "gridPos": {
                        "h": 10,
                        "w": 6,
                        "x": 12,
                        "y": 0
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, pressure_hpa AS \"Pressure\", location FROM \"{database_name}\".\"{table_name}\" WHERE location = '$Location' ORDER BY time ASC",
                        }
                    ],
                    "title": "Pressure",
                    "type": "stat"
                },
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "defaults": {
                            "color": {
                                "mode": "thresholds"
                            },
                            "custom": {
                                "fillOpacity": 9
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
                                        "value": 0
                                    },
                                    {
                                        "color": "red",
                                        "value": 30
                                    },
                                    {
                                        "color": "dark-red",
                                        "value": 50
                                    }
                                ]
                            },
                            "unit": "celsius"
                        }
                    },
                    "gridPos": {
                        "h": 16,
                        "w": 11,
                        "x": 0,
                        "y": 10
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, temperature_celsius AS \"Temperature\", location FROM \"{database_name}\".\"{table_name}\" WHERE location = '$Location' ORDER BY time ASC",
                        }
                    ],
                    "title": "Temperature",
                    "type": "timeseries"
                },
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "defaults": {
                            "color": {
                                "mode": "continuous-BlPu"
                            },
                            "custom": {
                                "fillOpacity": 9
                            },
                            "thresholds": {
                                "mode": "absolute",
                                "steps": [
                                    {
                                        "color": "green",
                                        "value": None
                                    },
                                    {
                                        "color": "red",
                                        "value": 45
                                    }
                                ]
                            },
                            "unit": "lengthmm"
                        }
                    },
                    "gridPos": {
                        "h": 16,
                        "w": 11,
                        "x": 11,
                        "y": 10
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, precipitation_mm AS \"Precipitation\", location FROM \"{database_name}\".\"{table_name}\" WHERE location = '$Location' ORDER BY time ASC"
                        }
                    ],
                    "title": "Precipitation",
                    "type": "timeseries"
                },
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "defaults": {
                            "color": {
                                "mode": "continuous-BlPu"
                            },
                            "max": 100,
                            "unit": "percent"
                        }
                    },
                    "gridPos": {
                        "h": 17,
                        "w": 11,
                        "x": 0,
                        "y": 26
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, cloud_percentage AS \"Cloud Percentage\", location FROM \"{database_name}\".\"{table_name}\" WHERE location = '$Location' ORDER BY time ASC"
                        }
                    ],
                    "title": "Cloud Coverage",
                    "type": "timeseries"
                },
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "defaults": {
                            "unit": "lengthkm"
                        }
                    },
                    "gridPos": {
                        "h": 17,
                        "w": 11,
                        "x": 11,
                        "y": 26
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, visibility_km as \"Visibility\", location FROM \"{database_name}\".\"{table_name}\" WHERE location = '$Location' ORDER BY time ASC"
                        }
                    ],
                    "title": "Visibility",
                    "type": "timeseries"
                }
            ],
            "templating": {
                "list": [
                    {
                        "datasource": grafana_data_source_name,
                        "definition": f"SELECT DISTINCT location FROM \"{database_name}\".\"{table_name}\"",
                        "name": "Location",
                        "query": f"SELECT DISTINCT location FROM \"{database_name}\".\"{table_name}\"",
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
