# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from .data_generator import DataGenerator

class GamingDataGenerator(DataGenerator):
    def __init__(self):
        self.measure_templates = [
            {
                "name": "X",
                "type": "DOUBLE",
                "max_variation": 2.1,
                "max": 5000.0,
                "min": -5000.0
            },
            {
                "name": "Y",
                "type": "DOUBLE",
                "max_variation": 2.1,
                "max": 5000.0,
                "min": -5000.0
            },
            {
                "name": "Z",
                "type": "DOUBLE",
                "max_variation": 2.1,
                "max": 5000.0,
                "min": -5000.0
            },
            {
                "name": "health",
                "type": "BIGINT",
                "max_variation": 60,
                "max": 100,
                "min": 1
            },
            {
                "name": "ping",
                "type": "BIGINT",
                "max_variation": 10,
                "max": 250,
                "min": 25
            },
            {
                "name": "current_equip_value",
                "type": "BIGINT",
                "max_variation": 150,
                "max": 1000000,
                "min": 10
            },
            {
                "name": "flash_duration",
                "type": "DOUBLE",
                "max_variation": 0.3,
                "max": 10.0,
                "min": 0.0
            },
            {
                "name": "pitch",
                "type": "DOUBLE",
                "max_variation": 20.0,
                "max": 90.0,
                "min": -90.0
            },
            {
                "name": "yaw",
                "type": "DOUBLE",
                "max_variation": 0.8,
                "max": 360.0,
                "min": 0.0
            }
        ]
        self.dimension_templates = [
            {
                "name": "player_id",
                "value_length": 25
            },
            {
                "name": "player_name",
                "value_length": 15
            },
            {
                "name": "clan",
                "random_options": ["mosdeff", "green_berets", "golden_ducks", "roberts", "club_z"]
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
                                        "color": "red",
                                        "value": None
                                    },
                                    {
                                        "color": "green",
                                        "value": 35
                                    }
                                ]
                            }
                        }
                    },
                    "gridPos": {
                        "h": 8,
                        "w": 4,
                        "x": 0,
                        "y": 0
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, health, player_id FROM \"{database_name}\".\"{table_name}\" WHERE player_id = '$Player' ORDER BY time ASC"
                        }
                    ],
                    "title": "Health",
                    "type": "stat"
                },
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "defaults": {
                            "unit": "currencyUSD"
                        }
                    },
                    "gridPos": {
                        "h": 8,
                        "w": 4,
                        "x": 4,
                        "y": 0
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, current_equip_value, player_id FROM \"{database_name}\".\"{table_name}\" WHERE player_id = '$Player' ORDER BY time ASC"
                        }
                    ],
                    "title": "Equip Value",
                    "type": "stat"
                },
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "defaults": {
                            "color": {
                                "fixedColor": "purple",
                                "mode": "shades"
                            },
                            "unit": "ms"
                        }
                    },
                    "gridPos": {
                        "h": 8,
                        "w": 7,
                        "x": 8,
                        "y": 0
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, ping, player_id FROM \"{database_name}\".\"{table_name}\" WHERE player_id = '$Player' ORDER BY time ASC"
                        }
                    ],
                    "title": "Ping",
                    "type": "timeseries"
                },
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "defaults": {
                            "custom": {
                                "show": "lines"
                            }
                        }
                    },
                    "gridPos": {
                        "h": 20,
                        "w": 8,
                        "x": 0,
                        "y": 8
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, X, Z, player_id FROM \"{database_name}\".\"{table_name}\" WHERE player_id = '$Player' ORDER BY time ASC"
                        }
                    ],
                    "title": "Coordinates",
                    "type": "xychart"
                },
                {
                    "datasource": grafana_data_source_name,
                    "gridPos": {
                        "h": 19,
                        "w": 7,
                        "x": 8,
                        "y": 8
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, Y, player_id FROM \"{database_name}\".\"{table_name}\" WHERE player_id = '$Player' ORDER BY time ASC"
                        }
                    ],
                    "title": "Elevation",
                    "type": "timeseries"
                },
                {
                    "datasource": grafana_data_source_name,
                    "gridPos": {
                        "h": 19,
                        "w": 8,
                        "x": 0,
                        "y": 28
                    },
                    "options": {
                        "cellValues": {
                            "unit": "s"
                        },
                        "color": {
                            "mode": "scheme",
                            "scheme": "RdYlGn"
                        }
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, flash_duration, player_id FROM \"{database_name}\".\"{table_name}\" WHERE player_id = '$Player' ORDER BY time ASC"
                        }
                    ],
                    "title": "Flash Duration",
                    "type": "heatmap"
                }
            ],
            "templating": {
                "list": [
                    {
                        "datasource": grafana_data_source_name,
                        "definition": f"SELECT DISTINCT player_id FROM \"{database_name}\".\"{table_name}\"",
                        "name": "Player",
                        "query": f"SELECT DISTINCT player_id FROM \"{database_name}\".\"{table_name}\"",
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
