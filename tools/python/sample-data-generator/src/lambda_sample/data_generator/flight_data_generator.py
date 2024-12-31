from .data_generator import DataGenerator

class FlightDataGenerator(DataGenerator):
    def __init__(self):
        self.measure_templates = [
            {
                "name": "fuel_level_gallons",
                "type": "BIGINT",
                "max_variation": 40,
                "max": 8000,
                "min": 1
            },
            {
                "name": "heading",
                "type": "BIGINT",
                "max_variation": 10,
                "max": 360,
                "min": 0
            },
            {
                "name": "air_temperature_celsius",
                "type": "DOUBLE",
                "max_variation": 2.0,
                "max": 40.0,
                "min": -90.0
            },
            {
                "name": "lat",
                "type": "DOUBLE",
                "max_variation": 0.5,
                "max": 90.0,
                "min": -90.0
            },
            {
                "name": "lon",
                "type": "DOUBLE",
                "max_variation": 0.5,
                "max": 180.0,
                "min": -180.0
            },
            {
                "name": "speed_knots",
                "type": "BIGINT",
                "max_variation": 10,
                "max": 250,
                "min": 200
            }
        ]
        self.dimension_templates = [
            {
                "name": "flight_id",
                "unique_options": ["FL123", "FL456", "FL890", "FL333", "FL100", "FL650", "FL256", "FL430", "FL211", "FL874"]
            },
            {
                "name": "aircraft_type",
                "random_options": ["Boeing 737", "Airbus A320"]
            }
        ]

    def generate(self, start_date, end_date, reporting_frequency, num_entities, precision="MILLISECONDS", generate_unique_options_fallback=False) -> list:
        num_flight_ids = len(self.dimension_templates[0]["unique_options"])
        if num_entities > num_flight_ids and not generate_unique_options_fallback:
            raise Exception(f"num_entities ({num_entities}) was greater than the number of flight IDs ({num_flight_ids})")
        return super().generate(start_date, end_date, reporting_frequency, num_entities, precision, generate_unique_options_fallback)
    
    def generate_dashboard(self, grafana_data_source_name, database_name, table_name) -> dict:
        return {
            "panels": [
                {
                    "datasource": grafana_data_source_name,
                    "gridPos": {
                        "h": 22,
                        "w": 20,
                        "x": 0,
                        "y": 0
                    },
                    "options": {
                        "layers": [
                            {
                                "name": "Layer 1",
                                "type": "route"
                            }
                        ],
                        "view": {
                            "id": "fit",
                            "lat": 0,
                            "lon": 0,
                            "padding": 75,
                            "zoom": 15
                        }
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, \"lat\", \"lon\", \"flight_id\" FROM \"{database_name}\".\"{table_name}\" WHERE \"flight_id\" = '$Flight' ORDER BY time ASC"
                        }
                    ],
                    "title": "Flight Path",
                    "type": "geomap"
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
                                        "color": "red",
                                        "value": None
                                    },
                                    {
                                        "color": "green",
                                        "value": 100
                                    }
                                ]
                            },
                            "unit": "gallons"
                        }
                    },
                    "gridPos": {
                        "h": 8,
                        "w": 5,
                        "x": 0,
                        "y": 22
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, \"fuel_level_gallons\", \"flight_id\" FROM \"{database_name}\".\"{table_name}\" WHERE flight_id = '$Flight' ORDER BY time ASC"
                        }
                    ],
                    "title": "Fuel Level",
                    "type": "stat"
                },
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "defaults": {
                            "unit": "degree"
                        }
                    },
                    "gridPos": {
                        "h": 8,
                        "w": 5,
                        "x": 5,
                        "y": 22
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, \"heading\", \"flight_id\", \"aircraft_type\" FROM \"{database_name}\".\"{table_name}\" WHERE \"flight_id\" = '$Flight' ORDER BY time ASC"
                        }
                    ],
                    "title": "Heading",
                    "type": "stat"
                },
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "defaults": {
                            "unit": "celsius"
                        }
                    },
                    "gridPos": {
                        "h": 8,
                        "w": 5,
                        "x": 10,
                        "y": 22
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, \"air_temperature_celsius\", \"flight_id\" FROM \"{database_name}\".\"{table_name}\" WHERE \"flight_id\" = '$Flight' ORDER BY time ASC"
                        }
                    ],
                    "title": "Air Temperature",
                    "type": "stat"
                },
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "defaults": {
                            "unit": "velocityknot"
                        }
                    },
                    "gridPos": {
                        "h": 8,
                        "w": 5,
                        "x": 15,
                        "y": 22
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, \"speed_knots\", \"flight_id\" FROM \"{database_name}\".\"{table_name}\" WHERE \"flight_id\" = '$Flight' ORDER BY time ASC"
                        }
                    ],
                    "title": "Speed",
                    "type": "stat"
                }
            ],
            "templating": {
                "list": [
                    {

                        "definition": f"SELECT DISTINCT flight_id FROM \"{database_name}\".\"{table_name}\"",
                        "name": "Flight",
                        "query": f"SELECT DISTINCT flight_id FROM \"{database_name}\".\"{table_name}\"",
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
