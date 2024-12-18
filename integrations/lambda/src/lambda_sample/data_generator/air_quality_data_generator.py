from .data_generator import DataGenerator

class AirQualityDataGenerator(DataGenerator):
    def __init__(self):
        self.measure_templates = [
            {
                "name": "PM2.5",
                "type": "DOUBLE",
                "max_variation": 1.0,
                "max": 150.0,
                "min": 15.0
            },
            {
                "name": "PM10",
                "type": "DOUBLE",
                "max_variation": 1.0,
                "max": 150.0,
                "min": 15.0
            },
            {
                "name": "CO_ppm",
                "type": "DOUBLE",
                "max_variation": 5.0,
                "max": 100.0,
                "min": 0.1
            },
            {
                "name": "NO2_ppb",
                "type": "DOUBLE",
                "max_variation": 1.0,
                "max": 300.0,
                "min": 0.5
            },
            {
                "name": "O2_percentage",
                "type": "DOUBLE",
                "max_variation": 1.0,
                "max": 25.0,
                "min": 20.8
            }
        ]
        self.dimension_templates = [
            {
                "name": "city",
                "unique_options": ["Los Angeles", "New York", "Vancouver", "Sydney", "Delhi", "Beijing", "London", "Miami", "Toronto", "Seattle", "Amsterdam"]
            }
        ]

    def generate(self, start_date, end_date, reporting_frequency, num_entities, precision="MILLISECONDS", generate_unique_options_fallback=False) -> list:
        num_cities = len(self.dimension_templates[0]["unique_options"])
        if num_entities > num_cities and not generate_unique_options_fallback:
            raise Exception(f"num_entities ({num_entities}) was greater than the number of cities ({num_cities})")
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
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT time, \"PM2.5\", PM10, CO_ppm AS \"CO PPM\", NO2_ppb AS \"NO2 PPB\", O2_percentage AS \"O2 Percentage\", city FROM \"{database_name}\".\"{table_name}\" WHERE city = '$City' ORDER BY time ASC"
                        }
                    ],
                    "title": "Air Quality",
                    "type": "timeseries"
                }
            ],
            "templating": {
                "list": [
                    {
                        f"definition": f"SELECT DISTINCT city FROM \"{database_name}\".\"{table_name}\"",
                        "name": "City",
                        "query": f"SELECT DISTINCT city FROM \"{database_name}\".\"{table_name}\"",
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
