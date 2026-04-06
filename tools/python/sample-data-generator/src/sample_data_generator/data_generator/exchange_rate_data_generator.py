# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from .data_generator import DataGenerator

class ExchangeRateDataGenerator(DataGenerator):
    def __init__(self):
        self.measure_templates = [
            {
                "name": "exchange_rate",
                "type": "DOUBLE",
                "max_variation": 1.0,
                "max": 90.0,
                "min": 0.62
            }
        ]
        self.dimension_templates = [
            {
                "name": "currency_pair",
                "unique_options": ["USD/EUR", "USD/CAD", "USD/GPP", "USD/CNY", "GBP/CAD", "GBP/JPY", "GBP/INR", "CHF/INR", "XAU/CNY", "UYU/CAD", "USD/XAU"]
            }
        ]

    def generate(self, start_date, end_date, reporting_frequency, num_entities, precision="MILLISECONDS", generate_unique_options_fallback=False) -> list:
        num_currency_pairs = len(self.dimension_templates[0]["unique_options"])
        if num_entities > num_currency_pairs and not generate_unique_options_fallback:
            raise Exception(f"num_entities ({num_entities}) was greater than the number of currency pairs ({num_currency_pairs})")
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
                            "rawQuery": f"SELECT currency_pair AS \"Currency Pair\", \"measure_value::double\" AS \"Exchange Rate\", time AS \"Time\" FROM \"{database_name}\".\"{table_name}\" WHERE time IN (SELECT MAX(time) FROM \"{database_name}\".\"{table_name}\") ORDER BY time ASC"
                        }
                    ],
                    "title": "Currency Exchange Rate",
                    "type": "table"
                }
            ],
            "time": {
                "from": "now-15m",
                "to": "now"
            },
            "title": "Amazon Timestream for LiveAnalytics Sample Dashboard"
        }
