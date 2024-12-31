from .data_generator import DataGenerator

class StockMarketDataGenerator(DataGenerator):
    def __init__(self):
        self.measure_templates = [
            {
                "name": "volume",
                "type": "BIGINT",
                "max_variation": 30000,
                "max": 100000000,
                "min": 1
            },
            {
                "name": "market_cap",
                "type": "BIGINT",
                "max_variation": 100,
                "max": 100000000000,
                "min": 1000000000,
            },
            {
                "name": "price_change",
                "type": "DOUBLE",
                "max_variation": 0.5,
                "max": 1000.0,
                "min": -1000.0
            },
            {
                "name": "percentage_change",
                "type": "DOUBLE",
                "max_variation": 10.0,
                "max": 100.0,
                "min": -100.0
            }
        ]
        self.dimension_templates = [
            {
                "name": "stock_symbol",
                "unique_options": ["AAPL", "TSLA", "DJIA", "SPOT", "NFLX", "MSFT", "MCD", "PG", "KO", "MMM", "IBM", "AMZN", "VZ", "JNJ", "WMT"]
            }
        ]

    def generate(self, start_date, end_date, reporting_frequency, num_entities, precision="MILLISECONDS", generate_unique_options_fallback=False) -> list:
        num_stock_symbols = len(self.dimension_templates[0]["unique_options"])
        if num_entities > num_stock_symbols and not generate_unique_options_fallback:
            raise Exception(f"num_entities ({num_entities}) was greater than the number of stock symbols ({num_stock_symbols})")
        return super().generate(start_date, end_date, reporting_frequency, num_entities, precision, generate_unique_options_fallback)
    
    def generate_dashboard(self, grafana_data_source_name, database_name, table_name) -> dict:
        return {
            "panels": [
                {
                    "datasource": grafana_data_source_name,
                    "fieldConfig": {
                        "overrides": [
                            {
                                "matcher": {
                                    "id": "byName",
                                    "options": "Percentage Change"
                                },
                                "properties": [
                                    {
                                        "id": "custom.cellOptions",
                                        "value": {
                                            "type": "color-text"
                                        }
                                    },
                                    {
                                        "id": "thresholds",
                                        "value": {
                                            "mode": "absolute",
                                            "steps": [
                                                {
                                                    "color": "red",
                                                    "value": None
                                                },
                                                {
                                                    "color": "green",
                                                    "value": 0
                                                }
                                            ]
                                        }
                                    }
                                ]
                            },
                            {
                                "matcher": {
                                    "id": "byName",
                                    "options": "Market Cap"
                                },
                                "properties": [
                                    {
                                        "id": "unit",
                                        "value": "none"
                                    }
                                ]
                            }
                        ]
                    },
                    "gridPos": {
                        "h": 22,
                        "w": 20,
                        "x": 0,
                        "y": 0
                    },
                    "targets": [
                        {
                            "datasource": grafana_data_source_name,
                            "rawQuery": f"SELECT stock_symbol AS \"Name\", market_cap AS \"Market Cap\", percentage_change AS \"Percentage Change\", time AS \"Time\" FROM \"{database_name}\".\"{table_name}\" WHERE time = (SELECT MAX(time) FROM \"{database_name}\".\"{table_name}\" AS sub WHERE sub.stock_symbol = \"{database_name}\".\"{table_name}\".stock_symbol)",
                        }
                    ],
                    "title": "Intraday Stock Market",
                    "type": "table"
                }
            ],
            "time": {
                "from": "now-15m",
                "to": "now"
            },
            "title": "Amazon Timestream for LiveAnalytics Sample Dashboard"
        }
