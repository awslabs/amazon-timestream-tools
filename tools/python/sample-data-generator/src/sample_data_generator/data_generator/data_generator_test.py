import pytest
import random
from datetime import datetime, timedelta
import sys

sys.path.append('../../')
from sample_data_generator.data_generator import *

@pytest.fixture
def config_factory():
    def _factory(data_generator_type, **kwargs):
        random.seed(42)  # Set a fixed seed for reproducibility
        gen = data_generator_type(**kwargs)
        start_date = datetime(2024, 1, 1, 12, 0, 0)
        end_date = datetime(2024, 1, 1, 12, 10, 0) # 10 minutes
        reporting_frequency = timedelta(minutes=1)
        num_entities = 5
        return {
            "generator": gen,
            "start_date": start_date,
            "end_date": end_date,
            "reporting_frequency": reporting_frequency,
            "num_entities": num_entities,
        }
    return _factory

class TestAirQualityDataGenerator:
    def test_initialization(self, config_factory):
        """
        Test that the generator initializes measure_templates and dimension_templates correctly.
        """
        config = config_factory(AirQualityDataGenerator)
        gen = config["generator"]
        assert len(gen.measure_templates) == 5, f"Expected 5 measure templates, got {len(gen.measure_templates)}."
        assert len(gen.dimension_templates) == 1, f"Expected 1 dimension template, got {len(gen.measure_templates)}."

    def test_generate_records(self, config_factory):
        """
        Test the generate method to ensure it produces the correct number of records and entities.
        """
        config = config_factory(AirQualityDataGenerator)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]
        num_entities = config["num_entities"]

        records = gen.generate(
            start_date=start_date,
            end_date=end_date,
            reporting_frequency=reporting_frequency,
            num_entities=num_entities,
            precision="SECONDS",
            generate_unique_options_fallback=False
        )

        expected_num_records = 11 * num_entities # 11 minutes * num_entities
        assert len(records) == expected_num_records, f"Should generate {expected_num_records} records, got {len(records)}."

        # Check dimensions and measures in the first record
        first_record = records[0]
        assert len(first_record["Measures"]) == 5, f"Expected 5 measures, got {len(first_record['Measures'])}."
        assert len(first_record["Dimensions"]) == 1, f"Expected 1 dimension, got {len(first_record['Dimensions'])}."

    def test_generate_exceeds_unique_cities_without_fallback(self, config_factory):
        """
        Test that an exception is raised when num_entities exceeds available unique cities
        and generate_unique_options_fallback is False.
        """
        config = config_factory(AirQualityDataGenerator)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]

        num_unique_cities = len(gen.dimension_templates[0]["unique_options"])
        with pytest.raises(Exception) as exc_info:
            gen.generate(
                start_date=start_date,
                end_date=end_date,
                reporting_frequency=reporting_frequency,
                num_entities=num_unique_cities + 1,
                precision="SECONDS",
                generate_unique_options_fallback=False
            )
        assert "num_entities" in str(exc_info.value), "Exception should mention 'num_entities'."

    def test_generate_with_fallback(self, config_factory):
        """
        Test that generate_unique_options_fallback allows more entities than unique cities.
        """
        config = config_factory(AirQualityDataGenerator)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]

        num_unique_cities = len(gen.dimension_templates[0]["unique_options"])
        num_entities = num_unique_cities + 5

        records = gen.generate(
            start_date=start_date,
            end_date=end_date,
            reporting_frequency=reporting_frequency,
            num_entities=num_entities,
            precision="SECONDS",
            generate_unique_options_fallback=True
        )

        expected_num_records = 11 * num_entities
        assert len(records) == expected_num_records, f"Should generate {expected_num_records} records with fallback, got {len(records)}."

    def test_generate_dashboard(self, config_factory):
        """
        Test that the generator produces a dashboard with required elements.
        """
        config = config_factory(AirQualityDataGenerator)
        gen = config["generator"]
        mock_data_source_name = "timestream_data_source_name"
        mock_database_name = "mock_database"
        mock_table_name = "mock_table"
        dashboard = gen.generate_dashboard(grafana_data_source_name=mock_data_source_name,
                                           database_name=mock_database_name, table_name=mock_table_name)

        # panels is a required key for the dashboard to function
        assert "panels" in dashboard, "Dashboard should contain a 'panels' key."
        # title is a required key for a dashboard-creation request to succeed
        assert "title" in dashboard, "Dashboard should contain a 'title' key." 

        # The number of panels has no relationship to dimension templates or measure templates, they are custom.
        assert len(dashboard["panels"]) == 1, f"Expected 1 panel, got {len(dashboard['panels'])}."

class TestCustomDataGenerator:
    def test_initialization(self, config_factory):
        """
        Test that the generator initializes measure_templates and dimension_templates correctly.
        """
        measure_templates = [
            {
                "name": "measure_template_1",
                "type": "DOUBLE",
                "max_variation": 1.0,
                "direction": Direction.BIDIRECTIONAL,
                "max": 90.0,
                "min": 0.62
            },
            {
                "name": "measure_template_2",
                "type": "BIGINT",
                "max_variation": 10,
                "direction": Direction.UP,
                "max": 300,
                "min": 0
            },
            {
                "name": "measure_template_3",
                "type": "BIGINT",
                "max_variation": 10,
                "direction": Direction.DOWN,
                "max": 0,
                "min": -100
            }
        ]

        dimension_templates = [
            {
                "name": "dimension_template_1",
                "unique_options": [
                    "lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit"
                ]
            },
            {
                "name": "dimension_template_2"
            }
        ]

        config = config_factory(CustomDataGenerator, measure_templates=measure_templates, dimension_templates=dimension_templates)
        gen = config["generator"]
        assert len(gen.measure_templates) == 3, f"Expected 3 measure templates, got {len(gen.measure_templates)}."
        assert len(gen.dimension_templates) == 2, f"Expected 2 dimension templates, got {len(gen.dimension_templates)}."

    def test_generate_records(self, config_factory):
        """
        Test the generate method to ensure it produces the correct number of records and entities.
        """
        measure_templates = [
            {
                "name": "measure_template_1",
                "type": "DOUBLE",
                "max_variation": 1.0,
                "direction": Direction.BIDIRECTIONAL,
                "max": 90.0,
                "min": 0.62
            },
            {
                "name": "measure_template_2",
                "type": "BIGINT",
                "max_variation": 10,
                "direction": Direction.UP,
                "max": 300,
                "min": 0
            },
            {
                "name": "measure_template_3",
                "type": "BIGINT",
                "max_variation": 10,
                "direction": Direction.DOWN,
                "max": 0,
                "min": -100
            }
        ]

        dimension_templates = [
            {
                "name": "dimension_template_1",
                "unique_options": [
                    "lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit"
                ]
            },
            {
                "name": "dimension_template_2"
            }
        ]

        config = config_factory(CustomDataGenerator, measure_templates=measure_templates, dimension_templates=dimension_templates)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]
        num_entities = config["num_entities"]

        records = gen.generate(
            start_date=start_date,
            end_date=end_date,
            reporting_frequency=reporting_frequency,
            num_entities=num_entities,
            precision="SECONDS",
            generate_unique_options_fallback=False
        )

        expected_num_records = 11 * num_entities # 11 minutes * num_entities
        assert len(records) == expected_num_records, f"Should generate {expected_num_records} records, got {len(records)}."

        # Check dimensions and measures in the first record
        first_record = records[0]
        assert len(first_record["Measures"]) == 3, f"Expected 3 measures, got {len(first_record['Measures'])}."
        assert len(first_record["Dimensions"]) == 2, f"Expected 2 dimensions, got {len(first_record['Dimensions'])}."

    def test_generate_dashboard(self, config_factory):
        """
        Test that the generator produces a dashboard with required elements.
        """
        measure_templates = [
            {
                "name": "measure_template_1",
                "type": "DOUBLE",
                "max_variation": 1.0,
                "direction": Direction.BIDIRECTIONAL,
                "max": 90.0,
                "min": 0.62
            },
            {
                "name": "measure_template_2",
                "type": "BIGINT",
                "max_variation": 10,
                "direction": Direction.UP,
                "max": 300,
                "min": 0
            },
            {
                "name": "measure_template_3",
                "type": "BIGINT",
                "max_variation": 10,
                "direction": Direction.DOWN,
                "max": 0,
                "min": -100
            }
        ]

        dimension_templates = [
            {
                "name": "dimension_template_1",
                "unique_options": [
                    "lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit"
                ]
            },
            {
                "name": "dimension_template_2"
            }
        ]

        config = config_factory(CustomDataGenerator, measure_templates=measure_templates, dimension_templates=dimension_templates)
        gen = config["generator"]
        mock_data_source_name = "timestream_data_source_name"
        mock_database_name = "mock_database"
        mock_table_name = "mock_table"
        dashboard = gen.generate_dashboard(grafana_data_source_name=mock_data_source_name,
                                           database_name=mock_database_name, table_name=mock_table_name)

        # panels is a required key for the dashboard to function
        assert "panels" in dashboard, "Dashboard should contain a 'panels' key."
        # title is a required key for a dashboard-creation request to succeed
        assert "title" in dashboard, "Dashboard should contain a 'title' key." 

        # CustomDataGenerator is the only data generator where the number of panels equals
        # the number of measure templates
        assert len(dashboard["panels"]) == len(gen.measure_templates), f"Expected {len(gen.measure_templates)} panels, got {len(dashboard['panels'])}"

class TestDevOpsDataGenerator:
    def test_initialization(self, config_factory):
        """
        Test that the generator initializes measure_templates and dimension_templates correctly.
        """
        config = config_factory(DevOpsDataGenerator)
        gen = config["generator"]
        assert len(gen.measure_templates) == 5, "Should have 5 measure templates."
        assert len(gen.dimension_templates) == 2, "Should have 2 dimension templates."

    def test_generate_records(self, config_factory):
        """
        Test the generate method to ensure it produces the correct number of records and entities.
        """
        config = config_factory(DevOpsDataGenerator)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]
        num_entities = config["num_entities"]

        records = gen.generate(
            start_date=start_date,
            end_date=end_date,
            reporting_frequency=reporting_frequency,
            num_entities=num_entities,
            precision="SECONDS",
            generate_unique_options_fallback=False
        )

        expected_num_records = 11 * num_entities # 11 minutes * num_entities
        assert len(records) == expected_num_records, f"Should generate {expected_num_records} records, got {len(records)}."

        # Check dimensions and measures in the first record
        first_record = records[0]
        assert len(first_record["Measures"]) == 5, f"Expected 5 measures, got {len(first_record['Measures'])}."
        assert len(first_record["Dimensions"]) == 2, f"Expected 2 dimensions, got {len(first_record['Dimensions'])}."

    def test_generate_dashboard(self, config_factory):
        """
        Test that the generator produces a dashboard with required elements.
        """
        config = config_factory(DevOpsDataGenerator)
        gen = config["generator"]
        mock_data_source_name = "timestream_data_source_name"
        mock_database_name = "mock_database"
        mock_table_name = "mock_table"
        dashboard = gen.generate_dashboard(grafana_data_source_name=mock_data_source_name,
                                           database_name=mock_database_name, table_name=mock_table_name)

        # panels is a required key for the dashboard to function
        assert "panels" in dashboard, "Dashboard should contain a 'panels' key."
        # title is a required key for a dashboard-creation request to succeed
        assert "title" in dashboard, "Dashboard should contain a 'title' key." 

        # The number of panels has no relationship to dimension templates or measure templates, they are custom.
        assert len(dashboard["panels"]) == 3, f"Expected 3 panels, got {len(dashboard['panels'])}"

class TestEnergyDataGenerator:
    def test_initialization(self, config_factory):
        """
        Test that the generator initializes measure_templates and dimension_templates correctly.
        """
        config = config_factory(EnergyDataGenerator)
        gen = config["generator"]
        assert len(gen.measure_templates) == 4, f"Expected 4 measure templates, got {len(gen.measure_templates)}."
        assert len(gen.dimension_templates) == 1, f"Expected 1 dimension template, got {len(gen.measure_templates)}."

    def test_generate_records(self, config_factory):
        """
        Test the generate method to ensure it produces the correct number of records and entities.
        """
        config = config_factory(EnergyDataGenerator)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]
        num_entities = config["num_entities"]

        records = gen.generate(
            start_date=start_date,
            end_date=end_date,
            reporting_frequency=reporting_frequency,
            num_entities=num_entities,
            precision="SECONDS",
            generate_unique_options_fallback=False
        )

        expected_num_records = 11 * num_entities # 11 minutes * num_entities
        assert len(records) == expected_num_records, f"Should generate {expected_num_records} records, got {len(records)}."

        # Check dimensions and measures in the first record
        first_record = records[0]
        assert len(first_record["Measures"]) == 4, f"Expected 4 measures, got {len(first_record['Measures'])}."
        assert len(first_record["Dimensions"]) == 1, f"Expected 1 dimension, got {len(first_record['Dimensions'])}."

    def test_generate_dashboard(self, config_factory):
        """
        Test that the generator produces a dashboard with required elements.
        """
        config = config_factory(EnergyDataGenerator)
        gen = config["generator"]
        mock_data_source_name = "timestream_data_source_name"
        mock_database_name = "mock_database"
        mock_table_name = "mock_table"
        dashboard = gen.generate_dashboard(grafana_data_source_name=mock_data_source_name,
                                           database_name=mock_database_name, table_name=mock_table_name)

        # panels is a required key for the dashboard to function
        assert "panels" in dashboard, "Dashboard should contain a 'panels' key."
        # title is a required key for a dashboard-creation request to succeed
        assert "title" in dashboard, "Dashboard should contain a 'title' key." 

        # The number of panels has no relationship to dimension templates or measure templates, they are custom.
        assert len(dashboard["panels"]) == 4, f"Expected 4 panels, got {len(dashboard['panels'])}."

class TestExchangeRateDataGenerator:
    def test_initialization(self, config_factory):
        """
        Test that the generator initializes measure_templates and dimension_templates correctly.
        """
        config = config_factory(ExchangeRateDataGenerator)
        gen = config["generator"]
        assert len(gen.measure_templates) == 1, f"Expected 1 measure template, got {len(gen.measure_templates)}."
        assert len(gen.dimension_templates) == 1, f"Expected 1 dimension template, got {len(gen.measure_templates)}."

    def test_generate_records(self, config_factory):
        """
        Test the generate method to ensure it produces the correct number of records and entities.
        """
        config = config_factory(ExchangeRateDataGenerator)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]
        num_entities = config["num_entities"]

        records = gen.generate(
            start_date=start_date,
            end_date=end_date,
            reporting_frequency=reporting_frequency,
            num_entities=num_entities,
            precision="SECONDS",
            generate_unique_options_fallback=False
        )

        expected_num_records = 11 * num_entities # 11 minutes * num_entities
        assert len(records) == expected_num_records, f"Should generate {expected_num_records} records, got {len(records)}."

        # Check dimensions and measures in the first record
        first_record = records[0]
        assert len(first_record["Measures"]) == 1, f"Expected 1 measure, got {len(first_record['Measures'])}."
        assert len(first_record["Dimensions"]) == 1, f"Expected 1 dimension, got {len(first_record['Dimensions'])}."

    def test_generate_exceeds_unique_currency_pairs_without_fallback(self, config_factory):
        """
        Test that an exception is raised when num_entities exceeds available unique currency pairs
        and generate_unique_options_fallback is False.
        """
        config = config_factory(ExchangeRateDataGenerator)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]

        num_unique_currency_pairs = len(gen.dimension_templates[0]["unique_options"])
        with pytest.raises(Exception) as exc_info:
            gen.generate(
                start_date=start_date,
                end_date=end_date,
                reporting_frequency=reporting_frequency,
                num_entities=num_unique_currency_pairs + 1,
                precision="SECONDS",
                generate_unique_options_fallback=False
            )
        assert "num_entities" in str(exc_info.value), "Exception should mention 'num_entities'."

    def test_generate_with_fallback(self, config_factory):
        """
        Test that generate_unique_options_fallback allows more entities than unique currency pairs.
        """
        config = config_factory(ExchangeRateDataGenerator)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]

        num_unique_currency_pairs = len(gen.dimension_templates[0]["unique_options"])
        num_entities = num_unique_currency_pairs + 5

        records = gen.generate(
            start_date=start_date,
            end_date=end_date,
            reporting_frequency=reporting_frequency,
            num_entities=num_entities,
            precision="SECONDS",
            generate_unique_options_fallback=True
        )

        expected_num_records = 11 * num_entities
        assert len(records) == expected_num_records, f"Should generate {expected_num_records} records with fallback, got {len(records)}."

    def test_generate_dashboard(self, config_factory):
        """
        Test that the generator produces a dashboard with required elements.
        """
        config = config_factory(ExchangeRateDataGenerator)
        gen = config["generator"]
        mock_data_source_name = "timestream_data_source_name"
        mock_database_name = "mock_database"
        mock_table_name = "mock_table"
        dashboard = gen.generate_dashboard(grafana_data_source_name=mock_data_source_name,
                                           database_name=mock_database_name, table_name=mock_table_name)

        # panels is a required key for the dashboard to function
        assert "panels" in dashboard, "Dashboard should contain a 'panels' key."
        # title is a required key for a dashboard-creation request to succeed
        assert "title" in dashboard, "Dashboard should contain a 'title' key." 

        # The number of panels has no relationship to dimension templates or measure templates, they are custom.
        assert len(dashboard["panels"]) == 1, f"Expected 1 panel, got {len(dashboard['panels'])}."

class TestFlightDataGenerator:
    def test_initialization(self, config_factory):
        """
        Test that the generator initializes airports, measure_templates, and dimension_templates correctly.
        """
        config = config_factory(FlightDataGenerator)
        gen = config["generator"]
        assert len(gen.airports) == 10, f"Expected 10 airports initialized, got {len(gen.airports)}."
        assert len(gen.measure_templates) == 6, f"Expected 6 measure templates, got {len(gen.measure_templates)}."
        assert len(gen.dimension_templates) == 2, f"Expected 2 dimension templates, got {len(gen.dimension_templates)}."

    def test_generate_records(self, config_factory):
        """
        Test the generate method to ensure it produces the correct number of records and entities.
        """
        config = config_factory(FlightDataGenerator)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]
        num_entities = config["num_entities"]

        records = gen.generate(
            start_date=start_date,
            end_date=end_date,
            reporting_frequency=reporting_frequency,
            num_entities=num_entities,
            precision="SECONDS",
            generate_unique_options_fallback=False
        )

        expected_num_records = 11 * num_entities # 11 minutes * num_entities
        assert len(records) == expected_num_records, f"Should generate {expected_num_records} records, got {len(records)}."

        # Check dimensions and measures in the first record
        first_record = records[0]
        assert len(first_record["Measures"]) == 6, f"Expected 6 measures, got {len(first_record['Measures'])}."
        assert len(first_record["Dimensions"]) == 2, f"Expected 2 dimensions, got {len(first_record['Dimensions'])}."

    def test_generate_exceeds_unique_flight_ids_without_fallback(self, config_factory):
        """
        Test that an exception is raised when num_entities exceeds available unique flight IDs
        and generate_unique_options_fallback is False.
        """
        config = config_factory(FlightDataGenerator)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]

        num_unique_flight_ids = len(gen.dimension_templates[0]["unique_options"])
        with pytest.raises(Exception) as exc_info:
            gen.generate(
                start_date=start_date,
                end_date=end_date,
                reporting_frequency=reporting_frequency,
                num_entities=num_unique_flight_ids + 1,
                precision="SECONDS",
                generate_unique_options_fallback=False
            )
        assert "num_entities" in str(exc_info.value), "Exception should mention 'num_entities'."

    def test_generate_with_fallback(self, config_factory):
        """
        Test that generate_unique_options_fallback allows more entities than unique flight IDs.
        """
        config = config_factory(FlightDataGenerator)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]

        num_unique_flight_ids = len(gen.dimension_templates[0]["unique_options"])
        num_entities = num_unique_flight_ids + 5

        records = gen.generate(
            start_date=start_date,
            end_date=end_date,
            reporting_frequency=reporting_frequency,
            num_entities=num_entities,
            precision="SECONDS",
            generate_unique_options_fallback=True
        )

        expected_num_records = 11 * num_entities
        assert len(records) == expected_num_records, f"Should generate {expected_num_records} records with fallback, got {len(records)}."

    def test_calculate_bearing(self, config_factory):
        """
        Test the _calculate_bearing helper method.
        """
        config = config_factory(FlightDataGenerator)
        gen = config["generator"]
        lat1, lon1 = 33.6407, -84.4277  # Atlanta
        lat2, lon2 = 40.0799, 116.6031  # Beijing
        bearing = gen._calculate_bearing(lat1, lon1, lat2, lon2)
        # Bearing from Atlanta to Beijing is approximately 342 degrees
        assert abs(bearing - 342) <= 5, f"Bearing should be approximately 342 degrees, got {bearing} degrees."

    def test_update_entity_position(self, config_factory):
        """
        Test the _update_entity_position method to ensure entity positions are updated correctly.
        """
        config = config_factory(FlightDataGenerator)
        gen = config["generator"]
        reporting_frequency = timedelta(hours=1)
        knot_speed = 500

        entity = {
            "current_lat": 33.6407,
            "current_lon": -84.4277,
            "target_lat": 40.0799,
            "target_lon": 116.6031,
            "latest_measures": {}
        }

        # Simulate the position update
        updated_lat = gen._update_entity_position(
            entity, "lat", reporting_frequency, knot_speed
        )
        updated_lon = gen._update_entity_position(
            entity, "lon", reporting_frequency, knot_speed
        )

        # Check that the positions have been updated
        assert updated_lat != 33.6407, f"Latitude should be updated from 33.6407 to {updated_lat}."
        assert updated_lon != -84.4277, f"Longitude should be updated from -84.4277 to {updated_lon}."

    def test_generate_measure_value(self, config_factory):
        """
        Test the _generate_measure_value method for different measure types.
        """
        config = config_factory(FlightDataGenerator)
        gen = config["generator"]
        entity = {"latest_measures": {}}
        is_first_datapoint = True

        # Test initial fuel_level_gallons
        fuel_measure = next(m for m in gen.measure_templates if m["name"] == "fuel_level_gallons")
        value = gen._generate_measure_value(fuel_measure, entity, is_first_datapoint)
        assert value == 10000, "Initial fuel_level_gallons should be set to initial_value 10000."
        entity["latest_measures"]["fuel_level_gallons"] = value

        # Test subsequent fuel_level_gallons with possible variation
        is_first_datapoint = False
        value = gen._generate_measure_value(fuel_measure, entity, is_first_datapoint)
        # Expected to decrease by up to 80 (as per max_variation)
        assert 9920 <= value < 10000, f"fuel_level_gallons should decrease by up to 80 to between 9920 and 10000, got {value}."

        # Test speed_knots measure variance
        speed_measure = next(m for m in gen.measure_templates if m["name"] == "speed_knots")
        entity["latest_measures"]["speed_knots"] = 450
        value = gen._generate_measure_value(speed_measure, entity, is_first_datapoint=False)
        assert 440 <= value <= 460, f"speed_knots should vary within expected range (440-460), got {value}."

    def test_generate_dashboard(self, config_factory):
        """
        Test that the generator produces a dashboard with required elements.
        """
        config = config_factory(FlightDataGenerator)
        gen = config["generator"]
        mock_data_source_name = "timestream_data_source_name"
        mock_database_name = "mock_database"
        mock_table_name = "mock_table"
        dashboard = gen.generate_dashboard(grafana_data_source_name=mock_data_source_name,
                                           database_name=mock_database_name, table_name=mock_table_name)

        # panels is a required key for the dashboard to function
        assert "panels" in dashboard, "Dashboard should contain a 'panels' key."
        # title is a required key for a dashboard-creation request to succeed
        assert "title" in dashboard, "Dashboard should contain a 'title' key." 

        # The number of panels has no relationship to dimension templates or measure templates, they are custom.
        assert len(dashboard["panels"]) == 5, f"Expected 5 panels, got {len(dashboard['panels'])}."

class TestGamingDataGenerator:
    def test_initialization(self, config_factory):
        """
        Test that the generator initializes measure_templates and dimension_templates correctly.
        """
        config = config_factory(GamingDataGenerator)
        gen = config["generator"]
        assert len(gen.measure_templates) == 9, f"Expected 9 measure templates, got {len(gen.measure_templates)}."
        assert len(gen.dimension_templates) == 3, f"Expected 3 dimension templates, got {len(gen.dimension_templates)}."

    def test_generate_records(self, config_factory):
        """
        Test the generate method to ensure it produces the correct number of records and entities.
        """
        config = config_factory(GamingDataGenerator)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]
        num_entities = config["num_entities"]

        records = gen.generate(
            start_date=start_date,
            end_date=end_date,
            reporting_frequency=reporting_frequency,
            num_entities=num_entities,
            precision="SECONDS",
            generate_unique_options_fallback=False
        )

        expected_num_records = 11 * num_entities # 11 minutes * num_entities
        assert len(records) == expected_num_records, f"Should generate {expected_num_records} records, got {len(records)}."

        # Check dimensions and measures in the first record
        first_record = records[0]
        assert len(first_record["Measures"]) == 9, f"Expected 9 measures, got {len(first_record['Measures'])}."
        assert len(first_record["Dimensions"]) == 3, f"Expected 3 dimensions, got {len(first_record['Dimensions'])}."

    def test_generate_dashboard(self, config_factory):
        """
        Test that the generator produces a dashboard with required elements.
        """
        config = config_factory(GamingDataGenerator)
        gen = config["generator"]
        mock_data_source_name = "timestream_data_source_name"
        mock_database_name = "mock_database"
        mock_table_name = "mock_table"
        dashboard = gen.generate_dashboard(grafana_data_source_name=mock_data_source_name,
                                           database_name=mock_database_name, table_name=mock_table_name)

        # panels is a required key for the dashboard to function
        assert "panels" in dashboard, "Dashboard should contain a 'panels' key."
        # title is a required key for a dashboard-creation request to succeed
        assert "title" in dashboard, "Dashboard should contain a 'title' key." 

        # The number of panels has no relationship to dimension templates or measure templates, they are custom.
        assert len(dashboard["panels"]) == 6, f"Expected 6 panels, got {len(dashboard['panels'])}."

class TestIoTDataGenerator:
    def test_initialization(self, config_factory):
        """
        Test that the generator initializes measure_templates and dimension_templates correctly.
        """
        config = config_factory(IoTDataGenerator)
        gen = config["generator"]
        assert len(gen.measure_templates) == 4, f"Expected 4 measure templates, got {len(gen.measure_templates)}."
        assert len(gen.dimension_templates) == 1, f"Expected 1 dimension template, got {len(gen.measure_templates)}."

    def test_generate_records(self, config_factory):
        """
        Test the generate method to ensure it produces the correct number of records and entities.
        """
        config = config_factory(IoTDataGenerator)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]
        num_entities = config["num_entities"]

        records = gen.generate(
            start_date=start_date,
            end_date=end_date,
            reporting_frequency=reporting_frequency,
            num_entities=num_entities,
            precision="SECONDS",
            generate_unique_options_fallback=False
        )

        expected_num_records = 11 * num_entities # 11 minutes * num_entities
        assert len(records) == expected_num_records, f"Should generate {expected_num_records} records, got {len(records)}."

        # Check dimensions and measures in the first record
        first_record = records[0]
        assert len(first_record["Measures"]) == 4, f"Expected 5 measures, got {len(first_record['Measures'])}."
        assert len(first_record["Dimensions"]) == 1, f"Expected 1 dimension, got {len(first_record['Dimensions'])}."

    def test_generate_dashboard(self, config_factory):
        """
        Test that the generator produces a dashboard with required elements.
        """
        config = config_factory(IoTDataGenerator)
        gen = config["generator"]
        mock_data_source_name = "timestream_data_source_name"
        mock_database_name = "mock_database"
        mock_table_name = "mock_table"
        dashboard = gen.generate_dashboard(grafana_data_source_name=mock_data_source_name,
                                           database_name=mock_database_name, table_name=mock_table_name)

        # panels is a required key for the dashboard to function
        assert "panels" in dashboard, "Dashboard should contain a 'panels' key."
        # title is a required key for a dashboard-creation request to succeed
        assert "title" in dashboard, "Dashboard should contain a 'title' key." 

        # The number of panels has no relationship to dimension templates or measure templates, they are custom.
        assert len(dashboard["panels"]) == 4, f"Expected 4 panels, got {len(dashboard['panels'])}."

class TestPatientDataGenerator:
    def test_initialization(self, config_factory):
        """
        Test that the generator initializes measure_templates and dimension_templates correctly.
        """
        config = config_factory(PatientDataGenerator)
        gen = config["generator"]
        assert len(gen.measure_templates) == 3, f"Expected 3 measure templates, got {len(gen.measure_templates)}."
        assert len(gen.dimension_templates) == 1, f"Expected 1 dimension template, got {len(gen.measure_templates)}."

    def test_generate_records(self, config_factory):
        """
        Test the generate method to ensure it produces the correct number of records and entities.
        """
        config = config_factory(PatientDataGenerator)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]
        num_entities = config["num_entities"]

        records = gen.generate(
            start_date=start_date,
            end_date=end_date,
            reporting_frequency=reporting_frequency,
            num_entities=num_entities,
            precision="SECONDS",
            generate_unique_options_fallback=False
        )

        expected_num_records = 11 * num_entities # 11 minutes * num_entities
        assert len(records) == expected_num_records, f"Should generate {expected_num_records} records, got {len(records)}."

        # Check dimensions and measures in the first record
        first_record = records[0]
        assert len(first_record["Measures"]) == 3, f"Expected 3 measures, got {len(first_record['Measures'])}."
        assert len(first_record["Dimensions"]) == 1, f"Expected 1 dimension, got {len(first_record['Dimensions'])}."

    def test_generate_dashboard(self, config_factory):
        """
        Test that the generator produces a dashboard with required elements.
        """
        config = config_factory(PatientDataGenerator)
        gen = config["generator"]
        mock_data_source_name = "timestream_data_source_name"
        mock_database_name = "mock_database"
        mock_table_name = "mock_table"
        dashboard = gen.generate_dashboard(grafana_data_source_name=mock_data_source_name,
                                           database_name=mock_database_name, table_name=mock_table_name)

        # panels is a required key for the dashboard to function
        assert "panels" in dashboard, "Dashboard should contain a 'panels' key."
        # title is a required key for a dashboard-creation request to succeed
        assert "title" in dashboard, "Dashboard should contain a 'title' key." 

        # The number of panels has no relationship to dimension templates or measure templates, they are custom.
        assert len(dashboard["panels"]) == 3, f"Expected 3 panels, got {len(dashboard['panels'])}."

class TestStockMarketDataGenerator:
    def test_initialization(self, config_factory):
        """
        Test that the generator initializes measure_templates and dimension_templates correctly.
        """
        config = config_factory(StockMarketDataGenerator)
        gen = config["generator"]
        assert len(gen.measure_templates) == 4, f"Expected 4 measure templates, got {len(gen.measure_templates)}."
        assert len(gen.dimension_templates) == 1, f"Expected 1 dimension template, got {len(gen.measure_templates)}."

    def test_generate_records(self, config_factory):
        """
        Test the generate method to ensure it produces the correct number of records and entities.
        """
        config = config_factory(StockMarketDataGenerator)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]
        num_entities = config["num_entities"]

        records = gen.generate(
            start_date=start_date,
            end_date=end_date,
            reporting_frequency=reporting_frequency,
            num_entities=num_entities,
            precision="SECONDS",
            generate_unique_options_fallback=False
        )

        expected_num_records = 11 * num_entities # 11 minutes * num_entities
        assert len(records) == expected_num_records, f"Should generate {expected_num_records} records, got {len(records)}."

        # Check dimensions and measures in the first record
        first_record = records[0]
        assert len(first_record["Measures"]) == 4, f"Expected 5 measures, got {len(first_record['Measures'])}."
        assert len(first_record["Dimensions"]) == 1, f"Expected 1 dimension, got {len(first_record['Dimensions'])}."

    def test_generate_exceeds_unique_stock_symbols_without_fallback(self, config_factory):
        """
        Test that an exception is raised when num_entities exceeds available unique stock
        market symbols and generate_unique_options_fallback is False.
        """
        config = config_factory(StockMarketDataGenerator)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]

        num_unique_stock_market_symbols = len(gen.dimension_templates[0]["unique_options"])
        with pytest.raises(Exception) as exc_info:
            gen.generate(
                start_date=start_date,
                end_date=end_date,
                reporting_frequency=reporting_frequency,
                num_entities=num_unique_stock_market_symbols + 1,
                precision="SECONDS",
                generate_unique_options_fallback=False
            )
        assert "num_entities" in str(exc_info.value), "Exception should mention 'num_entities'."

    def test_generate_with_fallback(self, config_factory):
        """
        Test that generate_unique_options_fallback allows more entities than unique stock market symbols.
        """
        config = config_factory(StockMarketDataGenerator)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]

        num_unique_stock_market_symbols = len(gen.dimension_templates[0]["unique_options"])
        num_entities = num_unique_stock_market_symbols + 5

        records = gen.generate(
            start_date=start_date,
            end_date=end_date,
            reporting_frequency=reporting_frequency,
            num_entities=num_entities,
            precision="SECONDS",
            generate_unique_options_fallback=True
        )

        expected_num_records = 11 * num_entities
        assert len(records) == expected_num_records, f"Should generate {expected_num_records} records with fallback, got {len(records)}."

    def test_generate_dashboard(self, config_factory):
        """
        Test that the generator produces a dashboard with required elements.
        """
        config = config_factory(StockMarketDataGenerator)
        gen = config["generator"]
        mock_data_source_name = "timestream_data_source_name"
        mock_database_name = "mock_database"
        mock_table_name = "mock_table"
        dashboard = gen.generate_dashboard(grafana_data_source_name=mock_data_source_name,
                                           database_name=mock_database_name, table_name=mock_table_name)

        # panels is a required key for the dashboard to function
        assert "panels" in dashboard, "Dashboard should contain a 'panels' key."
        # title is a required key for a dashboard-creation request to succeed
        assert "title" in dashboard, "Dashboard should contain a 'title' key." 

        # The number of panels has no relationship to dimension templates or measure templates, they are custom.
        assert len(dashboard["panels"]) == 1, f"Expected 1 panel, got {len(dashboard['panels'])}."

class TestWeatherDataGenerator:
    def test_initialization(self, config_factory):
        """
        Test that the generator initializes measure_templates and dimension_templates correctly.
        """
        config = config_factory(WeatherDataGenerator)
        gen = config["generator"]
        assert len(gen.measure_templates) == 7, f"Expected 7 measure templates, got {len(gen.measure_templates)}."
        assert len(gen.dimension_templates) == 1, f"Expected 1 dimension template, got {len(gen.measure_templates)}."

    def test_generate_records(self, config_factory):
        """
        Test the generate method to ensure it produces the correct number of records and entities.
        """
        config = config_factory(WeatherDataGenerator)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]
        num_entities = config["num_entities"]

        records = gen.generate(
            start_date=start_date,
            end_date=end_date,
            reporting_frequency=reporting_frequency,
            num_entities=num_entities,
            precision="SECONDS",
            generate_unique_options_fallback=False
        )

        expected_num_records = 11 * num_entities # 11 minutes * num_entities
        assert len(records) == expected_num_records, f"Should generate {expected_num_records} records, got {len(records)}."

        # Check dimensions and measures in the first record
        first_record = records[0]
        assert len(first_record["Measures"]) == 7, f"Expected 7 measures, got {len(first_record['Measures'])}."
        assert len(first_record["Dimensions"]) == 1, f"Expected 1 dimension, got {len(first_record['Dimensions'])}."

    def test_generate_exceeds_unique_locations_without_fallback(self, config_factory):
        """
        Test that an exception is raised when num_entities exceeds available unique locations
        and generate_unique_options_fallback is False.
        """
        config = config_factory(WeatherDataGenerator)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]

        num_unique_locations = len(gen.dimension_templates[0]["unique_options"])
        with pytest.raises(Exception) as exc_info:
            gen.generate(
                start_date=start_date,
                end_date=end_date,
                reporting_frequency=reporting_frequency,
                num_entities=num_unique_locations + 1,
                precision="SECONDS",
                generate_unique_options_fallback=False
            )
        assert "num_entities" in str(exc_info.value), "Exception should mention 'num_entities'."

    def test_generate_with_fallback(self, config_factory):
        """
        Test that generate_unique_options_fallback allows more entities than unique locations.
        """
        config = config_factory(WeatherDataGenerator)
        gen = config["generator"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        reporting_frequency = config["reporting_frequency"]

        num_unique_locations = len(gen.dimension_templates[0]["unique_options"])
        num_entities = num_unique_locations + 5

        records = gen.generate(
            start_date=start_date,
            end_date=end_date,
            reporting_frequency=reporting_frequency,
            num_entities=num_entities,
            precision="SECONDS",
            generate_unique_options_fallback=True
        )

        expected_num_records = 11 * num_entities
        assert len(records) == expected_num_records, f"Should generate {expected_num_records} records with fallback, got {len(records)}."

    def test_generate_dashboard(self, config_factory):
        """
        Test that the generator produces a dashboard with required elements.
        """
        config = config_factory(WeatherDataGenerator)
        gen = config["generator"]
        mock_data_source_name = "timestream_data_source_name"
        mock_database_name = "mock_database"
        mock_table_name = "mock_table"
        dashboard = gen.generate_dashboard(grafana_data_source_name=mock_data_source_name,
                                           database_name=mock_database_name, table_name=mock_table_name)

        # panels is a required key for the dashboard to function
        assert "panels" in dashboard, "Dashboard should contain a 'panels' key."
        # title is a required key for a dashboard-creation request to succeed
        assert "title" in dashboard, "Dashboard should contain a 'title' key." 

        # The number of panels has no relationship to dimension templates or measure templates, they are custom.
        assert len(dashboard["panels"]) == 7, f"Expected 7 panels, got {len(dashboard['panels'])}."
