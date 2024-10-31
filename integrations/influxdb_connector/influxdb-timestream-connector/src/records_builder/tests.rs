use super::build_records;
use crate::metric::{FieldValue, Metric};
use anyhow::Error;
use aws_sdk_timestreamwrite as timestream_write;
use std::env;

#[test]
fn test_mtmm_single_record() -> Result<(), Error> {
    // Single measure for multi-measure record

    setup_minimal_env_vars();
    setup_multi_table_multi_measure_env_vars();
    setup_table_mapping_env_variables(super::SchemaType::MultiTableMultiMeasure);
    let multi_table_multi_measure_schema = super::SchemaType::MultiTableMultiMeasure;
    let multi_table_multi_measure_builder = super::get_builder(
        multi_table_multi_measure_schema,
        String::from("influxdb-connector-measure"),
    );
    let metrics = [Metric::new(
        "readings".to_string(),
        vec![(String::from("goal"), String::from("baseline"))].into(),
        vec![(String::from("incline"), FieldValue::I64(125))],
        1577836800000,
    )];

    let records = build_records(
        &multi_table_multi_measure_builder,
        &metrics,
        &timestream_write::types::TimeUnit::Nanoseconds,
    )?;
    assert_eq!(records.len(), 1);
    let first_record = records
        .get("readings")
        .expect("Failed to unwrap")
        .first()
        .expect("Failed to unwrap");
    assert_eq!(first_record.time, Some(String::from("1577836800000")));

    assert_eq!(
        first_record.measure_name(),
        Some("influxdb-connector-measure")
    );
    assert_eq!(
        first_record.measure_value_type(),
        Some(&timestream_write::types::MeasureValueType::Multi)
    );
    assert!(first_record.measure_values().contains(
        &timestream_write::types::MeasureValue::builder()
            .name(String::from("incline"))
            .value(String::from("125"))
            .r#type(timestream_write::types::MeasureValueType::Bigint)
            .build()
            .expect("Failed to build measure")
    ));
    assert!(first_record.dimensions().contains(
        &timestream_write::types::Dimension::builder()
            .name(String::from("goal"))
            .value(String::from("baseline"))
            .build()
            .expect("Failed to build dimension")
    ));

    Ok(())
}

#[test]
fn test_mtmm_single_destination() -> Result<(), Error> {
    // Dataset all going to same table

    setup_minimal_env_vars();
    setup_multi_table_multi_measure_env_vars();
    setup_table_mapping_env_variables(super::SchemaType::MultiTableMultiMeasure);
    let multi_table_multi_measure_schema = super::SchemaType::MultiTableMultiMeasure;
    let multi_table_multi_measure_builder = super::get_builder(
        multi_table_multi_measure_schema,
        String::from("influxdb-connector-measure"),
    );
    let metrics = [
        Metric::new(
            "readings".to_string(),
            vec![(String::from("goal"), String::from("baseline"))].into(),
            vec![(String::from("incline"), FieldValue::I64(125))],
            1577836800000,
        ),
        Metric::new(
            "readings".to_string(),
            vec![(String::from("goal"), String::from("baseline"))].into(),
            vec![(String::from("incline"), FieldValue::I64(150))],
            1577836900032,
        ),
    ];

    let records = build_records(
        &multi_table_multi_measure_builder,
        &metrics,
        &timestream_write::types::TimeUnit::Nanoseconds,
    )?;
    assert_eq!(records.len(), 1);
    let readings = records.get("readings").expect("Failed to unwrap");
    let first_record = &readings[0];
    let second_record = &readings[1];
    assert_eq!(first_record.time, Some(String::from("1577836800000")));
    assert_eq!(second_record.time, Some(String::from("1577836900032")));

    assert_eq!(
        first_record.measure_name(),
        Some("influxdb-connector-measure")
    );
    assert_eq!(
        second_record.measure_name(),
        Some("influxdb-connector-measure")
    );
    assert_eq!(
        first_record.measure_value_type(),
        Some(&timestream_write::types::MeasureValueType::Multi)
    );
    assert_eq!(
        second_record.measure_value_type(),
        Some(&timestream_write::types::MeasureValueType::Multi)
    );
    assert!(first_record.measure_values().contains(
        &timestream_write::types::MeasureValue::builder()
            .name(String::from("incline"))
            .value(String::from("125"))
            .r#type(timestream_write::types::MeasureValueType::Bigint)
            .build()
            .expect("Failed to build measure")
    ));
    assert!(first_record.dimensions().contains(
        &timestream_write::types::Dimension::builder()
            .name(String::from("goal"))
            .value(String::from("baseline"))
            .build()
            .expect("Failed to build dimension")
    ));
    assert!(second_record.measure_values().contains(
        &timestream_write::types::MeasureValue::builder()
            .name(String::from("incline"))
            .value(String::from("150"))
            .r#type(timestream_write::types::MeasureValueType::Bigint)
            .build()
            .expect("Failed to build measure")
    ));
    assert!(second_record.dimensions().contains(
        &timestream_write::types::Dimension::builder()
            .name(String::from("goal"))
            .value(String::from("baseline"))
            .build()
            .expect("Failed to build dimension")
    ));

    Ok(())
}

#[test]
fn test_mtmm_multi_record() -> Result<(), Error> {
    // Dataset going to multiple table destinations

    setup_minimal_env_vars();
    setup_multi_table_multi_measure_env_vars();
    setup_table_mapping_env_variables(super::SchemaType::MultiTableMultiMeasure);
    let multi_table_multi_measure_schema = super::SchemaType::MultiTableMultiMeasure;
    let multi_table_multi_measure_builder = super::get_builder(
        multi_table_multi_measure_schema,
        String::from("influxdb-connector-measure"),
    );
    let metrics = [
        Metric::new(
            "readings".to_string(),
            vec![(String::from("goal"), String::from("baseline"))].into(),
            vec![(String::from("incline"), FieldValue::I64(125))],
            1577836800000,
        ),
        Metric::new(
            "velocity".to_string(),
            vec![(String::from("goal"), String::from("baseline"))].into(),
            vec![(String::from("km/h"), FieldValue::F64(4.6))],
            1577836911132,
        ),
    ];

    let records = build_records(
        &multi_table_multi_measure_builder,
        &metrics,
        &timestream_write::types::TimeUnit::Nanoseconds,
    )?;
    assert_eq!(records.len(), 2);
    let readings = records
        .get("readings")
        .expect("Failed to unwrap")
        .first()
        .expect("Failed to unwrap");
    let velocity = records
        .get("velocity")
        .expect("Failed to unwrap")
        .first()
        .expect("Failed to unwrap");
    assert_eq!(readings.time, Some(String::from("1577836800000")));
    assert_eq!(velocity.time, Some(String::from("1577836911132")));

    assert_eq!(readings.measure_name(), Some("influxdb-connector-measure"));
    assert_eq!(velocity.measure_name(), Some("influxdb-connector-measure"));
    assert_eq!(
        readings.measure_value_type(),
        Some(&timestream_write::types::MeasureValueType::Multi)
    );
    assert_eq!(
        velocity.measure_value_type(),
        Some(&timestream_write::types::MeasureValueType::Multi)
    );
    assert!(readings.measure_values().contains(
        &timestream_write::types::MeasureValue::builder()
            .name(String::from("incline"))
            .value(String::from("125"))
            .r#type(timestream_write::types::MeasureValueType::Bigint)
            .build()
            .expect("Failed to build measure")
    ));
    assert!(readings.dimensions().contains(
        &timestream_write::types::Dimension::builder()
            .name(String::from("goal"))
            .value(String::from("baseline"))
            .build()
            .expect("Failed to build dimension")
    ));
    assert!(velocity.measure_values().contains(
        &timestream_write::types::MeasureValue::builder()
            .name(String::from("km/h"))
            .value(String::from("4.6"))
            .r#type(timestream_write::types::MeasureValueType::Double)
            .build()
            .expect("Failed to build measure")
    ));
    assert!(velocity.dimensions().contains(
        &timestream_write::types::Dimension::builder()
            .name(String::from("goal"))
            .value(String::from("baseline"))
            .build()
            .expect("Failed to build dimension")
    ));

    Ok(())
}

#[test]
fn test_mtmm_empty_dimensions() -> Result<(), Error> {
    // Dataset with empty dimensions

    setup_minimal_env_vars();
    setup_multi_table_multi_measure_env_vars();
    setup_table_mapping_env_variables(super::SchemaType::MultiTableMultiMeasure);
    let multi_table_multi_measure_schema = super::SchemaType::MultiTableMultiMeasure;
    let multi_table_multi_measure_builder = super::get_builder(
        multi_table_multi_measure_schema,
        String::from("influxdb-connector-measure"),
    );
    let metrics = [Metric::new(
        "readings".to_string(),
        None,
        vec![(String::from("incline"), FieldValue::I64(125))],
        1577836800000,
    )];

    let records = build_records(
        &multi_table_multi_measure_builder,
        &metrics,
        &timestream_write::types::TimeUnit::Nanoseconds,
    )?;
    assert_eq!(records.len(), 1);
    let readings = records.get("readings").expect("Failed to unwrap");
    let first_record = &readings[0];
    assert_eq!(first_record.time, Some(String::from("1577836800000")));

    assert_eq!(
        first_record.measure_name(),
        Some("influxdb-connector-measure")
    );
    assert_eq!(
        first_record.measure_value_type(),
        Some(&timestream_write::types::MeasureValueType::Multi)
    );
    assert!(first_record.measure_values().contains(
        &timestream_write::types::MeasureValue::builder()
            .name(String::from("incline"))
            .value(String::from("125"))
            .r#type(timestream_write::types::MeasureValueType::Bigint)
            .build()
            .expect("Failed to build measure")
    ));
    assert!(first_record.dimensions().is_empty());

    Ok(())
}

#[test]
fn test_mtmm_varying_timestamp_records() -> Result<(), Error> {
    // Varying timestamp parsing

    setup_minimal_env_vars();
    setup_multi_table_multi_measure_env_vars();
    setup_table_mapping_env_variables(super::SchemaType::MultiTableMultiMeasure);
    let multi_table_multi_measure_schema = super::SchemaType::MultiTableMultiMeasure;
    let multi_table_multi_measure_builder = super::get_builder(
        multi_table_multi_measure_schema,
        String::from("influxdb-connector-measure"),
    );
    let metrics = [
        Metric::new(
            "readings".to_string(),
            vec![(String::from("goal"), String::from("baseline"))].into(),
            vec![(String::from("incline"), FieldValue::I64(125))],
            1577836866658,
        ),
        Metric::new(
            "velocity".to_string(),
            vec![(String::from("goal"), String::from("baseline"))].into(),
            vec![(String::from("km/h"), FieldValue::F64(4.6))],
            1577836911132,
        ),
    ];

    let records = build_records(
        &multi_table_multi_measure_builder,
        &metrics,
        &timestream_write::types::TimeUnit::Nanoseconds,
    )?;
    assert_eq!(records.len(), 2);

    let first_record = records
        .get("readings")
        .expect("Failed to unwrap")
        .first()
        .expect("Failed to unwrap");

    assert_eq!(first_record.time, Some(String::from("1577836866658")));
    assert_eq!(
        first_record.measure_name(),
        Some("influxdb-connector-measure")
    );
    assert_eq!(
        first_record.measure_value_type(),
        Some(&timestream_write::types::MeasureValueType::Multi)
    );
    assert!(first_record.measure_values().contains(
        &timestream_write::types::MeasureValue::builder()
            .name(String::from("incline"))
            .value(String::from("125"))
            .r#type(timestream_write::types::MeasureValueType::Bigint)
            .build()
            .expect("Failed to build measure")
    ));
    assert!(first_record.dimensions().contains(
        &timestream_write::types::Dimension::builder()
            .name(String::from("goal"))
            .value(String::from("baseline"))
            .build()
            .expect("Failed to build dimension")
    ));

    let second_record = records
        .get("velocity")
        .expect("Failed to unwrap")
        .first()
        .expect("Failed to unwrap");

    assert_eq!(second_record.time, Some(String::from("1577836911132")));
    assert_eq!(
        second_record.measure_name(),
        Some("influxdb-connector-measure")
    );
    assert_eq!(
        second_record.measure_value_type(),
        Some(&timestream_write::types::MeasureValueType::Multi)
    );

    assert!(second_record.measure_values().contains(
        &timestream_write::types::MeasureValue::builder()
            .name(String::from("km/h"))
            .value(String::from("4.6"))
            .r#type(timestream_write::types::MeasureValueType::Double)
            .build()
            .expect("Failed to build measure")
    ));

    assert!(second_record.dimensions().contains(
        &timestream_write::types::Dimension::builder()
            .name(String::from("goal"))
            .value(String::from("baseline"))
            .build()
            .expect("Failed to build dimension")
    ));

    Ok(())
}

#[test]
fn test_stmm_single_record() -> Result<(), Error> {
    // Single measure for multi-measure record

    setup_minimal_env_vars();
    setup_table_mapping_env_variables(super::SchemaType::SingleTableMultiMeasure);
    let single_table_multi_measure_builder = super::get_builder(
        super::SchemaType::SingleTableMultiMeasure,
        String::from("influxdb-connector-measure"),
    );
    let metrics = [Metric::new(
        "readings".to_string(),
        vec![(String::from("goal"), String::from("baseline"))].into(),
        vec![(String::from("incline"), FieldValue::I64(125))],
        1577836800000,
    )];

    let records = build_records(
        &single_table_multi_measure_builder,
        &metrics,
        &timestream_write::types::TimeUnit::Nanoseconds,
    )?;
    assert_eq!(records.len(), 1);
    // Table name should align with environment variable
    let first_record = records
        .get("influxdb-measures")
        .expect("Failed to unwrap")
        .first()
        .expect("Failed to unwrap");
    assert_eq!(first_record.time, Some(String::from("1577836800000")));

    assert_eq!(first_record.measure_name(), Some("readings"));
    assert_eq!(
        first_record.measure_value_type(),
        Some(&timestream_write::types::MeasureValueType::Multi)
    );
    assert!(first_record.measure_values().contains(
        &timestream_write::types::MeasureValue::builder()
            .name(String::from("incline"))
            .value(String::from("125"))
            .r#type(timestream_write::types::MeasureValueType::Bigint)
            .build()
            .expect("Failed to build measure")
    ));
    assert!(first_record.dimensions().contains(
        &timestream_write::types::Dimension::builder()
            .name(String::from("goal"))
            .value(String::from("baseline"))
            .build()
            .expect("Failed to build dimension")
    ));

    Ok(())
}

#[test]
fn test_stmm_multi_record() -> Result<(), Error> {
    // Dataset with differing metric names going to the same table

    setup_minimal_env_vars();
    setup_table_mapping_env_variables(super::SchemaType::SingleTableMultiMeasure);
    let single_table_multi_measure_builder = super::get_builder(
        super::SchemaType::SingleTableMultiMeasure,
        String::from("influxdb-connector-measure"),
    );
    let metrics = [
        Metric::new(
            "readings".to_string(),
            vec![(String::from("goal"), String::from("baseline"))].into(),
            vec![(String::from("incline"), FieldValue::I64(125))],
            1577836800000,
        ),
        Metric::new(
            "velocity".to_string(),
            vec![(String::from("goal"), String::from("baseline"))].into(),
            vec![(String::from("km/h"), FieldValue::F64(4.6))],
            1577836911132,
        ),
    ];

    let records = build_records(
        &single_table_multi_measure_builder,
        &metrics,
        &timestream_write::types::TimeUnit::Nanoseconds,
    )?;
    // All items should only be going to one table
    assert_eq!(records.len(), 1);
    // Table name should align with environment variable
    let records_vec = records.get("influxdb-measures").expect("failed to unwrap");
    let readings = records_vec.first().expect("Failed to unwrap");
    let velocity = records_vec.get(1).expect("Failed to unwrap");
    assert_eq!(readings.time, Some(String::from("1577836800000")));
    assert_eq!(velocity.time, Some(String::from("1577836911132")));

    assert_eq!(readings.measure_name(), Some("readings"));
    assert_eq!(velocity.measure_name(), Some("velocity"));
    assert_eq!(
        readings.measure_value_type(),
        Some(&timestream_write::types::MeasureValueType::Multi)
    );
    assert_eq!(
        velocity.measure_value_type(),
        Some(&timestream_write::types::MeasureValueType::Multi)
    );
    assert!(readings.measure_values().contains(
        &timestream_write::types::MeasureValue::builder()
            .name(String::from("incline"))
            .value(String::from("125"))
            .r#type(timestream_write::types::MeasureValueType::Bigint)
            .build()
            .expect("Failed to build measure")
    ));
    assert!(readings.dimensions().contains(
        &timestream_write::types::Dimension::builder()
            .name(String::from("goal"))
            .value(String::from("baseline"))
            .build()
            .expect("Failed to build dimension")
    ));
    assert!(velocity.measure_values().contains(
        &timestream_write::types::MeasureValue::builder()
            .name(String::from("km/h"))
            .value(String::from("4.6"))
            .r#type(timestream_write::types::MeasureValueType::Double)
            .build()
            .expect("Failed to build measure")
    ));
    assert!(velocity.dimensions().contains(
        &timestream_write::types::Dimension::builder()
            .name(String::from("goal"))
            .value(String::from("baseline"))
            .build()
            .expect("Failed to build dimension")
    ));

    Ok(())
}

fn setup_multi_table_multi_measure_env_vars() {
    env::set_var("measure_name_for_multi_measure_records", "influxdb-measure");
}

fn setup_table_mapping_env_variables(schema_type: super::SchemaType) {
    match schema_type {
        super::SchemaType::MultiTableMultiMeasure => {
            env::set_var("table_mapping", "multi-table");
        }
        super::SchemaType::SingleTableMultiMeasure => {
            env::set_var("table_mapping", "single-table");
            env::set_var("single_table_name", "influxdb-measures");
        }
    }
}

fn setup_minimal_env_vars() {
    env::set_var("enable_table_creation", "false");
    env::set_var("region", "us-west-2");
    env::set_var("database_name", "test-database");
    env::set_var("enable_database_creation", "false");
    env::set_var("enable_mag_store_writes", "false");
}
