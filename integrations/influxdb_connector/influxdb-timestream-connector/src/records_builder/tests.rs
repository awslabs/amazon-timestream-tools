// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

use super::build_records;
use crate::metric::{FieldValue, Metric};
use anyhow::Error;
use aws_sdk_timestreamwrite as timestream_write;
use std::env;

/// Tests single measure for multi-measure record.
#[tokio::test]
async fn test_mtmm_single_record() -> Result<(), Error> {
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

    let table_grouped_records = build_records(
        &multi_table_multi_measure_builder,
        &metrics,
        &timestream_write::types::TimeUnit::Nanoseconds,
    )
    .await?;
    assert_eq!(table_grouped_records.len(), 1);

    let attribute_grouped_records_vec = table_grouped_records
        .get("readings")
        .expect("Failed to get readings table group");

    let common_attributes = &attribute_grouped_records_vec
        .first()
        .expect("Failed to get common attributes")
        .common_attributes;
    let first_record = &attribute_grouped_records_vec
        .first()
        .expect("Failed to get record group")
        .records
        .first()
        .expect("Failed to get the first record");

    assert_eq!(first_record.time, Some(String::from("1577836800000")));

    assert_eq!(
        common_attributes.measure_name(),
        Some("influxdb-connector-measure")
    );
    assert_eq!(
        common_attributes.measure_value_type(),
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
    assert!(common_attributes.dimensions().contains(
        &timestream_write::types::Dimension::builder()
            .name(String::from("goal"))
            .value(String::from("baseline"))
            .build()
            .expect("Failed to build dimension")
    ));

    Ok(())
}

/// Tests dataset all going to the same table.
#[tokio::test]
async fn test_mtmm_single_destination() -> Result<(), Error> {
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

    let table_grouped_records = build_records(
        &multi_table_multi_measure_builder,
        &metrics,
        &timestream_write::types::TimeUnit::Nanoseconds,
    )
    .await?;
    assert_eq!(table_grouped_records.len(), 1);

    let attribute_grouped_records_vec = table_grouped_records
        .get("readings")
        .expect("Failed to get readings table group");

    // The records share common attributes (measure name, dimensions,
    // measure value type, and time unit)
    assert_eq!(attribute_grouped_records_vec.len(), 1);

    let common_attributes = &attribute_grouped_records_vec
        .first()
        .expect("Failed to get common attributes")
        .common_attributes;
    let first_record = &attribute_grouped_records_vec
        .first()
        .expect("Failed to get the first record group")
        .records
        .first()
        .expect("Failed to get the first record");
    let second_record = &attribute_grouped_records_vec
        .first()
        .expect("Failed to get the second record group")
        .records
        .get(1)
        .expect("Failed to get the second record");

    assert_eq!(first_record.time, Some(String::from("1577836800000")));
    assert_eq!(second_record.time, Some(String::from("1577836900032")));

    assert_eq!(
        common_attributes.measure_name(),
        Some("influxdb-connector-measure")
    );
    assert_eq!(
        common_attributes.measure_value_type(),
        Some(&timestream_write::types::MeasureValueType::Multi)
    );
    assert!(common_attributes.dimensions().contains(
        &timestream_write::types::Dimension::builder()
            .name(String::from("goal"))
            .value(String::from("baseline"))
            .build()
            .expect("Failed to build dimension")
    ));
    assert!(first_record.measure_values().contains(
        &timestream_write::types::MeasureValue::builder()
            .name(String::from("incline"))
            .value(String::from("125"))
            .r#type(timestream_write::types::MeasureValueType::Bigint)
            .build()
            .expect("Failed to build measure")
    ));
    assert!(second_record.measure_values().contains(
        &timestream_write::types::MeasureValue::builder()
            .name(String::from("incline"))
            .value(String::from("150"))
            .r#type(timestream_write::types::MeasureValueType::Bigint)
            .build()
            .expect("Failed to build measure")
    ));

    Ok(())
}

/// Tests dataset going to multiple table destinations.
#[tokio::test]
async fn test_mtmm_multi_record() -> Result<(), Error> {
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

    let table_grouped_records = build_records(
        &multi_table_multi_measure_builder,
        &metrics,
        &timestream_write::types::TimeUnit::Nanoseconds,
    )
    .await?;
    assert_eq!(table_grouped_records.len(), 2);

    let readings_attribute_grouped_records_vec = table_grouped_records
        .get("readings")
        .expect("Failed to get readings table group");
    let readings_common_attributes = &readings_attribute_grouped_records_vec
        .first()
        .expect("Failed to get the readings common attributes")
        .common_attributes;
    let readings_record = &readings_attribute_grouped_records_vec
        .first()
        .expect("Failed to get the readings record group")
        .records
        .first()
        .expect("Failed to get the readings record");

    let velocity_attribute_grouped_records_vec = table_grouped_records
        .get("velocity")
        .expect("Failed to get velocity table group");
    let velocity_common_attributes = &velocity_attribute_grouped_records_vec
        .first()
        .expect("Failed to get the velocity common attributes")
        .common_attributes;
    let velocity_record = &velocity_attribute_grouped_records_vec
        .first()
        .expect("Failed to get the velocity record group")
        .records
        .first()
        .expect("Failed to get the velocity record");

    assert_eq!(readings_record.time, Some(String::from("1577836800000")));
    assert_eq!(velocity_record.time, Some(String::from("1577836911132")));

    assert_eq!(
        readings_common_attributes.measure_name(),
        Some("influxdb-connector-measure")
    );
    assert_eq!(
        velocity_common_attributes.measure_name(),
        Some("influxdb-connector-measure")
    );
    assert_eq!(
        readings_common_attributes.measure_value_type(),
        Some(&timestream_write::types::MeasureValueType::Multi)
    );
    assert_eq!(
        velocity_common_attributes.measure_value_type(),
        Some(&timestream_write::types::MeasureValueType::Multi)
    );
    assert!(readings_record.measure_values().contains(
        &timestream_write::types::MeasureValue::builder()
            .name(String::from("incline"))
            .value(String::from("125"))
            .r#type(timestream_write::types::MeasureValueType::Bigint)
            .build()
            .expect("Failed to build measure")
    ));
    assert!(readings_common_attributes.dimensions().contains(
        &timestream_write::types::Dimension::builder()
            .name(String::from("goal"))
            .value(String::from("baseline"))
            .build()
            .expect("Failed to build dimension")
    ));
    assert!(velocity_record.measure_values().contains(
        &timestream_write::types::MeasureValue::builder()
            .name(String::from("km/h"))
            .value(String::from("4.6"))
            .r#type(timestream_write::types::MeasureValueType::Double)
            .build()
            .expect("Failed to build measure")
    ));
    assert!(velocity_common_attributes.dimensions().contains(
        &timestream_write::types::Dimension::builder()
            .name(String::from("goal"))
            .value(String::from("baseline"))
            .build()
            .expect("Failed to build dimension")
    ));

    Ok(())
}

/// Tests dataset with empty dimensions.
#[tokio::test]
async fn test_mtmm_empty_dimensions() -> Result<(), Error> {
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

    let table_grouped_records = build_records(
        &multi_table_multi_measure_builder,
        &metrics,
        &timestream_write::types::TimeUnit::Nanoseconds,
    )
    .await?;
    assert_eq!(table_grouped_records.len(), 1);

    let attribute_grouped_records_vec = table_grouped_records
        .get("readings")
        .expect("Failed to get readings table group");
    let common_attributes = &attribute_grouped_records_vec
        .first()
        .expect("Failed to get common attributes")
        .common_attributes;
    let first_record = &attribute_grouped_records_vec
        .first()
        .expect("Failed to get the first record group")
        .records
        .first()
        .expect("Failed to get the first record");

    assert_eq!(first_record.time, Some(String::from("1577836800000")));

    assert_eq!(
        common_attributes.measure_name(),
        Some("influxdb-connector-measure")
    );
    assert_eq!(
        common_attributes.measure_value_type(),
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
    assert!(common_attributes.dimensions().is_empty());

    Ok(())
}

/// Tests varying timestamp parsing.
#[tokio::test]
async fn test_mtmm_varying_timestamp_records() -> Result<(), Error> {
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

    let table_grouped_records = build_records(
        &multi_table_multi_measure_builder,
        &metrics,
        &timestream_write::types::TimeUnit::Nanoseconds,
    )
    .await?;
    assert_eq!(table_grouped_records.len(), 2);

    let readings_attribute_grouped_records_vec = table_grouped_records
        .get("readings")
        .expect("Failed to get readings table group");
    let readings_common_attributes = &readings_attribute_grouped_records_vec
        .first()
        .expect("Failed to get the readings common attributes")
        .common_attributes;
    let readings_record = &readings_attribute_grouped_records_vec
        .first()
        .expect("Failed to get the readings record group")
        .records
        .first()
        .expect("Failed to get the readings record");

    assert_eq!(readings_record.time, Some(String::from("1577836866658")));
    assert_eq!(
        readings_common_attributes.measure_name(),
        Some("influxdb-connector-measure")
    );
    assert_eq!(
        readings_common_attributes.measure_value_type(),
        Some(&timestream_write::types::MeasureValueType::Multi)
    );
    assert!(readings_record.measure_values().contains(
        &timestream_write::types::MeasureValue::builder()
            .name(String::from("incline"))
            .value(String::from("125"))
            .r#type(timestream_write::types::MeasureValueType::Bigint)
            .build()
            .expect("Failed to build measure")
    ));
    assert!(readings_common_attributes.dimensions().contains(
        &timestream_write::types::Dimension::builder()
            .name(String::from("goal"))
            .value(String::from("baseline"))
            .build()
            .expect("Failed to build dimension")
    ));

    let velocity_attribute_grouped_records_vec = table_grouped_records
        .get("velocity")
        .expect("Failed to get velocity table group");
    let velocity_common_attributes = &velocity_attribute_grouped_records_vec
        .first()
        .expect("Failed to get the velocity common attributes")
        .common_attributes;
    let velocity_record = &velocity_attribute_grouped_records_vec
        .first()
        .expect("Failed to get the velocity record group")
        .records
        .first()
        .expect("Failed to get the velocity record");

    assert_eq!(velocity_record.time, Some(String::from("1577836911132")));
    assert_eq!(
        velocity_common_attributes.measure_name(),
        Some("influxdb-connector-measure")
    );
    assert_eq!(
        velocity_common_attributes.measure_value_type(),
        Some(&timestream_write::types::MeasureValueType::Multi)
    );

    assert!(velocity_record.measure_values().contains(
        &timestream_write::types::MeasureValue::builder()
            .name(String::from("km/h"))
            .value(String::from("4.6"))
            .r#type(timestream_write::types::MeasureValueType::Double)
            .build()
            .expect("Failed to build measure")
    ));

    assert!(velocity_common_attributes.dimensions().contains(
        &timestream_write::types::Dimension::builder()
            .name(String::from("goal"))
            .value(String::from("baseline"))
            .build()
            .expect("Failed to build dimension")
    ));

    Ok(())
}

/// Tests single measure for multi-measure record.
#[tokio::test]
async fn test_stmm_single_record() -> Result<(), Error> {
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

    let table_grouped_records = build_records(
        &single_table_multi_measure_builder,
        &metrics,
        &timestream_write::types::TimeUnit::Nanoseconds,
    )
    .await?;
    assert_eq!(table_grouped_records.len(), 1);

    // Table name should align with environment variable
    let attribute_grouped_records_vec = table_grouped_records
        .get("influxdb-measures")
        .expect("Failed to get influxdb-measures table group");

    let common_attributes = &attribute_grouped_records_vec
        .first()
        .expect("Failed to get common attributes")
        .common_attributes;
    let first_record = &attribute_grouped_records_vec
        .first()
        .expect("Failed to get record group")
        .records
        .first()
        .expect("Failed to get the first record");

    assert_eq!(first_record.time, Some(String::from("1577836800000")));

    assert_eq!(common_attributes.measure_name(), Some("readings"));
    assert_eq!(
        common_attributes.measure_value_type(),
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
    assert!(common_attributes.dimensions().contains(
        &timestream_write::types::Dimension::builder()
            .name(String::from("goal"))
            .value(String::from("baseline"))
            .build()
            .expect("Failed to build dimension")
    ));

    Ok(())
}

/// Tests dataset with differing metric names going to the same table.
#[tokio::test]
async fn test_stmm_multi_record() -> Result<(), Error> {
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

    let table_grouped_records = build_records(
        &single_table_multi_measure_builder,
        &metrics,
        &timestream_write::types::TimeUnit::Nanoseconds,
    )
    .await?;
    // All items should only be going to one table
    assert_eq!(table_grouped_records.len(), 1);

    // Table name should align with environment variable
    let attribute_grouped_records_vec = table_grouped_records
        .get("influxdb-measures")
        .expect("Failed to get influxdb-measures table group");

    // The records do not share common attributes (measure name, dimensions,
    // measure value type, and time unit)
    assert_eq!(attribute_grouped_records_vec.len(), 2);

    let readings_common_attributes = &attribute_grouped_records_vec
        .iter()
        .find(|attribute_grouped_records| {
            attribute_grouped_records.common_attributes.measure_name() == Some("readings")
        })
        .expect("Failed to get the readings attributes group")
        .common_attributes;
    let readings_record = &attribute_grouped_records_vec
        .iter()
        .find(|attribute_grouped_records| {
            attribute_grouped_records.common_attributes.measure_name() == Some("readings")
        })
        .expect("Failed to get the readings attributes group")
        .records
        .first()
        .expect("Failed to get the readings record");

    let velocity_common_attributes = &attribute_grouped_records_vec
        .iter()
        .find(|attribute_grouped_records| {
            attribute_grouped_records.common_attributes.measure_name() == Some("velocity")
        })
        .expect("Failed to get the velocity attributes group")
        .common_attributes;
    let velocity_record = &attribute_grouped_records_vec
        .iter()
        .find(|attribute_grouped_records| {
            attribute_grouped_records.common_attributes.measure_name() == Some("velocity")
        })
        .expect("Failed to get the velocity attributes group")
        .records
        .first()
        .expect("Failed to get the velocity record");

    assert_eq!(readings_record.time, Some(String::from("1577836800000")));
    assert_eq!(velocity_record.time, Some(String::from("1577836911132")));

    assert_eq!(readings_common_attributes.measure_name(), Some("readings"));
    assert_eq!(velocity_common_attributes.measure_name(), Some("velocity"));
    assert_eq!(
        readings_common_attributes.measure_value_type(),
        Some(&timestream_write::types::MeasureValueType::Multi)
    );
    assert_eq!(
        velocity_common_attributes.measure_value_type(),
        Some(&timestream_write::types::MeasureValueType::Multi)
    );
    assert!(readings_record.measure_values().contains(
        &timestream_write::types::MeasureValue::builder()
            .name(String::from("incline"))
            .value(String::from("125"))
            .r#type(timestream_write::types::MeasureValueType::Bigint)
            .build()
            .expect("Failed to build measure")
    ));
    assert!(readings_common_attributes.dimensions().contains(
        &timestream_write::types::Dimension::builder()
            .name(String::from("goal"))
            .value(String::from("baseline"))
            .build()
            .expect("Failed to build dimension")
    ));
    assert!(velocity_record.measure_values().contains(
        &timestream_write::types::MeasureValue::builder()
            .name(String::from("km/h"))
            .value(String::from("4.6"))
            .r#type(timestream_write::types::MeasureValueType::Double)
            .build()
            .expect("Failed to build measure")
    ));
    assert!(velocity_common_attributes.dimensions().contains(
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
