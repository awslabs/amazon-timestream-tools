use super::parse_line_protocol;
use crate::metric::{self, Metric};

/// Determines whether two Metric structs have equal values for all struct fields.
fn metrics_are_equal(actual_metric: &Metric, expected_metric: &Metric) -> bool {
    if actual_metric.name() != expected_metric.name() {
        println!("Metric names are not equal");
        return false;
    }

    if actual_metric.timestamp() != expected_metric.timestamp() {
        println!("Metric timestamps are not equal");
        return false;
    }

    if actual_metric.tags() != expected_metric.tags() {
        println!("Metric Option value of tags did not match")
    }

    if let (Some(actual_tags), Some(expected_tags)) =
        (&actual_metric.tags(), &expected_metric.tags())
    {
        if actual_tags.len() != expected_tags.len() {
            println!("Metric number of tags are not equal");
            return false;
        }

        for (i, (tag_key, tag_value)) in actual_tags.iter().enumerate() {
            if *tag_key != *expected_tags[i].0 {
                println!("Metric tag keys are not equal");
                return false;
            }

            if *tag_value != *expected_tags[i].1 {
                println!("Metric tag values are not equal");
                return false;
            }
        }
    }

    if actual_metric.fields().len() != expected_metric.fields().len() {
        println!("Metric number of fields are not equal");
        return false;
    }

    for (i, (field_key, field_value)) in actual_metric.fields().iter().enumerate() {
        if *field_key != *expected_metric.fields()[i].0 {
            println!("Metric field keys are not equal");
            return false;
        }

        if field_value != &expected_metric.fields()[i].1 {
            println!("Metric field values are not equal");
            return false;
        }
    }

    true
}

/// Tests parsing a single valid line with an integer field value.
#[test]
fn test_parse_field_integer() -> Result<(), String> {
    let lp = String::from("readings incline=125i 1577836800000");

    let expected_metric = Metric::new(
        "readings".to_string(),
        None,
        vec![("incline".to_string(), metric::FieldValue::I64(125))],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    assert!(metrics_are_equal(&output_metrics[0], &expected_metric));
    Ok(())
}

/// Tests parsing a single valid line with a float field value.
#[test]
fn test_parse_field_float() -> Result<(), String> {
    let lp = String::from("readings incline=125 1577836800000");

    let expected_metric = Metric::new(
        "readings".to_string(),
        None,
        vec![("incline".to_string(), metric::FieldValue::F64(125.0))],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    assert!(metrics_are_equal(&output_metrics[0], &expected_metric));
    Ok(())
}

/// Tests parsing a single valid line with a string field value using double quotes.
#[test]
fn test_parse_field_string_double_quote() -> Result<(), String> {
    let lp = String::from("readings incline=\"125\" 1577836800000");

    let expected_metric = Metric::new(
        "readings".to_string(),
        None,
        vec![(
            "incline".to_string(),
            metric::FieldValue::String("125".to_string()),
        )],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    assert!(metrics_are_equal(&output_metrics[0], &expected_metric));
    Ok(())
}

/// Tests parsing a single valid line with a string field value using single quotes.
#[test]
fn test_parse_field_string_single_quote() -> Result<(), String> {
    let lp = String::from("readings incline=\"\'125\'\" 1577836800000");

    let expected_metric = Metric::new(
        "readings".to_string(),
        None,
        vec![(
            "incline".to_string(),
            metric::FieldValue::String("\'125\'".to_string()),
        )],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    assert!(metrics_are_equal(&output_metrics[0], &expected_metric));
    Ok(())
}

/// Tests parsing a single valid line with a boolean field value.
#[test]
fn test_parse_field_boolean() -> Result<(), String> {
    let lp = String::from("readings incline=true 1577836800000");

    let expected_metric = Metric::new(
        "readings".to_string(),
        None,
        vec![("incline".to_string(), metric::FieldValue::Boolean(true))],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    assert!(metrics_are_equal(&output_metrics[0], &expected_metric));
    Ok(())
}

/// Tests parsing a single invalid line with an invalid boolean field value.
#[test]
fn test_parse_field_boolean_invalid() -> Result<(), String> {
    let lp = String::from("readings incline=tree 1577836800000");

    let output_metrics = parse_line_protocol(&lp);

    assert!(output_metrics.is_err());
    Ok(())
}

/// Tests parsing a single valid line where the measurement name includes an unescaped equals sign.
#[test]
fn test_parse_measurement_unescaped_equals() -> Result<(), String> {
    let lp = String::from("read=ings,fleet=Alberta incline=125i,fuel_usage=21.30 1577836800000");

    let expected_metric = Metric::new(
        "read=ings".to_string(),
        Some(vec![("fleet".to_string(), "Alberta".to_string())]),
        vec![
            ("incline".to_string(), metric::FieldValue::I64(125)),
            ("fuel_usage".to_string(), metric::FieldValue::F64(21.30)),
        ],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    assert!(metrics_are_equal(&output_metrics[0], &expected_metric));
    Ok(())
}

/// Tests parsing a single valid line where the measurement name begins with an underscore.
#[test]
fn test_parse_measurement_underscore_begin() -> Result<(), String> {
    let lp = String::from("_readings,fleet=Alberta incline=125i,fuel_usage=21.30 1577836800000");
    let expected_metric = Metric::new(
        "_readings".to_string(),
        Some(vec![("fleet".to_string(), "Alberta".to_string())]),
        vec![
            ("incline".to_string(), metric::FieldValue::I64(125)),
            ("fuel_usage".to_string(), metric::FieldValue::F64(21.30)),
        ],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    assert!(metrics_are_equal(&output_metrics[0], &expected_metric));
    Ok(())
}

/// Tests parsing a single invalid line where fields are missing.
#[test]
fn test_parse_no_fields() -> Result<(), String> {
    let lp = String::from("readings,fleet=Alberta 1577836800000");

    let output_metrics = parse_line_protocol(&lp);

    assert!(output_metrics.is_err());
    Ok(())
}

/// Tests parsing a single valid line with multiple fields.
#[test]
fn test_parse_multiple_fields() -> Result<(), String> {
    let lp = String::from("readings incline=125i,fuel_usage=21.30 1577836800000");

    let expected_metric = Metric::new(
        "readings".to_string(),
        None,
        vec![
            ("incline".to_string(), metric::FieldValue::I64(125)),
            ("fuel_usage".to_string(), metric::FieldValue::F64(21.30)),
        ],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    assert!(metrics_are_equal(&output_metrics[0], &expected_metric));
    Ok(())
}

/// Tests parsing a single invalid line with multiple measurement names.
#[ignore]
#[test]
fn test_parse_multiple_measurements() -> Result<(), String> {
    let lp = String::from(
        "readings,readings2,fleet=Alberta incline=125i,fuel_usage=21.30 1577836800000",
    );

    let output_metrics = parse_line_protocol(&lp);

    assert!(output_metrics.is_err());
    Ok(())
}

/// Tests parsing a single invalid line without a timestamp.
#[test]
fn test_parse_no_timestamp() -> Result<(), String> {
    let lp = String::from("readings,fleet=Alberta incline=125i,fuel_usage=21.30");

    let output_metrics = parse_line_protocol(&lp);

    assert!(output_metrics.is_err());
    Ok(())
}

/// Tests parsing a single invalid line with a non-unix timestamp.
#[test]
fn test_parse_non_unix_timestamp() -> Result<(), String> {
    let lp =
        String::from("readings,fleet=Alberta incline=125i,fuel_usage=21.30 2020-01-01T00:00:00Z");

    let output_metrics = parse_line_protocol(&lp);

    assert!(output_metrics.is_err());
    Ok(())
}

/// Tests parsing a single invalid line with the timestamp in double quotes.
#[test]
fn test_parse_timestamp_with_quotes() -> Result<(), String> {
    let lp = String::from("readings,fleet=Alberta incline=125i,fuel_usage=21.30 \"1577836800000\"");

    let output_metrics = parse_line_protocol(&lp);
    assert!(output_metrics.is_err());
    Ok(())
}

/// Tests parsing a single invalid line with no whitespace between components.
#[test]
fn test_parse_no_whitespace() -> Result<(), String> {
    let lp = String::from("readings,fuel_usage=21.30,2020-01-01T00:00:00Z");

    let output_metrics = parse_line_protocol(&lp);

    assert!(output_metrics.is_err());
    Ok(())
}

/// Tests parsing a single invalid line with multiple timestamps.
#[test]
fn test_parse_multiple_timestamps() -> Result<(), String> {
    let lp = String::from("readings incline=125i 1577836800000000000 1577836800000");

    let output_metrics = parse_line_protocol(&lp);
    assert!(output_metrics.is_err());
    Ok(())
}

/// Tests parsing multiple valid lines with integer field values.
#[test]
fn test_parse_batch() -> Result<(), String> {
    let lp = String::from(
        "readings incline=125i 1577836800000
        readings incline=125i 1577836800000
        readings incline=125i 1577836800000
        readings incline=125i 1577836800000
        readings incline=125i 1577836800000
        readings incline=125i 1577836800000",
    );

    let expected_metric = Metric::new(
        "readings".to_string(),
        None,
        vec![("incline".to_string(), metric::FieldValue::I64(125))],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    for metric in output_metrics {
        assert!(metrics_are_equal(&metric, &expected_metric));
    }
    Ok(())
}

/// Tests parsing a single valid line with emojis included in the measurement name, tag key,
/// tag value, field key, and field value.
#[test]
fn test_parse_emojis() -> Result<(), String> {
    let lp = String::from("re😎dings,fl☕️et=🤙 in🤠line=\"😀\" 1577836800000");

    let expected_metric = Metric::new(
        "re😎dings".to_string(),
        Some(vec![("fl☕️et".to_string(), "🤙".to_string())]),
        vec![(
            "in🤠line".to_string(),
            metric::FieldValue::String("😀".to_string()),
        )],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    assert!(metrics_are_equal(&output_metrics[0], &expected_metric));
    Ok(())
}

/// Tests parsing a single valid line with escaped commas in the measurement name, tag key,
/// tag value, and field key.
#[test]
fn test_parse_escaped_comma() -> Result<(), String> {
    let lp = String::from(r"\,readings,fleet\,=A\,lberta inc\,line=125i 1577836800000");

    let expected_metric = Metric::new(
        ",readings".to_string(),
        Some(vec![("fleet,".to_string(), "A,lberta".to_string())]),
        vec![("inc,line".to_string(), metric::FieldValue::I64(125))],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    assert!(metrics_are_equal(&output_metrics[0], &expected_metric));
    Ok(())
}

/// Tests parsing a single valid line with escaped equals signs in the tag key, tag value,
/// and field key.
#[test]
fn test_parse_escaped_equals() -> Result<(), String> {
    let lp = String::from(r"readings,fleet\==A\=lberta inc\=line=125i 1577836800000");

    let expected_metric = Metric::new(
        "readings".to_string(),
        Some(vec![("fleet=".to_string(), "A=lberta".to_string())]),
        vec![(r"inc=line".to_string(), metric::FieldValue::I64(125))],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    assert!(metrics_are_equal(&output_metrics[0], &expected_metric));
    Ok(())
}

/// Tests parsing a single valid line with an unescaped equals sign in the measurement name.
#[test]
fn test_parse_unescaped_equals_measurement() -> Result<(), String> {
    let lp = String::from(r"rea=dings,fleet=Alberta incline=125i 1577836800000");

    let expected_metric = Metric::new(
        "rea=dings".to_string(),
        Some(vec![("fleet".to_string(), "Alberta".to_string())]),
        vec![(r"incline".to_string(), metric::FieldValue::I64(125))],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    assert!(metrics_are_equal(&output_metrics[0], &expected_metric));
    Ok(())
}

/// Tests parsing a single valid line with an escaped equals sign in the measurement name.
#[test]
fn test_parse_escaped_equals_measurement() -> Result<(), String> {
    let lp = String::from(r"rea\=dings,fleet=Alberta incline=125i 1577836800000");

    let expected_metric = Metric::new(
        "rea\\=dings".to_string(),
        Some(vec![("fleet".to_string(), "Alberta".to_string())]),
        vec![(r"incline".to_string(), metric::FieldValue::I64(125))],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    assert!(metrics_are_equal(&output_metrics[0], &expected_metric));
    Ok(())
}

/// Tests parsing a single valid line with an escaped space in the tag key, tag value,
/// and field key.
#[test]
fn test_parse_escaped_space() -> Result<(), String> {
    let lp = String::from(r"readings,fleet\ =A\ lberta inc\ line=125i 1577836800000");

    let expected_metric = Metric::new(
        "readings".to_string(),
        Some(vec![("fleet ".to_string(), "A lberta".to_string())]),
        vec![(r"inc line".to_string(), metric::FieldValue::I64(125))],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    assert!(metrics_are_equal(&output_metrics[0], &expected_metric));
    Ok(())
}

/// Tests parsing a single valid line with an escaped space in measurement name.
#[test]
fn test_parse_measurement_escaped_space() -> Result<(), String> {
    let lp = String::from(r"read\ ings,fleet=Alberta incline=125i 1577836800000");

    let expected_metric = Metric::new(
        "read ings".to_string(),
        Some(vec![("fleet".to_string(), "Alberta".to_string())]),
        vec![(r"incline".to_string(), metric::FieldValue::I64(125))],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    assert!(metrics_are_equal(&output_metrics[0], &expected_metric));
    Ok(())
}

/// Tests parsing a single valid line with escaped quotes in the field value.
#[test]
fn test_parse_escaped_double_quote_field_value() -> Result<(), String> {
    let lp = String::from("readings,fleet=Alberta incline=\"\\\"test\\\"\" 1577836800000");

    let expected_metric = Metric::new(
        "readings".to_string(),
        Some(vec![("fleet".to_string(), "Alberta".to_string())]),
        vec![(
            r"incline".to_string(),
            metric::FieldValue::String(String::from("\"test\"")),
        )],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    assert!(metrics_are_equal(&output_metrics[0], &expected_metric));
    Ok(())
}

/// Tests parsing a single invalid line with unescaped quotes in the field value.
#[test]
fn test_parse_non_escaped_double_quote_field_value() -> Result<(), String> {
    let lp = String::from("readings,fleet=Alberta incline=\"\"test\"\" 1577836800000");

    let output_metrics = parse_line_protocol(&lp);

    assert!(output_metrics.is_err());
    Ok(())
}

/// Tests parsing a single valid line with escaped backslashes in the field value.
#[test]
fn test_parse_escaped_backslash_field_value() -> Result<(), String> {
    let lp = String::from("readings,fleet=Alberta incline=\"\\\\test\\\\\" 1577836800000");

    let expected_metric = Metric::new(
        "readings".to_string(),
        Some(vec![("fleet".to_string(), "Alberta".to_string())]),
        vec![(
            r"incline".to_string(),
            metric::FieldValue::String(String::from("\\test\\")),
        )],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    assert!(metrics_are_equal(&output_metrics[0], &expected_metric));
    Ok(())
}

/// Tests parsing a single valid line with escaped backslashes in the measurement name, tag key,
/// tag value, and field key.
#[test]
fn test_parse_escaped_backslash_not_field_value() -> Result<(), String> {
    let lp = String::from("read\\\\ings,fl\\\\eet=Al\\\\berta inc\\\\line=125i 1577836800000");

    let expected_metric = Metric::new(
        "read\\ings".to_string(),
        Some(vec![("fl\\eet".to_string(), "Al\\berta".to_string())]),
        vec![("inc\\line".to_string(), metric::FieldValue::I64(125))],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    assert!(metrics_are_equal(&output_metrics[0], &expected_metric));
    Ok(())
}

/// Tests parsing a single valid line with one comment included.
#[test]
fn test_parse_single_point_single_comment() -> Result<(), String> {
    let lp = String::from(
        "# This is a comment
        readings,fleet=Alberta incline=125i,fuel_usage=21.30 1577836800000",
    );

    let expected_metric = Metric::new(
        "readings".to_string(),
        Some(vec![("fleet".to_string(), "Alberta".to_string())]),
        vec![
            ("incline".to_string(), metric::FieldValue::I64(125)),
            ("fuel_usage".to_string(), metric::FieldValue::F64(21.30)),
        ],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    assert!(metrics_are_equal(&output_metrics[0], &expected_metric));
    Ok(())
}

/// Tests parsing a single valid line with two comments included.
#[test]
fn test_parse_single_point_multiple_comments() -> Result<(), String> {
    let lp = String::from(
        "# This is a comment
        # This is another comment
        readings,fleet=Alberta incline=125i,fuel_usage=21.30 1577836800000",
    );

    let expected_metric = Metric::new(
        "readings".to_string(),
        Some(vec![("fleet".to_string(), "Alberta".to_string())]),
        vec![
            ("incline".to_string(), metric::FieldValue::I64(125)),
            ("fuel_usage".to_string(), metric::FieldValue::F64(21.30)),
        ],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    assert!(metrics_are_equal(&output_metrics[0], &expected_metric));
    Ok(())
}

/// Tests parsing two valid lines with one comment included.
#[test]
fn test_parse_multiple_points_single_comment() -> Result<(), String> {
    let lp = String::from(
        "# This is a comment
        readings,fleet=Alberta incline=125i,fuel_usage=21.30 1577836800000
        readings,fleet=Alberta incline=125i,fuel_usage=21.30 1577836800000",
    );

    let expected_metric = Metric::new(
        "readings".to_string(),
        Some(vec![("fleet".to_string(), "Alberta".to_string())]),
        vec![
            ("incline".to_string(), metric::FieldValue::I64(125)),
            ("fuel_usage".to_string(), metric::FieldValue::F64(21.30)),
        ],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    for metric in output_metrics {
        assert!(metrics_are_equal(&metric, &expected_metric));
    }
    Ok(())
}

/// Tests parsing two valid lines with two comments included.
#[test]
fn test_parse_multiple_points_multiple_comments() -> Result<(), String> {
    let lp = String::from(
        "# This is a comment
        # This is another comment
        readings,fleet=Alberta incline=125i,fuel_usage=21.30 1577836800000
        readings,fleet=Alberta incline=125i,fuel_usage=21.30 1577836800000",
    );

    let expected_metric = Metric::new(
        "readings".to_string(),
        Some(vec![("fleet".to_string(), "Alberta".to_string())]),
        vec![
            ("incline".to_string(), metric::FieldValue::I64(125)),
            ("fuel_usage".to_string(), metric::FieldValue::F64(21.30)),
        ],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    for metric in output_metrics {
        assert!(metrics_are_equal(&metric, &expected_metric));
    }
    Ok(())
}

/// Tests parsing one valid line with a nanosecond timestamp.
#[test]
fn test_parse_nanoseconds_timestamp() -> Result<(), String> {
    let lp =
        String::from("readings,fleet=Alberta incline=125i,fuel_usage=21.30 1577836800000000000");

    let expected_metric = Metric::new(
        "readings".to_string(),
        Some(vec![("fleet".to_string(), "Alberta".to_string())]),
        vec![
            ("incline".to_string(), metric::FieldValue::I64(125)),
            ("fuel_usage".to_string(), metric::FieldValue::F64(21.30)),
        ],
        1577836800000000000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    for metric in output_metrics {
        assert!(metrics_are_equal(&metric, &expected_metric));
    }
    Ok(())
}

/// Tests parsing one valid line with a microsecond timestamp.
#[test]
fn test_parse_microseconds_timestamp() -> Result<(), String> {
    let lp = String::from("readings,fleet=Alberta incline=125i,fuel_usage=21.30 1577836800000000");

    let expected_metric = Metric::new(
        "readings".to_string(),
        Some(vec![("fleet".to_string(), "Alberta".to_string())]),
        vec![
            ("incline".to_string(), metric::FieldValue::I64(125)),
            ("fuel_usage".to_string(), metric::FieldValue::F64(21.30)),
        ],
        1577836800000000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    for metric in output_metrics {
        assert!(metrics_are_equal(&metric, &expected_metric));
    }
    Ok(())
}

/// Tests parsing one valid line with a millisecond timestamp.
#[test]
fn test_parse_milliseconds_timestamp() -> Result<(), String> {
    let lp = String::from("readings,fleet=Alberta incline=125i,fuel_usage=21.30 1577836800000");

    let expected_metric = Metric::new(
        "readings".to_string(),
        Some(vec![("fleet".to_string(), "Alberta".to_string())]),
        vec![
            ("incline".to_string(), metric::FieldValue::I64(125)),
            ("fuel_usage".to_string(), metric::FieldValue::F64(21.30)),
        ],
        1577836800000,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    for metric in output_metrics {
        assert!(metrics_are_equal(&metric, &expected_metric));
    }
    Ok(())
}

/// Tests parsing one valid line with a second timestamp.
#[test]
fn test_parse_seconds_timestamp() -> Result<(), String> {
    let lp = String::from("readings,fleet=Alberta incline=125i,fuel_usage=21.30 1577836800");

    let expected_metric = Metric::new(
        "readings".to_string(),
        Some(vec![("fleet".to_string(), "Alberta".to_string())]),
        vec![
            ("incline".to_string(), metric::FieldValue::I64(125)),
            ("fuel_usage".to_string(), metric::FieldValue::F64(21.30)),
        ],
        1577836800,
    );

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    for metric in output_metrics {
        assert!(metrics_are_equal(&metric, &expected_metric));
    }
    Ok(())
}

/// Tests parsing empty line protocol.
#[test]
fn test_parse_empty() -> Result<(), String> {
    let lp = String::new();

    let output_metrics = parse_line_protocol(&lp).expect("Failed to parse line protocol");

    // Should return an empty Vec.
    assert!(output_metrics.is_empty());
    Ok(())
}
