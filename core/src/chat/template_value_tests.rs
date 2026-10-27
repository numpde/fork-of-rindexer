use super::Template;
use serde_json::{json, Value};

#[test]
fn nested_strings_preserve_quotes_backslashes_and_control_characters() {
    for text in [
        "say \"hello\"",
        r"C:\events\transfers",
        "first line\nsecond line\tvalue\r\n",
        "こんにちは Zoë 🦀",
    ] {
        let data = json!({ "details": { "message": text } });
        let output = Template::new("<{{details.message}}>".into()).parse_template_inline(&data);
        assert_eq!(output, format!("<{text}>"));
    }
}

#[test]
fn arrays_render_as_valid_json_without_losing_string_contents() {
    let values = json!(["Alice", "say \"hello\"", "line\nbreak", 42, true, null]);
    let output =
        Template::new("{{values}}".into()).parse_template_inline(&json!({ "values": values }));
    let decoded: Value =
        serde_json::from_str(&output).expect("rendered array must remain valid JSON");
    assert_eq!(decoded, values);
}

#[test]
fn objects_render_as_valid_json_without_losing_keys_or_nested_values() {
    let record = json!({ "owner": "Alice", "note": "a\"b", "nested": { "path": r"C:\events" } });
    let output =
        Template::new("{{record}}".into()).parse_template_inline(&json!({ "record": record }));
    let decoded: Value =
        serde_json::from_str(&output).expect("rendered object must remain valid JSON");
    assert_eq!(decoded, record);
}

#[test]
fn amount_fallback_preserves_the_original_invalid_string() {
    let value = "invalid \"amount\"\nnext line";
    let output = Template::new("{{format_value(value, 18)}}".into())
        .parse_template_inline(&json!({ "value": value }));
    assert_eq!(output, value);
}

#[test]
fn ordinary_strings_and_non_string_scalars_keep_their_display_values() {
    for (value, expected) in [
        (json!("Alice"), "Alice"),
        (json!(""), ""),
        (json!(42), "42"),
        (json!(true), "true"),
        (Value::Null, "null"),
    ] {
        let output =
            Template::new("{{value}}".into()).parse_template_inline(&json!({ "value": value }));
        assert_eq!(output, expected);
    }
}
