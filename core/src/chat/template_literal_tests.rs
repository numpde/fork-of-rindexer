use super::Template;
use serde_json::json;

#[test]
fn inserted_field_text_is_not_interpreted_as_a_later_placeholder() {
    let data = json!({ "message": "forwarded {{recipient}}", "recipient": "Alice" });
    let output = Template::new("{{message}} / {{recipient}}".into()).parse_template_inline(&data);
    assert_eq!(output, "forwarded {{recipient}} / Alice");
}

#[test]
fn inserted_function_text_is_not_evaluated() {
    let data = json!({ "message": "{{format_value(amount, 18)}}", "amount": "1" });
    let output = Template::new("{{message}} / {{format_value(amount, 18)}}".into())
        .parse_template_inline(&data);
    assert_eq!(output, "{{format_value(amount, 18)}} / 0.000000000000000001");
}

#[test]
fn repeated_source_placeholders_do_not_reexpand_inserted_text() {
    let data = json!({ "message": "{{message}}!" });
    let output = Template::new("{{message}}|{{message}}".into()).parse_template_inline(&data);
    assert_eq!(output, "{{message}}!|{{message}}!");
}

#[test]
fn unknown_expressions_and_unclosed_suffixes_remain_visible() {
    let data = json!({ "name": "Zoë" });
    let source = "こんにちは {{name}} {{missing}} {{unknown(name)}} {{unfinished";
    let output = Template::new(source.into()).parse_template_inline(&data);
    assert_eq!(output, "こんにちは Zoë {{missing}} {{unknown(name)}} {{unfinished");
}

#[test]
fn adjacent_fields_and_functions_resolve_from_the_source_template() {
    let data = json!({ "name": "Alice", "amount": "1", "details": { "unit": "ETH" } });
    let source = "{{name}}:{{format_value(amount, 18)}}{{details.unit}}/{{name}}";
    let output = Template::new(source.into()).parse_template_inline(&data);
    assert_eq!(output, "Alice:0.000000000000000001ETH/Alice");
}
