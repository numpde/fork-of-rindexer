use std::str::FromStr;

use alloy::primitives::U256;
use regex::Regex;
use serde_json::Value;

#[cfg(test)]
#[path = "template_amount_tests.rs"]
mod template_amount_tests;

#[cfg(test)]
#[path = "template_literal_tests.rs"]
mod template_literal_tests;

#[cfg(test)]
#[path = "template_value_tests.rs"]
mod template_value_tests;

#[derive(Debug, Clone)]
pub struct Template {
    value: String,
}

impl Template {
    pub fn new(value: String) -> Self {
        Self { value }
    }

    pub fn parse_template_inline(&self, event_data: &Value) -> String {
        let mut output = String::with_capacity(self.value.len());
        let mut remaining = self.value.as_str();

        // Only the configured template owns expressions; inserted event text is literal.
        while let Some((prefix, after_open)) = remaining.split_once("{{") {
            let Some((placeholder, suffix)) = after_open.split_once("}}") else {
                break;
            };
            output.push_str(prefix);
            let value = if placeholder.contains('(') {
                self.evaluate_function(placeholder, event_data)
            } else {
                self.get_nested_value(event_data, placeholder)
            };
            if let Some(value) = value {
                output.push_str(&value);
            } else {
                output.push_str("{{");
                output.push_str(placeholder);
                output.push_str("}}");
            }
            remaining = suffix;
        }
        output.push_str(remaining);
        output
    }

    fn get_nested_value(&self, data: &Value, path: &str) -> Option<String> {
        let keys: Vec<&str> = path.split('.').collect();
        let mut current = data;
        for key in keys {
            let value = current.get(key)?;
            current = value;
        }
        Some(match current {
            Value::String(text) => text.clone(),
            value => value.to_string(),
        })
    }

    fn evaluate_function(&self, function_call: &str, event_data: &Value) -> Option<String> {
        let re = Regex::new(r"(\w+)\(([^)]+)\)").unwrap();
        if let Some(captures) = re.captures(function_call) {
            let function_name = &captures[1];
            let args: Vec<&str> = captures[2].split(',').map(|s| s.trim()).collect();
            if function_name == "format_value" && args.len() == 2 {
                if let Some(value_str) = self.get_nested_value(event_data, args[0]) {
                    if let Ok(decimals) = args[1].parse::<u8>() {
                        return Some(self.format_value(&value_str, decimals));
                    }
                }
            }
        }
        None
    }

    fn format_value(&self, value: &str, decimals: u8) -> String {
        let Ok(value) = U256::from_str(value) else {
            return value.to_string();
        };
        let digits = value.to_string();
        let decimals = usize::from(decimals);
        if decimals == 0 {
            return digits;
        }

        // Decimal placement preserves small fractions and avoids overflowing 10^decimals.
        // The uint8 token scale bounds padding even for values near uint256::MAX.
        let padded = format!("{digits:0>width$}", width = digits.len().max(decimals + 1));
        let (integer_part, fractional_part) = padded.split_at(padded.len() - decimals);
        if fractional_part.bytes().all(|digit| digit == b'0') {
            integer_part.to_string()
        } else {
            format!("{integer_part}.{fractional_part}")
        }
    }
}
