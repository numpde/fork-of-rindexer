use std::str::FromStr;

use alloy::primitives::U256;
use regex::Regex;
use serde_json::Value;

#[cfg(test)]
#[path = "template_amount_tests.rs"]
mod template_amount_tests;

#[derive(Debug, Clone)]
pub struct Template {
    value: String,
}

impl Template {
    pub fn new(value: String) -> Self {
        Self { value }
    }

    pub fn parse_template_inline(&self, event_data: &Value) -> String {
        let mut template = self.value.clone();
        let placeholders = self.extract_placeholders(&template);

        for placeholder in placeholders {
            if placeholder.contains('(') {
                if let Some(value) = self.evaluate_function(&placeholder, event_data) {
                    template = template.replace(&format!("{{{{{placeholder}}}}}"), &value);
                }
            } else if let Some(value) = self.get_nested_value(event_data, &placeholder) {
                template = template.replace(&format!("{{{{{placeholder}}}}}"), &value);
            }
        }
        template
    }

    fn extract_placeholders(&self, template: &str) -> Vec<String> {
        let mut placeholders = Vec::new();
        let mut start = 0;
        while let Some(start_index) = template[start..].find("{{") {
            if let Some(end_index) = template[start + start_index + 2..].find("}}") {
                let placeholder =
                    &template[start + start_index + 2..start + start_index + 2 + end_index];
                placeholders.push(placeholder.to_string());
                start += start_index + 2 + end_index + 2;
            } else {
                break;
            }
        }
        placeholders
    }

    fn get_nested_value(&self, data: &Value, path: &str) -> Option<String> {
        let keys: Vec<&str> = path.split('.').collect();
        let mut current = data;
        for key in keys {
            let value = current.get(key)?;
            current = value;
        }
        Some(current.to_string().replace('"', ""))
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
