use super::Template;
use serde_json::json;

fn render(value: &str, decimals: u32) -> String {
    Template::new(format!("amount={{{{format_value(value, {decimals})}}}}"))
        .parse_template_inline(&json!({ "value": value }))
}

#[test]
fn fractional_amounts_preserve_leading_zeros() {
    assert_eq!(render("1", 18), "amount=0.000000000000000001");
    assert_eq!(render("1000000000000000001", 18), "amount=1.000000000000000001");
    assert_eq!(render("1234", 6), "amount=0.001234");
}

#[test]
fn full_width_unsigned_amounts_are_scaled_exactly() {
    assert_eq!(render("20000000000000000000", 18), "amount=20");
    assert_eq!(render("0x10000000000000000", 18), "amount=18.446744073709551616");
    assert_eq!(
        render(
            "115792089237316195423570985008687907853269984665640564039457584007913129639935",
            18
        ),
        "amount=115792089237316195423570985008687907853269984665640564039457.584007913129639935"
    );
}

#[test]
fn token_decimal_scales_do_not_overflow_integer_powers() {
    for decimals in [20, 64, 77, 255] {
        let expected = format!("amount=0.{}1", "0".repeat(decimals as usize - 1));
        assert_eq!(render("1", decimals), expected);
        assert_eq!(render("0", decimals), "amount=0");
    }
}

#[test]
fn unsupported_scales_leave_the_function_placeholder_unresolved() {
    for decimals in [256, u32::MAX] {
        assert_eq!(
            render("1", decimals),
            format!("amount={{{{format_value(value, {decimals})}}}}")
        );
    }
}

#[test]
fn whole_values_and_fractional_trailing_zeros_keep_their_shape() {
    assert_eq!(render("0", 18), "amount=0");
    assert_eq!(render("1000000000000000000", 18), "amount=1");
    assert_eq!(render("42", 0), "amount=42");
    assert_eq!(render("1200000000000000000", 18), "amount=1.200000000000000000");
}

#[test]
fn invalid_or_out_of_range_unsigned_values_remain_visible() {
    for value in [
        "not-an-amount",
        "-1",
        "115792089237316195423570985008687907853269984665640564039457584007913129639936",
    ] {
        assert_eq!(render(value, 18), format!("amount={value}"));
    }
}
