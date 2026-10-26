use super::ChatClients;
use alloy::primitives::U64;

fn accepts(from: u64, to: u64) -> bool {
    let clients = ChatClients {
        telegram: None,
        discord: None,
        slack: None,
        twilio: None,
        pagerduty: None,
        opsgenie: None,
    };
    clients.is_in_block_range_to_send(&U64::from(from), &U64::from(to))
}

#[test]
fn short_batches_are_eligible_even_near_genesis() {
    for (from, to) in [(0, 0), (0, 9), (1, 9), (10, 10), (20_000_000, 20_000_009)] {
        assert!(accepts(from, to), "chat should accept inclusive range {from}..={to}");
    }
}

#[test]
fn batches_over_ten_inclusive_blocks_are_suppressed() {
    for (from, to) in [(0, 10), (100, 110), (100, 1_000), (20_000_000, 20_000_010)] {
        assert!(!accepts(from, to), "chat should suppress inclusive range {from}..={to}");
    }
}

#[test]
fn eligibility_preserves_the_limit_at_maximum_block_height() {
    assert!(accepts(u64::MAX, u64::MAX));
    assert!(accepts(u64::MAX - 9, u64::MAX));
    assert!(!accepts(u64::MAX - 10, u64::MAX));
    assert!(!accepts(0, u64::MAX));
}

#[test]
fn reversed_ranges_are_ineligible_without_panicking() {
    for (from, to) in [(1, 0), (101, 100), (u64::MAX, 0)] {
        assert!(!accepts(from, to), "chat should reject reversed range {from}..={to}");
    }
}
