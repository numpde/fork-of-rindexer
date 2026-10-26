//! Durable checkpoint identity belongs to the configured query, not a runtime task ID.
use super::contract_setup::IndexingContractSetup;
use crate::manifest::contract::EventInputIndexedFilters;
use alloy::{
    primitives::{keccak256, Address, B256, U64},
    rpc::types::{Topic, ValueOrArray},
};
use serde_json::{json, Value};

/// Native transfers have one pipeline per network. Old network-wide contract
/// rows share this historical label but never certify a contract subscription.
pub(crate) const NETWORK_CURSOR_KEY: &str = "__event__";

fn normalized<T: Clone + Ord>(values: &ValueOrArray<T>) -> Vec<T> {
    let mut values = match values {
        ValueOrArray::Value(value) => vec![value.clone()],
        ValueOrArray::Array(values) => values.clone(),
    };
    values.sort();
    values.dedup();
    values
}

fn filters(filters: Option<&[EventInputIndexedFilters]>) -> Value {
    let mut filters = filters
        .into_iter()
        .flatten()
        .map(|filter| {
            let topics: [Topic; 4] = filter.clone().into();
            let topics = topics.map(|topic| {
                let mut values: Vec<B256> = topic.iter().copied().collect();
                values.sort();
                values.dedup();
                values
            });
            (filter.event_name.clone(), topics)
        })
        .collect::<Vec<_>>();
    filters.sort();
    filters.dedup();
    json!(filters)
}

fn canonicalize(value: &mut Value) {
    match value {
        Value::Object(object) => {
            let mut entries = std::mem::take(object).into_iter().collect::<Vec<_>>();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            for (key, mut value) in entries {
                canonicalize(&mut value);
                object.insert(key, value);
            }
        }
        Value::Array(values) => values.iter_mut().for_each(canonicalize),
        _ => {}
    }
}

pub(crate) fn subscription_key(setup: &IndexingContractSetup, start: Option<U64>) -> String {
    let scope = match setup {
        IndexingContractSetup::Address(details) => json!([
            "address",
            normalized::<Address>(&details.address),
            filters(details.indexed_filters.as_deref())
        ]),
        IndexingContractSetup::Filter(details) => json!([
            "filter",
            normalized(&details.events),
            filters(details.indexed_filters.as_deref())
        ]),
        IndexingContractSetup::Factory(details) => json!([
            "factory",
            normalized(&details.address),
            details.contract_name,
            details.event,
            normalized(&details.input_name),
            filters(details.indexed_filters.as_deref())
        ]),
    };
    // Starting coverage is part of the proof. End-block extensions can reuse
    // an existing prefix, while a newly earlier start must establish its own.
    let mut identity = json!(["contract-cursor-v1", scope, start.map(|block| block.to::<u64>())]);
    canonicalize(&mut identity);
    format!("v1-{:x}", keccak256(identity.to_string().as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::contract_setup::AddressDetails;

    fn address_scope(addresses: Vec<Address>, filters: Value) -> IndexingContractSetup {
        IndexingContractSetup::Address(AddressDetails {
            address: ValueOrArray::Array(addresses),
            indexed_filters: serde_json::from_value(filters).unwrap(),
        })
    }

    #[test]
    fn query_identity_normalizes_set_order_and_separates_coverage() {
        let a = Address::repeat_byte(1);
        let b = Address::repeat_byte(2);
        let filters =
            json!([{"event_name":"Transfer", "indexed_1":[format!("{a:#x}"),format!("{b:#x}")]}]);
        let reordered = json!([{"event_name":"Transfer", "indexed_1":[format!("{b:#x}"),format!("{a:#x}"),format!("{a:#x}")]}]);
        let scope = address_scope(vec![a, b], filters.clone());
        let equivalent = address_scope(vec![b, a, a], reordered);
        let start = Some(U64::from(100));
        assert_eq!(subscription_key(&scope, start), subscription_key(&equivalent, start));
        assert_ne!(subscription_key(&scope, start), subscription_key(&scope, Some(U64::from(99))));
        assert_ne!(
            subscription_key(&scope, start),
            subscription_key(&address_scope(vec![a], filters), start)
        );
        assert_ne!(
            subscription_key(&scope, start),
            subscription_key(&address_scope(vec![a, b], Value::Null), start)
        );
    }
}
