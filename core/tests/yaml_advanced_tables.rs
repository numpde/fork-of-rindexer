//! Advanced table expressions must pass the production manifest validator.
use rindexer::manifest::yaml::read_manifest;

const MANIFEST: &str = r#"
name: advanced-tables-test
project_type: no-code

storage:
  postgres:
    enabled: true

networks:
  - name: polygon
    chain_id: 137
    rpc: http://unused.invalid
    compute_units_per_second: 660
    max_block_range: "100"

contracts:
  - name: TestExchange
    details:
      - network: polygon
        address: "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
        start_block: "65000000"
        end_block: "65000100"
    abi: ./exchange.json
    include_events:
      - OrderFilled
    tables:
      - name: activities
        columns:
          - name: sequence_id
            type: uint256
          - name: tx_type
            type: string
          - name: user_id
            type: address
          - name: side
            type: string
          - name: amount_usdc
            type: uint256
          - name: price
            type: uint256
        events:
          - event: OrderFilled
            operations:
              # Operation 1: arithmetic in set values + $if with arithmetic
              - type: insert
                set:
                  - column: sequence_id
                    action: set
                    value: ($rindexer_block_number * 18446744073709551616 + $rindexer_tx_index * 4294967296 + $rindexer_log_index) * 10 + 0
                  - column: tx_type
                    action: set
                    value: TRADE
                  - column: user_id
                    action: set
                    value: $taker
                  - column: side
                    action: set
                    value: $if($makerAssetId == '0', 'BUY', 'SELL')
                  - column: amount_usdc
                    action: set
                    value: $makerAmountFilled / 1000000
                  - column: price
                    action: set
                    value: $makerAmountFilled / $takerAmountFilled
              # Operation 2: fan-out (second row from same event)
              - type: insert
                set:
                  - column: sequence_id
                    action: set
                    value: ($rindexer_block_number * 18446744073709551616 + $rindexer_tx_index * 4294967296 + $rindexer_log_index) * 10 + 1
                  - column: tx_type
                    action: set
                    value: TRADE
                  - column: user_id
                    action: set
                    value: $maker
                  - column: side
                    action: set
                    value: $if($makerAssetId == '0', 'SELL', 'BUY')
                  - column: amount_usdc
                    action: set
                    value: $takerAmountFilled / 1000000
                  - column: price
                    action: set
                    value: $makerAmountFilled / $takerAmountFilled
              # Operation 3: fan-out with arithmetic in where (upsert)
              - type: upsert
                where:
                  sequence_id: ($rindexer_block_number * 18446744073709551616 + $rindexer_tx_index * 4294967296 + $rindexer_log_index) * 10 + 2
                set:
                  - column: tx_type
                    action: set
                    value: FEE
                  - column: side
                    action: set
                    value: FEE
                  - column: price
                    action: set
                    value: "0"
                  - column: user_id
                    action: set
                    value: $taker
                  - column: amount_usdc
                    action: set
                    value: $fee / 1000000
"#;

const ABI: &str = r#"[{"anonymous":false,"inputs":[{"indexed":true,"name":"orderHash","type":"bytes32"},{"indexed":true,"name":"maker","type":"address"},{"indexed":true,"name":"taker","type":"address"},{"indexed":false,"name":"makerAssetId","type":"uint256"},{"indexed":false,"name":"takerAssetId","type":"uint256"},{"indexed":false,"name":"makerAmountFilled","type":"uint256"},{"indexed":false,"name":"takerAmountFilled","type":"uint256"},{"indexed":false,"name":"fee","type":"uint256"}],"name":"OrderFilled","type":"event"}]"#;

fn project(manifest: &str) -> tempfile::TempDir {
    let directory = tempfile::tempdir().expect("owned manifest fixture");
    std::fs::write(directory.path().join("exchange.json"), ABI).unwrap();
    std::fs::write(directory.path().join("rindexer.yaml"), manifest).unwrap();
    directory
}

#[test]
fn advanced_tables_pass_manifest_validation() {
    let directory = project(MANIFEST);
    let manifest = read_manifest(&directory.path().join("rindexer.yaml"))
        .expect("arithmetic, conditional values and fan-out are valid manifest operations");
    let table = &manifest.contracts[0].tables.as_ref().unwrap()[0];
    assert_eq!(table.events[0].operations.len(), 3);
}

#[test]
fn advanced_tables_reject_unknown_event_fields() {
    let directory = project(&MANIFEST.replace("$fee / 1000000", "$missing / 1000000"));
    let error = read_manifest(&directory.path().join("rindexer.yaml"))
        .expect_err("valid YAML with an unknown event field must fail manifest validation");
    assert!(error.to_string().contains("missing"), "{error}");
}
