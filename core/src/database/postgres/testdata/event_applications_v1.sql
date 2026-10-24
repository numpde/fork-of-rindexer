-- Historical receipt schema, retained as a migration input rather than generated output.
DROP TABLE rindexer_internal.event_applications;
CREATE TABLE rindexer_internal.event_applications (
    namespace TEXT NOT NULL REFERENCES rindexer_internal.event_application_streams(namespace) ON DELETE CASCADE,
    network TEXT NOT NULL,
    block_number NUMERIC NOT NULL,
    block_hash BYTEA NOT NULL,
    tx_hash BYTEA NOT NULL,
    log_index TEXT NOT NULL,
    PRIMARY KEY (namespace, network, block_hash, tx_hash, log_index)
);
