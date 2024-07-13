
CREATE TABLE programs (
    id VARCHAR NOT NULL,
    last_deployed_slot INT8 NOT NULL,
    executable_account VARCHAR NOT NULL,
    executable_data BYTEA NOT NULL,
    PRIMARY KEY (id, last_deployed_slot)
);