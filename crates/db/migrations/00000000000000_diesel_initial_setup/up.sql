-- This file was automatically created by Diesel to setup helper functions
-- and other internal bookkeeping. This file is safe to edit, any future
-- changes will be added to existing projects as new migrations.




-- Sets up a trigger for the given table to automatically set a column called
-- `updated_at` whenever the row is modified (unless `updated_at` was included
-- in the modified columns)
--
-- # Example
--
-- ```sql
-- CREATE TABLE users (id SERIAL PRIMARY KEY, updated_at TIMESTAMP NOT NULL DEFAULT NOW());
--
-- SELECT diesel_manage_updated_at('users');
-- ```

CREATE TABLE blocks (
    number BIGINT NOT NULL PRIMARY KEY,
    slot BIGINT NOT NULL UNIQUE,
    time TIMESTAMPTZ,
    processed BOOLEAN NOT NULL DEFAULT false,
    data JSONB NOT NULL
);

CREATE INDEX blocks_time_key ON blocks(time);
CREATE INDEX blocks_processed_key ON blocks(processed);