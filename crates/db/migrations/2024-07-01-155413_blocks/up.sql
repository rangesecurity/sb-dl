CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE blocks (
    id UUID NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
    number BIGINT NOT NULL UNIQUE,
    data JSONB NOT NULL
);