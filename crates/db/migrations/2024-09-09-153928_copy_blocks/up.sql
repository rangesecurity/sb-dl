CREATE TABLE blocks_2 (
    id UUID NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
    number BIGINT NOT NULL UNIQUE,
    slot BIGINT UNIQUE NULL,
    data JSONB NOT NULL
);
