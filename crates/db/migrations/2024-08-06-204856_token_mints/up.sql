CREATE TABLE token_mints (
    id UUID NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
    mint VARCHAR NOT NULL UNIQUE,
    name VARCHAR,
    symbol VARCHAR,
    decimals FLOAT4 NOT NULL,
    token_2022 BOOLEAN NOT NULL
);