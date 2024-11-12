CREATE TABLE squads (
    id UUID NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
    account VARCHAR NOT NULL UNIQUE,
    vaults TEXT[] NOT NULL UNIQUE,
    members TEXT[] NOT NULL,
    threshold INT NOT NULL,
    program_version INT NOT NULL
);