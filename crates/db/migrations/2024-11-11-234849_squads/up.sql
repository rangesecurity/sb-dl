CREATE TABLE squads (
    account VARCHAR NOT NULL PRIMARY KEY,
    vaults TEXT[] NOT NULL UNIQUE,
    members TEXT[] NOT NULL,
    threshold BIGINT NOT NULL,
    program_version BIGINT NOT NULL,
    voting_members_count BIGINT NOT NULL DEFAULT 0
);