CREATE TABLE idls (
    id VARCHAR NOT NULL,
    begin_height INT8 NOT NULL,
    end_height INT8,
    idl JSONB NOT NULL,
    PRIMARY KEY (id, begin_height)
);
