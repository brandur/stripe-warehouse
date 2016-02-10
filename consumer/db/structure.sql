BEGIN;

DROP TABLE IF EXISTS charges;

CREATE TABLE charges (
    id text PRIMARY KEY,
    amount bigint,
    sequence bigint,
    created timestamptz
);

COMMIT;
