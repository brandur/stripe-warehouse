BEGIN;

DROP TABLE IF EXISTS charges;

CREATE TABLE charges (
    id text PRIMARY KEY,
    amount int,
    created TIMESTAMPTZ
);

COMMIT;
