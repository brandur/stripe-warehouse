BEGIN;

DROP TABLE IF EXISTS charges;

CREATE TABLE charges (
    id text PRIMARY KEY,
    amount bigint,
    event_offset bigint,
    created timestamptz
);

COMMIT;
