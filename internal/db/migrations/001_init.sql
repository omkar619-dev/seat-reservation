-- 001_init.sql: core schema.
--
-- Where the correctness guarantees live:
--   seats            exactly one row per (show, label). A seat changes hands only through
--                    a conditional UPDATE guarded on status = 'available' (booking/store.go),
--                    so two buyers can never both flip the same row.
--   reservations     UNIQUE (show_id, user_id, idempotency_key) turns "the same key reserves
--                    exactly once" into a database guarantee instead of an application convention.
--                    Keys are scoped to the caller and the target resource (POST /shows/{id}/reserve):
--                    the same key against a different show is a different operation.
--   show_user_seats  one counter row per (show, user). The per-user limit is a conditional
--                    upsert on this row, so parallel requests from one user serialize here.

CREATE TABLE shows (
    id              uuid        PRIMARY KEY,
    name            text        NOT NULL,
    price_paise     bigint      NOT NULL CHECK (price_paise >= 0),
    per_user_limit  integer     NOT NULL CHECK (per_user_limit > 0),
    total_seats     integer     NOT NULL CHECK (total_seats > 0),
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE reservations (
    id               uuid        PRIMARY KEY,
    show_id          uuid        NOT NULL REFERENCES shows (id),
    user_id          text        NOT NULL,
    idempotency_key  text        NOT NULL,
    request_hash     text        NOT NULL,   -- sha256(show_id, seats): detects same key + different body
    seat_labels      text[]      NOT NULL,   -- kept after cancel, for history and replays
    amount_paise     bigint      NOT NULL CHECK (amount_paise >= 0),
    status           text        NOT NULL CHECK (status IN ('confirmed', 'cancelled')),
    created_at       timestamptz NOT NULL DEFAULT now(),
    cancelled_at     timestamptz,
    CONSTRAINT reservations_idempotency_key UNIQUE (show_id, user_id, idempotency_key)
);

-- fillfactor 80 leaves free space on each page so that claiming/releasing a seat can be a
-- HOT update: none of the indexed columns (id, show_id, label) ever change after creation.
CREATE TABLE seats (
    id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    show_id         uuid        NOT NULL REFERENCES shows (id),
    label           text        NOT NULL,
    position        integer     NOT NULL,
    status          text        NOT NULL DEFAULT 'available'
                                CHECK (status IN ('available', 'held', 'confirmed')),
    reservation_id  uuid        REFERENCES reservations (id),
    user_id         text,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT seats_show_label UNIQUE (show_id, label),
    -- A seat has an owner if and only if it is taken. Makes "half-released" rows impossible.
    CONSTRAINT seats_owner_iff_taken CHECK (
        (status =  'available' AND reservation_id IS NULL     AND user_id IS NULL) OR
        (status <> 'available' AND reservation_id IS NOT NULL AND user_id IS NOT NULL)
    )
) WITH (fillfactor = 80);

CREATE TABLE show_user_seats (
    show_id  uuid    NOT NULL REFERENCES shows (id),
    user_id  text    NOT NULL,
    seats    integer NOT NULL CHECK (seats >= 0),
    PRIMARY KEY (show_id, user_id)
);
