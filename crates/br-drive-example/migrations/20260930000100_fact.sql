-- The host's one fact table: every fact of the host, br-drive's included —
-- the library hands its facts through `DriveHost::record_facts`, in the
-- transaction of the gesture that changed its state. The reference table of
-- br-drive's README.
CREATE TABLE workspace_fact (
    id              uuid        PRIMARY KEY,
    noun            text        NOT NULL,
    key             jsonb       NOT NULL,
    seq             bigint      NOT NULL CHECK (seq >= 1),
    version         integer     NOT NULL CHECK (version >= 1),
    event_type      text        NOT NULL CHECK (event_type <> ''),
    payload         jsonb       NOT NULL,
    actor_id        uuid        NOT NULL,
    actor_kind      text        NOT NULL CHECK (actor_kind IN ('human', 'service')),
    is_runner       boolean     NOT NULL DEFAULT false,
    impersonator_id uuid,
    correlation_id  uuid        NOT NULL,
    causation_id    uuid,
    occurred_at     timestamptz NOT NULL,
    UNIQUE (noun, key, seq)
);
