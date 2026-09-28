-- The audit trail: what happened to a file, a page, a label or a rule, by
-- whom, as append-only facts — the shape the identity service gives its own
-- (`identity_fact`). The columns overwritten on every change go: the last
-- change of an object, and its whole history, are read from its facts. The
-- creation columns written once (`created_by`, `created_at`) stay: they are
-- facts already.
--
--   aggregate_type  what changed: file, page, label, ruleset
--   aggregate_id    its id — for a page, its file's id
--   page_number     the page's number (a page has no id of its own)
--   correlation_id  the gesture: every fact one gesture wrote shares it (a
--                   folder move's files, a report's pages); a reaction's
--                   message correlation when it has one
--   fact_type       the change, named as the library's cause (`Renamed`,
--                   `Reported`, `Edited`, `ProcessingFailed`, …)
--   payload         the cause as the live views sent it
--   actor_id        who acted: the effective principal
--   actor_kind      human, service (a service account, Jobs included) or
--                   runner (a service account holding the host's runner
--                   scope); `unknown` for the last changes carried over from
--                   0.3.0, which did not record who
--   impersonator_id the admin behind an impersonated session: the trail never
--                   loses the real hand
--   occurred_at     when
CREATE TABLE drive.fact (
    id              uuid        PRIMARY KEY,
    aggregate_type  text        NOT NULL CHECK (aggregate_type IN ('file', 'page', 'label', 'ruleset')),
    aggregate_id    uuid        NOT NULL,
    page_number     integer     CHECK (page_number IS NULL OR page_number >= 1),
    correlation_id  uuid        NOT NULL,
    fact_type       text        NOT NULL CHECK (fact_type <> ''),
    payload         jsonb       NOT NULL,
    actor_id        uuid,
    actor_kind      text        NOT NULL CHECK (actor_kind IN ('human', 'service', 'runner', 'unknown')),
    impersonator_id uuid,
    occurred_at     timestamptz NOT NULL,
    CHECK ((aggregate_type = 'page') = (page_number IS NOT NULL)),
    CHECK (actor_id IS NOT NULL OR actor_kind = 'unknown'),
    CHECK (impersonator_id IS NULL OR actor_kind = 'human')
);

-- An object's last change, and its history, newest first.
CREATE INDEX fact_aggregate_idx
    ON drive.fact (aggregate_type, aggregate_id, page_number, occurred_at DESC, id DESC);
-- The erase finds a person's facts by either hand.
CREATE INDEX fact_actor_idx ON drive.fact (actor_id) WHERE actor_id IS NOT NULL;
CREATE INDEX fact_impersonator_idx ON drive.fact (impersonator_id) WHERE impersonator_id IS NOT NULL;

-- Backfill: the last change each object carried becomes its one fact. A
-- file, a label or a rule never changed since its creation needs none (its
-- last change is its creation); every page gets one (a page has no creation
-- column). The page's writer was recorded, its kind was not.
INSERT INTO drive.fact (id, aggregate_type, aggregate_id, page_number, correlation_id,
                        fact_type, payload, actor_id, actor_kind, impersonator_id, occurred_at)
SELECT gen_random_uuid(), 'file', f.id, NULL, f.id, 'LastChangeCarriedOver',
       '{"kind": "LastChangeCarriedOver"}'::jsonb, NULL, 'unknown', NULL, f.updated_at
FROM drive.file f
WHERE f.updated_at > f.created_at;

INSERT INTO drive.fact (id, aggregate_type, aggregate_id, page_number, correlation_id,
                        fact_type, payload, actor_id, actor_kind, impersonator_id, occurred_at)
SELECT gen_random_uuid(), 'page', p.file_id, p.number, p.file_id, 'LastChangeCarriedOver',
       jsonb_build_object('kind', 'LastChangeCarriedOver', 'origin', upper(p.origin)),
       p.updated_by, 'unknown', NULL, p.updated_at
FROM drive.file_page p;

INSERT INTO drive.fact (id, aggregate_type, aggregate_id, page_number, correlation_id,
                        fact_type, payload, actor_id, actor_kind, impersonator_id, occurred_at)
SELECT gen_random_uuid(), 'label', l.id, NULL, l.id, 'LastChangeCarriedOver',
       '{"kind": "LastChangeCarriedOver"}'::jsonb, NULL, 'unknown', NULL, l.updated_at
FROM drive.label l
WHERE l.updated_at > l.created_at;

INSERT INTO drive.fact (id, aggregate_type, aggregate_id, page_number, correlation_id,
                        fact_type, payload, actor_id, actor_kind, impersonator_id, occurred_at)
SELECT gen_random_uuid(), 'ruleset', r.id, NULL, r.id, 'LastChangeCarriedOver',
       '{"kind": "LastChangeCarriedOver"}'::jsonb, NULL, 'unknown', NULL, r.updated_at
FROM drive.ruleset r
WHERE r.updated_at > r.created_at;

ALTER TABLE drive.file DROP COLUMN updated_at;

-- The `protected` mark goes: no host uses it. A host keeps its own per-file
-- rules in its gate (on the row's metadata, say).
ALTER TABLE drive.file DROP COLUMN protected;
ALTER TABLE drive.file_page DROP COLUMN updated_by, DROP COLUMN updated_at;
ALTER TABLE drive.label DROP COLUMN updated_at;
ALTER TABLE drive.ruleset DROP COLUMN updated_at;
