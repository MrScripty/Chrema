-- Optional private protocol tables. Same owned schema as generic journal.
-- No grants, body store, triggers on existing tables or production migration.
CREATE TABLE __SCHEMA__.composition_checkpoint_records (
 run_id text NOT NULL REFERENCES __SCHEMA__.agent_workflow_runs(id),
 sequence integer NOT NULL CHECK(sequence BETWEEN 1 AND 3),
 record_sha256 text NOT NULL CHECK(record_sha256 ~ '^[0-9a-f]{64}$'),
 payload jsonb NOT NULL CHECK(jsonb_typeof(payload)='object'),
 PRIMARY KEY(run_id,sequence),
 UNIQUE(run_id,sequence,record_sha256)
);
CREATE TABLE __SCHEMA__.composition_checkpoint_heads (
 run_id text PRIMARY KEY REFERENCES __SCHEMA__.agent_workflow_runs(id),
 sequence integer NOT NULL CHECK(sequence BETWEEN 1 AND 3),
 record_sha256 text NOT NULL,
 FOREIGN KEY(run_id,sequence,record_sha256)
 REFERENCES __SCHEMA__.composition_checkpoint_records(run_id,sequence,record_sha256)
);
CREATE FUNCTION __SCHEMA__.immutable_composition_checkpoint_record() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN
 RAISE EXCEPTION 'Composition checkpoint history is immutable';
END $$;
CREATE TRIGGER immutable_composition_checkpoint_record BEFORE UPDATE OR DELETE
ON __SCHEMA__.composition_checkpoint_records FOR EACH ROW
EXECUTE FUNCTION __SCHEMA__.immutable_composition_checkpoint_record();
