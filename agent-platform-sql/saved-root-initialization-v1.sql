-- Optional owned initialization protocol; no grants or production migration.
CREATE TABLE __SCHEMA__.saved_root_initializations (
 run_id text NOT NULL REFERENCES __SCHEMA__.agent_workflow_runs(id),
 sequence integer NOT NULL CHECK(sequence BETWEEN 1 AND 4),
 record_sha256 text NOT NULL CHECK(record_sha256 ~ '^[0-9a-f]{64}$'),
 payload jsonb NOT NULL CHECK(jsonb_typeof(payload)='object'),
 PRIMARY KEY(run_id,sequence)
);
CREATE FUNCTION __SCHEMA__.immutable_saved_root_initialization() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN
 RAISE EXCEPTION 'Saved root initialization history is immutable';
END $$;
CREATE TRIGGER immutable_saved_root_initialization BEFORE UPDATE OR DELETE
ON __SCHEMA__.saved_root_initializations FOR EACH ROW
EXECUTE FUNCTION __SCHEMA__.immutable_saved_root_initialization();
