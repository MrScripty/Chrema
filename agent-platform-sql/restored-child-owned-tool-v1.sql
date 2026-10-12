-- Optional generic private continuation metadata. No bodies, grants or deployment.
CREATE TABLE __SCHEMA__.restored_child_tool_flows (
 run_id text NOT NULL REFERENCES __SCHEMA__.agent_workflow_runs(id),
 sequence integer NOT NULL CHECK(sequence BETWEEN 1 AND 6),
 record_sha256 text NOT NULL CHECK(record_sha256 ~ '^[a-f0-9]{64}$'),
 payload jsonb NOT NULL CHECK(jsonb_typeof(payload)='object'),
 PRIMARY KEY(run_id,sequence),
 UNIQUE(run_id,sequence,record_sha256)
);
CREATE FUNCTION __SCHEMA__.immutable_restored_child_tool_flow() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'Restored child tool flow is immutable'; END $$;
CREATE TRIGGER immutable_restored_child_tool_flow BEFORE UPDATE OR DELETE
ON __SCHEMA__.restored_child_tool_flows FOR EACH ROW
EXECUTE FUNCTION __SCHEMA__.immutable_restored_child_tool_flow();
