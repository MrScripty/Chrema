-- Optional private metadata only. No grants, body store or production migration.
CREATE TABLE __SCHEMA__.reserved_answer_acceptances (
 run_id text PRIMARY KEY REFERENCES __SCHEMA__.agent_workflow_runs(id),
 receipt_sha256 text NOT NULL CHECK(receipt_sha256 ~ '^[0-9a-f]{64}$'),
 payload jsonb NOT NULL CHECK(jsonb_typeof(payload)='object'),
 UNIQUE(run_id,receipt_sha256)
);
CREATE TABLE __SCHEMA__.reserved_answer_completions (
 run_id text PRIMARY KEY,
 accepted_sha256 text NOT NULL,
 receipt_sha256 text NOT NULL CHECK(receipt_sha256 ~ '^[0-9a-f]{64}$'),
 payload jsonb NOT NULL CHECK(jsonb_typeof(payload)='object'),
 FOREIGN KEY(run_id,accepted_sha256)
 REFERENCES __SCHEMA__.reserved_answer_acceptances(run_id,receipt_sha256)
);
CREATE FUNCTION __SCHEMA__.immutable_reserved_answer_receipt() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'Reserved answer receipts are immutable'; END $$;
CREATE TRIGGER immutable_reserved_answer_acceptance BEFORE UPDATE OR DELETE
ON __SCHEMA__.reserved_answer_acceptances FOR EACH ROW
EXECUTE FUNCTION __SCHEMA__.immutable_reserved_answer_receipt();
CREATE TRIGGER immutable_reserved_answer_completion BEFORE UPDATE OR DELETE
ON __SCHEMA__.reserved_answer_completions FOR EACH ROW
EXECUTE FUNCTION __SCHEMA__.immutable_reserved_answer_receipt();
