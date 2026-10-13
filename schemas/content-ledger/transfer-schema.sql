-- Generic transfer records. The six relation names are fixed; bootstrap replaces
-- only validated schema and external content-version relation identifiers.

CREATE TABLE __SCHEMA__.content_transfer_policy_definitions (
  application_namespace text NOT NULL,
  policy_id text NOT NULL,
  policy_version integer NOT NULL CHECK (policy_version > 0),
  config_schema jsonb NOT NULL,
  config jsonb NOT NULL,
  config_schema_sha256 text NOT NULL,
  config_sha256 text NOT NULL,
  identity_sha256 text NOT NULL,
  recorded_at timestamptz NOT NULL,
  PRIMARY KEY (application_namespace,policy_id,policy_version)
);

CREATE TABLE __SCHEMA__.content_transfer_assessment_definitions (
  application_namespace text NOT NULL,
  assessment_definition_id text NOT NULL,
  assessment_definition_version integer NOT NULL CHECK (assessment_definition_version > 0),
  result_schema jsonb NOT NULL,
  result_schema_sha256 text NOT NULL,
  identity_sha256 text NOT NULL,
  recorded_at timestamptz NOT NULL,
  PRIMARY KEY (application_namespace,assessment_definition_id,assessment_definition_version)
);

CREATE TABLE __SCHEMA__.content_transfer_requests (
  id text PRIMARY KEY,
  application_namespace text NOT NULL,
  content_version_id text NOT NULL,
  destination_kind text NOT NULL,
  destination_id text NOT NULL,
  destination_action text NOT NULL,
  policy_id text NOT NULL,
  policy_version integer NOT NULL,
  requester_identity jsonb NOT NULL,
  retry_key text NOT NULL,
  identity_sha256 text NOT NULL,
  recorded_at timestamptz NOT NULL,
  UNIQUE (application_namespace,retry_key),
  UNIQUE (id,application_namespace,content_version_id),
  UNIQUE (id,application_namespace,content_version_id,destination_kind,destination_id,destination_action),
  UNIQUE (id,application_namespace,content_version_id,destination_kind,destination_id,destination_action,policy_id,policy_version),
  FOREIGN KEY (application_namespace,policy_id,policy_version)
    REFERENCES __SCHEMA__.content_transfer_policy_definitions(application_namespace,policy_id,policy_version),
  FOREIGN KEY (application_namespace,content_version_id)
    REFERENCES __CONTENT_VERSIONS__(application_namespace,id)
);
CREATE INDEX content_transfer_requests_content_idx
  ON __SCHEMA__.content_transfer_requests (application_namespace,content_version_id,recorded_at,id);

CREATE TABLE __SCHEMA__.content_transfer_assessments (
  id text PRIMARY KEY,
  application_namespace text NOT NULL,
  assessment_definition_id text NOT NULL,
  assessment_definition_version integer NOT NULL,
  content_version_id text NOT NULL,
  request_id text,
  destination_kind text,
  destination_id text,
  destination_action text,
  evaluator_identity jsonb NOT NULL,
  configuration_id text,
  configuration_version integer,
  outcome text NOT NULL CHECK (outcome IN ('passed','failed','inconclusive')),
  result jsonb NOT NULL,
  limitations jsonb,
  execution_reference text,
  identity_sha256 text NOT NULL,
  recorded_at timestamptz NOT NULL,
  CHECK ((destination_kind IS NULL) = (destination_id IS NULL)
    AND (destination_kind IS NULL) = (destination_action IS NULL)),
  CHECK ((configuration_id IS NULL) = (configuration_version IS NULL)),
  CHECK (configuration_version IS NULL OR configuration_version > 0),
  FOREIGN KEY (application_namespace,assessment_definition_id,assessment_definition_version)
    REFERENCES __SCHEMA__.content_transfer_assessment_definitions
      (application_namespace,assessment_definition_id,assessment_definition_version),
  FOREIGN KEY (application_namespace,content_version_id)
    REFERENCES __CONTENT_VERSIONS__(application_namespace,id),
  FOREIGN KEY (request_id,application_namespace,content_version_id)
    REFERENCES __SCHEMA__.content_transfer_requests(id,application_namespace,content_version_id),
  FOREIGN KEY (request_id,application_namespace,content_version_id,destination_kind,destination_id,destination_action)
    REFERENCES __SCHEMA__.content_transfer_requests
      (id,application_namespace,content_version_id,destination_kind,destination_id,destination_action)
);
CREATE INDEX content_transfer_assessments_lookup_idx
  ON __SCHEMA__.content_transfer_assessments (application_namespace,content_version_id,request_id,recorded_at,id);

CREATE TABLE __SCHEMA__.content_transfer_decisions (
  id text PRIMARY KEY,
  application_namespace text NOT NULL,
  request_id text NOT NULL UNIQUE,
  content_version_id text NOT NULL,
  destination_kind text NOT NULL,
  destination_id text NOT NULL,
  destination_action text NOT NULL,
  policy_id text NOT NULL,
  policy_version integer NOT NULL,
  assessment_ids text[] NOT NULL,
  authorization_status text NOT NULL CHECK (authorization_status IN ('authorized','unauthorized','unknown')),
  authorization_checked_at timestamptz NOT NULL,
  authorization_authority_version text,
  authorization_evidence jsonb NOT NULL,
  authorization_evidence_sha256 text NOT NULL,
  outcome text NOT NULL CHECK (outcome IN ('eligible','withheld','denied')),
  delivery_enabled boolean NOT NULL DEFAULT false CHECK (delivery_enabled = false),
  identity_sha256 text NOT NULL,
  recorded_at timestamptz NOT NULL,
  UNIQUE (id,request_id,content_version_id,destination_kind,destination_id,destination_action),
  CHECK (array_position(assessment_ids,NULL) IS NULL),
  CHECK (cardinality(assessment_ids) <= 100),
  FOREIGN KEY (request_id,application_namespace,content_version_id,destination_kind,destination_id,destination_action,policy_id,policy_version)
    REFERENCES __SCHEMA__.content_transfer_requests
      (id,application_namespace,content_version_id,destination_kind,destination_id,destination_action,policy_id,policy_version)
);

CREATE TABLE __SCHEMA__.content_transfer_delivery_outcomes (
  id text PRIMARY KEY,
  decision_id text NOT NULL,
  request_id text NOT NULL,
  content_version_id text NOT NULL,
  destination_kind text NOT NULL,
  destination_id text NOT NULL,
  destination_action text NOT NULL,
  attempt_key text NOT NULL,
  outcome text NOT NULL CHECK (outcome IN ('delivered','failed','unknown')),
  details jsonb,
  failure_code text,
  identity_sha256 text NOT NULL,
  recorded_at timestamptz NOT NULL,
  UNIQUE (decision_id,attempt_key),
  FOREIGN KEY (decision_id,request_id,content_version_id,destination_kind,destination_id,destination_action)
    REFERENCES __SCHEMA__.content_transfer_decisions
      (id,request_id,content_version_id,destination_kind,destination_id,destination_action)
);
CREATE INDEX content_transfer_delivery_request_idx
  ON __SCHEMA__.content_transfer_delivery_outcomes (request_id,recorded_at,id);

CREATE FUNCTION __SCHEMA__.guard_content_transfer_immutable()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
  RAISE EXCEPTION '% records are immutable', TG_TABLE_NAME USING ERRCODE='23514';
END;
$$;

-- Foreign-key constraint triggers do not run in replica mode. Reject every
-- attempted insert before its rows are considered, including conflict replays.
CREATE FUNCTION __SCHEMA__.guard_content_transfer_insert_mode()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
  IF current_setting('session_replication_role') NOT IN ('origin','local') THEN
    RAISE EXCEPTION 'content transfer inserts require active foreign-key checks'
      USING ERRCODE='23514';
  END IF;
  RETURN NULL;
END;
$$;

CREATE FUNCTION __SCHEMA__.guard_content_transfer_replication_role_row()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
  IF current_setting('session_replication_role') NOT IN ('origin','local') THEN
    RAISE EXCEPTION 'session_replication_role must be origin or local for writes'
      USING ERRCODE='23514';
  END IF;
  IF TG_OP='DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER content_transfer_policy_definitions_insert_mode
BEFORE INSERT ON __SCHEMA__.content_transfer_policy_definitions
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_insert_mode();
CREATE TRIGGER content_transfer_assessment_definitions_insert_mode
BEFORE INSERT ON __SCHEMA__.content_transfer_assessment_definitions
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_insert_mode();
CREATE TRIGGER content_transfer_assessments_insert_mode
BEFORE INSERT ON __SCHEMA__.content_transfer_assessments
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_insert_mode();
CREATE TRIGGER content_transfer_requests_insert_mode
BEFORE INSERT ON __SCHEMA__.content_transfer_requests
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_insert_mode();
CREATE TRIGGER content_transfer_decisions_insert_mode
BEFORE INSERT ON __SCHEMA__.content_transfer_decisions
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_insert_mode();
CREATE TRIGGER content_transfer_delivery_outcomes_insert_mode
BEFORE INSERT ON __SCHEMA__.content_transfer_delivery_outcomes
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_insert_mode();

CREATE TRIGGER content_transfer_policy_definitions_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.content_transfer_policy_definitions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_replication_role_row();
CREATE TRIGGER content_transfer_assessment_definitions_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.content_transfer_assessment_definitions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_replication_role_row();
CREATE TRIGGER content_transfer_assessments_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.content_transfer_assessments
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_replication_role_row();
CREATE TRIGGER content_transfer_requests_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.content_transfer_requests
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_replication_role_row();
CREATE TRIGGER content_transfer_decisions_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.content_transfer_decisions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_replication_role_row();
CREATE TRIGGER content_transfer_delivery_outcomes_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.content_transfer_delivery_outcomes
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_replication_role_row();

CREATE TRIGGER content_transfer_policy_definitions_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.content_transfer_policy_definitions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_immutable();
CREATE TRIGGER content_transfer_assessment_definitions_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.content_transfer_assessment_definitions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_immutable();
CREATE TRIGGER content_transfer_assessments_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.content_transfer_assessments
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_immutable();
CREATE TRIGGER content_transfer_requests_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.content_transfer_requests
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_immutable();
CREATE TRIGGER content_transfer_decisions_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.content_transfer_decisions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_immutable();
CREATE TRIGGER content_transfer_delivery_outcomes_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.content_transfer_delivery_outcomes
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_immutable();

CREATE TRIGGER content_transfer_policy_definitions_no_truncate
BEFORE TRUNCATE ON __SCHEMA__.content_transfer_policy_definitions
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_immutable();
CREATE TRIGGER content_transfer_assessment_definitions_no_truncate
BEFORE TRUNCATE ON __SCHEMA__.content_transfer_assessment_definitions
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_immutable();
CREATE TRIGGER content_transfer_assessments_no_truncate
BEFORE TRUNCATE ON __SCHEMA__.content_transfer_assessments
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_immutable();
CREATE TRIGGER content_transfer_requests_no_truncate
BEFORE TRUNCATE ON __SCHEMA__.content_transfer_requests
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_immutable();
CREATE TRIGGER content_transfer_decisions_no_truncate
BEFORE TRUNCATE ON __SCHEMA__.content_transfer_decisions
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_immutable();
CREATE TRIGGER content_transfer_delivery_outcomes_no_truncate
BEFORE TRUNCATE ON __SCHEMA__.content_transfer_delivery_outcomes
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_immutable();

-- Immutable records remain protected when session_replication_role is replica.
ALTER TABLE __SCHEMA__.content_transfer_policy_definitions
  ENABLE ALWAYS TRIGGER content_transfer_policy_definitions_immutable;
ALTER TABLE __SCHEMA__.content_transfer_assessment_definitions
  ENABLE ALWAYS TRIGGER content_transfer_assessment_definitions_immutable;
ALTER TABLE __SCHEMA__.content_transfer_assessments
  ENABLE ALWAYS TRIGGER content_transfer_assessments_immutable;
ALTER TABLE __SCHEMA__.content_transfer_requests
  ENABLE ALWAYS TRIGGER content_transfer_requests_immutable;
ALTER TABLE __SCHEMA__.content_transfer_decisions
  ENABLE ALWAYS TRIGGER content_transfer_decisions_immutable;
ALTER TABLE __SCHEMA__.content_transfer_delivery_outcomes
  ENABLE ALWAYS TRIGGER content_transfer_delivery_outcomes_immutable;

ALTER TABLE __SCHEMA__.content_transfer_policy_definitions
  ENABLE ALWAYS TRIGGER content_transfer_policy_definitions_no_truncate;
ALTER TABLE __SCHEMA__.content_transfer_assessment_definitions
  ENABLE ALWAYS TRIGGER content_transfer_assessment_definitions_no_truncate;
ALTER TABLE __SCHEMA__.content_transfer_assessments
  ENABLE ALWAYS TRIGGER content_transfer_assessments_no_truncate;
ALTER TABLE __SCHEMA__.content_transfer_requests
  ENABLE ALWAYS TRIGGER content_transfer_requests_no_truncate;
ALTER TABLE __SCHEMA__.content_transfer_decisions
  ENABLE ALWAYS TRIGGER content_transfer_decisions_no_truncate;
ALTER TABLE __SCHEMA__.content_transfer_delivery_outcomes
  ENABLE ALWAYS TRIGGER content_transfer_delivery_outcomes_no_truncate;

ALTER TABLE __SCHEMA__.content_transfer_policy_definitions
  ENABLE ALWAYS TRIGGER content_transfer_policy_definitions_insert_mode;
ALTER TABLE __SCHEMA__.content_transfer_assessment_definitions
  ENABLE ALWAYS TRIGGER content_transfer_assessment_definitions_insert_mode;
ALTER TABLE __SCHEMA__.content_transfer_assessments
  ENABLE ALWAYS TRIGGER content_transfer_assessments_insert_mode;
ALTER TABLE __SCHEMA__.content_transfer_requests
  ENABLE ALWAYS TRIGGER content_transfer_requests_insert_mode;
ALTER TABLE __SCHEMA__.content_transfer_decisions
  ENABLE ALWAYS TRIGGER content_transfer_decisions_insert_mode;
ALTER TABLE __SCHEMA__.content_transfer_delivery_outcomes
  ENABLE ALWAYS TRIGGER content_transfer_delivery_outcomes_insert_mode;

ALTER TABLE __SCHEMA__.content_transfer_policy_definitions
  ENABLE ALWAYS TRIGGER content_transfer_policy_definitions_replication_role_row_guard;
ALTER TABLE __SCHEMA__.content_transfer_assessment_definitions
  ENABLE ALWAYS TRIGGER content_transfer_assessment_definitions_replication_role_row_guard;
ALTER TABLE __SCHEMA__.content_transfer_assessments
  ENABLE ALWAYS TRIGGER content_transfer_assessments_replication_role_row_guard;
ALTER TABLE __SCHEMA__.content_transfer_requests
  ENABLE ALWAYS TRIGGER content_transfer_requests_replication_role_row_guard;
ALTER TABLE __SCHEMA__.content_transfer_decisions
  ENABLE ALWAYS TRIGGER content_transfer_decisions_replication_role_row_guard;
ALTER TABLE __SCHEMA__.content_transfer_delivery_outcomes
  ENABLE ALWAYS TRIGGER content_transfer_delivery_outcomes_replication_role_row_guard;

-- CONTENT TRANSFER DELIVERY EXTENSION --
-- This section is installed only by bootstrapContentTransferDeliveryPostgres.
-- A committed attempt claim precedes any external effect. An outcome without
-- its claim is invalid, and an unresolved claim blocks automatic resend.
CREATE TABLE __SCHEMA__.content_transfer_delivery_attempts (
  id text PRIMARY KEY CHECK (id ~ '^transfer-attempt-[0-9a-f]{64}$'),
  application_namespace text NOT NULL
    CHECK (application_namespace ~ '^[a-z0-9][a-z0-9._:-]{0,199}$'),
  decision_id text NOT NULL,
  request_id text NOT NULL,
  content_version_id text NOT NULL,
  destination_kind text NOT NULL
    CHECK (destination_kind ~ '^[a-z0-9][a-z0-9._:-]{0,199}$'),
  destination_id text NOT NULL CHECK (btrim(destination_id) <> '' AND char_length(destination_id) <= 500),
  destination_action text NOT NULL
    CHECK (destination_action ~ '^[a-z0-9][a-z0-9._:-]{0,199}$'),
  adapter_id text NOT NULL CHECK (adapter_id ~ '^[a-z0-9][a-z0-9._:-]{0,199}$'),
  adapter_version integer NOT NULL CHECK (adapter_version > 0),
  attempt_key text NOT NULL CHECK (btrim(attempt_key) <> '' AND char_length(attempt_key) <= 500),
  identity_sha256 text NOT NULL UNIQUE CHECK (identity_sha256 ~ '^[0-9a-f]{64}$'),
  claimed_at timestamptz NOT NULL,
  UNIQUE (decision_id,attempt_key),
  UNIQUE (id,decision_id,request_id,content_version_id),
  FOREIGN KEY (decision_id,request_id,content_version_id,
    destination_kind,destination_id,destination_action)
    REFERENCES __SCHEMA__.content_transfer_decisions
      (id,request_id,content_version_id,destination_kind,destination_id,destination_action),
  CHECK (id='transfer-attempt-' || identity_sha256)
);
CREATE INDEX content_transfer_delivery_attempts_request_idx
  ON __SCHEMA__.content_transfer_delivery_attempts (request_id,claimed_at,id);

CREATE FUNCTION __SCHEMA__.guard_content_transfer_attempt_shape()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE decision_row __SCHEMA__.content_transfer_decisions%ROWTYPE;
BEGIN
  SELECT * INTO STRICT decision_row
  FROM __SCHEMA__.content_transfer_decisions WHERE id=NEW.decision_id;
  IF decision_row.outcome <> 'eligible'
    OR decision_row.delivery_enabled IS DISTINCT FROM false
    OR NEW.application_namespace IS DISTINCT FROM decision_row.application_namespace
    OR NEW.request_id IS DISTINCT FROM decision_row.request_id
    OR NEW.content_version_id IS DISTINCT FROM decision_row.content_version_id
    OR NEW.destination_kind IS DISTINCT FROM decision_row.destination_kind
    OR NEW.destination_id IS DISTINCT FROM decision_row.destination_id
    OR NEW.destination_action IS DISTINCT FROM decision_row.destination_action THEN
    RAISE EXCEPTION 'delivery attempt is not bound to one eligible exact decision'
      USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER content_transfer_delivery_attempts_shape
BEFORE INSERT ON __SCHEMA__.content_transfer_delivery_attempts
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_attempt_shape();
CREATE TRIGGER content_transfer_delivery_attempts_insert_mode
BEFORE INSERT ON __SCHEMA__.content_transfer_delivery_attempts
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_insert_mode();
CREATE TRIGGER content_transfer_delivery_attempts_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.content_transfer_delivery_attempts
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_replication_role_row();
CREATE TRIGGER content_transfer_delivery_attempts_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.content_transfer_delivery_attempts
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_immutable();
CREATE TRIGGER content_transfer_delivery_attempts_no_truncate
BEFORE TRUNCATE ON __SCHEMA__.content_transfer_delivery_attempts
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_content_transfer_immutable();

ALTER TABLE __SCHEMA__.content_transfer_delivery_attempts
  ENABLE ALWAYS TRIGGER content_transfer_delivery_attempts_shape;
ALTER TABLE __SCHEMA__.content_transfer_delivery_attempts
  ENABLE ALWAYS TRIGGER content_transfer_delivery_attempts_insert_mode;
ALTER TABLE __SCHEMA__.content_transfer_delivery_attempts
  ENABLE ALWAYS TRIGGER content_transfer_delivery_attempts_replication_role_row_guard;
ALTER TABLE __SCHEMA__.content_transfer_delivery_attempts
  ENABLE ALWAYS TRIGGER content_transfer_delivery_attempts_immutable;
ALTER TABLE __SCHEMA__.content_transfer_delivery_attempts
  ENABLE ALWAYS TRIGGER content_transfer_delivery_attempts_no_truncate;

ALTER TABLE __SCHEMA__.content_transfer_delivery_outcomes
  ADD CONSTRAINT content_transfer_delivery_outcome_claim_fk
  FOREIGN KEY (decision_id,attempt_key)
  REFERENCES __SCHEMA__.content_transfer_delivery_attempts (decision_id,attempt_key);
