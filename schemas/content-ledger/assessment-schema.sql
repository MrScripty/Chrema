-- Fallible application-owned assessments of exact content versions.
-- These records do not alter observed usage or authorize publication or transfer.
CREATE TABLE __SCHEMA__.content_assessment_definitions (
  application_namespace text NOT NULL CHECK (application_namespace ~ '^[a-z0-9][a-z0-9._:-]{0,199}$'),
  definition_id text NOT NULL CHECK (definition_id ~ '^[a-z0-9][a-z0-9._:-]{0,199}$'),
  definition_version integer NOT NULL CHECK (definition_version > 0),
  schema_profile text NOT NULL CHECK (schema_profile='capture-v1'),
  result_schema jsonb NOT NULL,
  result_schema_sha256 text NOT NULL CHECK (result_schema_sha256 ~ '^[0-9a-f]{64}$'),
  identity_sha256 text NOT NULL UNIQUE CHECK (identity_sha256 ~ '^[0-9a-f]{64}$'),
  recorded_at timestamptz NOT NULL,
  PRIMARY KEY (application_namespace,definition_id,definition_version)
);

CREATE TABLE __SCHEMA__.content_assessments (
  id text PRIMARY KEY CHECK (id ~ '^content-assessment-[0-9a-f]{64}$'),
  application_namespace text NOT NULL CHECK (application_namespace ~ '^[a-z0-9][a-z0-9._:-]{0,199}$'),
  assessment_key text NOT NULL CHECK (btrim(assessment_key)<>'' AND char_length(assessment_key)<=500),
  key_sha256 text NOT NULL UNIQUE CHECK (key_sha256 ~ '^[0-9a-f]{64}$'),
  content_version_id text NOT NULL CHECK (content_version_id ~ '^content-version-[0-9a-f]{64}$'),
  definition_id text NOT NULL,
  definition_version integer NOT NULL,
  evaluator_kind text NOT NULL CHECK (evaluator_kind ~ '^[a-z0-9][a-z0-9._:-]{0,199}$'),
  evaluator_id text NOT NULL CHECK (btrim(evaluator_id)<>'' AND char_length(evaluator_id)<=500),
  source_reference_kind text NOT NULL CHECK (source_reference_kind ~ '^[a-z0-9][a-z0-9._:-]{0,199}$'),
  source_reference_id text NOT NULL CHECK (btrim(source_reference_id)<>'' AND char_length(source_reference_id)<=500),
  assessed_at timestamptz NOT NULL,
  result jsonb NOT NULL,
  identity_sha256 text NOT NULL CHECK (identity_sha256 ~ '^[0-9a-f]{64}$'),
  recorded_at timestamptz NOT NULL,
  UNIQUE (application_namespace,assessment_key),
  FOREIGN KEY (application_namespace,content_version_id)
    REFERENCES __CONTENT_VERSIONS__(application_namespace,id),
  FOREIGN KEY (application_namespace,definition_id,definition_version)
    REFERENCES __SCHEMA__.content_assessment_definitions(application_namespace,definition_id,definition_version),
  CHECK (id='content-assessment-' || key_sha256)
);
CREATE INDEX content_assessments_version_idx
  ON __SCHEMA__.content_assessments (application_namespace,content_version_id,assessed_at,id);

CREATE FUNCTION __SCHEMA__.guard_assessment_definition()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
  IF NOT __SCHEMA__.capture_js_json_safe(NEW.result_schema)
     OR coalesce(__SCHEMA__.capture_schema_node_count(NEW.result_schema,0),-1)<1 THEN
    RAISE EXCEPTION 'content assessment schema uses unsupported or malformed JSON Schema'
      USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER content_assessment_definition_schema_valid
BEFORE INSERT ON __SCHEMA__.content_assessment_definitions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_assessment_definition();

CREATE FUNCTION __SCHEMA__.guard_assessment_result()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE definition_schema jsonb;
BEGIN
  IF NOT __SCHEMA__.capture_js_json_safe(NEW.result) THEN
    RAISE EXCEPTION 'content assessment result contains values outside capture-v1'
      USING ERRCODE='23514';
  END IF;
  SELECT result_schema INTO STRICT definition_schema
  FROM __SCHEMA__.content_assessment_definitions
  WHERE application_namespace=NEW.application_namespace
    AND definition_id=NEW.definition_id
    AND definition_version=NEW.definition_version;
  IF NOT __SCHEMA__.capture_value_matches(NEW.result,definition_schema) THEN
    RAISE EXCEPTION 'content assessment result does not match its registered definition'
      USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER content_assessment_result_valid
BEFORE INSERT ON __SCHEMA__.content_assessments
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_assessment_result();

CREATE FUNCTION __SCHEMA__.guard_assessment_immutable()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
  RAISE EXCEPTION '% records are immutable', TG_TABLE_NAME USING ERRCODE='23514';
END;
$$;
CREATE TRIGGER content_assessment_definitions_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.content_assessment_definitions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_assessment_immutable();
CREATE TRIGGER content_assessments_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.content_assessments
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_assessment_immutable();
CREATE TRIGGER content_assessment_definitions_no_truncate
BEFORE TRUNCATE ON __SCHEMA__.content_assessment_definitions
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_assessment_immutable();
CREATE TRIGGER content_assessments_no_truncate
BEFORE TRUNCATE ON __SCHEMA__.content_assessments
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_assessment_immutable();

CREATE FUNCTION __SCHEMA__.guard_assessment_replication_role()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
  IF current_setting('session_replication_role') NOT IN ('origin','local') THEN
    RAISE EXCEPTION 'session_replication_role must be origin or local for writes'
      USING ERRCODE='23514';
  END IF;
  RETURN NULL;
END;
$$;
CREATE FUNCTION __SCHEMA__.guard_assessment_replication_role_row()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
  IF current_setting('session_replication_role') NOT IN ('origin','local') THEN
    RAISE EXCEPTION 'session_replication_role must be origin or local for writes'
      USING ERRCODE='23514';
  END IF;
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER content_assessment_definitions_replication_role
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON __SCHEMA__.content_assessment_definitions
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_assessment_replication_role();
CREATE TRIGGER content_assessments_replication_role
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON __SCHEMA__.content_assessments
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_assessment_replication_role();
CREATE TRIGGER content_assessment_definitions_replication_role_row
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.content_assessment_definitions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_assessment_replication_role_row();
CREATE TRIGGER content_assessments_replication_role_row
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.content_assessments
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_assessment_replication_role_row();

ALTER TABLE __SCHEMA__.content_assessment_definitions
  ENABLE ALWAYS TRIGGER content_assessment_definitions_immutable;
ALTER TABLE __SCHEMA__.content_assessments
  ENABLE ALWAYS TRIGGER content_assessments_immutable;
ALTER TABLE __SCHEMA__.content_assessment_definitions
  ENABLE ALWAYS TRIGGER content_assessment_definitions_no_truncate;
ALTER TABLE __SCHEMA__.content_assessments
  ENABLE ALWAYS TRIGGER content_assessments_no_truncate;
ALTER TABLE __SCHEMA__.content_assessment_definitions
  ENABLE ALWAYS TRIGGER content_assessment_definitions_replication_role;
ALTER TABLE __SCHEMA__.content_assessments
  ENABLE ALWAYS TRIGGER content_assessments_replication_role;
ALTER TABLE __SCHEMA__.content_assessment_definitions
  ENABLE ALWAYS TRIGGER content_assessment_definitions_replication_role_row;
ALTER TABLE __SCHEMA__.content_assessments
  ENABLE ALWAYS TRIGGER content_assessments_replication_role_row;
