-- Generic immutable capture, observed-usage, and relationship ledger.
-- Bootstrap replaces the three identifier placeholders with validated names.
-- Content versions can be captured before any observed usage is recorded.
-- Capture metadata does not verify the bytes or prove semantic influence,
-- factual support, or disclosure safety.

CREATE TABLE __SCHEMA__.content_extension_definitions (
  application_namespace text NOT NULL CHECK (application_namespace ~ '^[a-z0-9][a-z0-9._:-]{0,199}$'),
  definition_id text NOT NULL CHECK (definition_id ~ '^[a-z0-9][a-z0-9._:-]{0,199}$'),
  definition_version integer NOT NULL CHECK (definition_version > 0),
  schema_profile text NOT NULL DEFAULT 'capture-v1' CHECK (schema_profile='capture-v1'),
  value_schema jsonb NOT NULL,
  schema_sha256 text NOT NULL CHECK (schema_sha256 ~ '^[0-9a-f]{64}$'),
  identity_sha256 text NOT NULL CHECK (identity_sha256 ~ '^[0-9a-f]{64}$'),
  recorded_at timestamptz NOT NULL,
  PRIMARY KEY (application_namespace,definition_id,definition_version)
);

CREATE TABLE __SCHEMA__.content_versions (
  id text PRIMARY KEY CHECK (id ~ '^content-version-[0-9a-f]{64}$'),
  run_id text NOT NULL REFERENCES __JOURNAL_RUNS__(id),
  application_namespace text NOT NULL CHECK (application_namespace ~ '^[a-z0-9][a-z0-9._:-]{0,199}$'),
  content_id text NOT NULL CHECK (btrim(content_id) <> ''),
  content_type text NOT NULL CHECK (content_type ~ '^[a-z0-9][a-z0-9._:-]{0,199}$'),
  content_schema_version integer NOT NULL CHECK (content_schema_version > 0),
  extension_definition_id text,
  extension_definition_version integer,
  extensions jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(extensions)='object'),
  source_kind text NOT NULL CHECK (source_kind IN ('workflow-event-response','attempt-final-output','host-capture')),
  source_record_id text NOT NULL CHECK (btrim(source_record_id) <> ''),
  source_field text NOT NULL CHECK (btrim(source_field) <> ''),
  capture_status text NOT NULL CHECK (capture_status IN ('complete','partial','redacted','unavailable')),
  capture_source text,
  content_sha256 text CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
  hash_encoding text CHECK (hash_encoding ~ '^[a-z0-9][a-z0-9._:-]{0,199}$'),
  byte_length bigint CHECK (byte_length >= 0),
  identity_sha256 text NOT NULL UNIQUE CHECK (identity_sha256 ~ '^[0-9a-f]{64}$'),
  recorded_at timestamptz NOT NULL,
  UNIQUE (run_id,id),
  UNIQUE (application_namespace,id),
  UNIQUE (application_namespace,content_id,id),
  FOREIGN KEY (application_namespace,extension_definition_id,extension_definition_version)
    REFERENCES __SCHEMA__.content_extension_definitions(application_namespace,definition_id,definition_version),
  CHECK ((extension_definition_id IS NULL) = (extension_definition_version IS NULL)),
  CHECK (extension_definition_id IS NOT NULL OR extensions='{}'::jsonb),
  CHECK ((content_sha256 IS NULL) = (hash_encoding IS NULL)),
  CHECK (source_kind <> 'host-capture' OR (capture_status='complete'
    AND content_sha256 IS NOT NULL AND byte_length IS NOT NULL
    AND hash_encoding IS NOT NULL AND hash_encoding='raw-bytes-v1')),
  CHECK (capture_status <> 'complete' OR (content_sha256 IS NOT NULL AND byte_length IS NOT NULL)),
  CHECK (id='content-version-' || identity_sha256)
);
CREATE INDEX content_versions_stable_content_idx
  ON __SCHEMA__.content_versions (application_namespace,content_id,recorded_at DESC,id);

CREATE TABLE __SCHEMA__.observed_content_usage (
  id text PRIMARY KEY CHECK (id ~ '^content-usage-[0-9a-f]{64}$'),
  content_version_id text NOT NULL,
  run_id text NOT NULL,
  assignment_id text NOT NULL,
  attempt_id text NOT NULL,
  thread_id text,
  turn_id text,
  direction text NOT NULL CHECK (direction IN ('supplied-input','produced-output')),
  usage_role text NOT NULL CHECK (usage_role ~ '^[a-z0-9][a-z0-9._:-]{0,199}$'),
  observation_key text NOT NULL CHECK (btrim(observation_key) <> ''),
  capture_completeness text NOT NULL CHECK (capture_completeness IN ('complete','partial','redacted','unavailable')),
  row_lineage_status text NOT NULL CHECK (row_lineage_status IN ('exact','unknown','not-applicable')),
  query_truncated boolean,
  identity_sha256 text NOT NULL UNIQUE CHECK (identity_sha256 ~ '^[0-9a-f]{64}$'),
  recorded_at timestamptz NOT NULL,
  UNIQUE (run_id,observation_key),
  FOREIGN KEY (run_id,assignment_id,attempt_id)
    REFERENCES __JOURNAL_ATTEMPTS__(run_id,assignment_id,id),
  FOREIGN KEY (run_id,content_version_id)
    REFERENCES __SCHEMA__.content_versions(run_id,id),
  CHECK (id='content-usage-' || identity_sha256)
);
CREATE INDEX observed_content_usage_attempt_idx
  ON __SCHEMA__.observed_content_usage (attempt_id,recorded_at,id);

-- A relationship is an attributed link, not proof of semantic influence,
-- factual support, disclosure safety, or publication approval.
CREATE TABLE __SCHEMA__.content_relationships (
  id text PRIMARY KEY CHECK (id ~ '^content-relationship-[0-9a-f]{64}$'),
  application_namespace text NOT NULL CHECK (application_namespace ~ '^[a-z0-9][a-z0-9._:-]{0,199}$'),
  relationship_type text NOT NULL CHECK (relationship_type ~ '^[a-z0-9][a-z0-9._:-]{0,199}$'),
  relationship_schema_version integer NOT NULL CHECK (relationship_schema_version > 0),
  from_version_id text NOT NULL CHECK (from_version_id ~ '^content-version-[0-9a-f]{64}$'),
  to_version_id text NOT NULL CHECK (to_version_id ~ '^content-version-[0-9a-f]{64}$'),
  source_reference_kind text NOT NULL CHECK (source_reference_kind ~ '^[a-z0-9][a-z0-9._:-]{0,199}$'),
  source_reference_id text NOT NULL CHECK (btrim(source_reference_id) <> ''),
  extension_definition_id text,
  extension_definition_version integer,
  extensions jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(extensions)='object'),
  identity_sha256 text NOT NULL UNIQUE CHECK (identity_sha256 ~ '^[0-9a-f]{64}$'),
  recorded_at timestamptz NOT NULL,
  FOREIGN KEY (application_namespace,from_version_id)
    REFERENCES __SCHEMA__.content_versions(application_namespace,id),
  FOREIGN KEY (application_namespace,to_version_id)
    REFERENCES __SCHEMA__.content_versions(application_namespace,id),
  FOREIGN KEY (application_namespace,extension_definition_id,extension_definition_version)
    REFERENCES __SCHEMA__.content_extension_definitions(application_namespace,definition_id,definition_version),
  CHECK ((extension_definition_id IS NULL) = (extension_definition_version IS NULL)),
  CHECK (extension_definition_id IS NOT NULL OR extensions='{}'::jsonb),
  CHECK (id='content-relationship-' || identity_sha256)
);
CREATE INDEX content_relationships_from_idx
  ON __SCHEMA__.content_relationships (application_namespace,from_version_id,recorded_at,id);
CREATE INDEX content_relationships_to_idx
  ON __SCHEMA__.content_relationships (application_namespace,to_version_id,recorded_at,id);

CREATE FUNCTION __SCHEMA__.guard_capture_definition()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
  IF NOT __SCHEMA__.capture_js_json_safe(NEW.value_schema)
     OR coalesce(__SCHEMA__.capture_schema_node_count(NEW.value_schema,0),-1)<1 THEN
    RAISE EXCEPTION 'content extension schema uses unsupported or malformed JSON Schema'
      USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER content_extension_schema_valid
BEFORE INSERT ON __SCHEMA__.content_extension_definitions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_capture_definition();

CREATE FUNCTION __SCHEMA__.guard_content_version_extensions()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE definition_schema jsonb;
BEGIN
  IF NOT __SCHEMA__.capture_js_json_safe(NEW.extensions) THEN
    RAISE EXCEPTION 'content extensions contain values outside capture-v1'
      USING ERRCODE='23514';
  END IF;
  IF NEW.extension_definition_id IS NULL THEN RETURN NEW; END IF;
  SELECT value_schema INTO STRICT definition_schema
  FROM __SCHEMA__.content_extension_definitions
  WHERE application_namespace=NEW.application_namespace
    AND definition_id=NEW.extension_definition_id
    AND definition_version=NEW.extension_definition_version;
  IF NOT __SCHEMA__.capture_value_matches(NEW.extensions,definition_schema) THEN
    RAISE EXCEPTION 'content extensions do not match registered definition %.% version %',
      NEW.application_namespace,NEW.extension_definition_id,NEW.extension_definition_version
      USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER content_version_extensions_valid
BEFORE INSERT ON __SCHEMA__.content_versions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_content_version_extensions();

CREATE FUNCTION __SCHEMA__.guard_content_relationship_extensions()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE definition_schema jsonb;
BEGIN
  IF NOT __SCHEMA__.capture_js_json_safe(NEW.extensions) THEN
    RAISE EXCEPTION 'relationship extensions contain values outside capture-v1'
      USING ERRCODE='23514';
  END IF;
  IF NEW.extension_definition_id IS NULL THEN RETURN NEW; END IF;
  SELECT value_schema INTO STRICT definition_schema
  FROM __SCHEMA__.content_extension_definitions
  WHERE application_namespace=NEW.application_namespace
    AND definition_id=NEW.extension_definition_id
    AND definition_version=NEW.extension_definition_version;
  IF NOT __SCHEMA__.capture_value_matches(NEW.extensions,definition_schema) THEN
    RAISE EXCEPTION 'relationship extensions do not match registered definition %.% version %',
      NEW.application_namespace,NEW.extension_definition_id,NEW.extension_definition_version
      USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER content_relationship_extensions_valid
BEFORE INSERT ON __SCHEMA__.content_relationships
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_content_relationship_extensions();

CREATE FUNCTION __SCHEMA__.guard_capture_immutable()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
  RAISE EXCEPTION '% records are immutable', TG_TABLE_NAME USING ERRCODE='23514';
END;
$$;
CREATE TRIGGER content_extension_definitions_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.content_extension_definitions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_capture_immutable();
CREATE TRIGGER content_versions_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.content_versions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_capture_immutable();
CREATE TRIGGER observed_content_usage_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.observed_content_usage
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_capture_immutable();
CREATE TRIGGER content_relationships_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.content_relationships
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_capture_immutable();

CREATE TRIGGER content_extension_definitions_no_truncate
BEFORE TRUNCATE ON __SCHEMA__.content_extension_definitions
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_capture_immutable();
CREATE TRIGGER content_versions_no_truncate
BEFORE TRUNCATE ON __SCHEMA__.content_versions
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_capture_immutable();
CREATE TRIGGER observed_content_usage_no_truncate
BEFORE TRUNCATE ON __SCHEMA__.observed_content_usage
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_capture_immutable();
CREATE TRIGGER content_relationships_no_truncate
BEFORE TRUNCATE ON __SCHEMA__.content_relationships
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_capture_immutable();

-- Foreign-key and validation constraint triggers do not run in replica mode.
-- Statement guards cover TRUNCATE and zero-row DML; row guards cover changed rows.
CREATE FUNCTION __SCHEMA__.guard_replication_role()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
  IF current_setting('session_replication_role') NOT IN ('origin','local') THEN
    RAISE EXCEPTION 'session_replication_role must be origin or local for writes'
      USING ERRCODE='23514';
  END IF;
  RETURN NULL;
END;
$$;

CREATE FUNCTION __SCHEMA__.guard_replication_role_row()
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

CREATE TRIGGER content_extension_definitions_replication_role_guard
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON __SCHEMA__.content_extension_definitions
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_replication_role();
CREATE TRIGGER content_versions_replication_role_guard
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON __SCHEMA__.content_versions
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_replication_role();
CREATE TRIGGER observed_content_usage_replication_role_guard
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON __SCHEMA__.observed_content_usage
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_replication_role();
CREATE TRIGGER content_relationships_replication_role_guard
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON __SCHEMA__.content_relationships
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_replication_role();

ALTER TABLE __SCHEMA__.content_extension_definitions
  ENABLE ALWAYS TRIGGER content_extension_definitions_immutable;
ALTER TABLE __SCHEMA__.content_versions
  ENABLE ALWAYS TRIGGER content_versions_immutable;
ALTER TABLE __SCHEMA__.observed_content_usage
  ENABLE ALWAYS TRIGGER observed_content_usage_immutable;
ALTER TABLE __SCHEMA__.content_relationships
  ENABLE ALWAYS TRIGGER content_relationships_immutable;

ALTER TABLE __SCHEMA__.content_extension_definitions
  ENABLE ALWAYS TRIGGER content_extension_definitions_no_truncate;
ALTER TABLE __SCHEMA__.content_versions
  ENABLE ALWAYS TRIGGER content_versions_no_truncate;
ALTER TABLE __SCHEMA__.observed_content_usage
  ENABLE ALWAYS TRIGGER observed_content_usage_no_truncate;
ALTER TABLE __SCHEMA__.content_relationships
  ENABLE ALWAYS TRIGGER content_relationships_no_truncate;

CREATE TRIGGER content_extension_definitions_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.content_extension_definitions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_replication_role_row();
CREATE TRIGGER content_versions_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.content_versions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_replication_role_row();
CREATE TRIGGER observed_content_usage_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.observed_content_usage
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_replication_role_row();
CREATE TRIGGER content_relationships_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.content_relationships
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_replication_role_row();

ALTER TABLE __SCHEMA__.content_extension_definitions
  ENABLE ALWAYS TRIGGER content_extension_definitions_replication_role_guard;
ALTER TABLE __SCHEMA__.content_versions
  ENABLE ALWAYS TRIGGER content_versions_replication_role_guard;
ALTER TABLE __SCHEMA__.observed_content_usage
  ENABLE ALWAYS TRIGGER observed_content_usage_replication_role_guard;
ALTER TABLE __SCHEMA__.content_relationships
  ENABLE ALWAYS TRIGGER content_relationships_replication_role_guard;

ALTER TABLE __SCHEMA__.content_extension_definitions
  ENABLE ALWAYS TRIGGER content_extension_definitions_replication_role_row_guard;
ALTER TABLE __SCHEMA__.content_versions
  ENABLE ALWAYS TRIGGER content_versions_replication_role_row_guard;
ALTER TABLE __SCHEMA__.observed_content_usage
  ENABLE ALWAYS TRIGGER observed_content_usage_replication_role_row_guard;
ALTER TABLE __SCHEMA__.content_relationships
  ENABLE ALWAYS TRIGGER content_relationships_replication_role_row_guard;
