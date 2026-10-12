-- Generic agent workflow journal for one caller-owned PostgreSQL schema.
-- The bootstrap replaces __SCHEMA__ only with a validated, quoted identifier.
-- Application definitions, principal/account tables, and policy triggers are excluded.

CREATE TABLE __SCHEMA__.agent_workflow_versions (
  id text PRIMARY KEY CHECK (btrim(id) <> ''),
  workflow_type text NOT NULL CHECK (btrim(workflow_type) <> ''),
  predecessor_id text REFERENCES __SCHEMA__.agent_workflow_versions(id),
  definition jsonb NOT NULL,
  definition_sha256 text NOT NULL CHECK (definition_sha256 ~ '^[0-9a-f]{64}$'),
  code_revision text NOT NULL CHECK (btrim(code_revision) <> ''),
  change_reason text NOT NULL CHECK (btrim(change_reason) <> ''),
  improvement_hypothesis text NOT NULL CHECK (btrim(improvement_hypothesis) <> ''),
  reviewed_by text NOT NULL CHECK (btrim(reviewed_by) <> ''),
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (workflow_type,definition_sha256),
  CHECK (predecessor_id IS NULL OR predecessor_id <> id)
);

CREATE TABLE __SCHEMA__.agent_model_price_versions (
  id text PRIMARY KEY CHECK (btrim(id) <> ''),
  provider text NOT NULL CHECK (btrim(provider) <> ''),
  model text NOT NULL CHECK (btrim(model) <> ''),
  service_tier text,
  currency text NOT NULL DEFAULT 'USD' CHECK (currency ~ '^[A-Z]{3}$'),
  input_per_million numeric(20,8) NOT NULL CHECK (input_per_million >= 0),
  cached_input_per_million numeric(20,8) NOT NULL CHECK (cached_input_per_million >= 0),
  cache_write_input_per_million numeric(20,8) CHECK (cache_write_input_per_million >= 0),
  output_per_million numeric(20,8) NOT NULL CHECK (output_per_million >= 0),
  reasoning_output_per_million numeric(20,8) CHECK (reasoning_output_per_million >= 0),
  effective_from timestamptz NOT NULL,
  effective_until timestamptz,
  source_reference text NOT NULL CHECK (btrim(source_reference) <> ''),
  reviewed_by text NOT NULL CHECK (btrim(reviewed_by) <> ''),
  price_sha256 text NOT NULL CHECK (price_sha256 ~ '^[0-9a-f]{64}$'),
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (effective_until IS NULL OR effective_until > effective_from),
  UNIQUE NULLS NOT DISTINCT (provider,model,service_tier,effective_from)
);

CREATE TABLE __SCHEMA__.agent_workflow_runs (
  id text PRIMARY KEY CHECK (btrim(id) <> ''),
  workflow_version_id text NOT NULL REFERENCES __SCHEMA__.agent_workflow_versions(id),
  workflow_type text NOT NULL CHECK (btrim(workflow_type) <> ''),
  owner_kind text NOT NULL CHECK (owner_kind IN ('authenticated','public','system')),
  owner_principal_id text,
  owner_account_id text,
  owner_assistant_session_id text,
  authorization_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
  input_payload jsonb NOT NULL,
  input_sha256 text NOT NULL CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
  identity_sha256 text NOT NULL CHECK (identity_sha256 ~ '^[0-9a-f]{64}$'),
  status text NOT NULL DEFAULT 'queued' CHECK (status IN (
    'queued','running','validating','ready-for-admission','admitted',
    'failed','cancelled','blocked','completed'
  )),
  queue_priority integer NOT NULL DEFAULT 0,
  claimed_by text,
  claimed_at timestamptz,
  heartbeat_at timestamptz,
  lease_expires_at timestamptz,
  recovery_count integer NOT NULL DEFAULT 0 CHECK (recovery_count >= 0),
  resumed_from_run_id text REFERENCES __SCHEMA__.agent_workflow_runs(id),
  run_family_budget_version smallint,
  run_family_max_descendant_runs bigint,
  run_family_max_total_attempts bigint,
  run_family_max_total_tokens bigint,
  validated_manifest_sha256 text CHECK (validated_manifest_sha256 ~ '^[0-9a-f]{64}$'),
  last_validation_passed_at timestamptz,
  admitted_manifest_sha256 text CHECK (admitted_manifest_sha256 ~ '^[0-9a-f]{64}$'),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  started_at timestamptz,
  completed_at timestamptz,
  failure jsonb,
  final_summary jsonb,
  CHECK (
    (owner_kind='authenticated' AND owner_principal_id IS NOT NULL AND owner_assistant_session_id IS NULL)
    OR (owner_kind='public' AND owner_principal_id IS NULL AND owner_account_id IS NULL AND owner_assistant_session_id IS NOT NULL)
    OR (owner_kind='system' AND owner_principal_id IS NOT NULL AND owner_account_id IS NULL AND owner_assistant_session_id IS NULL)
  ),
  CHECK (
    (claimed_by IS NULL AND claimed_at IS NULL AND heartbeat_at IS NULL AND lease_expires_at IS NULL)
    OR (claimed_by IS NOT NULL AND claimed_at IS NOT NULL AND heartbeat_at IS NOT NULL AND lease_expires_at IS NOT NULL)
  ),
  CHECK (status <> 'ready-for-admission' OR (validated_manifest_sha256 IS NOT NULL AND last_validation_passed_at IS NOT NULL)),
  CHECK (status <> 'admitted' OR (admitted_manifest_sha256 IS NOT NULL AND completed_at IS NOT NULL)),
  CONSTRAINT agent_workflow_runs_completed_state_check
    CHECK (status <> 'completed' OR (completed_at IS NOT NULL AND final_summary IS NOT NULL AND admitted_manifest_sha256 IS NULL)),
  CHECK (completed_at IS NULL OR started_at IS NULL OR completed_at >= started_at),
  CHECK (resumed_from_run_id IS NULL OR resumed_from_run_id <> id),
  CHECK ((run_family_budget_version IS NULL) = (run_family_max_descendant_runs IS NULL)),
  CHECK (run_family_budget_version IS NULL OR
    (run_family_budget_version IN (1,2,3) AND resumed_from_run_id IS NULL
      AND run_family_max_descendant_runs BETWEEN 0 AND 9007199254740991)),
  CHECK (CASE WHEN run_family_budget_version IN (2,3) THEN
      run_family_max_total_attempts IS NOT NULL
      AND run_family_max_total_attempts BETWEEN 0 AND 9007199254740991
    ELSE run_family_max_total_attempts IS NULL END),
  CHECK (CASE WHEN run_family_budget_version=3 THEN
      run_family_max_total_tokens IS NOT NULL
      AND run_family_max_total_tokens BETWEEN 0 AND 9007199254740991
    ELSE run_family_max_total_tokens IS NULL END)
);
CREATE INDEX agent_workflow_runs_queue_idx
  ON __SCHEMA__.agent_workflow_runs (queue_priority DESC,created_at,id)
  WHERE status='queued';
CREATE INDEX agent_workflow_runs_owner_idx
  ON __SCHEMA__.agent_workflow_runs (owner_kind,owner_principal_id,owner_account_id,created_at DESC,id);
CREATE INDEX agent_workflow_runs_expired_lease_idx
  ON __SCHEMA__.agent_workflow_runs (lease_expires_at,id)
  WHERE status='running';

-- The root declaration lives in the immutable run identity. A separate family
-- row and immutable member rows provide a lifetime descendant count.
CREATE TABLE __SCHEMA__.agent_workflow_run_families (
  root_run_id text PRIMARY KEY REFERENCES __SCHEMA__.agent_workflow_runs(id) ON DELETE RESTRICT,
  budget_version smallint NOT NULL CHECK (budget_version IN (1,2,3)),
  max_descendant_runs bigint NOT NULL CHECK (max_descendant_runs BETWEEN 0 AND 9007199254740991),
  max_total_attempts bigint,
  max_total_tokens bigint,
  CHECK ((budget_version IN (2,3) AND max_total_attempts IS NOT NULL
      AND max_total_attempts BETWEEN 0 AND 9007199254740991)
    OR (budget_version=1 AND max_total_attempts IS NULL)),
  CHECK ((budget_version=3 AND max_total_tokens IS NOT NULL
      AND max_total_tokens BETWEEN 0 AND 9007199254740991)
    OR (budget_version IN (1,2) AND max_total_tokens IS NULL))
);
CREATE TABLE __SCHEMA__.agent_workflow_run_family_members (
  run_id text PRIMARY KEY REFERENCES __SCHEMA__.agent_workflow_runs(id) ON DELETE RESTRICT,
  root_run_id text NOT NULL REFERENCES __SCHEMA__.agent_workflow_run_families(root_run_id) ON DELETE RESTRICT,
  parent_run_id text NOT NULL REFERENCES __SCHEMA__.agent_workflow_runs(id) ON DELETE RESTRICT,
  CHECK (run_id<>root_run_id AND run_id<>parent_run_id)
);
CREATE INDEX agent_workflow_run_family_members_root_idx
  ON __SCHEMA__.agent_workflow_run_family_members (root_run_id,run_id);

CREATE TABLE __SCHEMA__.agent_workflow_assignments (
  id text PRIMARY KEY CHECK (btrim(id) <> ''),
  run_id text NOT NULL REFERENCES __SCHEMA__.agent_workflow_runs(id) ON DELETE CASCADE,
  parent_assignment_id text,
  assignment_ordinal integer NOT NULL CHECK (assignment_ordinal > 0),
  marker text NOT NULL UNIQUE CHECK (btrim(marker) <> ''),
  step_key text NOT NULL CHECK (btrim(step_key) <> ''),
  assigned_to text NOT NULL CHECK (btrim(assigned_to) <> ''),
  execution text NOT NULL CHECK (execution IN ('subagent','current-agent')),
  status text NOT NULL CHECK (status IN ('dispatching','running','unknown','completed','failed','interrupted')),
  agent_thread_id text,
  identity_sha256 text NOT NULL CHECK (identity_sha256 ~ '^[0-9a-f]{64}$'),
  created_at timestamptz NOT NULL,
  started_at timestamptz,
  updated_at timestamptz NOT NULL,
  completed_at timestamptz,
  finished_at timestamptz,
  failure text,
  finish_sha256 text CHECK (finish_sha256 ~ '^[0-9a-f]{64}$'),
  UNIQUE (run_id,assignment_ordinal),
  UNIQUE (run_id,id),
  FOREIGN KEY (run_id,parent_assignment_id)
    REFERENCES __SCHEMA__.agent_workflow_assignments(run_id,id),
  CHECK (finished_at IS NULL OR finished_at >= created_at),
  CHECK (parent_assignment_id IS NULL OR parent_assignment_id <> id)
);
CREATE INDEX agent_workflow_assignments_run_idx
  ON __SCHEMA__.agent_workflow_assignments (run_id,assignment_ordinal);
CREATE INDEX agent_workflow_assignments_thread_idx
  ON __SCHEMA__.agent_workflow_assignments (agent_thread_id,updated_at DESC)
  WHERE agent_thread_id IS NOT NULL;

CREATE TABLE __SCHEMA__.agent_workflow_assignment_targets (
  run_id text NOT NULL,
  assignment_id text NOT NULL,
  target_ordinal integer NOT NULL CHECK (target_ordinal >= 0),
  target_type text NOT NULL CHECK (btrim(target_type) <> ''),
  target_id text NOT NULL CHECK (btrim(target_id) <> ''),
  expected_version bigint CHECK (expected_version >= 0),
  PRIMARY KEY (assignment_id,target_ordinal),
  UNIQUE (assignment_id,target_type,target_id),
  FOREIGN KEY (run_id,assignment_id)
    REFERENCES __SCHEMA__.agent_workflow_assignments(run_id,id) ON DELETE CASCADE
);
CREATE INDEX agent_workflow_assignment_targets_active_idx
  ON __SCHEMA__.agent_workflow_assignment_targets (target_type,target_id,run_id,assignment_id);

CREATE TABLE __SCHEMA__.agent_workflow_attempts (
  id text PRIMARY KEY CHECK (btrim(id) <> ''),
  run_id text NOT NULL,
  assignment_id text NOT NULL,
  attempt_ordinal integer NOT NULL CHECK (attempt_ordinal > 0),
  prompt text NOT NULL CHECK (btrim(prompt) <> '' AND octet_length(prompt) <= 1048576),
  prompt_sha256 text NOT NULL CHECK (prompt_sha256 ~ '^[0-9a-f]{64}$'),
  requested_model text NOT NULL CHECK (btrim(requested_model) <> ''),
  requested_reasoning_effort text,
  requested_service_tier text,
  observed_provider text,
  observed_model text,
  observed_reasoning_effort text,
  observed_service_tier text,
  capability_profile jsonb NOT NULL,
  capability_sha256 text NOT NULL CHECK (capability_sha256 ~ '^[0-9a-f]{64}$'),
  thread_id text,
  turn_id text,
  started_at timestamptz NOT NULL,
  completed_at timestamptz,
  outcome text CHECK (outcome IN ('completed','failed','interrupted')),
  telemetry_status text NOT NULL DEFAULT 'pending' CHECK (telemetry_status IN ('pending','complete','incomplete','inconsistent')),
  input_tokens bigint CHECK (input_tokens >= 0),
  cached_input_tokens bigint CHECK (cached_input_tokens >= 0),
  cache_write_input_tokens bigint CHECK (cache_write_input_tokens >= 0),
  output_tokens bigint CHECK (output_tokens >= 0),
  reasoning_output_tokens bigint CHECK (reasoning_output_tokens >= 0),
  total_tokens bigint CHECK (total_tokens >= 0),
  resource_reserved_total_tokens bigint CHECK (resource_reserved_total_tokens BETWEEN 0 AND 9007199254740991),
  resource_usage_basis text CHECK (resource_usage_basis IN ('attempt','unknown')),
  price_version_id text REFERENCES __SCHEMA__.agent_model_price_versions(id),
  api_equivalent_cost numeric(20,8) CHECK (api_equivalent_cost >= 0),
  api_equivalent_cost_currency text CHECK (api_equivalent_cost_currency ~ '^[A-Z]{3}$'),
  billed_cost numeric(20,8) CHECK (billed_cost >= 0),
  billed_cost_currency text CHECK (billed_cost_currency ~ '^[A-Z]{3}$'),
  result_payload jsonb,
  result_sha256 text CHECK (result_sha256 ~ '^[0-9a-f]{64}$'),
  error text,
  finish_sha256 text CHECK (finish_sha256 ~ '^[0-9a-f]{64}$'),
  UNIQUE (assignment_id,attempt_ordinal),
  UNIQUE (thread_id,turn_id),
  UNIQUE (run_id,assignment_id,id),
  FOREIGN KEY (run_id,assignment_id)
    REFERENCES __SCHEMA__.agent_workflow_assignments(run_id,id) ON DELETE CASCADE,
  CHECK ((outcome IS NULL) = (completed_at IS NULL)),
  CHECK ((api_equivalent_cost IS NULL) = (api_equivalent_cost_currency IS NULL)),
  CHECK ((billed_cost IS NULL) = (billed_cost_currency IS NULL)),
  CHECK (input_tokens IS NULL OR coalesce(cached_input_tokens,0) + coalesce(cache_write_input_tokens,0) <= input_tokens),
  CHECK (output_tokens IS NULL OR coalesce(reasoning_output_tokens,0) <= output_tokens),
  CHECK (total_tokens IS NULL OR input_tokens IS NULL OR output_tokens IS NULL OR total_tokens >= input_tokens + output_tokens),
  CHECK (resource_usage_basis IS NULL OR outcome IS NOT NULL),
  CHECK (resource_reserved_total_tokens IS NULL OR outcome IS NOT NULL OR (
    input_tokens IS NULL AND cached_input_tokens IS NULL
    AND cache_write_input_tokens IS NULL AND output_tokens IS NULL
    AND reasoning_output_tokens IS NULL AND total_tokens IS NULL))
);
CREATE INDEX agent_workflow_attempts_assignment_idx
  ON __SCHEMA__.agent_workflow_attempts (assignment_id,attempt_ordinal);
CREATE INDEX agent_workflow_attempts_run_started_idx
  ON __SCHEMA__.agent_workflow_attempts (run_id,started_at DESC,id DESC);

CREATE TABLE __SCHEMA__.agent_workflow_events (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  run_id text NOT NULL REFERENCES __SCHEMA__.agent_workflow_runs(id) ON DELETE CASCADE,
  assignment_id text,
  attempt_id text,
  source_key text NOT NULL CHECK (btrim(source_key) <> '' AND octet_length(source_key) <= 2048),
  event_type text NOT NULL CHECK (btrim(event_type) <> ''),
  thread_id text,
  turn_id text,
  occurred_at timestamptz,
  received_at timestamptz NOT NULL DEFAULT now(),
  payload jsonb NOT NULL,
  payload_sha256 text NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
  UNIQUE (run_id,source_key),
  FOREIGN KEY (run_id,assignment_id)
    REFERENCES __SCHEMA__.agent_workflow_assignments(run_id,id) ON DELETE CASCADE,
  FOREIGN KEY (run_id,assignment_id,attempt_id)
    REFERENCES __SCHEMA__.agent_workflow_attempts(run_id,assignment_id,id) ON DELETE CASCADE,
  CHECK (attempt_id IS NULL OR assignment_id IS NOT NULL),
  CHECK (octet_length(payload::text) <= 1048576)
);
CREATE INDEX agent_workflow_events_attempt_idx
  ON __SCHEMA__.agent_workflow_events (attempt_id,id) WHERE attempt_id IS NOT NULL;
CREATE INDEX agent_workflow_events_thread_turn_idx
  ON __SCHEMA__.agent_workflow_events (thread_id,turn_id,id)
  WHERE thread_id IS NOT NULL;

CREATE TABLE __SCHEMA__.agent_workflow_staged_commands (
  id text PRIMARY KEY CHECK (btrim(id) <> ''),
  run_id text NOT NULL,
  assignment_id text NOT NULL,
  attempt_id text NOT NULL,
  command_ordinal integer NOT NULL CHECK (command_ordinal > 0),
  command_type text NOT NULL CHECK (btrim(command_type) <> ''),
  schema_version integer NOT NULL CHECK (schema_version > 0),
  payload jsonb NOT NULL,
  payload_sha256 text NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
  expected_versions jsonb NOT NULL DEFAULT '{}'::jsonb,
  dependency_ids text[] NOT NULL DEFAULT '{}',
  status text NOT NULL DEFAULT 'staged' CHECK (status IN ('staged','validated','admitted','rejected')),
  manifest_sha256 text CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
  staged_at timestamptz NOT NULL DEFAULT now(),
  validated_at timestamptz,
  admitted_at timestamptz,
  UNIQUE (run_id,command_ordinal),
  UNIQUE (run_id,id),
  FOREIGN KEY (run_id,assignment_id,attempt_id)
    REFERENCES __SCHEMA__.agent_workflow_attempts(run_id,assignment_id,id) ON DELETE RESTRICT,
  CHECK (array_position(dependency_ids,id) IS NULL),
  CHECK (status='staged' OR manifest_sha256 IS NOT NULL),
  CONSTRAINT agent_workflow_staged_payload_size CHECK (octet_length(payload::text) <= 4194304)
);
CREATE INDEX agent_workflow_staged_commands_run_idx
  ON __SCHEMA__.agent_workflow_staged_commands (run_id,command_ordinal);

CREATE TABLE __SCHEMA__.agent_workflow_validations (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  run_id text NOT NULL REFERENCES __SCHEMA__.agent_workflow_runs(id) ON DELETE CASCADE,
  manifest_sha256 text NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
  validator_name text NOT NULL CHECK (btrim(validator_name) <> ''),
  validator_version text NOT NULL CHECK (btrim(validator_version) <> ''),
  passed boolean NOT NULL,
  issues jsonb NOT NULL DEFAULT '[]'::jsonb,
  measurements jsonb NOT NULL DEFAULT '{}'::jsonb,
  completed_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (run_id,manifest_sha256,validator_name,validator_version)
);
CREATE INDEX agent_workflow_validations_manifest_idx
  ON __SCHEMA__.agent_workflow_validations (run_id,manifest_sha256,passed);

CREATE TABLE __SCHEMA__.agent_workflow_admissions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id text NOT NULL UNIQUE REFERENCES __SCHEMA__.agent_workflow_runs(id) ON DELETE RESTRICT,
  manifest_sha256 text NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
  admitted_by text NOT NULL CHECK (btrim(admitted_by) <> ''),
  receipt jsonb NOT NULL,
  receipt_sha256 text NOT NULL CHECK (receipt_sha256 ~ '^[0-9a-f]{64}$'),
  admitted_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (run_id,manifest_sha256)
);

CREATE FUNCTION __SCHEMA__.guard_agent_workflow_immutable_record()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  RAISE EXCEPTION '% records are immutable', TG_TABLE_NAME;
END;
$$;

CREATE TRIGGER agent_workflow_versions_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.agent_workflow_versions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_immutable_record();
CREATE TRIGGER agent_model_price_versions_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.agent_model_price_versions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_immutable_record();
-- Attempt rows are the lifetime budget ledger. Their completion fields may be
-- updated, but deleting rows or truncating the table cannot refund a slot.
CREATE TRIGGER agent_workflow_attempts_delete_immutable
BEFORE DELETE ON __SCHEMA__.agent_workflow_attempts
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_immutable_record();
CREATE TRIGGER agent_workflow_attempts_truncate_immutable
BEFORE TRUNCATE ON __SCHEMA__.agent_workflow_attempts
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_immutable_record();
CREATE TRIGGER agent_workflow_events_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.agent_workflow_events
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_immutable_record();
CREATE TRIGGER agent_workflow_validations_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.agent_workflow_validations
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_immutable_record();
CREATE TRIGGER agent_workflow_admissions_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.agent_workflow_admissions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_immutable_record();

CREATE FUNCTION __SCHEMA__.guard_agent_workflow_attempt_completion()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF OLD.outcome IS NOT NULL AND NEW IS DISTINCT FROM OLD THEN
    RAISE EXCEPTION 'completed agent workflow attempts are immutable';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER agent_workflow_attempt_completion_immutable
BEFORE UPDATE ON __SCHEMA__.agent_workflow_attempts
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_attempt_completion();

CREATE FUNCTION __SCHEMA__.guard_agent_workflow_run_identity()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF NEW.workflow_version_id IS DISTINCT FROM OLD.workflow_version_id OR
     NEW.workflow_type IS DISTINCT FROM OLD.workflow_type OR
     NEW.owner_kind IS DISTINCT FROM OLD.owner_kind OR
     NEW.owner_principal_id IS DISTINCT FROM OLD.owner_principal_id OR
     NEW.owner_account_id IS DISTINCT FROM OLD.owner_account_id OR
     NEW.owner_assistant_session_id IS DISTINCT FROM OLD.owner_assistant_session_id OR
     NEW.authorization_snapshot IS DISTINCT FROM OLD.authorization_snapshot OR
     NEW.input_payload IS DISTINCT FROM OLD.input_payload OR
     NEW.input_sha256 IS DISTINCT FROM OLD.input_sha256 OR
     NEW.identity_sha256 IS DISTINCT FROM OLD.identity_sha256 OR
     NEW.queue_priority IS DISTINCT FROM OLD.queue_priority OR
     NEW.resumed_from_run_id IS DISTINCT FROM OLD.resumed_from_run_id OR
     NEW.run_family_budget_version IS DISTINCT FROM OLD.run_family_budget_version OR
     NEW.run_family_max_descendant_runs IS DISTINCT FROM OLD.run_family_max_descendant_runs OR
     NEW.run_family_max_total_attempts IS DISTINCT FROM OLD.run_family_max_total_attempts OR
     NEW.run_family_max_total_tokens IS DISTINCT FROM OLD.run_family_max_total_tokens OR
     NEW.created_at IS DISTINCT FROM OLD.created_at THEN
    RAISE EXCEPTION 'agent workflow run identity and input are immutable';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER agent_workflow_run_identity_immutable
BEFORE UPDATE ON __SCHEMA__.agent_workflow_runs
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_run_identity();

CREATE FUNCTION __SCHEMA__.guard_agent_workflow_run_family()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE root_parent text; root_version smallint; root_max bigint; root_attempt_max bigint; root_token_max bigint;
BEGIN
  SELECT resumed_from_run_id,run_family_budget_version,run_family_max_descendant_runs,run_family_max_total_attempts,run_family_max_total_tokens
  INTO root_parent,root_version,root_max,root_attempt_max,root_token_max
  FROM __SCHEMA__.agent_workflow_runs WHERE id=NEW.root_run_id;
  IF NOT FOUND OR root_parent IS NOT NULL
     OR root_version IS DISTINCT FROM NEW.budget_version
     OR root_max IS DISTINCT FROM NEW.max_descendant_runs
     OR root_attempt_max IS DISTINCT FROM NEW.max_total_attempts
     OR root_token_max IS DISTINCT FROM NEW.max_total_tokens THEN
    RAISE EXCEPTION 'run family declaration must match its immutable root run'
      USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER agent_workflow_run_family_declaration_guard
BEFORE INSERT ON __SCHEMA__.agent_workflow_run_families
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_run_family();
CREATE TRIGGER agent_workflow_run_families_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.agent_workflow_run_families
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_immutable_record();
CREATE TRIGGER agent_workflow_run_families_truncate_immutable
BEFORE TRUNCATE ON __SCHEMA__.agent_workflow_run_families
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_immutable_record();

-- The family row is the serialization point. READ COMMITTED gives the count a
-- fresh snapshot after waiting for that lock; other snapshot modes fail closed.
CREATE FUNCTION __SCHEMA__.guard_agent_workflow_run_family_member()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE maximum bigint; parent_id text; parent_root text; occupied bigint;
BEGIN
  IF current_setting('transaction_isolation')<>'read committed' THEN
    RAISE EXCEPTION 'run family membership requires READ COMMITTED isolation'
      USING ERRCODE='23514';
  END IF;
  SELECT max_descendant_runs INTO maximum
  FROM __SCHEMA__.agent_workflow_run_families WHERE root_run_id=NEW.root_run_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'run family root does not exist' USING ERRCODE='23514';
  END IF;
  SELECT resumed_from_run_id INTO parent_id
  FROM __SCHEMA__.agent_workflow_runs WHERE id=NEW.run_id;
  IF NOT FOUND OR parent_id IS NULL OR parent_id IS DISTINCT FROM NEW.parent_run_id THEN
    RAISE EXCEPTION 'run family member must match its run parent' USING ERRCODE='23514';
  END IF;
  SELECT root_run_id INTO parent_root
  FROM __SCHEMA__.agent_workflow_run_families WHERE root_run_id=parent_id;
  IF NOT FOUND THEN
    SELECT root_run_id INTO parent_root
    FROM __SCHEMA__.agent_workflow_run_family_members WHERE run_id=parent_id;
  END IF;
  IF parent_root IS DISTINCT FROM NEW.root_run_id THEN
    RAISE EXCEPTION 'run family member has foreign ancestry' USING ERRCODE='23514';
  END IF;
  SELECT count(*) INTO occupied FROM __SCHEMA__.agent_workflow_run_family_members
  WHERE root_run_id=NEW.root_run_id;
  IF occupied>=maximum THEN
    RAISE EXCEPTION 'run family descendant budget is exhausted' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER agent_workflow_run_family_member_guard
BEFORE INSERT ON __SCHEMA__.agent_workflow_run_family_members
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_run_family_member();
CREATE TRIGGER agent_workflow_run_family_members_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.agent_workflow_run_family_members
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_immutable_record();
CREATE TRIGGER agent_workflow_run_family_members_truncate_immutable
BEFORE TRUNCATE ON __SCHEMA__.agent_workflow_run_family_members
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_immutable_record();

-- Runtime receives EXECUTE only on this narrow lock helper, not UPDATE on the
-- immutable family row. Recheck the parent after acquiring the root lock.
CREATE FUNCTION __SCHEMA__.lock_agent_workflow_run_family(parent_id text)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE root_id text; locked_id text; checked_id text;
BEGIN
  SELECT root_run_id INTO root_id FROM __SCHEMA__.agent_workflow_run_families
  WHERE root_run_id=parent_id;
  IF root_id IS NULL THEN
    SELECT root_run_id INTO root_id FROM __SCHEMA__.agent_workflow_run_family_members
    WHERE run_id=parent_id;
  END IF;
  IF root_id IS NULL THEN RETURN NULL; END IF;
  SELECT root_run_id INTO locked_id FROM __SCHEMA__.agent_workflow_run_families
  WHERE root_run_id=root_id FOR UPDATE;
  IF locked_id IS NULL THEN
    RAISE EXCEPTION 'run family root is unavailable' USING ERRCODE='23514';
  END IF;
  SELECT root_run_id INTO checked_id FROM __SCHEMA__.agent_workflow_run_families
  WHERE root_run_id=parent_id;
  IF checked_id IS NULL THEN
    SELECT root_run_id INTO checked_id FROM __SCHEMA__.agent_workflow_run_family_members
    WHERE run_id=parent_id;
  END IF;
  IF checked_id IS DISTINCT FROM locked_id THEN
    RAISE EXCEPTION 'run family ancestry changed during lock' USING ERRCODE='23514';
  END IF;
  RETURN locked_id;
END;
$$;

-- Direct settlement already holds its attempt row. NOWAIT makes contention a
-- retryable 55P03 error rather than waiting in reverse lock order. Retry only
-- after rolling back the transaction, or to a savepoint before the UPDATE.
CREATE FUNCTION __SCHEMA__.try_lock_agent_workflow_run_family(parent_id text)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE root_id text; locked_id text; checked_id text;
BEGIN
  SELECT root_run_id INTO root_id FROM __SCHEMA__.agent_workflow_run_families
    WHERE root_run_id=parent_id;
  IF root_id IS NULL THEN
    SELECT root_run_id INTO root_id FROM __SCHEMA__.agent_workflow_run_family_members
      WHERE run_id=parent_id;
  END IF;
  IF root_id IS NULL THEN RETURN NULL; END IF;
  SELECT root_run_id INTO locked_id FROM __SCHEMA__.agent_workflow_run_families
    WHERE root_run_id=root_id FOR UPDATE NOWAIT;
  IF locked_id IS NULL THEN
    RAISE EXCEPTION 'run family root is unavailable' USING ERRCODE='23514';
  END IF;
  SELECT root_run_id INTO checked_id FROM __SCHEMA__.agent_workflow_run_families
    WHERE root_run_id=parent_id;
  IF checked_id IS NULL THEN
    SELECT root_run_id INTO checked_id FROM __SCHEMA__.agent_workflow_run_family_members
      WHERE run_id=parent_id;
  END IF;
  IF checked_id IS DISTINCT FROM locked_id THEN
    RAISE EXCEPTION 'run family ancestry changed during lock' USING ERRCODE='23514';
  END IF;
  RETURN locked_id;
END;
$$;

CREATE FUNCTION __SCHEMA__.guard_agent_workflow_run_family_consistency()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE declared_version smallint; declared_max bigint; declared_attempt_max bigint; declared_token_max bigint; parent_root text;
  member_root text; member_parent text;
BEGIN
  SELECT budget_version,max_descendant_runs,max_total_attempts,max_total_tokens INTO declared_version,declared_max,declared_attempt_max,declared_token_max
  FROM __SCHEMA__.agent_workflow_run_families WHERE root_run_id=NEW.id;
  SELECT root_run_id,parent_run_id INTO member_root,member_parent
  FROM __SCHEMA__.agent_workflow_run_family_members WHERE run_id=NEW.id;
  IF NEW.run_family_budget_version IS NOT NULL THEN
    IF NEW.resumed_from_run_id IS NOT NULL OR declared_version IS NULL
       OR declared_version IS DISTINCT FROM NEW.run_family_budget_version
       OR declared_max IS DISTINCT FROM NEW.run_family_max_descendant_runs
       OR declared_attempt_max IS DISTINCT FROM NEW.run_family_max_total_attempts
       OR declared_token_max IS DISTINCT FROM NEW.run_family_max_total_tokens
       OR member_root IS NOT NULL THEN
      RAISE EXCEPTION 'run family declaration is incomplete' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
  END IF;
  IF declared_version IS NOT NULL OR NEW.resumed_from_run_id IS NULL THEN
    IF member_root IS NOT NULL OR declared_version IS NOT NULL THEN
      RAISE EXCEPTION 'run family membership is inconsistent' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
  END IF;
  SELECT root_run_id INTO parent_root FROM __SCHEMA__.agent_workflow_run_families
  WHERE root_run_id=NEW.resumed_from_run_id;
  IF NOT FOUND THEN
    SELECT root_run_id INTO parent_root FROM __SCHEMA__.agent_workflow_run_family_members
    WHERE run_id=NEW.resumed_from_run_id;
  END IF;
  IF parent_root IS NULL THEN
    IF member_root IS NOT NULL THEN
      RAISE EXCEPTION 'unbudgeted run cannot claim family membership' USING ERRCODE='23514';
    END IF;
  ELSIF member_root IS DISTINCT FROM parent_root
     OR member_parent IS DISTINCT FROM NEW.resumed_from_run_id THEN
    RAISE EXCEPTION 'budgeted descendant lacks exact family membership' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER agent_workflow_run_family_consistency
AFTER INSERT ON __SCHEMA__.agent_workflow_runs
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_run_family_consistency();

-- Resolve ancestry from immutable run parents, not from member rows, so an
-- attempt on a v2/v3 descendant with missing membership fails immediately.
-- The family row serializes admission; READ COMMITTED refreshes the count
-- snapshot after waiting for that lock. The trigger runs with owner rights
-- because runtime has no UPDATE privilege on immutable family declarations.
CREATE FUNCTION __SCHEMA__.guard_agent_workflow_total_attempt_budget()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE current_id text := NEW.run_id; parent_id text; root_id text;
  root_version smallint; root_max bigint; root_token_max bigint;
  declared_version smallint; declared_max bigint; declared_token_max bigint;
  member_root text; member_parent text; occupied bigint; committed_tokens numeric; prior_overrun boolean;
  visited_ids text[] := ARRAY[]::text[];
BEGIN
  LOOP
    IF current_id = ANY(visited_ids) THEN
      RAISE EXCEPTION 'run family attempt ancestry is invalid' USING ERRCODE='23514';
    END IF;
    visited_ids := array_append(visited_ids,current_id);
    SELECT resumed_from_run_id,run_family_budget_version,run_family_max_total_attempts,run_family_max_total_tokens
      INTO parent_id,root_version,root_max,root_token_max
      FROM __SCHEMA__.agent_workflow_runs WHERE id=current_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'run family attempt ancestry is incomplete' USING ERRCODE='23514';
    END IF;
    EXIT WHEN parent_id IS NULL;
    current_id := parent_id;
  END LOOP;
  root_id := current_id;
  IF root_version IS DISTINCT FROM 2 AND root_version IS DISTINCT FROM 3 THEN
    IF NEW.resource_reserved_total_tokens IS NOT NULL OR NEW.resource_usage_basis IS NOT NULL THEN
      RAISE EXCEPTION 'resource accounting requires version 3 family' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
  END IF;
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'run family attempts require READ COMMITTED isolation' USING ERRCODE='23514';
  END IF;
  SELECT budget_version,max_total_attempts,max_total_tokens INTO declared_version,declared_max,declared_token_max
    FROM __SCHEMA__.agent_workflow_run_families WHERE root_run_id=root_id FOR UPDATE;
  IF NOT FOUND OR declared_version IS DISTINCT FROM root_version
     OR declared_max IS DISTINCT FROM root_max
     OR declared_token_max IS DISTINCT FROM root_token_max THEN
    RAISE EXCEPTION 'run family attempt declaration is incomplete' USING ERRCODE='23514';
  END IF;
  current_id := NEW.run_id;
  WHILE current_id <> root_id LOOP
    SELECT resumed_from_run_id INTO parent_id
      FROM __SCHEMA__.agent_workflow_runs WHERE id=current_id;
    SELECT root_run_id,parent_run_id INTO member_root,member_parent
      FROM __SCHEMA__.agent_workflow_run_family_members WHERE run_id=current_id;
    IF NOT FOUND OR member_root IS DISTINCT FROM root_id
       OR member_parent IS DISTINCT FROM parent_id THEN
      RAISE EXCEPTION 'run family attempt membership is incomplete' USING ERRCODE='23514';
    END IF;
    current_id := parent_id;
  END LOOP;
  SELECT count(*) INTO occupied FROM __SCHEMA__.agent_workflow_attempts AS attempt
    WHERE attempt.run_id=root_id OR EXISTS (
      SELECT 1 FROM __SCHEMA__.agent_workflow_run_family_members AS member
      WHERE member.run_id=attempt.run_id AND member.root_run_id=root_id
    );
  IF occupied >= declared_max THEN
    RAISE EXCEPTION 'run family total attempt budget is exhausted' USING ERRCODE='23514';
  END IF;
  IF root_version = 3 THEN
    IF NEW.resource_reserved_total_tokens IS NULL THEN
      RAISE EXCEPTION 'version 3 attempt requires a token reservation' USING ERRCODE='23514';
    END IF;
    IF NEW.outcome IS NOT NULL OR NEW.resource_usage_basis IS NOT NULL THEN
      RAISE EXCEPTION 'version 3 attempt must begin before settlement' USING ERRCODE='23514';
    END IF;
    IF NEW.input_tokens IS NOT NULL OR NEW.cached_input_tokens IS NOT NULL
       OR NEW.cache_write_input_tokens IS NOT NULL OR NEW.output_tokens IS NOT NULL
       OR NEW.reasoning_output_tokens IS NOT NULL OR NEW.total_tokens IS NOT NULL THEN
      RAISE EXCEPTION 'version 3 attempt cannot begin with token usage' USING ERRCODE='23514';
    END IF;
    SELECT coalesce(sum(CASE WHEN attempt.resource_usage_basis='attempt'
      THEN attempt.total_tokens ELSE attempt.resource_reserved_total_tokens END),0)
      ,coalesce(bool_or(attempt.resource_usage_basis='attempt'
        AND attempt.total_tokens > attempt.resource_reserved_total_tokens),false)
      INTO committed_tokens,prior_overrun
      FROM __SCHEMA__.agent_workflow_attempts AS attempt
      WHERE attempt.run_id=root_id OR EXISTS (
        SELECT 1 FROM __SCHEMA__.agent_workflow_run_family_members AS member
        WHERE member.run_id=attempt.run_id AND member.root_run_id=root_id
      );
    IF prior_overrun OR committed_tokens + NEW.resource_reserved_total_tokens > declared_token_max THEN
      RAISE EXCEPTION 'run family total token budget is exhausted' USING ERRCODE='23514';
    END IF;
  ELSIF NEW.resource_reserved_total_tokens IS NOT NULL OR NEW.resource_usage_basis IS NOT NULL THEN
    RAISE EXCEPTION 'resource accounting requires version 3 family' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER agent_workflow_total_attempt_budget
BEFORE INSERT ON __SCHEMA__.agent_workflow_attempts
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_total_attempt_budget();

-- The reservation is admission accounting, not a provider-side hard cap.
-- Actual measured usage can exceed it; any per-attempt overrun remains recorded
-- and blocks all later admissions, even with remaining family capacity. A direct SQL
-- UPDATE already holds the attempt row, so acquire the family row with NOWAIT:
-- concurrent admission fails this write for retry instead of deadlocking.
CREATE FUNCTION __SCHEMA__.guard_agent_workflow_resource_settlement()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE root_id text; family_version smallint;
BEGIN
  IF NEW.resource_reserved_total_tokens IS DISTINCT FROM OLD.resource_reserved_total_tokens THEN
    RAISE EXCEPTION 'attempt token reservation is immutable' USING ERRCODE='23514';
  END IF;
  IF NEW.outcome IS NULL THEN
    IF NEW.resource_usage_basis IS NOT NULL THEN
      RAISE EXCEPTION 'active attempt cannot have resource usage basis' USING ERRCODE='23514';
    END IF;
    IF NEW.resource_reserved_total_tokens IS NOT NULL AND (
       NEW.input_tokens IS NOT NULL OR NEW.cached_input_tokens IS NOT NULL
       OR NEW.cache_write_input_tokens IS NOT NULL OR NEW.output_tokens IS NOT NULL
       OR NEW.reasoning_output_tokens IS NOT NULL OR NEW.total_tokens IS NOT NULL) THEN
      RAISE EXCEPTION 'active version 3 attempt cannot have token usage' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
  END IF;
  SELECT root_run_id INTO root_id FROM __SCHEMA__.agent_workflow_run_families
    WHERE root_run_id=NEW.run_id;
  IF root_id IS NULL THEN
    SELECT root_run_id INTO root_id FROM __SCHEMA__.agent_workflow_run_family_members
      WHERE run_id=NEW.run_id;
  END IF;
  IF root_id IS NOT NULL THEN
    SELECT budget_version INTO family_version FROM __SCHEMA__.agent_workflow_run_families
      WHERE root_run_id=root_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'run family root is unavailable' USING ERRCODE='23514';
    END IF;
  END IF;
  IF family_version = 3 THEN
    IF __SCHEMA__.try_lock_agent_workflow_run_family(NEW.run_id) IS DISTINCT FROM root_id THEN
      RAISE EXCEPTION 'run family ancestry changed during settlement' USING ERRCODE='23514';
    END IF;
    IF NEW.resource_reserved_total_tokens IS NULL OR NEW.resource_usage_basis IS NULL THEN
      RAISE EXCEPTION 'version 3 attempt requires reservation and usage basis' USING ERRCODE='23514';
    END IF;
    IF NEW.input_tokens > 9007199254740991 OR
       NEW.cached_input_tokens > 9007199254740991 OR
       NEW.cache_write_input_tokens > 9007199254740991 OR
       NEW.output_tokens > 9007199254740991 OR
       NEW.reasoning_output_tokens > 9007199254740991 OR
       NEW.total_tokens > 9007199254740991 THEN
      RAISE EXCEPTION 'version 3 token counters exceed safe integer range' USING ERRCODE='23514';
    END IF;
    IF NEW.resource_usage_basis = 'attempt' AND (
      NEW.telemetry_status <> 'complete' OR NEW.total_tokens IS NULL
      OR NEW.input_tokens IS NULL OR NEW.output_tokens IS NULL
      OR NEW.cached_input_tokens IS NULL OR NEW.cache_write_input_tokens IS NULL
      OR NEW.reasoning_output_tokens IS NULL) THEN
      RAISE EXCEPTION 'measured attempt usage requires complete token telemetry' USING ERRCODE='23514';
    END IF;
  ELSIF NEW.resource_usage_basis IS NOT NULL THEN
    RAISE EXCEPTION 'resource usage basis requires version 3 family' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER agent_workflow_resource_settlement
BEFORE UPDATE ON __SCHEMA__.agent_workflow_attempts
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_resource_settlement();

CREATE FUNCTION __SCHEMA__.guard_agent_workflow_assignment_identity()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF NEW.run_id IS DISTINCT FROM OLD.run_id OR
     NEW.parent_assignment_id IS DISTINCT FROM OLD.parent_assignment_id OR
     NEW.assignment_ordinal IS DISTINCT FROM OLD.assignment_ordinal OR
     NEW.marker IS DISTINCT FROM OLD.marker OR
     NEW.step_key IS DISTINCT FROM OLD.step_key OR
     NEW.assigned_to IS DISTINCT FROM OLD.assigned_to OR
     NEW.execution IS DISTINCT FROM OLD.execution OR
     NEW.identity_sha256 IS DISTINCT FROM OLD.identity_sha256 OR
     NEW.created_at IS DISTINCT FROM OLD.created_at THEN
    RAISE EXCEPTION 'agent workflow assignment identity is immutable';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER agent_workflow_assignment_identity_immutable
BEFORE UPDATE ON __SCHEMA__.agent_workflow_assignments
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_assignment_identity();

CREATE FUNCTION __SCHEMA__.guard_agent_workflow_attempt_identity()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF NEW.run_id IS DISTINCT FROM OLD.run_id OR
     NEW.assignment_id IS DISTINCT FROM OLD.assignment_id OR
     NEW.attempt_ordinal IS DISTINCT FROM OLD.attempt_ordinal OR
     NEW.prompt IS DISTINCT FROM OLD.prompt OR
     NEW.prompt_sha256 IS DISTINCT FROM OLD.prompt_sha256 OR
     NEW.requested_model IS DISTINCT FROM OLD.requested_model OR
     NEW.requested_reasoning_effort IS DISTINCT FROM OLD.requested_reasoning_effort OR
     NEW.requested_service_tier IS DISTINCT FROM OLD.requested_service_tier OR
     NEW.capability_profile IS DISTINCT FROM OLD.capability_profile OR
     NEW.capability_sha256 IS DISTINCT FROM OLD.capability_sha256 OR
     NEW.started_at IS DISTINCT FROM OLD.started_at THEN
    RAISE EXCEPTION 'agent workflow attempt prompt and requested settings are immutable';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER agent_workflow_attempt_identity_immutable
BEFORE UPDATE ON __SCHEMA__.agent_workflow_attempts
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_attempt_identity();

CREATE FUNCTION __SCHEMA__.guard_agent_workflow_staged_identity()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF (
    NEW.run_id IS DISTINCT FROM OLD.run_id OR
    NEW.assignment_id IS DISTINCT FROM OLD.assignment_id OR
    NEW.attempt_id IS DISTINCT FROM OLD.attempt_id OR
    NEW.command_ordinal IS DISTINCT FROM OLD.command_ordinal OR
    NEW.command_type IS DISTINCT FROM OLD.command_type OR
    NEW.schema_version IS DISTINCT FROM OLD.schema_version OR
    NEW.payload IS DISTINCT FROM OLD.payload OR
    NEW.payload_sha256 IS DISTINCT FROM OLD.payload_sha256 OR
    NEW.expected_versions IS DISTINCT FROM OLD.expected_versions OR
    NEW.dependency_ids IS DISTINCT FROM OLD.dependency_ids OR
    (OLD.manifest_sha256 IS NOT NULL AND NEW.manifest_sha256 IS DISTINCT FROM OLD.manifest_sha256)
  ) THEN
    RAISE EXCEPTION 'staged command identity and payload are immutable';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER agent_workflow_staged_identity_immutable
BEFORE UPDATE ON __SCHEMA__.agent_workflow_staged_commands
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_staged_identity();

CREATE TRIGGER agent_workflow_assignment_targets_immutable
BEFORE UPDATE OR DELETE ON __SCHEMA__.agent_workflow_assignment_targets
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_immutable_record();

-- PostgreSQL suppresses ordinary constraint triggers in replica mode. Keep the
-- journal mutable in normal origin/local sessions. Statement guards also cover
-- TRUNCATE and DML that affects no rows; row guards cover each changed row.
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

CREATE TRIGGER agent_workflow_versions_replication_role_guard
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON __SCHEMA__.agent_workflow_versions
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_replication_role();
CREATE TRIGGER agent_model_price_versions_replication_role_guard
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON __SCHEMA__.agent_model_price_versions
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_replication_role();
CREATE TRIGGER agent_workflow_runs_replication_role_guard
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON __SCHEMA__.agent_workflow_runs
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_replication_role();
CREATE TRIGGER agent_workflow_run_families_replication_role_guard
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON __SCHEMA__.agent_workflow_run_families
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_replication_role();
CREATE TRIGGER agent_workflow_run_family_members_replication_role_guard
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON __SCHEMA__.agent_workflow_run_family_members
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_replication_role();
CREATE TRIGGER agent_workflow_assignments_replication_role_guard
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON __SCHEMA__.agent_workflow_assignments
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_replication_role();
CREATE TRIGGER agent_workflow_assignment_targets_replication_role_guard
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON __SCHEMA__.agent_workflow_assignment_targets
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_replication_role();
CREATE TRIGGER agent_workflow_attempts_replication_role_guard
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON __SCHEMA__.agent_workflow_attempts
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_replication_role();
CREATE TRIGGER agent_workflow_events_replication_role_guard
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON __SCHEMA__.agent_workflow_events
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_replication_role();
CREATE TRIGGER agent_workflow_staged_commands_replication_role_guard
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON __SCHEMA__.agent_workflow_staged_commands
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_replication_role();
CREATE TRIGGER agent_workflow_validations_replication_role_guard
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON __SCHEMA__.agent_workflow_validations
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_replication_role();
CREATE TRIGGER agent_workflow_admissions_replication_role_guard
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON __SCHEMA__.agent_workflow_admissions
FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_replication_role();

CREATE TRIGGER agent_workflow_versions_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.agent_workflow_versions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_replication_role_row();
CREATE TRIGGER agent_model_price_versions_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.agent_model_price_versions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_replication_role_row();
CREATE TRIGGER agent_workflow_runs_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.agent_workflow_runs
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_replication_role_row();
CREATE TRIGGER agent_workflow_run_families_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.agent_workflow_run_families
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_replication_role_row();
CREATE TRIGGER agent_workflow_run_family_members_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.agent_workflow_run_family_members
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_replication_role_row();
CREATE TRIGGER agent_workflow_assignments_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.agent_workflow_assignments
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_replication_role_row();
CREATE TRIGGER agent_workflow_assignment_targets_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.agent_workflow_assignment_targets
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_replication_role_row();
CREATE TRIGGER agent_workflow_attempts_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.agent_workflow_attempts
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_replication_role_row();
CREATE TRIGGER agent_workflow_events_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.agent_workflow_events
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_replication_role_row();
CREATE TRIGGER agent_workflow_staged_commands_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.agent_workflow_staged_commands
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_replication_role_row();
CREATE TRIGGER agent_workflow_validations_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.agent_workflow_validations
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_replication_role_row();
CREATE TRIGGER agent_workflow_admissions_replication_role_row_guard
BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA__.agent_workflow_admissions
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_replication_role_row();

ALTER TABLE __SCHEMA__.agent_workflow_versions
  ENABLE ALWAYS TRIGGER agent_workflow_versions_replication_role_guard;
ALTER TABLE __SCHEMA__.agent_model_price_versions
  ENABLE ALWAYS TRIGGER agent_model_price_versions_replication_role_guard;
ALTER TABLE __SCHEMA__.agent_workflow_runs
  ENABLE ALWAYS TRIGGER agent_workflow_runs_replication_role_guard;
ALTER TABLE __SCHEMA__.agent_workflow_run_families
  ENABLE ALWAYS TRIGGER agent_workflow_run_families_replication_role_guard;
ALTER TABLE __SCHEMA__.agent_workflow_run_family_members
  ENABLE ALWAYS TRIGGER agent_workflow_run_family_members_replication_role_guard;
ALTER TABLE __SCHEMA__.agent_workflow_assignments
  ENABLE ALWAYS TRIGGER agent_workflow_assignments_replication_role_guard;
ALTER TABLE __SCHEMA__.agent_workflow_assignment_targets
  ENABLE ALWAYS TRIGGER agent_workflow_assignment_targets_replication_role_guard;
ALTER TABLE __SCHEMA__.agent_workflow_attempts
  ENABLE ALWAYS TRIGGER agent_workflow_attempts_replication_role_guard;
ALTER TABLE __SCHEMA__.agent_workflow_attempts
  ENABLE ALWAYS TRIGGER agent_workflow_total_attempt_budget;
ALTER TABLE __SCHEMA__.agent_workflow_attempts
  ENABLE ALWAYS TRIGGER agent_workflow_attempts_delete_immutable;
ALTER TABLE __SCHEMA__.agent_workflow_attempts
  ENABLE ALWAYS TRIGGER agent_workflow_attempts_truncate_immutable;
ALTER TABLE __SCHEMA__.agent_workflow_events
  ENABLE ALWAYS TRIGGER agent_workflow_events_replication_role_guard;
ALTER TABLE __SCHEMA__.agent_workflow_staged_commands
  ENABLE ALWAYS TRIGGER agent_workflow_staged_commands_replication_role_guard;
ALTER TABLE __SCHEMA__.agent_workflow_validations
  ENABLE ALWAYS TRIGGER agent_workflow_validations_replication_role_guard;
ALTER TABLE __SCHEMA__.agent_workflow_admissions
  ENABLE ALWAYS TRIGGER agent_workflow_admissions_replication_role_guard;

ALTER TABLE __SCHEMA__.agent_workflow_versions
  ENABLE ALWAYS TRIGGER agent_workflow_versions_replication_role_row_guard;
ALTER TABLE __SCHEMA__.agent_model_price_versions
  ENABLE ALWAYS TRIGGER agent_model_price_versions_replication_role_row_guard;
ALTER TABLE __SCHEMA__.agent_workflow_runs
  ENABLE ALWAYS TRIGGER agent_workflow_runs_replication_role_row_guard;
ALTER TABLE __SCHEMA__.agent_workflow_run_families
  ENABLE ALWAYS TRIGGER agent_workflow_run_families_replication_role_row_guard;
ALTER TABLE __SCHEMA__.agent_workflow_run_family_members
  ENABLE ALWAYS TRIGGER agent_workflow_run_family_members_replication_role_row_guard;
ALTER TABLE __SCHEMA__.agent_workflow_assignments
  ENABLE ALWAYS TRIGGER agent_workflow_assignments_replication_role_row_guard;
ALTER TABLE __SCHEMA__.agent_workflow_assignment_targets
  ENABLE ALWAYS TRIGGER agent_workflow_assignment_targets_replication_role_row_guard;
ALTER TABLE __SCHEMA__.agent_workflow_attempts
  ENABLE ALWAYS TRIGGER agent_workflow_attempts_replication_role_row_guard;
ALTER TABLE __SCHEMA__.agent_workflow_events
  ENABLE ALWAYS TRIGGER agent_workflow_events_replication_role_row_guard;
ALTER TABLE __SCHEMA__.agent_workflow_staged_commands
  ENABLE ALWAYS TRIGGER agent_workflow_staged_commands_replication_role_row_guard;
ALTER TABLE __SCHEMA__.agent_workflow_validations
  ENABLE ALWAYS TRIGGER agent_workflow_validations_replication_role_row_guard;
ALTER TABLE __SCHEMA__.agent_workflow_admissions
  ENABLE ALWAYS TRIGGER agent_workflow_admissions_replication_role_row_guard;

CREATE FUNCTION __SCHEMA__.guard_agent_workflow_run_readiness()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
  IF NEW.status='ready-for-admission' AND NOT EXISTS (
    SELECT 1 FROM __SCHEMA__.agent_workflow_validations validation
    WHERE validation.run_id=NEW.id
      AND validation.manifest_sha256=NEW.validated_manifest_sha256
      AND validation.passed
  ) THEN
    RAISE EXCEPTION 'ready workflow run requires a passing validation for its manifest';
  END IF;
  IF NEW.status='admitted' AND NOT EXISTS (
    SELECT 1 FROM __SCHEMA__.agent_workflow_admissions admission
    WHERE admission.run_id=NEW.id
      AND admission.manifest_sha256=NEW.admitted_manifest_sha256
  ) THEN
    RAISE EXCEPTION 'admitted workflow run requires its admission receipt';
  END IF;
  RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER agent_workflow_run_readiness_guard
AFTER INSERT OR UPDATE OF status,validated_manifest_sha256,admitted_manifest_sha256
ON __SCHEMA__.agent_workflow_runs
DEFERRABLE INITIALLY IMMEDIATE
FOR EACH ROW EXECUTE FUNCTION __SCHEMA__.guard_agent_workflow_run_readiness();
