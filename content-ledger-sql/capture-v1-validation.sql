-- JavaScript strings count supplementary characters as two UTF-16 units.
CREATE FUNCTION __SCHEMA__.capture_utf16_length(value text)
RETURNS integer LANGUAGE plpgsql IMMUTABLE STRICT SET search_path=pg_catalog AS $$
DECLARE i integer; units integer := 0;
BEGIN
  FOR i IN 1..char_length(value) LOOP
    units := units + CASE WHEN ascii(substr(value,i,1))>65535 THEN 2 ELSE 1 END;
  END LOOP;
  RETURN units;
END;
$$;

-- JSONB numeric values must survive a finite JavaScript-number round trip.
CREATE FUNCTION __SCHEMA__.capture_js_json_safe(value jsonb)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE STRICT SET search_path=pg_catalog AS $$
DECLARE item jsonb; number_value double precision;
BEGIN
  IF jsonb_typeof(value)='number' THEN
    number_value := (value #>> '{}')::double precision;
    RETURN number_value::text NOT IN ('Infinity','-Infinity','NaN')
      AND to_jsonb(number_value)=value;
  ELSIF jsonb_typeof(value)='array' THEN
    FOR item IN SELECT element FROM jsonb_array_elements(value) elements(element) LOOP
      IF NOT __SCHEMA__.capture_js_json_safe(item) THEN RETURN false; END IF;
    END LOOP;
  ELSIF jsonb_typeof(value)='object' THEN
    FOR item IN SELECT element FROM jsonb_each(value) elements(key,element) LOOP
      IF NOT __SCHEMA__.capture_js_json_safe(item) THEN RETURN false; END IF;
    END LOOP;
  END IF;
  RETURN true;
EXCEPTION WHEN data_exception THEN RETURN false;
END;
$$;

-- Return a bounded node count, or -1 for any unsupported schema node.
CREATE FUNCTION __SCHEMA__.capture_schema_node_count(node jsonb, depth integer)
RETURNS integer LANGUAGE plpgsql IMMUTABLE STRICT SET search_path=pg_catalog AS $$
DECLARE
  kind text;
  keyword text;
  item jsonb;
  property record;
  child_count integer;
  node_count integer := 1;
  lower_bound numeric;
  upper_bound numeric;
BEGIN
  IF depth>12 OR jsonb_typeof(node)<>'object' THEN RETURN -1; END IF;
  kind := node->>'type';
  IF kind IS NULL OR kind NOT IN ('null','boolean','number','integer','string','array','object') THEN RETURN -1; END IF;
  FOR keyword IN SELECT jsonb_object_keys(node) LOOP
    IF keyword NOT IN ('type','enum','const','title','description')
       AND NOT (kind IN ('number','integer') AND keyword IN ('minimum','maximum','exclusiveMinimum','exclusiveMaximum'))
       AND NOT (kind='string' AND keyword IN ('minLength','maxLength','format'))
       AND NOT (kind='array' AND keyword IN ('items','minItems','maxItems','uniqueItems'))
       AND NOT (kind='object' AND keyword IN ('properties','required','additionalProperties','minProperties','maxProperties'))
    THEN RETURN -1; END IF;
  END LOOP;
  FOR keyword IN SELECT unnest(ARRAY['title','description']) LOOP
    IF node ? keyword THEN
      IF jsonb_typeof(node->keyword)<>'string'
         OR btrim(node->>keyword,
           U&'\0009\000A\000B\000C\000D\0020\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000\FEFF')=''
         OR __SCHEMA__.capture_utf16_length(node->>keyword) > (CASE WHEN keyword='title' THEN 200 ELSE 2000 END)
      THEN RETURN -1; END IF;
    END IF;
  END LOOP;
  IF node ? 'enum' THEN
    IF jsonb_typeof(node->'enum')<>'array' THEN RETURN -1; END IF;
    IF jsonb_array_length(node->'enum') NOT BETWEEN 1 AND 100
       OR (SELECT count(*)<>count(DISTINCT value) FROM jsonb_array_elements(node->'enum') values(value))
    THEN RETURN -1; END IF;
  END IF;
  IF node ? 'const' AND node ? 'enum' THEN RETURN -1; END IF;
  IF kind IN ('number','integer') THEN
    FOR keyword IN SELECT unnest(ARRAY['minimum','maximum','exclusiveMinimum','exclusiveMaximum']) LOOP
      IF node ? keyword AND jsonb_typeof(node->keyword)<>'number' THEN RETURN -1; END IF;
    END LOOP;
    lower_bound := coalesce(node->>'exclusiveMinimum',node->>'minimum')::numeric;
    upper_bound := coalesce(node->>'exclusiveMaximum',node->>'maximum')::numeric;
    IF lower_bound>upper_bound THEN RETURN -1; END IF;
  END IF;
  IF kind IN ('string','array','object') THEN
    FOR keyword IN SELECT unnest(CASE kind
      WHEN 'string' THEN ARRAY['minLength','maxLength']
      WHEN 'array' THEN ARRAY['minItems','maxItems']
      ELSE ARRAY['minProperties','maxProperties'] END) LOOP
      IF node ? keyword THEN
        IF jsonb_typeof(node->keyword)<>'number' THEN RETURN -1; END IF;
        IF (node->>keyword)::numeric <> trunc((node->>keyword)::numeric)
           OR (node->>keyword)::numeric < 0
           OR (node->>keyword)::numeric > (CASE WHEN kind='string' THEN 1000000 ELSE 100000 END)
        THEN RETURN -1; END IF;
      END IF;
    END LOOP;
    IF kind='string' THEN
      IF node ? 'format' THEN RETURN -1; END IF;
      IF node ? 'minLength' AND node ? 'maxLength'
         AND (node->>'minLength')::numeric>(node->>'maxLength')::numeric THEN RETURN -1; END IF;
    ELSIF kind='array' THEN
      IF NOT node ? 'items' THEN RETURN -1; END IF;
      IF node ? 'uniqueItems' AND jsonb_typeof(node->'uniqueItems')<>'boolean' THEN RETURN -1; END IF;
      IF node ? 'minItems' AND node ? 'maxItems'
         AND (node->>'minItems')::numeric>(node->>'maxItems')::numeric THEN RETURN -1; END IF;
      child_count := __SCHEMA__.capture_schema_node_count(node->'items',depth+1);
      IF child_count IS NULL OR child_count<1 THEN RETURN -1; END IF;
      node_count := node_count+child_count;
    ELSE
      IF node ? 'properties' AND jsonb_typeof(node->'properties')<>'object' THEN RETURN -1; END IF;
      FOR property IN SELECT key,value FROM jsonb_each(coalesce(node->'properties','{}'::jsonb)) LOOP
        IF property.key='' OR __SCHEMA__.capture_utf16_length(property.key)>200 THEN RETURN -1; END IF;
        child_count := __SCHEMA__.capture_schema_node_count(property.value,depth+1);
        IF child_count IS NULL OR child_count<1 THEN RETURN -1; END IF;
        node_count := node_count+child_count;
        IF node_count>500 THEN RETURN -1; END IF;
      END LOOP;
      IF node ? 'required' THEN
        IF jsonb_typeof(node->'required')<>'array' THEN RETURN -1; END IF;
        IF (SELECT count(*)<>count(DISTINCT value) FROM jsonb_array_elements(node->'required') values(value)) THEN RETURN -1; END IF;
        FOR item IN SELECT value FROM jsonb_array_elements(node->'required') values(value) LOOP
          IF jsonb_typeof(item)<>'string' OR item #>> '{}'=''
             OR NOT (coalesce(node->'properties','{}'::jsonb) ? (item #>> '{}'))
          THEN RETURN -1; END IF;
        END LOOP;
      END IF;
      IF node ? 'additionalProperties' THEN
        IF jsonb_typeof(node->'additionalProperties')='object' THEN
          child_count := __SCHEMA__.capture_schema_node_count(node->'additionalProperties',depth+1);
          IF child_count IS NULL OR child_count<1 THEN RETURN -1; END IF;
          node_count := node_count+child_count;
        ELSIF jsonb_typeof(node->'additionalProperties')<>'boolean' THEN RETURN -1;
        END IF;
      END IF;
      IF node ? 'minProperties' AND node ? 'maxProperties'
         AND (node->>'minProperties')::numeric>(node->>'maxProperties')::numeric THEN RETURN -1; END IF;
    END IF;
  END IF;
  IF node_count>500 THEN RETURN -1; END IF;
  RETURN node_count;
EXCEPTION WHEN data_exception THEN RETURN -1;
END;
$$;

CREATE FUNCTION __SCHEMA__.capture_value_matches(candidate jsonb, definition jsonb)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE STRICT SET search_path=pg_catalog AS $$
DECLARE
  kind text := definition->>'type';
  text_value text;
  numeric_value numeric;
  item jsonb;
  property record;
  keyword text;
BEGIN
  IF kind='integer' THEN
    IF jsonb_typeof(candidate)<>'number' THEN RETURN false; END IF;
    numeric_value := (candidate #>> '{}')::numeric;
    IF numeric_value<>trunc(numeric_value) THEN RETURN false; END IF;
  ELSIF kind='number' THEN
    IF jsonb_typeof(candidate)<>'number' THEN RETURN false; END IF;
    numeric_value := (candidate #>> '{}')::numeric;
  ELSIF jsonb_typeof(candidate) IS DISTINCT FROM kind THEN RETURN false;
  END IF;
  IF definition ? 'enum' AND NOT EXISTS (
    SELECT 1 FROM jsonb_array_elements(definition->'enum') allowed(value) WHERE allowed.value=candidate
  ) THEN RETURN false; END IF;
  IF definition ? 'const' AND definition->'const' IS DISTINCT FROM candidate THEN RETURN false; END IF;
  IF kind IN ('number','integer') THEN
    IF definition ? 'minimum' AND numeric_value<(definition->>'minimum')::numeric THEN RETURN false; END IF;
    IF definition ? 'maximum' AND numeric_value>(definition->>'maximum')::numeric THEN RETURN false; END IF;
    IF definition ? 'exclusiveMinimum' AND numeric_value<=(definition->>'exclusiveMinimum')::numeric THEN RETURN false; END IF;
    IF definition ? 'exclusiveMaximum' AND numeric_value>=(definition->>'exclusiveMaximum')::numeric THEN RETURN false; END IF;
  ELSIF kind='string' THEN
    text_value := candidate #>> '{}';
    IF definition ? 'minLength' AND char_length(text_value)<(definition->>'minLength')::integer THEN RETURN false; END IF;
    IF definition ? 'maxLength' AND char_length(text_value)>(definition->>'maxLength')::integer THEN RETURN false; END IF;
  ELSIF kind='array' THEN
    IF definition ? 'minItems' AND jsonb_array_length(candidate)<(definition->>'minItems')::integer THEN RETURN false; END IF;
    IF definition ? 'maxItems' AND jsonb_array_length(candidate)>(definition->>'maxItems')::integer THEN RETURN false; END IF;
    IF coalesce((definition->>'uniqueItems')::boolean,false) AND (
      SELECT count(*)<>count(DISTINCT value) FROM jsonb_array_elements(candidate) values(value)
    ) THEN RETURN false; END IF;
    FOR item IN SELECT value FROM jsonb_array_elements(candidate) values(value) LOOP
      IF NOT __SCHEMA__.capture_value_matches(item,definition->'items') THEN RETURN false; END IF;
    END LOOP;
  ELSIF kind='object' THEN
    IF definition ? 'minProperties'
      AND (SELECT count(*) FROM jsonb_object_keys(candidate))<(definition->>'minProperties')::integer THEN RETURN false; END IF;
    IF definition ? 'maxProperties'
      AND (SELECT count(*) FROM jsonb_object_keys(candidate))>(definition->>'maxProperties')::integer THEN RETURN false; END IF;
    IF EXISTS (
      SELECT 1 FROM jsonb_array_elements_text(coalesce(definition->'required','[]'::jsonb)) required(name)
      WHERE NOT candidate ? required.name
    ) THEN RETURN false; END IF;
    FOR property IN SELECT key,value FROM jsonb_each(candidate) LOOP
      IF coalesce(definition->'properties','{}'::jsonb) ? property.key THEN
        IF NOT __SCHEMA__.capture_value_matches(property.value,definition->'properties'->property.key) THEN RETURN false; END IF;
      ELSIF definition->'additionalProperties'='false'::jsonb THEN RETURN false;
      ELSIF jsonb_typeof(definition->'additionalProperties')='object' THEN
        IF NOT __SCHEMA__.capture_value_matches(property.value,definition->'additionalProperties') THEN RETURN false; END IF;
      END IF;
    END LOOP;
  END IF;
  RETURN true;
EXCEPTION WHEN data_exception THEN RETURN false;
END;
$$;
