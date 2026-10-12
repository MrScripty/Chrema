-- Optional owned upgrade only; install v1 first. No grants or production migration.
-- V1 loaders remain strict four-row; v2 uses the same immutable relation.
-- Refuse repeated or drifted upgrades instead of replacing another policy cap.
DO $$
DECLARE relation_oid oid := to_regclass('__SCHEMA__.saved_root_initializations');
BEGIN
 IF relation_oid IS NULL OR NOT EXISTS (
  SELECT 1 FROM pg_constraint WHERE conrelid=relation_oid
   AND conname='saved_root_initializations_sequence_check' AND contype='c' AND convalidated
   AND pg_get_constraintdef(oid)='CHECK (((sequence >= 1) AND (sequence <= 4)))'
 ) OR NOT EXISTS (
  SELECT 1 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
   JOIN pg_class c ON c.oid=t.tgrelid
  WHERE t.tgrelid=relation_oid AND t.tgname='immutable_saved_root_initialization'
   AND NOT t.tgisinternal AND t.tgenabled='O' AND t.tgtype=27
   AND p.proname='immutable_saved_root_initialization' AND p.pronamespace=c.relnamespace
   AND btrim(p.prosrc)=E'BEGIN\n RAISE EXCEPTION \'Saved root initialization history is immutable\';\nEND'
 ) THEN RAISE EXCEPTION 'Expected immutable saved initialization v1 relation'; END IF;
END $$;
ALTER TABLE __SCHEMA__.saved_root_initializations
 DROP CONSTRAINT saved_root_initializations_sequence_check;
ALTER TABLE __SCHEMA__.saved_root_initializations
 ADD CONSTRAINT saved_root_initializations_sequence_check CHECK(sequence BETWEEN 1 AND 8);
