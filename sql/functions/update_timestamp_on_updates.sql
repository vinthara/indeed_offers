DROP FUNCTION IF EXISTS update_timestamp_on_updates();

CREATE OR REPLACE FUNCTION update_timestamp_on_updates()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
   NEW.updated_at = CURRENT_TIMESTAMP(0); 
   RETURN NEW;
END;
$BODY$;

COMMENT ON FUNCTION update_timestamp_on_updates()
    IS 'Update jobs.updated_at on rows updated';