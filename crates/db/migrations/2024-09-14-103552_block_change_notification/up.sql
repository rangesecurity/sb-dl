CREATE OR REPLACE FUNCTION notify_block_changes()
RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('block_changes', NEW.id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER block_changes_trigger
AFTER INSERT OR UPDATE
ON blocks
FOR EACH ROW
EXECUTE FUNCTION notify_block_changes();