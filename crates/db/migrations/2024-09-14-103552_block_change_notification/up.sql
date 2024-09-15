CREATE OR REPLACE FUNCTION notify_block_changes()
RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('block_changes', NEW.id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION notify_block2_changes()
RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('block2_changes', NEW.id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER block_changes_trigger
AFTER INSERT OR UPDATE
ON blocks
FOR EACH ROW
EXECUTE FUNCTION notify_block_changes();

CREATE TRIGGER block2_changes_trigger
AFTER INSERT OR UPDATE
ON blocks_2
FOR EACH ROW
EXECUTE FUNCTION notify_block2_changes();