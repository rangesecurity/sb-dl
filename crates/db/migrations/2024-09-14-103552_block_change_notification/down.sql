DROP TRIGGER IF EXISTS block_changes_trigger ON blocks;
DROP TRIGGER IF EXISTS block2_changes_trigger ON blocks_2;
DROP FUNCTION IF EXISTS notify_block_changes();
DROP FUNCTION IF EXISTS notify_block2_changes();