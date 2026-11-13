-- Fresh initial jobs are BLOCKED by default: global scan visibility is unverified.
-- For isolated experiments only, explicitly add this option to WITH:
-- 'scan.snapshot.allow-unverified-consistency' = 'true',
-- This opt-in does not verify consistency or implement a read barrier.
-- Run the signal-table DDL from docs/HCS_CDC23_INITIAL_SNAPSHOT.md in the target PDB first.
-- Source parallelism must be 1; configure reliable checkpoint storage for recovery tests.
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '10s';

CREATE TABLE hcs_initial_changes (event STRING) WITH (
  'connector' = 'gaussdb-cdc',
  'hcs.cms-endpoints' = 'cms-a.example.com:8000,cms-b.example.com:8001',
  'database' = 'your_pdb',
  'username' = 'your_database_user',
  'password' = 'replace_locally',
  'hcs.tenant-name' = 'your_tenant',
  'hcs.tenant-password' = 'replace_locally',
  'slot.name' = 'your_dedicated_initial_slot',
  'schema-name' = 'public',
  'table-name' = 'cdc_probe',
  'scan.startup.mode' = 'initial',
  'scan.snapshot.signal-table' = 'cdc_meta.flink_snapshot_signal',
  'scan.slot.create' = 'true',
  'scan.snapshot.storage' = 'disk',
  'scan.snapshot.disk.cache.mb' = '16',
  'scan.snapshot.disk.write-buffer.bytes' = '1048576',
  'scan.snapshot.disk.max.row.bytes' = '16777216',
  'scan.snapshot.disk.max.bytes' = '10737418240',
  'scan.snapshot.fetch.size' = '1000',
  'scan.snapshot.batch.rows' = '1000',
  'scan.snapshot.batch.bytes' = '1048576',
  'scan.snapshot.timeout.seconds' = '300',
  'output.format' = 'json',
  'sslmode' = 'require'
);

SELECT * FROM hcs_initial_changes;
