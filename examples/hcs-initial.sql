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
  'scan.snapshot.max.rows' = '100000',
  'scan.snapshot.max.bytes' = '67108864',
  'scan.snapshot.fetch.size' = '1000',
  'scan.snapshot.timeout.seconds' = '300',
  'output.format' = 'json',
  'sslmode' = 'require'
);

SELECT * FROM hcs_initial_changes;
