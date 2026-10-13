-- Replace placeholders; use a dedicated slot pre-created in the target tenant/database.
-- This connector currently captures incremental changes only.
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '10 s';
CREATE TABLE hcs_changes (event STRING) WITH (
    'connector' = 'gaussdb-cdc',
    'hcs.cms-endpoints' = 'cms-a.example.com:8000,cms-b.example.com:8000',
    'database' = 'your_pdb',
    'username' = 'your_database_user',
    'password' = 'replace_locally',
    'hcs.tenant-name' = 'your_tenant',
    'hcs.tenant-password' = 'replace_locally',
    'slot.name' = 'your_precreated_dedicated_slot',
    'schema-name' = 'public',
    'table-name' = 'cdc_probe',
    'scan.startup.mode' = 'incremental',
    'output.format' = 'json',
    'sslmode' = 'require'
);
SELECT * FROM hcs_changes;
