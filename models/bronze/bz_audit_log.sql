{{
  config(
    materialized='table'
  )
}}

-- Audit Log Table for Bronze Layer Processing
-- This table tracks the processing status of all bronze layer models

WITH audit_base AS (
    SELECT 
        1 as record_id,
        'SYSTEM_INIT' as source_table,
        CURRENT_TIMESTAMP() as load_timestamp,
        'DBT_PROCESS' as processed_by,
        0 as processing_time,
        'INITIALIZED' as status
    WHERE FALSE  -- This ensures no actual records are inserted during initial creation
)

SELECT 
    record_id::NUMBER as record_id,
    source_table::VARCHAR(255) as source_table,
    load_timestamp::TIMESTAMP_NTZ as load_timestamp,
    processed_by::VARCHAR(100) as processed_by,
    processing_time::NUMBER as processing_time,
    status::VARCHAR(50) as status
FROM audit_base
