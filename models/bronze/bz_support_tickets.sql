{{
  config(
    materialized='table'
  )
}}

-- Bronze Layer: Support Tickets Table
-- Transforms raw support ticket data with basic cleansing and standardization
-- Source: RAW.support_tickets -> BRONZE.bz_support_tickets

WITH source_data AS (
    SELECT 
        ticket_id,
        user_id,
        ticket_type,
        resolution_status,
        open_date,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('raw_zoom', 'support_tickets') }}
),

-- Data Quality and Cleansing
cleansed_data AS (
    SELECT 
        -- Primary Key
        TRIM(UPPER(ticket_id)) as ticket_id,
        
        -- Foreign Key with cleansing
        CASE 
            WHEN TRIM(user_id) = '' OR user_id IS NULL THEN 'UNKNOWN_USER'
            ELSE TRIM(UPPER(user_id))
        END as user_id,
        
        -- Ticket information with cleansing
        CASE 
            WHEN TRIM(ticket_type) = '' OR ticket_type IS NULL THEN 'UNKNOWN_TYPE'
            ELSE UPPER(TRIM(ticket_type))
        END as ticket_type,
        
        CASE 
            WHEN TRIM(resolution_status) = '' OR resolution_status IS NULL THEN 'UNKNOWN_STATUS'
            ELSE UPPER(TRIM(resolution_status))
        END as resolution_status,
        
        open_date,
        
        -- Metadata columns
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) as load_timestamp,
        CURRENT_TIMESTAMP() as update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') as source_system
        
    FROM source_data
    WHERE ticket_id IS NOT NULL  -- Filter out records without primary key
)

SELECT 
    ticket_id,
    user_id,
    ticket_type,
    resolution_status,
    open_date,
    load_timestamp,
    update_timestamp,
    source_system
FROM cleansed_data
