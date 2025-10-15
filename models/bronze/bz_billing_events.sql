{{
  config(
    materialized='table'
  )
}}

-- Bronze Layer: Billing Events Table
-- Transforms raw billing event data with basic cleansing and standardization
-- Source: RAW.billing_events -> BRONZE.bz_billing_events

WITH source_data AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        amount,
        event_date,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('raw_zoom', 'billing_events') }}
),

-- Data Quality and Cleansing
cleansed_data AS (
    SELECT 
        -- Primary Key
        TRIM(UPPER(event_id)) as event_id,
        
        -- Foreign Key with cleansing
        CASE 
            WHEN TRIM(user_id) = '' OR user_id IS NULL THEN 'UNKNOWN_USER'
            ELSE TRIM(UPPER(user_id))
        END as user_id,
        
        -- Event information with cleansing
        CASE 
            WHEN TRIM(event_type) = '' OR event_type IS NULL THEN 'UNKNOWN_EVENT'
            ELSE UPPER(TRIM(event_type))
        END as event_type,
        
        -- Amount validation
        CASE 
            WHEN amount IS NULL THEN 0.00
            ELSE amount
        END as amount,
        
        event_date,
        
        -- Metadata columns
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) as load_timestamp,
        CURRENT_TIMESTAMP() as update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') as source_system
        
    FROM source_data
    WHERE event_id IS NOT NULL  -- Filter out records without primary key
)

SELECT 
    event_id,
    user_id,
    event_type,
    amount,
    event_date,
    load_timestamp,
    update_timestamp,
    source_system
FROM cleansed_data
