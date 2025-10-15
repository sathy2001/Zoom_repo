{{
  config(
    materialized='table'
  )
}}

-- Bronze Layer: Licenses Table
-- Transforms raw license data with basic cleansing and standardization
-- Source: RAW.licenses -> BRONZE.bz_licenses

WITH source_data AS (
    SELECT 
        license_id,
        license_type,
        assigned_to_user_id,
        start_date,
        end_date,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('raw_zoom', 'licenses') }}
),

-- Data Quality and Cleansing
cleansed_data AS (
    SELECT 
        -- Primary Key
        TRIM(UPPER(license_id)) as license_id,
        
        -- License information with cleansing
        CASE 
            WHEN TRIM(license_type) = '' OR license_type IS NULL THEN 'UNKNOWN_LICENSE'
            ELSE UPPER(TRIM(license_type))
        END as license_type,
        
        CASE 
            WHEN TRIM(assigned_to_user_id) = '' OR assigned_to_user_id IS NULL THEN 'UNASSIGNED'
            ELSE TRIM(UPPER(assigned_to_user_id))
        END as assigned_to_user_id,
        
        start_date,
        end_date,
        
        -- Metadata columns
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) as load_timestamp,
        CURRENT_TIMESTAMP() as update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') as source_system
        
    FROM source_data
    WHERE license_id IS NOT NULL  -- Filter out records without primary key
)

SELECT 
    license_id,
    license_type,
    assigned_to_user_id,
    start_date,
    end_date,
    load_timestamp,
    update_timestamp,
    source_system
FROM cleansed_data
