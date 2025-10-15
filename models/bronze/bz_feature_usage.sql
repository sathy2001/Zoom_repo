{{
  config(
    materialized='table'
  )
}}

-- Bronze Layer: Feature Usage Table
-- Transforms raw feature usage data with basic cleansing and standardization
-- Source: RAW.feature_usage -> BRONZE.bz_feature_usage

WITH source_data AS (
    SELECT 
        usage_id,
        meeting_id,
        feature_name,
        usage_count,
        usage_date,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('raw_zoom', 'feature_usage') }}
),

-- Data Quality and Cleansing
cleansed_data AS (
    SELECT 
        -- Primary Key
        TRIM(UPPER(usage_id)) as usage_id,
        
        -- Foreign Key with cleansing
        CASE 
            WHEN TRIM(meeting_id) = '' OR meeting_id IS NULL THEN 'UNKNOWN_MEETING'
            ELSE TRIM(UPPER(meeting_id))
        END as meeting_id,
        
        -- Feature information with cleansing
        CASE 
            WHEN TRIM(feature_name) = '' OR feature_name IS NULL THEN 'UNKNOWN_FEATURE'
            ELSE UPPER(TRIM(feature_name))
        END as feature_name,
        
        -- Usage count validation
        CASE 
            WHEN usage_count IS NULL OR usage_count < 0 THEN 0
            ELSE usage_count
        END as usage_count,
        
        usage_date,
        
        -- Metadata columns
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) as load_timestamp,
        CURRENT_TIMESTAMP() as update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') as source_system
        
    FROM source_data
    WHERE usage_id IS NOT NULL  -- Filter out records without primary key
)

SELECT 
    usage_id,
    meeting_id,
    feature_name,
    usage_count,
    usage_date,
    load_timestamp,
    update_timestamp,
    source_system
FROM cleansed_data
