{{
  config(
    materialized='table'
  )
}}

-- Bronze Layer: Meetings Table
-- Transforms raw meeting data with basic cleansing and standardization
-- Source: RAW.meetings -> BRONZE.bz_meetings

WITH source_data AS (
    SELECT 
        meeting_id,
        host_id,
        meeting_topic,
        start_time,
        end_time,
        duration_minutes,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('raw_zoom', 'meetings') }}
),

-- Data Quality and Cleansing
cleansed_data AS (
    SELECT 
        -- Primary Key
        TRIM(UPPER(meeting_id)) as meeting_id,
        
        -- Meeting Information with basic cleansing
        CASE 
            WHEN TRIM(host_id) = '' OR host_id IS NULL THEN 'UNKNOWN_HOST'
            ELSE TRIM(UPPER(host_id))
        END as host_id,
        
        CASE 
            WHEN TRIM(meeting_topic) = '' OR meeting_topic IS NULL THEN 'UNTITLED_MEETING'
            ELSE TRIM(meeting_topic)
        END as meeting_topic,
        
        start_time,
        end_time,
        
        -- Duration validation and cleansing
        CASE 
            WHEN duration_minutes IS NULL OR duration_minutes < 0 THEN 0
            ELSE duration_minutes
        END as duration_minutes,
        
        -- Metadata columns
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) as load_timestamp,
        CURRENT_TIMESTAMP() as update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') as source_system
        
    FROM source_data
    WHERE meeting_id IS NOT NULL  -- Filter out records without primary key
)

SELECT 
    meeting_id,
    host_id,
    meeting_topic,
    start_time,
    end_time,
    duration_minutes,
    load_timestamp,
    update_timestamp,
    source_system
FROM cleansed_data
