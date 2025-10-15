{{
  config(
    materialized='table'
  )
}}

-- Bronze Layer: Participants Table
-- Transforms raw participant data with basic cleansing and standardization
-- Source: RAW.participants -> BRONZE.bz_participants

WITH source_data AS (
    SELECT 
        participant_id,
        meeting_id,
        user_id,
        join_time,
        leave_time,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('raw_zoom', 'participants') }}
),

-- Data Quality and Cleansing
cleansed_data AS (
    SELECT 
        -- Primary Key
        TRIM(UPPER(participant_id)) as participant_id,
        
        -- Foreign Keys with cleansing
        CASE 
            WHEN TRIM(meeting_id) = '' OR meeting_id IS NULL THEN 'UNKNOWN_MEETING'
            ELSE TRIM(UPPER(meeting_id))
        END as meeting_id,
        
        CASE 
            WHEN TRIM(user_id) = '' OR user_id IS NULL THEN 'UNKNOWN_USER'
            ELSE TRIM(UPPER(user_id))
        END as user_id,
        
        -- Time information
        join_time,
        leave_time,
        
        -- Metadata columns
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) as load_timestamp,
        CURRENT_TIMESTAMP() as update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') as source_system
        
    FROM source_data
    WHERE participant_id IS NOT NULL  -- Filter out records without primary key
)

SELECT 
    participant_id,
    meeting_id,
    user_id,
    join_time,
    leave_time,
    load_timestamp,
    update_timestamp,
    source_system
FROM cleansed_data
