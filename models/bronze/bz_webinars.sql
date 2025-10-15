{{
  config(
    materialized='table'
  )
}}

-- Bronze Layer: Webinars Table
-- Transforms raw webinar data with basic cleansing and standardization
-- Source: RAW.webinars -> BRONZE.bz_webinars

WITH source_data AS (
    SELECT 
        webinar_id,
        host_id,
        webinar_topic,
        start_time,
        end_time,
        registrants,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('raw_zoom', 'webinars') }}
),

-- Data Quality and Cleansing
cleansed_data AS (
    SELECT 
        -- Primary Key
        TRIM(UPPER(webinar_id)) as webinar_id,
        
        -- Host information with cleansing
        CASE 
            WHEN TRIM(host_id) = '' OR host_id IS NULL THEN 'UNKNOWN_HOST'
            ELSE TRIM(UPPER(host_id))
        END as host_id,
        
        CASE 
            WHEN TRIM(webinar_topic) = '' OR webinar_topic IS NULL THEN 'UNTITLED_WEBINAR'
            ELSE TRIM(webinar_topic)
        END as webinar_topic,
        
        start_time,
        end_time,
        
        -- Registrants validation
        CASE 
            WHEN registrants IS NULL OR registrants < 0 THEN 0
            ELSE registrants
        END as registrants,
        
        -- Metadata columns
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) as load_timestamp,
        CURRENT_TIMESTAMP() as update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') as source_system
        
    FROM source_data
    WHERE webinar_id IS NOT NULL  -- Filter out records without primary key
)

SELECT 
    webinar_id,
    host_id,
    webinar_topic,
    start_time,
    end_time,
    registrants,
    load_timestamp,
    update_timestamp,
    source_system
FROM cleansed_data
