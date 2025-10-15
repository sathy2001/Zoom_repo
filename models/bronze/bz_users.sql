{{
  config(
    materialized='table'
  )
}}

-- Bronze Layer: Users Table
-- Transforms raw user data with basic cleansing and standardization
-- Source: RAW.users -> BRONZE.bz_users

WITH source_data AS (
    SELECT 
        user_id,
        user_name,
        email,
        company,
        plan_type,
        load_timestamp,
        update_timestamp,
        source_system
    FROM {{ source('raw_zoom', 'users') }}
),

-- Data Quality and Cleansing
cleansed_data AS (
    SELECT 
        -- Primary Key
        TRIM(UPPER(user_id)) as user_id,
        
        -- User Information with basic cleansing
        CASE 
            WHEN TRIM(user_name) = '' OR user_name IS NULL THEN 'UNKNOWN_USER'
            ELSE TRIM(user_name)
        END as user_name,
        
        CASE 
            WHEN TRIM(email) = '' OR email IS NULL THEN 'UNKNOWN_EMAIL'
            ELSE LOWER(TRIM(email))
        END as email,
        
        CASE 
            WHEN TRIM(company) = '' OR company IS NULL THEN 'UNKNOWN_COMPANY'
            ELSE TRIM(company)
        END as company,
        
        CASE 
            WHEN TRIM(plan_type) = '' OR plan_type IS NULL THEN 'UNKNOWN_PLAN'
            ELSE UPPER(TRIM(plan_type))
        END as plan_type,
        
        -- Metadata columns
        COALESCE(load_timestamp, CURRENT_TIMESTAMP()) as load_timestamp,
        CURRENT_TIMESTAMP() as update_timestamp,
        COALESCE(source_system, 'ZOOM_PLATFORM') as source_system
        
    FROM source_data
    WHERE user_id IS NOT NULL  -- Filter out records without primary key
)

SELECT 
    user_id,
    user_name,
    email,
    company,
    plan_type,
    load_timestamp,
    update_timestamp,
    source_system
FROM cleansed_data
