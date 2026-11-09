-- ==============================================================================
-- MODEL: airports
-- TẦNG NGUỒN: staging.airports -> TẦNG ĐÍCH: raw_data.airports
-- CHIẾN LƯỢC: Incremental (Merge/Upsert) dựa trên khóa chính 'faa'
-- ==============================================================================

{{ config(
    materialized='incremental',
    unique_key='faa',
    incremental_strategy='merge'
) }}

SELECT
    faa,            -- Mã sân bay theo FAA (Khóa chính)
    name,           -- Tên sân bay
    latitude,       -- Vĩ độ
    longitude,      -- Kinh độ
    altitude,       -- Độ cao
    timezone,       -- Múi giờ
    dst,            -- Tiêu chuẩn giờ mùa hè
    timezone_name   -- Tên múi giờ
FROM {{ source('staging', 'airports') }}
