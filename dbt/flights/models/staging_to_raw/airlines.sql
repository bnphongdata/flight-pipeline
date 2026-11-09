-- ==============================================================================
-- MODEL: airlines
-- TẦNG NGUỒN: staging.airlines -> TẦNG ĐÍCH: raw_data.airlines
-- CHIẾN LƯỢC: Incremental (Merge/Upsert) dựa trên khóa chính 'carrier'
-- ==============================================================================

{{ config(
    materialized='incremental',
    unique_key='carrier',
    incremental_strategy='merge'
) }}

SELECT
    carrier,  -- Mã định danh hãng hàng không (Khóa chính)
    name      -- Tên đầy đủ hãng hàng không
FROM {{ source('staging', 'airlines') }}
