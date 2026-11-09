-- ==============================================================================
-- MODEL: planes
-- TẦNG NGUỒN: staging.planes -> TẦNG ĐÍCH: raw_data.planes
-- CHIẾN LƯỢC: Incremental (Merge/Upsert) theo khóa 'tailnum'
-- LÀM SẠCH DỮ LIỆU: Loại bỏ các giá trị 'NA' hoặc không hợp lệ trước khi ép kiểu số
-- ==============================================================================

{{ config(
    materialized='incremental',
    unique_key='tailnum',
    incremental_strategy='merge'
) }}

SELECT
    tailnum,
    -- Chuẩn hóa trường năm: Nếu chỉ chứa chữ số thì ép kiểu INTEGER, ngược lại NULL
    CASE 
        WHEN year ~ '^[0-9]+$' THEN year::int 
        ELSE NULL 
    END AS year,
    type,
    manufacturer,
    model,
    engines,
    seats,
    -- Chuẩn hóa trường tốc độ: Kiểm tra chuỗi số hợp lệ trước khi ép kiểu FLOAT
    CASE 
        WHEN speed ~ '^-?[0-9]+(\.[0-9]+)?$' THEN speed::float 
        ELSE NULL 
    END AS speed,
    engine
FROM {{ source('staging', 'planes') }}
