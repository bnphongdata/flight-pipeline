-- ==============================================================================
-- MODEL: weather
-- TẦNG NGUỒN: staging.weather -> TẦNG ĐÍCH: raw_data.weather
-- CHIẾN LƯỢC: Incremental (Merge) theo tổ hợp khóa: ['origin', 'year', 'month', 'day', 'hour']
-- LÀM SẠCH DỮ LIỆU:
--   1. Sử dụng Window Function row_number() để khử trùng lặp (deduplication), lấy bản ghi mới nhất.
--   2. Chuẩn hóa các giá trị chuỗi (ví dụ: 'NA') về NULL và ép kiểu số (numeric/float) an toàn.
-- ==============================================================================

{{ 
  config(
    materialized='incremental',
    unique_key=['origin', 'year', 'month', 'day', 'hour'],
    incremental_strategy='merge'
  ) 
}}

WITH latest_record AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY origin, year, month, day, hour 
            ORDER BY time_hour DESC NULLS LAST
        ) AS rn
    FROM {{ source('staging', 'weather') }}
)
SELECT
    origin,
    year,
    month,
    day,
    hour,
    -- Chuẩn hóa và ép kiểu số (chấp nhận số thập phân và số âm, chuyển 'NA' thành NULL)
    CASE 
        WHEN temp ~ '^-?[0-9]+(\.[0-9]+)?$' THEN temp::numeric 
        ELSE NULL 
    END AS temp,
    CASE 
        WHEN dewp ~ '^-?[0-9]+(\.[0-9]+)?$' THEN dewp::numeric 
        ELSE NULL 
    END AS dewp,
    CASE 
        WHEN humid ~ '^-?[0-9]+(\.[0-9]+)?$' THEN humid::numeric 
        ELSE NULL 
    END AS humid,
    CASE 
        WHEN wind_dir ~ '^-?[0-9]+(\.[0-9]+)?$' THEN wind_dir::float 
        ELSE NULL 
    END AS wind_dir,
    CASE 
        WHEN wind_speed ~ '^-?[0-9]+(\.[0-9]+)?$' THEN wind_speed::numeric 
        ELSE NULL 
    END AS wind_speed,
    CASE 
        WHEN wind_gust ~ '^-?[0-9]+(\.[0-9]+)?$' THEN wind_gust::numeric 
        ELSE NULL 
    END AS wind_gust,
    precip,
    CASE 
        WHEN pressure ~ '^-?[0-9]+(\.[0-9]+)?$' THEN pressure::numeric 
        ELSE NULL 
    END AS pressure,
    visib,  
    time_hour
FROM latest_record
WHERE rn = 1
