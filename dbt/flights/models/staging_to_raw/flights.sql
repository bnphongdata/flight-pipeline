-- ==============================================================================
-- MODEL: flights
-- TẦNG NGUỒN: staging.flights -> TẦNG ĐÍCH: raw_data.flights
-- CHIẾN LƯỢC: Incremental (Merge) dựa trên tổ hợp khóa định danh chuyến bay:
--             ['carrier', 'flight', 'year', 'month', 'day', 'hour', 'minute']
-- ==============================================================================

{{ 
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['carrier', 'flight', 'year', 'month', 'day', 'hour', 'minute']
  ) 
}}

SELECT
    carrier,          -- Hãng hàng không
    flight,           -- Số hiệu chuyến bay
    year,             -- Năm bay
    month,            -- Tháng bay
    day,              -- Ngày bay
    hour,             -- Giờ
    minute,           -- Phút
    actual_dep_time,  -- Thời gian cất cánh thực tế
    sched_dep_time,   -- Thời gian cất cánh theo lịch
    dep_delay,        -- Số phút trễ cất cánh
    actual_arr_time,  -- Thời gian hạ cánh thực tế
    sched_arr_time,   -- Thời gian hạ cánh theo lịch
    arr_delay,        -- Số phút trễ hạ cánh
    tailnum,          -- Số đăng bạ máy bay
    origin,           -- Sân bay đi
    dest,             -- Sân bay đến
    air_time,         -- Thời gian bay trên không (phút)
    distance,         -- Quãng đường bay (dặm)
    time_hour         -- Thời điểm theo giờ (timestamp)
FROM {{ source('staging', 'flights') }}
