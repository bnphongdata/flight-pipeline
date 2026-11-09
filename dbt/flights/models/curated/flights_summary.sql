-- ==============================================================================
-- MODEL: flights_summary
-- TẦNG: CURATED (DATA MART)
-- MÔ TẢ: Tổng hợp số lượng chuyến bay và tính toán độ trễ trung bình cất/hạ cánh
--        theo từng hãng hàng không, năm và tháng.
-- ==============================================================================

SELECT
    carrier,                                   -- Mã hãng hàng không
    year,                                      -- Năm
    month,                                     -- Tháng
    COUNT(*) AS total_flights,                 -- Tổng số chuyến bay đã thực hiện
    ROUND(AVG(dep_delay)::numeric, 2) AS avg_dep_delay,  -- Thời gian trễ khởi hành trung bình (phút)
    ROUND(AVG(arr_delay)::numeric, 2) AS avg_arr_delay   -- Thời gian trễ hạ cánh trung bình (phút)
FROM {{ source('raw_data', 'flights') }}
GROUP BY 
    carrier, 
    year, 
    month
ORDER BY 
    carrier, 
    year, 
    month
