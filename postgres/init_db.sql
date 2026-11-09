-- ==============================================================================
-- KHỞI TẠO CƠ SỞ DỮ LIỆU DATA WAREHOUSE (dw_flights)
-- ==============================================================================

-- 1. Tạo người dùng chuyên dụng cho Pipeline (Non-root user)
CREATE USER pipeline_user WITH ENCRYPTED PASSWORD 'Password1234**';
GRANT ALL PRIVILEGES ON DATABASE dw_flights TO pipeline_user;

-- 2. Khởi tạo các Schema phục vụ kiến trúc ELT
-- - STAGING: Lưu trữ dữ liệu thô vừa nạp từ các file CSV
-- - RAW_DATA: Dữ liệu đã được chuẩn hóa kiểu dữ liệu, validate và lưu trữ dài hạn
-- - CURATED: Tầng Data Mart phục vụ báo cáo phân tích tổng hợp
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS curated;

GRANT ALL ON SCHEMA staging, raw_data, curated TO pipeline_user;

-- ==============================================================================
-- CÁC BẢNG TẦNG RAW DATA (raw_data)
-- Có ràng buộc toàn vẹn khóa chính (PK) và khóa ngoại (FK)
-- ==============================================================================

-- Bảng danh mục hãng hàng không
CREATE TABLE raw_data.airlines (
    carrier VARCHAR(2),
    name VARCHAR NOT NULL,
    CONSTRAINT p_airline PRIMARY KEY (carrier)
);

-- Bảng danh mục sân bay
CREATE TABLE raw_data.airports (
    faa VARCHAR(3),
    name VARCHAR NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    altitude FLOAT NOT NULL,
    timezone INTEGER NOT NULL,
    dst VARCHAR(1) NOT NULL,
    timezone_name VARCHAR NOT NULL,
    CONSTRAINT p_airport PRIMARY KEY (faa)
);

-- Bảng thông tin máy bay
CREATE TABLE raw_data.planes (
    tailnum VARCHAR(6),
    year INTEGER,
    type VARCHAR NOT NULL,
    manufacturer VARCHAR NOT NULL,
    model VARCHAR NOT NULL,
    engines INTEGER NOT NULL,
    seats INTEGER NOT NULL,
    speed FLOAT,
    engine VARCHAR NOT NULL,
    CONSTRAINT p_plane PRIMARY KEY (tailnum)
);

-- Bảng dữ liệu thời tiết tại các sân bay
CREATE TABLE raw_data.weather (
    origin VARCHAR(3),
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    temp NUMERIC,
    dewp NUMERIC,
    humid NUMERIC,
    wind_dir FLOAT,
    wind_speed NUMERIC,
    wind_gust NUMERIC,
    precip NUMERIC,
    pressure NUMERIC,
    visib NUMERIC,
    time_hour TIMESTAMPTZ NOT NULL,
    CONSTRAINT p_weather PRIMARY KEY (origin, year, month, day, hour),
    CONSTRAINT fk_airport FOREIGN KEY (origin) REFERENCES raw_data.airports(faa)
);

-- Bảng lịch sử các chuyến bay (Fact Table)
CREATE TABLE raw_data.flights (
    carrier VARCHAR(2),
    flight INTEGER NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    hour INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    actual_dep_time INTEGER,
    sched_dep_time INTEGER,
    dep_delay INTEGER,
    actual_arr_time INTEGER,
    sched_arr_time INTEGER,
    arr_delay INTEGER,
    tailnum VARCHAR(6) NOT NULL,
    origin VARCHAR(3) NOT NULL,
    dest VARCHAR(3) NOT NULL,
    air_time FLOAT NOT NULL,
    distance FLOAT NOT NULL,
    time_hour TIMESTAMPTZ NOT NULL,
    CONSTRAINT p_flight PRIMARY KEY (carrier, flight, year, month, day, hour, minute),
    CONSTRAINT fk_airline FOREIGN KEY (carrier) REFERENCES raw_data.airlines(carrier),
    CONSTRAINT fk_plane FOREIGN KEY (tailnum) REFERENCES raw_data.planes(tailnum),
    CONSTRAINT fk_origin FOREIGN KEY (origin) REFERENCES raw_data.airports(faa),
    CONSTRAINT fk_dest FOREIGN KEY (dest) REFERENCES raw_data.airports(faa)
);

-- ==============================================================================
-- CÁC BẢNG TẦNG STAGING (staging)
-- Dữ liệu nhận trực tiếp từ CSV, dùng kiểu chuỗi (VARCHAR) linh hoạt để tránh lỗi nạp
-- ==============================================================================

CREATE TABLE staging.airlines (
    carrier VARCHAR(2),
    name VARCHAR NOT NULL
);

CREATE TABLE staging.airports (
    faa VARCHAR(3),
    name VARCHAR NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    altitude FLOAT NOT NULL,
    timezone INTEGER NOT NULL,
    dst VARCHAR(1) NOT NULL,
    timezone_name VARCHAR NOT NULL
);

CREATE TABLE staging.planes (
    tailnum VARCHAR(6),
    year VARCHAR,
    type VARCHAR NOT NULL,
    manufacturer VARCHAR NOT NULL,
    model VARCHAR NOT NULL,
    engines INTEGER NOT NULL,
    seats INTEGER NOT NULL,
    speed VARCHAR,
    engine VARCHAR NOT NULL
);

CREATE TABLE staging.weather (
    origin VARCHAR(3),
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    temp VARCHAR,
    dewp VARCHAR,
    humid VARCHAR,
    wind_dir VARCHAR,
    wind_speed VARCHAR,
    wind_gust VARCHAR,
    precip NUMERIC,
    pressure VARCHAR,
    visib NUMERIC,
    time_hour TIMESTAMPTZ NOT NULL
);

CREATE TABLE staging.flights (
    idx int,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    actual_dep_time INTEGER,
    sched_dep_time INTEGER,
    dep_delay INTEGER,
    actual_arr_time INTEGER,
    sched_arr_time INTEGER,
    arr_delay INTEGER,
    carrier VARCHAR(2),
    flight INTEGER NOT NULL,
    tailnum VARCHAR(6) NOT NULL,
    origin VARCHAR(3) NOT NULL,
    dest VARCHAR(3) NOT NULL,
    air_time FLOAT NOT NULL,
    distance FLOAT NOT NULL,
    hour INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    time_hour TIMESTAMPTZ NOT NULL
);

-- Cấp đầy đủ quyền thao tác bảng cho người dùng pipeline
GRANT ALL ON ALL TABLES IN SCHEMA staging, raw_data, curated TO pipeline_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging, raw_data, curated GRANT ALL ON TABLES TO pipeline_user;
