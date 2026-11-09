# flight-pipeline

Pipeline Dữ Liệu Tự Động Hóa (Airflow + dbt Core + PostgreSQL)

Dự án Data Engineering xây dựng quy trình trích xuất, nạp và biến đổi dữ liệu (ELT - Extract, Load, Transform) hoàn chỉnh từ đầu đến cuối (End-to-End Pipeline). Hệ thống kết hợp sức mạnh điều phối của **Apache Airflow**, khả năng biến đổi và kiểm thử mô hình dữ liệu của **dbt Core**, cùng cơ sở dữ liệu **PostgreSQL** đóng vai trò Data Warehouse, tất cả được đóng gói và vận hành đồng bộ thông qua **Docker Compose**.

---

## 1. Tổng Quan Kiến Trúc (Architecture Overview)

Dự án áp dụng mô hình kiến trúc dữ liệu 3 tầng (Medallion-style / Layered Architecture):

```
       [CSV Data Sources]
              │
              ▼ (Airflow PythonOperator: Postgres COPY)
      ┌────────────────┐
      │ STAGING SCHEMA │  (Dữ liệu thô vừa nạp, kiểu chuỗi linh hoạt)
      └────────────────┘
              │
              ▼ (dbt Core: Incremental Merge / Data Cleansing)
      ┌────────────────┐
      │ RAW DATA SCHEMA│  (Dữ liệu chuẩn hóa, có ràng buộc PK/FK)
      └────────────────┘
              │
              ▼ (dbt Core: Aggregation / Business Metrics)
      ┌────────────────┐
      │ CURATED SCHEMA │  (Data Mart tổng hợp phục vụ phân tích/BI)
      └────────────────┘
```

1. **Tầng Staging (`staging`)**:
   - Dữ liệu từ các file CSV (`airlines`, `airports`, `flights`, `planes`, `weather`) được nạp trực tiếp vào các bảng staging tương ứng bằng lệnh `COPY` tối ưu tốc độ.
   - Các trường dữ liệu dùng kiểu linh hoạt để tránh lỗi khi nạp (ingestion).

2. **Tầng Raw Data (`raw_data`)**:
   - dbt Core thực hiện làm sạch dữ liệu (loại bỏ giá trị ngoại lai, chuẩn hóa các trường số từ chuỗi 'NA', khử trùng lặp dữ liệu bằng window functions).
   - Áp dụng chiến lược `incremental` với phương thức `merge` (upsert) dựa trên khóa chính của từng bảng.

3. **Tầng Curated (`curated`)**:
   - Model `flights_summary` tổng hợp số lượng chuyến bay và tính toán thời gian trễ khởi hành / hạ cánh trung bình theo từng hãng hàng không, năm và tháng.

---

## 2. Công Nghệ Sử Dụng

- **Hệ điều phối luồng (Orchestration)**: [Apache Airflow](https://airflow.apache.org/) (phiên bản 2.9.1)
- **Công cụ biến đổi dữ liệu (Transformation)**: [dbt Core](https://www.getdbt.com/) (`dbt-postgres`) được cài đặt trong môi trường ảo độc lập trên Airflow container
- **Kho dữ liệu (Data Warehouse)**: [PostgreSQL](https://www.postgresql.org/) (phiên bản 15)
- **Container hóa (Containerization)**: [Docker](https://www.docker.com/) & [Docker Compose](https://docs.docker.com/compose/)

---

## 3. Cấu Trúc Thư Mục Dự Án

```text
airflow-dbt-pipeline/
├── airflow/
│   ├── dags/
│   │   └── flights.py             # DAG điều phối toàn bộ pipeline ELT
│   ├── plugins/
│   │   └── utils/
│   │       └── load_dw.py         # Hàm tiện ích kết nối DB và nạp CSV vào staging
│   └── Dockerfile                 # Dockerfile build image Airflow tùy chỉnh (tích hợp dbt)
├── data/
│   ├── nyc_airlines.csv           # Dữ liệu mẫu hãng hàng không
│   ├── nyc_airports.csv           # Dữ liệu mẫu sân bay
│   ├── nyc_flights.csv            # Dữ liệu mẫu lịch sử chuyến bay
│   ├── nyc_planes.csv             # Dữ liệu mẫu thông tin máy bay
│   └── nyc_weather.csv            # Dữ liệu mẫu thời tiết tại các sân bay
├── dbt/
│   ├── profiles.yml               # Cấu hình kết nối dbt tới Data Warehouse
│   └── flights/
│       ├── dbt_project.yml        # Cấu hình dự án dbt
│       └── models/
│           ├── sources.yml        # Định nghĩa sources và tests kiểm tra tính toàn vẹn
│           ├── staging_to_raw/    # Models chuyển đổi và làm sạch từ staging sang raw_data
│           │   ├── airlines.sql
│           │   ├── airports.sql
│           │   ├── flights.sql
│           │   ├── planes.sql
│           │   └── weather.sql
│           └── curated/           # Models tổng hợp báo cáo phục vụ kinh doanh/phân tích
│               ├── flights_summary.sql
│               └── schemas.yml
├── postgres/
│   └── init_db.sql                # Script SQL khởi tạo schema, phân quyền và tạo bảng
├── .env                           # Biến môi trường cấu hình mật khẩu và kết nối
├── .gitignore                     # Danh sách file loại trừ khi quản lý mã nguồn
├── docker-compose.yml             # Cấu hình triển khai các dịch vụ container
└── README.md                      # Tài liệu hướng dẫn dự án
```

---

## 4. Hướng Dẫn Cài Đặt & Chạy Dự Án

### Yêu cầu tiên quyết
- Đã cài đặt [Docker Desktop](https://www.docker.com/products/docker-desktop/) (hỗ trợ Docker Compose v2).
- Khuyến nghị tối thiểu: RAM 4GB trống và 10GB dung lượng ổ đĩa.

### Bước 1: Kiểm tra cấu hình môi trường
Xem và điều chỉnh tệp `.env` nếu cần thay đổi mật khẩu hoặc thông tin kết nối (mặc định đã cấu hình sẵn cho môi trường thử nghiệm):
```bash
POSTGRES_PASSWORD=Password1234**
POSTGRES_DB=dw_flights
POSTGRES_USER=pipeline_user
POSTGRES_HOST=flights_db
AIRFLOW_POSTGRES_USER=airflow
AIRFLOW_POSTGRES_PASSWORD=airflow
AIRFLOW_POSTGRES_DB=airflow
```

### Bước 2: Khởi động hệ thống bằng Docker Compose
Mở terminal hoặc PowerShell tại thư mục dự án và chạy:
```powershell
docker compose up -d --build
```

Lệnh trên sẽ:
1. Tải các image cơ sở và build custom image cho Airflow (cài đặt `dbt-postgres` trong môi trường ảo `/dbt_venv`).
2. Khởi tạo cơ sở dữ liệu `dw_flights` và chạy `postgres/init_db.sql` tạo sẵn các schema và bảng.
3. Chạy `airflow-init` để nâng cấp database metadata của Airflow và tạo tài khoản quản trị.
4. Khởi chạy `airflow-webserver` và `airflow-scheduler`.

### Bước 3: Kiểm tra trạng thái các dịch vụ
Chạy lệnh sau để đảm bảo tất cả các container đều ở trạng thái hoạt động (Up/healthy):
```powershell
docker compose ps
```

---

## 5. Vận Hành & Giám Sát Pipeline

### Truy cập Giao diện Quản trị Airflow
1. Mở trình duyệt và truy cập: [http://localhost:8080](http://localhost:8080)
2. Đăng nhập bằng tài khoản mặc định:
   - **Username**: `admin`
   - **Password**: `admin`

### Kích hoạt và theo dõi DAG
1. Tìm DAG mang tên: `flights_pipeline_elt`.
2. Bật công tắc kích hoạt (Unpause) DAG và bấm nút **Trigger DAG** (biểu tượng Play) để bắt đầu thực thi pipeline.
3. Theo dõi tiến trình qua **Graph View** hoặc **Grid View**:
   - Các task `load_<table_name>_csv_to_staging`: Nạp dữ liệu CSV vào staging.
   - Các task `dbt_load_<table_name>`: Biến đổi từ staging sang raw_data.
   - Task `dbt_flights_summary`: Tổng hợp báo cáo vào schema curated.

---

## 6. Kiểm Tra Dữ Liệu Trong PostgreSQL

Bạn có thể kết nối trực tiếp vào Data Warehouse bằng công cụ như DBeaver, pgAdmin hoặc thông qua dòng lệnh docker:

```powershell
docker exec -it flights_db psql -U pipeline_user -d dw_flights
```

### Một số truy vấn kiểm tra nhanh:
```sql
-- Kiểm tra số lượng bản ghi tầng raw_data
SELECT 'flights' AS table_name, count(*) FROM raw_data.flights
UNION ALL
SELECT 'airlines', count(*) FROM raw_data.airlines
UNION ALL
SELECT 'airports', count(*) FROM raw_data.airports;

-- Xem kết quả tổng hợp trong tầng curated
SELECT * FROM curated.flights_summary ORDER BY year, month, total_flights DESC LIMIT 10;
```

---

## 7. Dừng Và Xóa Môi Trường

Khi không còn sử dụng, bạn có thể dừng các container:
```powershell
docker compose down
```

Nếu muốn xóa toàn bộ container và các volume dữ liệu:
```powershell
docker compose down -v
```
