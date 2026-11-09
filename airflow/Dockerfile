# Sử dụng image chính thức của Apache Airflow 2.9.1
FROM apache/airflow:2.9.1

# Chuyển sang quyền root để cài đặt các package hệ thống
USER root

# Cập nhật danh sách gói và cài đặt git
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Tạo môi trường ảo riêng biệt cho dbt để tránh xung đột thư viện (dependency conflict) với Airflow
RUN python -m venv /dbt_venv && \
    /dbt_venv/bin/pip install --no-cache-dir dbt-postgres

# Đưa thư mục bin của dbt_venv vào biến môi trường PATH để có thể chạy lệnh 'dbt' trực tiếp
ENV PATH="/dbt_venv/bin:$PATH"

# Chuyển quyền thực thi trở lại cho user airflow vì mục đích bảo mật
USER airflow
