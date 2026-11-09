"""
DAG điều phối Pipeline dữ liệu hàng không (Flights Data Pipeline)
Quy trình ELT:
  1. Ingestion: Nạp file CSV từ /opt/airflow/data vào schema 'staging' trong PostgreSQL.
  2. Transformation (Staging -> Raw Data): Chạy dbt models để làm sạch, validate và merge vào schema 'raw_data'.
  3. Curated Mart: Tổng hợp số liệu thống kê chuyến bay vào schema 'curated' phục vụ phân tích/báo cáo.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from utils.load_dw import load_dw

# Cấu hình mặc định cho các task trong DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Danh sách bảng và tệp CSV nguồn tương ứng
tables_to_load = {
    "airlines": "/opt/airflow/data/nyc_airlines.csv",
    "airports": "/opt/airflow/data/nyc_airports.csv",
    "flights": "/opt/airflow/data/nyc_flights.csv",
    "planes": "/opt/airflow/data/nyc_planes.csv",
    "weather": "/opt/airflow/data/nyc_weather.csv",
}

with DAG(
    dag_id='flights_pipeline_elt',
    default_args=default_args,
    description='Pipeline tự động hóa nạp và biến đổi dữ liệu hàng không với Airflow & dbt',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['flights', 'dbt', 'postgres', 'elt'],
) as dag:

    # Task dbt: Tổng hợp dữ liệu phân tích tầng Curated (Curated Data Mart)
    dbt_model_flights_summary = BashOperator(
        task_id='dbt_flights_summary',
        bash_command='dbt run --target curated --select curated.flights_summary',
    )

    # Duyệt qua từng nguồn dữ liệu để tạo luồng nạp vào Staging và biến đổi sang Raw Data
    for tab in tables_to_load:
        # Task 1: Nạp dữ liệu CSV vào bảng tương ứng trong schema staging
        csv_load = PythonOperator(
            task_id=f'load_{tab}_csv_to_staging',
            python_callable=load_dw,
            op_kwargs={
                'table': tab,
                'source': tables_to_load[tab]
            }
        )

        # Task 2: Chạy dbt để biến đổi và đồng bộ từ staging sang raw_data
        dbt_model_run = BashOperator(
            task_id=f'dbt_load_{tab}',
            bash_command=f'dbt run --select staging_to_raw.{tab}'
        )

        # Thiết lập phụ thuộc: Phải nạp vào staging xong mới chạy dbt model
        csv_load >> dbt_model_run

        # Khi bảng flights được xử lý xong ở tầng raw_data, tiến hành tổng hợp vào tầng curated
        if tab == "flights":
            dbt_model_run >> dbt_model_flights_summary
