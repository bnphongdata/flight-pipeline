"""
Module hỗ trợ kết nối và nạp dữ liệu vào Data Warehouse (PostgreSQL)
Sử dụng COPY command của PostgreSQL để tối ưu tốc độ nạp dữ liệu lớn từ file CSV.
"""

import os
import logging
import psycopg2
import psycopg2.extras
from airflow.hooks.base import BaseHook

def _connect_db(host, port, username, password, database):
    """
    Tạo và mở kết nối tới cơ sở dữ liệu PostgreSQL.

    :param host: Địa chỉ máy chủ CSDL
    :param port: Cổng kết nối (mặc định 5432)
    :param username: Tên đăng nhập CSDL
    :param password: Mật khẩu kết nối
    :param database: Tên CSDL cần kết nối
    :return: Đối tượng kết nối (conn) và con trỏ truy vấn (cursor)
    """
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=username,
        password=password,
        dbname=database
    )
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return conn, cursor


def _get_source_path(source):
    """
    Xác định đường dẫn tuyệt đối của tệp dữ liệu nguồn.
    Nếu source đã là đường dẫn tuyệt đối thì giữ nguyên, ngược lại ghép với thư mục hiện tại.

    :param source: Đường dẫn tệp nguồn (tuyệt đối hoặc tương đối)
    :return: Đường dẫn tuyệt đối của tệp
    """
    if os.path.isabs(source):
        return source
    current_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_path, source)


def load_dw(table, source, schema='staging', delimiter=',', conn_id='postgres'):
    """
    Nạp dữ liệu từ tệp CSV vào bảng đích trong tầng Staging của Data Warehouse.
    Quy trình:
      1. Lấy thông tin kết nối thông qua Airflow Connection (conn_id).
      2. Mở kết nối đến PostgreSQL.
      3. Xóa dữ liệu cũ trong bảng staging trước khi nạp mới (Truncate/Delete).
      4. Dùng lệnh `COPY ... FROM STDIN` để nạp dữ liệu tốc độ cao.
      5. Commit và đóng kết nối an toàn.

    :param table: Tên bảng đích (ví dụ: 'airlines', 'airports')
    :param source: Đường dẫn tới tệp dữ liệu CSV
    :param schema: Tên schema lưu trữ (mặc định: 'staging')
    :param delimiter: Ký tự phân cách cột trong CSV (mặc định: ',')
    :param conn_id: ID kết nối trong Airflow Connection
    """
    conn_info = BaseHook.get_connection(conn_id)
    host = conn_info.host
    port = conn_info.port
    username = conn_info.login
    password = conn_info.password
    database = conn_info.schema

    conn, cursor = _connect_db(host, port, username, password, database)
    source_path = _get_source_path(source)

    try:
        with open(source_path, 'r', encoding='utf-8') as f:
            logging.info(f"Bắt đầu nạp dữ liệu từ: {source_path} vào bảng {schema}.{table}")
            copy_sql = f"""
                DELETE FROM {schema}.{table} WHERE TRUE;
                COPY {schema}.{table} FROM STDIN WITH CSV DELIMITER '{delimiter}' HEADER
            """
            cursor.copy_expert(copy_sql, f)
            conn.commit()
            logging.info(f"Đã nạp thành công dữ liệu vào {schema}.{table}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Lỗi khi nạp dữ liệu vào {schema}.{table}: {str(e)}")
        raise e
    finally:
        cursor.close()
        conn.close()
