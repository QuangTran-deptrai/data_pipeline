from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import requests
import logging
import os

log = logging.getLogger(__name__)

def fetch_all_records(api_url, api_headers):
    all_records = []
    page = 1
    while True:
        if "page=" in api_url:
            paged_url = api_url.rsplit("page=", 1)[0] + f"page={page}"
        elif "?" in api_url:
            paged_url = f"{api_url}&page={page}"
        else:
            paged_url = f"{api_url}?page={page}"
        response = requests.get(paged_url, headers=api_headers)
        response.raise_for_status()
        data = response.json()
        records = data.get('records', [])
        all_records.extend(records)
        if not data.get('has_next_page') or not records:
            break
        page += 1
    return all_records

def fetch_and_store_events(**kwargs):
    api_url = kwargs['params'].get('api_url', 'https://app.circle.so/api/admin/v2/events?page=1&per_page=60')
    CIRCLE_API_KEY = os.getenv("CIRCLE_API_KEY")
    api_headers = {
        "Authorization": f"Bearer {CIRCLE_API_KEY}",
        "Content-Type": "application/json"
    }

    log.info(f"Đang gọi API: {api_url}")
    try:
        records = fetch_all_records(api_url, api_headers)
        log.info(f"Đã lấy {len(records)} bản ghi từ API (nhiều trang).")
    except requests.exceptions.RequestException as e:
        log.error(f"Lỗi khi gọi API: {e}")
        raise

    db_conn_id = kwargs['params'].get('db_conn_id', 'QUANG')
    table_name = kwargs['params'].get('table_name', 'events')
    log.info(f"Đang kết nối đến cơ sở dữ liệu với Conn ID: {db_conn_id}")

    try:
        hook = MySqlHook(mysql_conn_id=db_conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()
        log.info("Kết nối cơ sở dữ liệu thành công.")

        if not records:
            log.info("Không có bản ghi nào từ API để chèn.")
            return

        insert_sql = f"""
        INSERT INTO {table_name} (
            event_id, ten_events, slug, host, community_member_id, in_person_location,
            starts_at, ends_at, created_at, updated_at, url, cover_image_url, description,
            price, max_attendees, current_attendees, status, category, location_url
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            ten_events = VALUES(ten_events),
            slug = VALUES(slug),
            host = VALUES(host),
            community_member_id = VALUES(community_member_id),
            in_person_location = VALUES(in_person_location),
            starts_at = VALUES(starts_at),
            ends_at = VALUES(ends_at),
            created_at = VALUES(created_at),
            updated_at = VALUES(updated_at),
            url = VALUES(url),
            cover_image_url = VALUES(cover_image_url),
            description = VALUES(description),
            price = VALUES(price),
            max_attendees = VALUES(max_attendees),
            current_attendees = VALUES(current_attendees),
            status = VALUES(status),
            category = VALUES(category),
            location_url = VALUES(location_url)
        """

        values_to_insert = []
        for record in records:
            event_id = record.get('id')
            ten_events = record.get('name')
            slug = record.get('slug')
            host = record.get('host')
            community_member_id = record.get('community_member_id')
            in_person_location = record.get('in_person_location')
            starts_at_str = record.get('starts_at')
            ends_at_str = record.get('ends_at')
            created_at_str = record.get('created_at')
            updated_at_str = record.get('updated_at')
            url = record.get('url')
            cover_image_url = record.get('cover_image_url')
            description = record.get('body')
            price = record.get('price')
            max_attendees = record.get('max_attendees')
            current_attendees = record.get('current_attendees', 0)
            status = record.get('status', 'upcoming')
            category = record.get('category')
            location_url = record.get('virtual_location_url')

            def parse_dt(dtstr):
                if dtstr:
                    try:
                        return datetime.fromisoformat(dtstr.replace('Z', '+00:00'))
                    except Exception:
                        log.warning(f"Could not parse datetime: {dtstr}")
                return None

            starts_at = parse_dt(starts_at_str)
            ends_at = parse_dt(ends_at_str)
            created_at = parse_dt(created_at_str)
            updated_at = parse_dt(updated_at_str)

            # Kiểm tra trường bắt buộc
            if None in (event_id, ten_events, slug, community_member_id, starts_at, ends_at, url):
                log.warning(f"Bỏ qua bản ghi do thiếu trường bắt buộc: {record}")
                continue

            values_to_insert.append((
                event_id, ten_events, slug, host, community_member_id, in_person_location,
                starts_at, ends_at, created_at, updated_at, url, cover_image_url, description,
                price, max_attendees, current_attendees, status, category, location_url
            ))

        if values_to_insert:
            cur.executemany(insert_sql, values_to_insert)
            conn.commit()
            log.info(f"Đã chèn {len(values_to_insert)} event vào DB.")
        else:
            log.info("Không có bản ghi hợp lệ nào để chèn vào DB.")

        cur.close()
        conn.close()
        log.info("Đã đóng kết nối cơ sở dữ liệu.")

    except Exception as e:
        log.error(f"Lỗi khi thao tác với database: {e}")
        raise

from airflow.utils.dates import days_ago

with DAG(
    dag_id='events_ingestion',
    start_date=days_ago(1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    params={
        'api_url': 'https://app.circle.so/api/admin/v2/events?page=1&per_page=60',
        'db_conn_id': 'QUANG',
        'table_name': 'events',
    }
) as dag:
    fetch_and_store_events_task = PythonOperator(
        task_id='fetch_and_store_events_task',
        python_callable=fetch_and_store_events,
        provide_context=True,
    )