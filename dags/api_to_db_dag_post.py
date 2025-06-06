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
        records = data.get('records', []) or data.get('data', [])
        all_records.extend(records)
        if not data.get('has_next_page') or not records:
            break
        page += 1
    return all_records

def fetch_and_store_posts(**kwargs):
    api_url = kwargs['params'].get('api_url', 'https://app.circle.so/api/admin/v2/posts?page=1&per_page=60')
    CIRCLE_API_KEY = os.getenv("CIRCLE_API_KEY")
    api_headers = {
        "Authorization": f"Bearer {CIRCLE_API_KEY}",
        "Content-Type": "application/json"
    }

    log.info(f"Đang gọi API: {api_url}")
    try:
        records = fetch_all_records(api_url, api_headers)
        log.info(f"Đã lấy {len(records)} bản ghi từ API (nhiều trang).")
        if records:
            log.info(f"Sample record: {records[0]}")
    except requests.exceptions.RequestException as e:
        log.error(f"Lỗi khi gọi API: {e}")
        raise

    db_conn_id = kwargs['params'].get('db_conn_id', 'QUANG')
    table_name = kwargs['params'].get('table_name', 'posts')
    log.info(f"Đang kết nối đến cơ sở dữ liệu với Conn ID: {db_conn_id}")

    try:
        hook = MySqlHook(mysql_conn_id=db_conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()
        log.info("Kết nối cơ sở dữ liệu thành công.")

        # Lấy danh sách community_member_id đã có
        cur.execute("SELECT community_member_id FROM community_members")
        existing_member_ids = set(row[0] for row in cur.fetchall())

        if not records:
            log.info("Không có bản ghi nào từ API để chèn.")
            return

        insert_sql = f"""
        INSERT INTO {table_name} (
            post_id, ten_posts, slug, status, published_at, created_at, updated_at, body,
            url, user_id, user_name, user_avatar_url, likes_count, comments_count
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            ten_posts = VALUES(ten_posts),
            slug = VALUES(slug),
            status = VALUES(status),
            published_at = VALUES(published_at),
            created_at = VALUES(created_at),
            updated_at = VALUES(updated_at),
            body = VALUES(body),
            url = VALUES(url),
            user_id = VALUES(user_id),
            user_name = VALUES(user_name),
            user_avatar_url = VALUES(user_avatar_url),
            likes_count = VALUES(likes_count),
            comments_count = VALUES(comments_count)
        """

        def parse_dt(dtstr):
            if dtstr:
                try:
                    return datetime.fromisoformat(dtstr.replace('Z', '+00:00'))
                except Exception:
                    log.warning(f"Could not parse datetime: {dtstr}")
            return None

        values_to_insert = []
        for record in records:
            post_id = record.get('id')
            ten_posts = record.get('name')
            slug = record.get('slug')
            status = record.get('status')
            published_at = parse_dt(record.get('published_at'))
            created_at = parse_dt(record.get('created_at'))
            updated_at = parse_dt(record.get('updated_at'))
            body = record.get('body', {}).get('body') if isinstance(record.get('body'), dict) else record.get('body')
            url = record.get('url')
            user_id = record.get('user', {}).get('id') if isinstance(record.get('user'), dict) else record.get('user_id')
            user_name = record.get('user', {}).get('name') if isinstance(record.get('user'), dict) else record.get('user_name')
            user_avatar_url = record.get('user', {}).get('avatar_url') if isinstance(record.get('user'), dict) else record.get('user_avatar_url')
            likes_count = record.get('likes_count', 0)
            comments_count = record.get('comments_count', 0)

            # Kiểm tra foreign key
            if not user_id or user_id not in existing_member_ids:
                log.warning(f"Bỏ qua post id={post_id} vì user_id={user_id} không tồn tại trong community_members")
                continue

            # Kiểm tra trường bắt buộc
            if None in (post_id, ten_posts, slug, status, created_at, url, user_id):
                log.warning(f"Bỏ qua bản ghi do thiếu trường bắt buộc: {record}")
                continue

            values_to_insert.append((
                post_id, ten_posts, slug, status, published_at, created_at, updated_at, body,
                url, user_id, user_name, user_avatar_url, likes_count, comments_count
            ))

        if values_to_insert:
            cur.executemany(insert_sql, values_to_insert)
            conn.commit()
            log.info(f"Đã chèn/cập nhật {len(values_to_insert)} posts vào DB.")
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
    dag_id='posts_ingestion',
    start_date=days_ago(1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    params={
        'api_url': 'https://app.circle.so/api/admin/v2/posts?page=1&per_page=60',
        'db_conn_id': 'QUANG',
        'table_name': 'posts',
    }
) as dag:
    fetch_and_store_posts_task = PythonOperator(
        task_id='fetch_and_store_posts_task',
        python_callable=fetch_and_store_posts,
        provide_context=True,
    )