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

def fetch_and_store_comments(**kwargs):
    api_url = kwargs['params'].get('api_url', 'https://app.circle.so/api/admin/v2/comments?page=1&per_page=10')
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
            log.info(f"Sample comment record: {records[0]}")
    except requests.exceptions.RequestException as e:
        log.error(f"Lỗi khi gọi API: {e}")
        raise

    db_conn_id = kwargs['params'].get('db_conn_id', 'QUANG')
    table_name = kwargs['params'].get('table_name', 'comments')
    log.info(f"Đang kết nối đến cơ sở dữ liệu với Conn ID: {db_conn_id}")

    try:
        hook = MySqlHook(mysql_conn_id=db_conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()
        log.info("Kết nối cơ sở dữ liệu thành công.")

        if not records:
            log.info("Không có bản ghi nào từ API để chèn.")
            return

        # Lấy danh sách post_id và community_member_id đã có
        cur.execute("SELECT post_id FROM posts")
        existing_post_ids = set(row[0] for row in cur.fetchall())
        cur.execute("SELECT community_member_id FROM community_members")
        existing_member_ids = set(row[0] for row in cur.fetchall())

        insert_sql = f"""
        INSERT INTO {table_name} (
            comment_id, created_at, body, user_id, post_id, user_name, user_avatar_url
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            created_at = VALUES(created_at),
            body = VALUES(body),
            user_id = VALUES(user_id),
            post_id = VALUES(post_id),
            user_name = VALUES(user_name),
            user_avatar_url = VALUES(user_avatar_url)
        """

        values_to_insert = []
        for record in records:
            comment_id = record.get('id')
            created_at_str = record.get('created_at')
            # body có thể là dict hoặc string
            body = record.get('body', {}).get('body') if isinstance(record.get('body'), dict) else record.get('body')
            # user và post có thể là dict hoặc id trực tiếp
            user = record.get('user', {})
            post = record.get('post', {})
            user_id = user.get('id') if isinstance(user, dict) else record.get('user_id')
            post_id = post.get('id') if isinstance(post, dict) else record.get('post_id')
            user_name = user.get('name') if isinstance(user, dict) else record.get('user_name')
            user_avatar_url = user.get('avatar_url') if isinstance(user, dict) else record.get('user_avatar_url')

            # Parse created_at
            created_at = None
            if created_at_str:
                try:
                    created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                except Exception:
                    log.warning(f"Could not parse created_at: {created_at_str}")

            # Kiểm tra foreign key
            if not post_id or post_id not in existing_post_ids:
                log.warning(f"Bỏ qua comment id={comment_id} vì post_id={post_id} không tồn tại trong posts")
                continue
            if not user_id or user_id not in existing_member_ids:
                log.warning(f"Bỏ qua comment id={comment_id} vì user_id={user_id} không tồn tại trong community_members")
                continue

            # Kiểm tra trường bắt buộc
            if None in (comment_id, created_at, body, user_id, post_id):
                log.warning(f"Bỏ qua bản ghi do thiếu trường bắt buộc: {record}")
                continue

            values_to_insert.append((
                comment_id, created_at, body, user_id, post_id, user_name, user_avatar_url
            ))

        if values_to_insert:
            cur.executemany(insert_sql, values_to_insert)
            conn.commit()
            log.info(f"Đã chèn/cập nhật {len(values_to_insert)} comment vào DB.")
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
    dag_id='comments_ingestion',
    start_date=days_ago(1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    params={
        'api_url': 'https://app.circle.so/api/admin/v2/comments?page=1&per_page=10',
        'db_conn_id': 'QUANG',
        'table_name': 'comments',
    }
) as dag:
    fetch_and_store_comments_task = PythonOperator(
        task_id='fetch_and_store_comments_task',
        python_callable=fetch_and_store_comments,
        provide_context=True,
    )