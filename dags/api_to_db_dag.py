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

import os

def fetch_and_store_members(**kwargs):
    api_url = kwargs['params'].get('api_url', 'https://app.circle.so/api/admin/v2/community_members?page=1&per_page=10')
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
            log.info(f"Sample member record: {records[0]}")
    except requests.exceptions.RequestException as e:
        log.error(f"Lỗi khi gọi API: {e}")
        raise

    db_conn_id = kwargs['params'].get('db_conn_id', 'QUANG')
    table_name = kwargs['params'].get('table_name', 'community_members')
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
            community_member_id, first_name, last_name, ten_community_members, email, avatar_url,
            location, description, created_at, updated_at, posts_count, comments_count,
            total_points, current_level, current_level_name, points_to_next_level, level_progress
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            first_name=VALUES(first_name),
            last_name=VALUES(last_name),
            ten_community_members=VALUES(ten_community_members),
            email=VALUES(email),
            avatar_url=VALUES(avatar_url),
            location=VALUES(location),
            description=VALUES(description),
            created_at=VALUES(created_at),
            updated_at=VALUES(updated_at),
            posts_count=VALUES(posts_count),
            comments_count=VALUES(comments_count),
            total_points=VALUES(total_points),
            current_level=VALUES(current_level),
            current_level_name=VALUES(current_level_name),
            points_to_next_level=VALUES(points_to_next_level),
            level_progress=VALUES(level_progress)
        """

        def parse_name(name):
            if not name:
                return None, None
            parts = name.strip().split(' ', 1)
            if len(parts) == 2:
                return parts[0], parts[1]
            return None, None

        values = []
        for r in records:
            member_id = r.get('user_id')
            member_name = r.get('name')
            first_name = r.get('first_name')
            last_name = r.get('last_name')
            if not first_name or not last_name:
                first_name, last_name = parse_name(member_name)
        
            # --- avatar_url ---
            avatar_url = None
            for pf in r.get('profile_fields', []):
                if pf.get('key') == 'avatar_url':
                    avatar_url = pf.get('community_member_profile_field', {}).get('text') or pf.get('community_member_profile_field', {}).get('url')
                    break
            if not avatar_url:
                avatar_url = r.get('flattened_profile_fields', {}).get('avatar_url') or r.get('avatar_url')
        
            # --- location ---
            location = None
            for pf in r.get('profile_fields', []):
                if pf.get('key') == 'location':
                    locs = pf.get('community_member_profile_field', {}).get('location_connections', [])
                    if locs and isinstance(locs, list) and len(locs) > 0:
                        if isinstance(locs[0], dict):
                            location = locs[0].get('name')
                        else:
                            location = str(locs[0])
                    break
            if not location:
                location = r.get('flattened_profile_fields', {}).get('location')
        
            # --- description ---
            description = None
            for pf in r.get('profile_fields', []):
                if pf.get('key') == 'bio':
                    description = pf.get('community_member_profile_field', {}).get('text') or pf.get('community_member_profile_field', {}).get('textarea')
                    break
            if not description:
                for pf in r.get('profile_fields', []):
                    if pf.get('key') == 'headline':
                        description = pf.get('community_member_profile_field', {}).get('text')
                        break
            if not description:
                description = r.get('flattened_profile_fields', {}).get('bio') or r.get('flattened_profile_fields', {}).get('headline') or r.get('description')
        
            values.append((
                member_id,
                first_name,
                last_name,
                member_name,
                r.get('email'),
                avatar_url,
                location,
                description,
                r.get('created_at'),
                r.get('updated_at'),
                r.get('posts_count', 0),
                r.get('comments_count', 0),
                r.get('gamification_stats', {}).get('total_points', 0),
                r.get('gamification_stats', {}).get('current_level', 1),
                r.get('gamification_stats', {}).get('current_level_name'),
                r.get('gamification_stats', {}).get('points_to_next_level', 10),
                r.get('gamification_stats', {}).get('level_progress', 0)
            ))

        if values:
            cur.executemany(insert_sql, values)
            conn.commit()
            log.info(f"Đã chèn/cập nhật {len(values)} community_members vào DB.")
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
    dag_id='community_members_ingestion',
    start_date=days_ago(1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    params={
        'api_url': 'https://app.circle.so/api/admin/v2/community_members?page=1&per_page=10',
        'db_conn_id': 'QUANG',
        'table_name': 'community_members',
    }
) as dag:
    ingest_task = PythonOperator(
        task_id='fetch_and_store_members_task',
        python_callable=fetch_and_store_members,
        provide_context=True,
    )