from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import logging

log = logging.getLogger(__name__)

def generate_post_challenges(**kwargs):
    hook = MySqlHook(mysql_conn_id='QUANG')
    conn = hook.get_conn()
    cur = conn.cursor()

    # Lấy danh sách level theo thứ tự required_points tăng dần
    cur.execute("SELECT level_name, required_points FROM level_configs ORDER BY required_points ASC")
    level_rows = cur.fetchall()
    level_names = [row[0] for row in level_rows]

    # Lấy max post_challenge_id hiện tại
    cur.execute("SELECT COALESCE(MAX(post_challenge_id), 0) FROM post_challenges")
    current_max_id = cur.fetchone()[0]

    # Tạo thử thách cho từng post
    cur.execute("SELECT post_id, user_id FROM posts")
    post_rows = cur.fetchall()
    for post_id, user_id in post_rows:
        points_reward = 10
        cur.execute("SELECT level_name FROM ranking WHERE community_member_id=%s", (user_id,))
        row = cur.fetchone()
        if row:
            user_level_name = row[0]
            user_level_index = level_names.index(user_level_name) if user_level_name in level_names else 0
            points_reward += user_level_index * 2

        # Kiểm tra đã tồn tại thử thách cho post này chưa
        cur.execute("SELECT post_challenge_id FROM post_challenges WHERE post_id=%s AND type='comment'", (post_id,))
        result = cur.fetchone()
        if result:
            post_challenge_id = result[0]
        else:
            current_max_id += 1
            post_challenge_id = current_max_id

        cur.execute("""
            INSERT INTO post_challenges (
                post_challenge_id, type, post_id, is_show, points_reward, due_date, created_at, updated_at
            )
            VALUES (%s, 'comment', %s, 1, %s, %s, NOW(), NOW())
            ON DUPLICATE KEY UPDATE
                is_show=VALUES(is_show),
                points_reward=VALUES(points_reward),
                due_date=VALUES(due_date),
                updated_at=NOW()
        """, (post_challenge_id, post_id, points_reward, (datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')))

    # Tạo thử thách cho từng event
    cur.execute("SELECT event_id FROM events")
    event_ids = [row[0] for row in cur.fetchall()]
    for event_id in event_ids:
        cur.execute("SELECT post_challenge_id FROM post_challenges WHERE event_id=%s AND type='event'", (event_id,))
        result = cur.fetchone()
        if result:
            post_challenge_id = result[0]
        else:
            current_max_id += 1
            post_challenge_id = current_max_id

        cur.execute("""
            INSERT INTO post_challenges (
                post_challenge_id, type, event_id, is_show, points_reward, due_date, created_at, updated_at
            )
            VALUES (%s, 'event', %s, 1, 20, %s, NOW(), NOW())
            ON DUPLICATE KEY UPDATE
                is_show=VALUES(is_show),
                points_reward=VALUES(points_reward),
                due_date=VALUES(due_date),
                updated_at=NOW()
        """, (post_challenge_id, event_id, (datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')))

    conn.commit()
    cur.close()
    conn.close()
    log.info(f"Đã cập nhật thử thách cho {len(post_rows)} post và {len(event_ids)} event.")

with DAG(
    dag_id='post_challenges_etl',
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/30 * * * *",
    catchup=False
) as dag:
    update_post_challenges = PythonOperator(
        task_id='generate_post_challenges',
        python_callable=generate_post_challenges,
        provide_context=True,
    )