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

    
    cur.execute("SELECT level, required_points FROM level_configs ORDER BY required_points ASC")
    level_rows = cur.fetchall()
    level_dict = {row[0]: row[1] for row in level_rows}


    cur.execute("SELECT post_id, user_id FROM posts")
    post_rows = cur.fetchall()
    for post_id, user_id in post_rows:
        
        points_reward = 10
        cur.execute("SELECT level FROM ranking WHERE community_member_id=%s", (user_id,))
        row = cur.fetchone()
        if row:
            user_level = row[0]
            
            points_reward += (user_level - 1) * 2
        cur.execute("""
        INSERT INTO post_challenges (type, post_id, is_show, points_reward, due_date, created_at, updated_at)
        VALUES ('comment', %s, 1, %s, %s, NOW(), NOW())
        ON DUPLICATE KEY UPDATE
            is_show=VALUES(is_show),
            points_reward=VALUES(points_reward),
            due_date=VALUES(due_date),
            updated_at=NOW()
        """, (post_id, points_reward, (datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')))


    cur.execute("SELECT event_id FROM events")
    event_ids = [row[0] for row in cur.fetchall()]
    for event_id in event_ids:
        cur.execute("""
        INSERT INTO post_challenges (type, event_id, is_show, points_reward, due_date, created_at, updated_at)
        VALUES ('event', %s, 1, 20, %s, NOW(), NOW())
        ON DUPLICATE KEY UPDATE
            is_show=VALUES(is_show),
            points_reward=VALUES(points_reward),
            due_date=VALUES(due_date),
            updated_at=NOW()
        """, (event_id, (datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')))

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