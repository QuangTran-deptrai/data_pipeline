from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import logging

log = logging.getLogger(__name__)

def calculate_and_update_ranking(**kwargs):
    hook = MySqlHook(mysql_conn_id='QUANG')
    conn = hook.get_conn()
    cur = conn.cursor()

    
    cur.execute("SELECT level, level_name, required_points FROM level_configs ORDER BY required_points ASC")
    level_rows = cur.fetchall()
    level_list = []
    for row in level_rows:
        level_list.append({
            'level': row[0],
            'level_name': row[1],
            'required_points': row[2]
        })

    
    query = """
    SELECT
        m.community_member_id,
        COUNT(DISTINCT p.post_id)*10 + COUNT(DISTINCT c.comment_id)*2 + COUNT(DISTINCT e.event_id)*20 AS points
    FROM community_members m
    LEFT JOIN posts p ON p.user_id = m.community_member_id
    LEFT JOIN comments c ON c.user_id = m.community_member_id
    LEFT JOIN events e ON e.community_member_id = m.community_member_id
    GROUP BY m.community_member_id
    """
    cur.execute(query)
    members = cur.fetchall()

    for member_id, points in members:
        
        level = level_list[0]
        for l in level_list:
            if points >= l['required_points']:
                level = l
            else:
                break
        
        next_level = None
        for l in level_list:
            if l['required_points'] > level['required_points']:
                next_level = l
                break
        points_to_next_level = (next_level['required_points'] - points) if next_level else 0

        
        cur.execute("SELECT avatar_url FROM community_members WHERE community_member_id=%s", (member_id,))
        avatar_url = cur.fetchone()[0] if cur.rowcount > 0 else None


        cur.execute("""
        INSERT INTO ranking (community_member_id, circle_ranking, level, level_name, points, points_to_next_level, avatar_url, ranking_date, updated_at, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, CURDATE(), NOW(), NOW())
        ON DUPLICATE KEY UPDATE
            level=VALUES(level),
            level_name=VALUES(level_name),
            points=VALUES(points),
            points_to_next_level=VALUES(points_to_next_level),
            avatar_url=VALUES(avatar_url),
            ranking_date=CURDATE(),
            updated_at=NOW()
        """, (member_id, member_id, level['level'], level['level_name'], points, points_to_next_level, avatar_url))
    conn.commit()
    cur.close()
    conn.close()
    log.info(f"Đã cập nhật bảng ranking cho {len(members)} thành viên.")

with DAG(
    dag_id='ranking_etl',
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/30 * * * *", 
    catchup=False
) as dag:
    update_ranking = PythonOperator(
        task_id='calculate_and_update_ranking',
        python_callable=calculate_and_update_ranking,
        provide_context=True,
    )