import os
import pyodbc
from dotenv import load_dotenv

from src.services.sql_query_loader import SqlQueryLoader

load_dotenv()


def get_db_connection():
    conn_str = os.getenv("DB_CONNECTION_STRING")
    return pyodbc.connect(conn_str)


def get_data_query():
    sql_loader = SqlQueryLoader("src\sql\select_data.sql")
    query = sql_loader.load()
    replacements = {
        "TABLE_NAME": os.getenv("SOURCE_DATA_TABLE"),
    }
    query_with_replacements = sql_loader.replace_placeholders(query, replacements)
    return query_with_replacements


def get_sql_chunks(db_to_worker_pipes, worker_readiness_queue, chunk_size):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = get_data_query()
        cursor.execute(query)

        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break  # No more data to fetch

            worker_id = worker_readiness_queue.get()
            db_to_worker_pipes[worker_id].send(rows)

        # Send STOP signal to all workers after all data is processed
        for conn in db_to_worker_pipes:
            conn.send("STOP")
    except Exception as e:
        print(e)
        return None
