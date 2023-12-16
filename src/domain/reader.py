import pyodbc


def get_sql_chunks(db_to_worker_pipes, worker_readiness_queue, chunk_size):
    try:
        cnxn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=GeorgeWork;UID=python;PWD=mypassword(!)"
        )
        cursor = cnxn.cursor()
        cursor.execute(
            "SELECT * FROM [dbo].[big_data]"
        )  # Remove TOP 1 to select all rows

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
