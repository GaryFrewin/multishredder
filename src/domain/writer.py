import pyodbc
from dotenv import load_dotenv
import os

load_dotenv()


def write_to_sql(processed_data_queue, batch_size):
    cnxn = pyodbc.connect(os.getenv("DB_CONNECTION_STRING"))
    cursor = cnxn.cursor()

    while True:
        data = processed_data_queue.get()
        if data == "STOP":
            break

        batch = []
        for record in data:
            batch.append(record)
            ##do filtering and sorting here
            if len(batch) >= batch_size:
                sql_query, values = record.generate_insert_sql()
                cursor.executemany(
                    sql_query,
                    [values],
                )
                cnxn.commit()
                batch = []

        # Insert remaining records in the batch
        if batch:
            # cursor.executemany(
            #     """
            #     INSERT INTO ProcessedData (Grp_Product, Pct_Product, Pct_Pet_Sex)
            #     VALUES (?, ?, ?)
            #     """,
            #     batch,
            # )
            # cnxn.commit()
            pass

    cursor.close()
    cnxn.close()
