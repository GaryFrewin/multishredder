import pyodbc


def write_to_sql(processed_data_queue, batch_size):
    cnxn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=GeorgeWork;UID=python;PWD=mypassword(!)"
    )
    cursor = cnxn.cursor()

    # Create the table if it does not exist
    cursor.execute(
        """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ProcessedData' AND xtype='U')
        CREATE TABLE ProcessedData (
            Grp_Product NVARCHAR(10),
            Pct_Product NVARCHAR(10),
            Pct_Pet_Sex NVARCHAR(10)
        )
    """
    )
    cnxn.commit()

    while True:
        data = processed_data_queue.get()
        if data == "STOP":
            break

        batch = []
        for record in data:
            batch.append((record.field1, record.field2, record.field3))
            if len(batch) >= batch_size:
                cursor.executemany(
                    """
                    INSERT INTO ProcessedData (Grp_Product, Pct_Product, Pct_Pet_Sex)
                    VALUES (?, ?, ?)
                    """,
                    batch,
                )
                cnxn.commit()
                batch = []

        # Insert remaining records in the batch
        if batch:
            cursor.executemany(
                """
                INSERT INTO ProcessedData (Grp_Product, Pct_Product, Pct_Pet_Sex)
                VALUES (?, ?, ?)
                """,
                batch,
            )
            cnxn.commit()

    cursor.close()
    cnxn.close()
