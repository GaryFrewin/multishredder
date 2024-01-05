# Example test using the fixture
from queue import Queue
from src.domain.shredder import XMLProcessor


def test_query(db_connection):
    # arrange

    # 1. Get the data spec from the database
    cursor = db_connection.cursor()
    cursor.execute("SELECT * FROM metatable")
    data_spec = cursor.fetchall()

    processor = XMLProcessor(data_spec)

    # 2. Get the data from the database
    cursor.execute("SELECT * FROM source_table")
    rows = cursor.fetchall()

    queue: Queue = Queue()
    # act
    processor._process_rows(rows, queue)
