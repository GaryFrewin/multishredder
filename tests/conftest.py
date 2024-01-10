import os
import pytest
import pyodbc
import sqlite3
from dotenv import load_dotenv

load_dotenv()


# Step 2: Database Connection Fixture
@pytest.fixture
def db_connection():
    conn_str = os.getenv("DB_CONNECTION_STRING")
    conn = pyodbc.connect(conn_str)

    # Yield the connection to be used by the test
    yield conn

    # Step 6: Teardown (Optional)
    conn.close()


# fmt: on
