import pytest
import pyodbc
import sqlite3


# Step 2: Database Connection Fixture
@pytest.fixture
def db_connection():
    conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-OC90MUN\SQLEXPRESS;DATABASE=GF_Python;Trusted_Connection=yes;"
    conn = pyodbc.connect(conn_str)

    # Yield the connection to be used by the test
    yield conn

    # Step 6: Teardown (Optional)
    conn.close()


# fmt: on
