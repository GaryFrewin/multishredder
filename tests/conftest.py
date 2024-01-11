import os
import pytest
import pyodbc
import sqlite3
from fixtures.r1xml import r1_quote_xml
from fixtures.r2xml import r2_quote_xml
from fixtures.r3xml import r3_quote_xml
from fixtures.requestxml import request_quote_xml

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
