import pytest
import pyodbc
import sqlite3


# Step 2: Database Connection Fixture
@pytest.fixture
def db_connection():
    # Create an in-memory SQLite database
    conn_str = "Driver=Devart ODBC Driver for SQLite;Database=:memory:"
    conn = pyodbc.connect(conn_str, autocommit=True)
    cursor = conn.cursor()
    create_table_schemas(cursor)
    conn.commit()

    populate_test_data(cursor)
    conn.commit()

    # Yield the connection to be used by the test
    yield conn

    # Step 6: Teardown (Optional)
    conn.close()


def create_table_schemas(cursor):
    # Create metatable
    cursor.execute(
        """
        CREATE TABLE metatable (
            source_field TEXT,
            target_field TEXT,
            field_type TEXT
        )
    """
    )

    # Create source_table with XML data columns
    cursor.execute(
        """
        CREATE TABLE source_table (
            id INTEGER,
            r1 TEXT,  -- XML data
            r2 TEXT   -- XML data
        )
    """
    )

    # Create target_table with sample columns
    cursor.execute(
        """
        CREATE TABLE target_table (
            qid INTEGER,
            field1 TEXT,
            field2 TEXT,
            field3 TEXT
            -- Add more columns as needed
        )
    """
    )


def populate_test_data(cursor):
    meta_data = [
        # (source_field, target_field, data_source)
        ("id", "qid", "root"),
        ("field1", "field1", "xml"),
        ("field2", "field2", "xml"),
        # Add more mappings as needed
    ]
    cursor.executemany("INSERT INTO metatable VALUES (?, ?, ?)", meta_data)

    # Inserting data into source_table
    source_data = [
        (
            1,
            "<product><r1><field1>Value11</field1></r1>",
            "<r1><field1>Value12</field1></r1></product>",
        ),
        (2, "<field1>Value21</field1><field2>Value22</field2>"),
        (3, "<r1><field1>Value31</field1></r1>", "<r1><field2>Value32</field2></r1>")
        # Add more rows as needed
    ]
    cursor.executemany("INSERT INTO source_table VALUES (?, ?, ?)", source_data)
