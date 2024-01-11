import os
import pytest
from src.services.sql_query_loader import SqlQueryLoader
from src.services.data_spec_builder import MetaDataBuilder


@pytest.fixture
def db_select_string():
    sql_loader = SqlQueryLoader("src\sql\select_metadata.sql")
    query = sql_loader.load()
    replacements = {
        "US_SOURCES_TABLE": os.getenv("US_SOURCES_TABLE"),
        "UNSTRUCTURED_TABLES_TABLE": os.getenv("UNSTRUCTURED_TABLES_TABLE"),
    }
    query_with_replacements = sql_loader.replace_placeholders(query, replacements)
    return query_with_replacements


def test_builder_returns_expected_types(db_connection, db_select_string):
    conn = db_connection
    cursor = conn.cursor()

    cursor.execute(db_select_string)

    data = cursor.fetchall()

    metadata_class_builder = MetaDataBuilder(data)

    metadata_classes = metadata_class_builder.build()

    assert len(metadata_classes) == 7
