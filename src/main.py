import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import time
import argparse
import pyodbc
import logging
from dotenv import load_dotenv
from src.domain.config import Config
from src.domain.orchestrator import MultiProcessOrchestrator
from src.domain.shredder import XMLProcessor
from src.services.data_spec_builder import MetaDataBuilder
from src.services.sql_query_loader import SqlQueryLoader

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--chunksize",
        type=int,
        default=1000,
        help="Num of rows to read from SQL at a time",
    )
    parser.add_argument(
        "--workers", type=int, default=2, help="Number of worker processes"
    )
    parser.add_argument(
        "--writers", type=int, default=10, help="Number of writer processes"
    )
    parser.add_argument("--batchsize", type=int, default=1000, help="Batch write size")
    return parser.parse_args()


def get_db_connection():
    conn_str = os.getenv("DB_CONNECTION_STRING")
    print(f"Connection string {conn_str}")
    return pyodbc.connect(conn_str)


def fetch_metadata():
    sql_loader = SqlQueryLoader("src/sql/select_metadata.sql")
    query = sql_loader.load()
    replacements = {
        "US_SOURCES_TABLE": os.getenv("US_SOURCES_TABLE"),
        "UNSTRUCTURED_TABLES_TABLE": os.getenv("UNSTRUCTURED_TABLES_TABLE"),
    }
    query_with_replacements = sql_loader.replace_placeholders(query, replacements)

    # Use a with statement to ensure the connection is closed after use
    with pyodbc.connect(os.getenv("DB_CONNECTION_STRING")) as conn:
        cursor = conn.cursor()
        cursor.execute(query_with_replacements)
        return cursor.fetchall()


def execute_orchestrator(config, classes):
    orchestrator = MultiProcessOrchestrator(config, classes)
    tic = time.perf_counter()
    orchestrator.execute()
    toc = time.perf_counter()
    print(f"Finished in {toc - tic:0.4f} seconds")


if __name__ == "__main__":
    logger.info("Starting main")
    logger.info(os.environ)
    args = parse_arguments()
    logger.info(f"Arguments: {args}")
    config = Config(args)
    logger.info(f"Config: {config}")
    metadata = fetch_metadata()
    logger.info(f"Metadata: {metadata}")
    metadata_class_builder = MetaDataBuilder(metadata)
    classes = metadata_class_builder.build()
    execute_orchestrator(config, classes)
