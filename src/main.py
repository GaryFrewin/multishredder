import os
import time

import argparse
from dotenv import load_dotenv

from src.domain.config import Config
from src.domain.orchestrator import MultiProcessOrchestrator
from src.domain.shredder import XMLProcessor
from src.services.data_spec_builder import MetaDataBuilder
from src.services.sql_query_loader import SqlQueryLoader

load_dotenv()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Define the arguments
    parser.add_argument(
        "--chunksize",
        type=int,
        default=1000,
        help="Num of rows to read from SQL at a time",
    )  # noqa E501
    parser.add_argument(
        "--workers", type=int, default=2, help="Number of worker processes"
    )  # noqa E501
    parser.add_argument(
        "--writers", type=int, default=10, help="Number of writer processes"
    )  # noqa E501
    parser.add_argument(
        "--batchsize", type=int, default=1000, help="Batch write size"
    )  # noqa E501

    # Parse the arguments
    args = parser.parse_args()

    # Create the config object
    config = Config(args)

    sql_loader = SqlQueryLoader("src\sql\select_metadata.sql")
    query = sql_loader.load()
    replacements = {
        "US_SOURCES_TABLE": os.getenv("US_SOURCES_TABLE"),
        "UNSTRUCTURED_TABLES_TABLE": os.getenv("UNSTRUCTURED_TABLES_TABLE"),
    }
    query_with_replacements = sql_loader.replace_placeholders(query, replacements)

    # get classes for each metadata object
    # metadata_class_builder = MetaDataBuilder(data)
    worker = XMLProcessor()

    orchestrator = MultiProcessOrchestrator(config)
    tic = time.perf_counter()
    orchestrator.execute()
    toc = time.perf_counter()
    print(f"Finished in {toc - tic:0.4f} seconds")
