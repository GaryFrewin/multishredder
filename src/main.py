import time

import argparse
from domain.config import Config
from domain.orchestrator import MultiProcessOrchestrator
from domain.shredder import XMLProcessor


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

    worker = XMLProcessor()

    orchestrator = MultiProcessOrchestrator(config)
    tic = time.perf_counter()
    orchestrator.execute()
    toc = time.perf_counter()
    print(f"Finished in {toc - tic:0.4f} seconds")
