from multiprocessing import Process, Pipe, Queue
import time
import pyodbc
import xml.etree.ElementTree as ET
import argparse


def get_sql_chunks(db_to_worker_pipes, worker_readiness_queue, chunk_size):
    try:
        cnxn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=GeorgeWork;UID=python;PWD=mypassword(!)"
        )
        cursor = cnxn.cursor()
        cursor.execute(
            "SELECT * FROM [dbo].[big_data]"
        )  # Remove TOP 1 to select all rows

        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break  # No more data to fetch

            worker_id = worker_readiness_queue.get()
            db_to_worker_pipes[worker_id].send(rows)

        # Send STOP signal to all workers after all data is processed
        for conn in db_to_worker_pipes:
            conn.send("STOP")
    except Exception as e:
        print(e)
        return None


class ProcessedData:
    def __init__(self, field1, field2, field3):
        self.field1 = field1
        self.field2 = field2
        self.field3 = field3

    def __str__(self):
        return f"ProcessedData(field1={self.field1}, field2={self.field2}, field3={self.field3})"


class XMLProcessor:
    def _process_rows(self, rows, processed_data_queue):
        processed_rows = []

        for row in rows:
            xml_data = row.r3  # Assuming 'r3' is the column with XML data

            # Parse the XML data
            try:
                root = ET.fromstring(xml_data)
                for cp_pet_r3 in root.findall("CpPetR3"):
                    # Extract the 'Val' attribute from each element
                    grp_product = cp_pet_r3.find("Grp_Product").get("Val")
                    pct_product = cp_pet_r3.find("Pct_Product").get("Val")
                    pct_pet_sex = cp_pet_r3.find("Pct_Pet_Sex").get("Val")

                    # Create a ProcessedData object and put it in the queue
                    processed_row = ProcessedData(grp_product, pct_product, pct_pet_sex)
                    processed_rows.append(processed_row)

            except ET.ParseError as e:
                print(f"Error parsing XML in row: {e}")

        processed_data_queue.put(processed_rows)

    def execute(self, worker_input_pipe, idx, readiness_queue, processed_data_queue):
        readiness_queue.put(idx)
        while True:
            rows = worker_input_pipe.recv()

            if rows == "STOP":
                readiness_queue.put("STOP")
                break

            # print(f"Worker {idx} received {len(rows)} rows")
            self._process_rows(rows, processed_data_queue)
            readiness_queue.put(idx)

        worker_input_pipe.close()


class Config:
    def __init__(self, args):
        self.chunk_size = args.chunksize
        self.num_workers = args.workers
        self.num_writers = args.writers
        self.batch_size = args.batchsize


class MultiProcessOrchestrator:
    def __init__(self, config: Config):
        self.config = config
        self.db_reader = get_sql_chunks
        self.worker = XMLProcessor()  # a list of ProcessSequencer objects
        self.db_writer = write_to_sql

        self.db_to_worker_pipes = []
        self.worker_input_pipes = []
        self.worker_readiness_queue = Queue()
        self.processed_data_queues = [Queue() for _ in range(self.config.num_writers)]

    def _create_worker_processes(self):
        processes = []
        for i in range(self.config.num_workers):
            input_end, output_end = Pipe()
            self.db_to_worker_pipes.append(input_end)
            self.worker_input_pipes.append(output_end)

            # Assign each worker process to a data queue in a round-robin fashion
            assigned_queue = self.processed_data_queues[i % self.config.num_writers]
            processes.append(
                Process(
                    target=self.worker.execute,
                    args=(
                        output_end,
                        i,
                        self.worker_readiness_queue,
                        assigned_queue,
                    ),
                )
            )
        return processes

    def _start_writer_process(self):
        writer_processes = []
        for queue in self.processed_data_queues:
            writer_process = Process(
                target=self.db_writer, args=(queue, self.config.batch_size)
            )
            writer_process.start()
            writer_processes.append(writer_process)
        return writer_processes

    def _start_reader_process(self):
        reader_process = Process(
            target=self.db_reader,
            args=(
                self.db_to_worker_pipes,
                self.worker_readiness_queue,
                self.config.chunk_size,
            ),
        )
        print("Starting reader process")
        reader_process.start()
        return reader_process

    def _start_workers(self, workers):
        for worker in workers:
            worker.start()

    def _wait_for_reader(self, reader):
        reader.join()
        print("Reader process finished")

    def _close_reader_to_worker_pipes(self):
        for conn in self.db_to_worker_pipes:
            conn.close()

    def _wait_for_workers(self, workers):
        for worker in workers:
            worker.join()
        print("Worker processes finished")

    def _send_stop_signal_to_writer_and_wait(self, writers):
        for queue in self.processed_data_queues:
            queue.put("STOP")

        for writer in writers:
            writer.join()

    def execute(self):
        print("Starting pipeline")
        processes = self._create_worker_processes()
        db_writers = self._start_writer_process()
        db_reader = self._start_reader_process()
        self._start_workers(processes)
        self._wait_for_reader(db_reader)
        self._close_reader_to_worker_pipes()
        self._wait_for_workers(processes)
        self._send_stop_signal_to_writer_and_wait(db_writers)
        print("Pipeline finished")


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
        "--workers", type=int, default=8, help="Number of worker processes"
    )  # noqa E501
    parser.add_argument(
        "--writers", type=int, default=4, help="Number of writer processes"
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
