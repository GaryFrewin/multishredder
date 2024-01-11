from multiprocessing import Pipe, Process, Queue
from src.domain.config import Config
from src.domain.reader import get_sql_chunks
from src.domain.shredder import XMLProcessor
from src.domain.writer import write_to_sql
from src.services.data_spec_builder import MetaData


class MultiProcessOrchestrator:
    def __init__(self, config: Config, classes: list[MetaData]):
        self.config = config
        self.db_reader = get_sql_chunks
        self.worker = XMLProcessor(classes)  # a list of ProcessSequencer objects
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
