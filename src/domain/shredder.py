from lxml import etree
from src.domain.processed_data import ProcessedData
from src.services.data_spec_builder import MetaData
from pyodbc import Row


class XMLProcessor:
    def __init__(self, data_spec_classes: list[MetaData]):
        self.data_spec_classes = data_spec_classes

    def execute(self, worker_input_pipe, idx, readiness_queue, processed_data_queue):
        readiness_queue.put(idx)
        while True:
            rows = worker_input_pipe.recv()

            if rows == "STOP":
                readiness_queue.put("STOP")
                break

            self._process_rows(rows, processed_data_queue)
            readiness_queue.put(idx)

        worker_input_pipe.close()

    def _get_classes_for_row(self, row: Row):
        if row.action in ["quote", "retrieve"]:
            return [
                cls
                for cls in self.data_spec_classes
                if cls.xml_src_column in ["r1", "r2", "r3"]
            ]
        else:
            return []

    def _get_shredding_strategy(self, spec):
        pass

    def _process_rows(self, rows, processed_data_queue):
        processed_rows = []
        for row in rows:
            classes_for_row = self._get_classes_for_row(row)
            for spec in classes_for_row:
                processed_data_list = self._process_row(row, spec)
                if processed_data_list:
                    for processed_data in processed_data_list:
                        processed_rows.append(processed_data)
        processed_data_queue.put(processed_rows)

    def _process_row(self, row: Row, spec: MetaData):
        processed_data = ProcessedData()
        # add root data
        for source_field, target_spec in spec.root_mapping.items():
            try:
                target_field, data_type = target_spec
                value = getattr(row, source_field)
                processed_data.add_attribute(target_field, value)
            except AttributeError as e:
                print(f"Attribute {source_field} not found in row: {e}")
                pass

        # Extract XML data from the row
        column = spec.xml_src_column
        try:
            xml_data = getattr(row, column)
            if xml_data is None:
                return None
            processed_data_list = spec.shredding_strategy.shred(
                spec, xml_data, processed_data
            )
        except AttributeError as e:
            print(f"XML column {column} not found in row: {e}")

        return processed_data_list
