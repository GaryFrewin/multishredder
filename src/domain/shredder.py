import xml.etree.ElementTree as ET

from src.domain.processed_data import ProcessedData


class XMLProcessor:
    def __init__(self, data_spec):
        self.data_spec = data_spec

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

    def _process_rows(self, rows, processed_data_queue):
        processed_rows = []
        for row in rows:
            processed_data = self._process_row(row)
            processed_rows.append(processed_data)
        processed_data_queue.put(processed_rows)

    def _process_row(self, row):
        processed_data = ProcessedData()
        for meta in self.data_spec:
            if meta.field_type == "root":
                processed_data.add_attribute(
                    meta.target_field, getattr(row, meta.source_field)
                )
            elif meta.field_type == "xml":
                xml_data = getattr(row, meta.source_field)
                extracted_data = self._process_xml(xml_data, self.data_spec)
                for key, value in extracted_data.items():
                    processed_data.add_attribute(key, value)
        return processed_data

    def _process_xml(self, xml_data, data_spec):
        root = ET.fromstring(xml_data)
        extracted_data = {}
        for element in root:
            for meta in data_spec:
                if meta.source_field == element.tag and meta.field_type == "xml":
                    extracted_data[meta.target_field] = element.text
        return extracted_data
