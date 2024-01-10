from lxml import etree
from domain.processed_data import ProcessedData
from services.data_spec_builder import MetaData


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

    def _get_classes_for_row(self, row):
        if row.action == "quote":
            return [
                cls
                for cls in self.data_spec_classes
                if cls.xml_src_column in ["request", "response", "r1", "r2", "r3"]
            ]
        else:
            return []

    def _process_rows(self, rows, processed_data_queue):
        processed_rows = []
        for row in rows:
            classes_for_row = self._get_classes_for_row(row)
            for spec in classes_for_row:
                processed_data = self._process_row(row, spec)
                processed_rows.append(processed_data)
        processed_data_queue.put(processed_rows)

    def _process_row(self, row, spec: MetaData):
        processed_data = ProcessedData()
        # add root data
        for source_field, target_spec in spec.root_mapping.items():
            try:
                target_field, data_type = target_spec
                value = getattr(row, source_field.lower())
                processed_data.add_attribute(target_field, value)
            except AttributeError:
                # print(f"Attribute {source_field} not found in row")
                pass

        # add xml data
        column = spec.xml_src_column
        try:
            xml_data = getattr(row, column)
            root = etree.fromstring(xml_data)
            extracted_data = {}
            for element in root.iter():
                # Check if the element's tag matches spec.src_node
                if element.tag == spec.xml_src_node:
                    # Iterate over the xml_mapping specifications
                    for child in element:
                        for source_field, target_spec in spec.xml_mapping.items():
                            target_field, data_type = target_spec
                            if source_field == child.tag:
                                value = child.get("Val")
                                processed_data.add_attribute(target_field, value)
        except AttributeError:
            # print(f"XML column {column} not found in row")
            pass

        return processed_data
