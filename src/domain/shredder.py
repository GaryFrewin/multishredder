from domain.processed_data import ProcessedData
import xml.etree.ElementTree as ET


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
