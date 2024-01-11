from abc import ABC, abstractmethod
from lxml import etree


class ShreddingStrategy(ABC):
    @abstractmethod
    def shred(self, spec, xml_data):
        pass


class R3Strategy(ShreddingStrategy):
    @classmethod
    def shred(cls, spec, xml_data, processed_data):
        processed_data_list = []
        # add xml data
        try:
            root = etree.fromstring(xml_data)
            for element in root.iter():
                # Check if the element's tag matches spec.src_node

                if element.tag == spec.xml_src_node:
                    # clone/deepcopy the processed_data for ea
                    processed_data_clone = processed_data.clone()
                    # Iterate over the xml_mapping specifications
                    for child in element:
                        for source_field, target_spec in spec.xml_mapping.items():
                            target_field, data_type = target_spec
                            if source_field == child.tag:
                                value = child.get("Val")
                                processed_data_clone.add_attribute(target_field, value)
                    processed_data_list.append(processed_data_clone)
        except AttributeError as e:
            print(f"Error processing XML data: {e}")
            pass

        return processed_data_list
