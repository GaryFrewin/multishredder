from collections import defaultdict
from dataclasses import dataclass, make_dataclass
from typing import Any


@dataclass(frozen=True)
class MetaData:
    object_id: int
    xml_src_node: str
    target_table: str
    action: str
    root_mapping: dict
    xml_mapping: dict


class MetaDataBuilder:
    def __init__(self, meta_data):
        self.meta_data = meta_data
        self.metadata_by_obj_id = defaultdict(list)
        for row in meta_data:
            ## separate metadata into different obj_id
            self.metadata_by_obj_id[row.UnstructuredSourcesId].append(row)

    def build(self):
        all_objects = []

        for obj_id in self.metadata_by_obj_id:
            metadata = self.metadata_by_obj_id[obj_id][0]

            object_id = metadata.UnstructuredSourcesId
            action = metadata.SrcMethodOrAction
            src_node = metadata.SrcNode
            target_table = metadata.TgtTable
            root_map = self._create_root_map(self.metadata_by_obj_id[obj_id])
            xml_map = self._create_xml_map(self.metadata_by_obj_id[obj_id])

            # Create an instance of MetaData and append to all_objects
            metadata_instance = MetaData(
                object_id, src_node, target_table, action, root_map, xml_map
            )
            all_objects.append(metadata_instance)

        return all_objects

    def _create_root_map(self, metadata):
        mapping = defaultdict(lambda: defaultdict(dict))
        for row in metadata:
            if row.DataLevel == "root":
                mapping[row.SourceFieldName] = (row.OutputFieldName, row.DataType)
        return mapping

    def _create_xml_map(self, metadata):
        mapping = defaultdict(lambda: defaultdict(dict))
        for row in metadata:
            if row.DataLevel == "xml":
                mapping[row.SourceFieldName] = (row.OutputFieldName, row.DataType)
        return mapping
