from collections import defaultdict
from dataclasses import dataclass, make_dataclass
from typing import Any

from src.domain.shredding_strategies import R2andR3Strategy, ShreddingStrategy


@dataclass(frozen=True)
class MetaData:
    object_id: int
    xml_src_column: str
    xml_src_node: str
    target_table: str
    action: str
    root_mapping: dict
    xml_mapping: dict
    shredding_strategy: ShreddingStrategy


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
            xml_src_column = metadata.SrcColumn
            action = metadata.SrcMethodOrAction
            src_node = metadata.SrcNode
            target_table = metadata.TgtTable
            root_map = self._create_root_map(self.metadata_by_obj_id[obj_id])
            xml_map = self._create_xml_map(self.metadata_by_obj_id[obj_id])

            # Create an instance of MetaData and append to all_objects
            metadata_instance = MetaData(
                object_id,
                xml_src_column,
                src_node,
                target_table,
                action,
                root_map,
                xml_map,
                shredding_strategy=self._get_shredding_strategy(),
            )

            all_objects.append(metadata_instance)

        return all_objects

    def _get_shredding_strategy(self):
        return R2andR3Strategy

    def _create_root_map(self, metadata):
        mapping = defaultdict(dict)
        for row in metadata:
            if row.DataLevel == "root":
                mapping[row.SourceFieldName] = (row.OutputFieldName, row.DataType)
        return mapping

    def _create_xml_map(self, metadata):
        mapping = defaultdict(dict)
        for row in metadata:
            if row.DataLevel == "xml":
                mapping[row.SourceFieldName] = (row.OutputFieldName, row.DataType)
        return mapping
