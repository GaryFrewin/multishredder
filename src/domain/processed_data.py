from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Dict

from src.services.data_spec_builder import MetaData


@dataclass
class ProcessedData:
    spec: MetaData
    dynamic_attrs: Dict[str, Any] = field(default_factory=dict)
    # Dynamic fields will be added based on the metadata mapping
    

    def add_attribute(self, name, value):
        self.dynamic_attrs[name] = value

    def generate_insert_sql(self):
        TABLE_NAME = self.spec.target_table

        column_names = ", ".join(self.dynamic_attrs.keys())
        placeholders = ", ".join(["?"] * len(self.dynamic_attrs))
        values = list(self.dynamic_attrs.values())
        # INSERT INTO ProcessedData (Grp_Product, Pct) SELECT ?, ?
        sql = f"INSERT INTO {TABLE_NAME} ({column_names}) VALUES ({placeholders})"
        return sql, values

    def clone(self):
        # Deepcopy the spec and dynamic_attrs
        spec_copy = deepcopy(self.spec)
        dynamic_attrs_copy = deepcopy(self.dynamic_attrs)
        # Create a new instance of ProcessedData with copied attributes
        return ProcessedData(spec=spec_copy, dynamic_attrs=dynamic_attrs_copy)
