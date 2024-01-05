from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class ProcessedData:
    # Dynamic fields will be added based on the metadata mapping
    dynamic_attrs: Dict[str, Any] = field(default_factory=dict)

    def add_attribute(self, name, value):
        self.dynamic_attrs[name] = value

    def generate_insert_sql(self, table_name):
        column_names = ", ".join(self.dynamic_attrs.keys())
        placeholders = ", ".join(["?"] * len(self.dynamic_attrs))
        sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        values = list(self.dynamic_attrs.values())
        return sql, values
