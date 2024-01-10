from collections import defaultdict, namedtuple
from dataclasses import dataclass, field, fields, make_dataclass
import timeit
from typing import Any, Dict
import xml.etree.ElementTree as ET
from lxml import etree

r1_tag = "CpPetR1"
r2_tag = "CpPetR2"
r3_tag = "CpPetR3"


def get_rx_example(node_tag):
    return f"""
        <CpPetProduct>
            <{node_tag}>
                <Dat_ProductName Val="TL01500" />
                <Dat_CoInsurance Val="0" />
                <Amt_Seed Val="1" />
                <Cvr_Seed Val="Time Limited - 1.5k" />
                <Dat_ProductName1 Val="TL01500" />
                <Dat_CoInsurance1 Val="0" />
                <Amt_Seed1 Val="1" />
                <Cvr_Seed1 Val="Time Limited - 1.5k" />
            </{node_tag}>
            <{node_tag}>
                <Dat_ProductName Val="MB02000" />
                <Amt_Seed Val="1" />
                <Cvr_Seed Val="Max Benefit - 2k" />
                <Dat_ProductName1 Val="MB02000" />
                <Amt_Seed1 Val="1" />
                <Cvr_Seed1 Val="Max Benefit - 2k" />
            </{node_tag}>
        </CpPetProduct>
        """


# fmt: off

MockSourceTableRow = namedtuple("MockRow", "id action request response r1 r2 r3")
mock_quote_row = MockSourceTableRow(1,"quote",None,None,get_rx_example(r1_tag),get_rx_example(r2_tag),get_rx_example(r3_tag))
mock_buy_row = MockSourceTableRow(1,"buy",None,None,get_rx_example(r1_tag),get_rx_example(r2_tag),get_rx_example(r3_tag))

MockMetaDataRow = namedtuple("MockRow", "obj_id source_field target_field root_or_xml xml_row_node target_table")
mock_meta_data = [
    MockMetaDataRow("1", "id", "qid", "root", None, "r3_table"),
    MockMetaDataRow("1", "Dat_ProductName", "Dat_ProductName_O", "xml", "CpPetR3", "r3_table"),
    MockMetaDataRow("1", "Dat_CoInsurance", "Dat_CoInsurance_O", "xml", "CpPetR3", "r3_table"),
    MockMetaDataRow("1", "Amt_Seed", "Amt_Seed_O", "xml", "CpPetR3", "r3_table"),
    MockMetaDataRow("1", "Dat_ProductName1", "Dat_ProductName_O1", "xml", "CpPetR3", "r3_table"),
    MockMetaDataRow("1", "Dat_CoInsurance1", "Dat_CoInsurance_O1", "xml", "CpPetR3", "r3_table"),
    MockMetaDataRow("1", "Amt_Seed1", "Amt_Seed_O1", "xml", "CpPetR3", "r3_table"),
    MockMetaDataRow("2", "id", "qid", "root", None, "r3_table"),
    MockMetaDataRow("2", "Dat_ProductName", "Dat_ProductName_O", "xml", "CpPetR3", "r3_table"),
    MockMetaDataRow("2", "Dat_CoInsurance", "Dat_CoInsurance_O", "xml", "CpPetR3", "r3_table"),
] 

# fmt: on


def create_class(name, fields):
    return make_dataclass(name, fields)


# Group metadata by obj_id
metadata_by_obj_id = defaultdict(list)
for meta in mock_meta_data:
    metadata_by_obj_id[meta.obj_id].append(meta)


def preprocess_metadata(metadata):
    mapping = defaultdict(lambda: defaultdict(dict))
    for meta in metadata:
        if meta.root_or_xml == "xml":
            mapping[meta.obj_id][meta.xml_row_node][
                meta.source_field
            ] = meta.target_field
    return mapping


# Example usage
metadata_mapping = preprocess_metadata(mock_meta_data)


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


def parse_xml_lxml(xml_data, obj_id, metadata_mapping):
    root = etree.fromstring(xml_data.encode())
    parsed_objects = []

    for node in root.iter():
        if node.tag in metadata_mapping[obj_id]:
            parsed_obj = ProcessedData()
            children_by_tag = {child.tag: child for child in node}

            for source_field, target_field in metadata_mapping[obj_id][
                node.tag
            ].items():
                child = children_by_tag.get(source_field)
                value = child.get("Val") if child is not None else None
                parsed_obj.add_attribute(target_field, value)

            parsed_objects.append(parsed_obj)

    return parsed_objects


# Timing the parsing
time = timeit.timeit(
    lambda: parse_xml_lxml(mock_quote_row.r3, "1", metadata_mapping), number=100000
)
print(f"Time taken with lxml: {time}")


# def parse_row(row):
#     # this line should produce an object with the appropriate atrributes
#     r3_output_rows = Quotev3ParserStrategy(row.action).parse(row)
#     ##
#     request_rows = RequestParserStrategy(row.action).parse(row)
#     response_policy_output_rows = ResponseParserStrategy(row.action).parse(row)
#     response_pet_output_rows = ResponseParserStrategy(row.action).parse(row)
#     r1_output_rows = Quotev3ParserStrategy(row.action).parse(row)
#     r2_output_rows = Quotev3ParserStrategy(row.action).parse(row)


# Time processing with xml.etree.ElementTree
# etree_time_xml1 = timeit.timeit(lambda: parse_xml(r3_example_1, "CpPetR3"), number=1000)
# etree_time_xml2 = timeit.timeit(
#     lambda: process_with_etree(xml2, xml2_product_node_tag), number=1000
# )

# # Time processing with lxml
# lxml_time_xml1 = timeit.timeit(
#     lambda: process_with_lxml(xml1, xml1_product_node_tag), number=1000
# )
# lxml_time_xml2 = timeit.timeit(
#     lambda: process_with_lxml(xml2, xml2_product_node_tag), number=1000
# )

# print(f"xml.etree.ElementTree processing time for xml1: {etree_time_xml1}")
# print(f"xml.etree.ElementTree processing time for xml2: {etree_time_xml2}")
# print(f"lxml processing time for xml1: {lxml_time_xml1}")
# print(f"lxml processing time for xml2: {lxml_time_xml2}")
