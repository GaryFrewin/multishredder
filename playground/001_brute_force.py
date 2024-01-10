from collections import namedtuple
from dataclasses import dataclass, field, fields, make_dataclass
import timeit
from typing import Any, Dict
import xml.etree.ElementTree as ET
from lxml import etree


# rule 1: 5 column contains xml
# request, response, r1, r2, r3
# request, response, r1 will likely product 1 row EACH
# r2 and r3 will likely produce multiple rows EACH

# rule 2: action and brand fields
# action field can have values 'quote', 'buy', 'retreive', 'validate'
# brand field can have values such as range(1,5)
# action/brand determines the expected structure of the xml in request and response

# r3_example_1
# In this example, the action is 'quote' and the brand is 3

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
            </{node_tag}>
            <{node_tag}>
                <Dat_ProductName Val="MB02000" />
                <Amt_Seed Val="1" />
                <Cvr_Seed Val="Max Benefit - 2k" />
            </{node_tag}>
        </CpPetProduct>
        """


# column_source_node_2 contains the node to row map...


# r3_example_2
# In thisconclusion
# r3 doesn't change much based on actions and brand
# we're always in creating one row per CpPetR3 object...
# and the filed mapping will come from the data_spec in sql.
# r2 will be the same. Except we will be looking for CpPetR2 instead of CpPetR3
# r1 will be the same. Except we will be looking for CpPetR1 instead of CpPetR3

# request xml goes into 4 separate tables with very different columns
# ReqPetInfoV3
# ReqCustomerInfoV3
# ReqBuy
# ReqRetrievalV3
# request can be populate for quote, buy, and retrieve
# request only seems to be populated for specific brands...
# example 1: action = quote and brand in 8, 3, 2, 1, 5
# this should produce:

request_example_1 = """
    <Quotation>
        <Record>
            <Brand>PDSA</Brand>
            <PolicyStartDate>2024-01-10</PolicyStartDate>
            <PolicyHolder>
                <Title>MRS</Title>
                <Forename>Amanda</Forename>
                <Surname>Molloy</Surname>
            </PolicyHolder>
            <Pet>
                <Name>Winston</Name>
                <DateOfBirth>2020-11-01</DateOfBirth>
                <PurchasePrice>1000</PurchasePrice>
            </Pet>
            <Pet>
                <Name>Rocky</Name>
                <DateOfBirth>2020-11-01</DateOfBirth>
                <PurchasePrice>100</PurchasePrice>
            </Pet>
        </Record>
    </Quotation>
"""

# example 2 action = buy and all brands
# this example contains a lot of redundant data
# that is nested??...
# we're interested in:
# Qid	xml	Policy
# PQid	xml	Pet
# Product	xml	Pet
# PolicyNumber	xml	Pet
# BreedId_ParentA	xml	Pet
# BreedId_ParentB	xml	Pet

# 1 rpw per pet in output table

request_example_2 = """
    <?xml version="1.0" encoding="UTF-8"?> 
    <Buy>
        <Policy>
            <Bid>CD1733871886D3A1</Bid>
            <Qid>62491970015F852F</Qid>
            <Sid/>
            <Src>6</Src>
            <Token>BCCDE596AC</Token>
            <Brand>1</Brand>
            <Agent>5028</Agent>
        </Policy>
        <Pet>
            <PQid>9266351A57AE2600</PQid>
            <Pid>4</Pid>
            <PetID/>
            <Breed_NotExcluded>false</Breed_NotExcluded>
            <R0>
                <PolicyNumber>PETSB1072795W</PolicyNumber>
                <DeclineResult/>
                <DeclineDesc/>
            </R0>
            <R2>
                <PolicyNumber>PETSB1072795W</PolicyNumber>
            </R2>
        </Pet>
        <Pet>
            <PQid>9266351A57AE2600</PQid>
            <Pid>4</Pid>
            <PetID/>
            <Breed_NotExcluded>false</Breed_NotExcluded>
            <R0>
                <PolicyNumber>PETSB1072795W</PolicyNumber>
                <DeclineResult/>
                <DeclineDesc/>
            </R0>
            <R2>
                <PolicyNumber>PETSB1072795W</PolicyNumber>
            </R2>
        </Pet>
    </Buy>
"""
# fmt: off

MockSourceTableRow = namedtuple("MockRow", "id action request response r1 r2 r3")
mock_quote_row = MockSourceTableRow(1,"quote",request_example_1,None,get_rx_example(r1_tag),get_rx_example(r2_tag),get_rx_example(r3_tag))
mock_buy_row = MockSourceTableRow(1,"buy",request_example_2,None,get_rx_example(r1_tag),get_rx_example(r2_tag),get_rx_example(r3_tag))

MockMetaDataRow = namedtuple("MockRow", "source_field target_field root_or_xml xml_row_node target_table")
mock_meta_data = [
    MockMetaDataRow("id", "qid", "root", None, "r3_table"),
    MockMetaDataRow("Dat_ProductName", "Dat_ProductName_O", "xml", "CpPetR3", "r3_table"),
    MockMetaDataRow("Dat_CoInsurance", "Dat_CoInsurance_O", "xml", "CpPetR3", "r3_table"),
    MockMetaDataRow("Amt_Seed", "Amt_Seed_O", "xml", "CpPetR3", "r3_table")
] 

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



class ParserStrategy:
    def __init__(self, data_spec):
        self.data_spec = data_spec

    def parse(self, row):
        # This method should be implemented in subclasses
        raise NotImplementedError





class Quotev3ParserStrategy(ParserStrategy):
    def parse(self, row):
        xml_data = row.r3
        root = ET.fromstring(xml_data)
        processed_data_objects = []

        xml_row_nodes = {meta.xml_row_node for meta in self.data_spec if meta.root_or_xml == 'xml'}

        for node_tag in xml_row_nodes:
            for node in root.findall(f".//{node_tag}"):
                processed_data = ProcessedData()
                # Initialize all fields from metadata with None
                for meta in self.data_spec:
                    if meta.root_or_xml == 'xml':
                        processed_data.add_attribute(meta.target_field, None)
                # Add qid as a static field
                processed_data.add_attribute('qid', row.id)

                for meta in self.data_spec:
                    if meta.root_or_xml == 'xml' and meta.xml_row_node == node_tag:
                        subelement = node.find(meta.source_field)
                        if subelement is not None:
                            processed_data.add_attribute(meta.target_field, subelement.get('Val'))

                # Only append if there are dynamic attributes added
                if processed_data.dynamic_attrs:
                    processed_data_objects.append(processed_data)

        return processed_data_objects

# parse_xml(r3_example_1, "CpPetR3")


strategy = Quotev3ParserStrategy(mock_meta_data)

time = timeit.timeit(lambda: strategy.parse(mock_quote_row), number=100000)
print(f"Time taken: {time}")


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
