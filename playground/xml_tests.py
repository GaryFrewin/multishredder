import timeit
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
r3_example_1 = """
<CpPetProduct>
    <CpPetR3>
        <Dat_ProductName Val="TL01500" />
        <Dat_CoInsurance Val="0" />
        <Amt_Seed Val="1" />
        <Cvr_Seed Val="Time Limited - 1.5k" />
    </CpPetR3>
    <CpPetR3>
        <Dat_ProductName Val="MB02000" />
        <Amt_Seed Val="1" />
        <Cvr_Seed Val="Max Benefit - 2k" />
    </CpPetR3>
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


xml1_product_node_tag = "r1"
xml2_product_node_tag = None


class ParserStrategy:
    def __init__(self, product_node_tag):
        self.product_node_tag = product_node_tag

    def parse(self, row):
        pass


class RequestParserStrategy(ParserStrategy):
    pass


class ResponseParserStrategy(ParserStrategy):
    pass


class Quotev3ParserStrategy(ParserStrategy):
    pass


def parse_row(row):
    request_rows = RequestParserStrategy(row.action).parse(row)

    response_policy_output_rows = ResponseParserStrategy(row.action).parse(row)
    response_pet_output_rows = ResponseParserStrategy(row.action).parse(row)
    r1_output_rows = Quotev3ParserStrategy(row.action).parse(row)
    r2_output_rows = Quotev3ParserStrategy(row.action).parse(row)
    r3_output_rows = Quotev3ParserStrategy(row.action).parse(row)


class DummyProduct:
    def __init__(self, field1=None, field2=None):
        self.field1 = field1
        self.field2 = field2

    def __repr__(self):
        return f"Product(field1={self.field1}, field2={self.field2})"


def parse_xml(xml):
    root = ET.fromstring(xml)
    print(root)
    for element in root:
        print(element.tag, element.text)


# Function to process XML and create DummyProduct instances using xml.etree.ElementTree
def process_with_etree(xml_string, product_node_tag):
    root = ET.fromstring(xml_string)
    products = []

    if product_node_tag:
        for product_node in root.findall(product_node_tag):
            field1 = (
                product_node.find("field1").text
                if product_node.find("field1") is not None
                else None
            )
            field2 = (
                product_node.find("field2").text
                if product_node.find("field2") is not None
                else None
            )
            products.append(DummyProduct(field1, field2))
    else:
        field1 = root.find("field1").text if root.find("field1") is not None else None
        field2 = root.find("field2").text if root.find("field2") is not None else None
        products.append(DummyProduct(field1, field2))

    return products


# Function to process XML and create DummyProduct instances using lxml
def process_with_lxml(xml_string, product_node_tag):
    root = etree.fromstring(xml_string)
    products = []

    if product_node_tag:
        for product_node in root.findall(product_node_tag):
            field1 = (
                product_node.find("field1").text
                if product_node.find("field1") is not None
                else None
            )
            field2 = (
                product_node.find("field2").text
                if product_node.find("field2") is not None
                else None
            )
            products.append(DummyProduct(field1, field2))
    else:
        field1 = root.find("field1").text if root.find("field1") is not None else None
        field2 = root.find("field2").text if root.find("field2") is not None else None
        products.append(DummyProduct(field1, field2))

    return products


# Time processing with xml.etree.ElementTree
etree_time_xml1 = timeit.timeit(
    lambda: process_with_etree(xml1, xml1_product_node_tag), number=1000
)
etree_time_xml2 = timeit.timeit(
    lambda: process_with_etree(xml2, xml2_product_node_tag), number=1000
)

# Time processing with lxml
lxml_time_xml1 = timeit.timeit(
    lambda: process_with_lxml(xml1, xml1_product_node_tag), number=1000
)
lxml_time_xml2 = timeit.timeit(
    lambda: process_with_lxml(xml2, xml2_product_node_tag), number=1000
)

print(f"xml.etree.ElementTree processing time for xml1: {etree_time_xml1}")
print(f"xml.etree.ElementTree processing time for xml2: {etree_time_xml2}")
print(f"lxml processing time for xml1: {lxml_time_xml1}")
print(f"lxml processing time for xml2: {lxml_time_xml2}")
