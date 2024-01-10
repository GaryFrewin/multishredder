from collections import namedtuple
from lxml import etree
import timeit

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


class PetR3:
    mapping = {
        "CpPetR3": {
            "Dat_ProductName": "Dat_ProductName_O",
            "Dat_CoInsurance": "Dat_CoInsurance_O",
            "Amt_Seed": "Amt_Seed_O",
            "Dat_ProductName1": "Dat_ProductName_O1",
            "Dat_CoInsurance1": "Dat_CoInsurance_O1",
            "Amt_Seed1": "Amt_Seed_O1",
        }
    }

    def __init__(
        self,
        Dat_ProductName_O=None,
        Dat_CoInsurance_O=None,
        Amt_Seed_O=None,
        Dat_ProductName_O1=None,
        Dat_CoInsurance_O1=None,
        Amt_Seed_O1=None,
    ):
        self.Dat_ProductName_O = Dat_ProductName_O
        self.Dat_CoInsurance_O = Dat_CoInsurance_O
        self.Amt_Seed_O = Amt_Seed_O
        self.Dat_ProductName_O1 = Dat_ProductName_O1
        self.Dat_CoInsurance_O1 = Dat_CoInsurance_O1
        self.Amt_Seed_O1 = Amt_Seed_O1

    @classmethod
    def from_xml(cls, xml):
        args = {}
        for child in xml:
            target_field = cls.mapping[xml.tag].get(child.tag)
            if target_field:  # Only process if the field is in the mapping
                args[target_field] = child.get("Val", None)
        return cls(**args)


# fmt: off
MockSourceTableRow = namedtuple("MockRow", "id action request response r1 r2 r3")
mock_quote_row = MockSourceTableRow(1,"quote",None,None,get_rx_example(r1_tag),get_rx_example(r2_tag),get_rx_example(r3_tag))
mock_buy_row = MockSourceTableRow(1,"buy",None,None,get_rx_example(r1_tag),get_rx_example(r2_tag),get_rx_example(r3_tag))
# fmt: on


def parse_pet_r3(xml_string):
    root = etree.fromstring(xml_string)
    pet_r3_objects = []

    for xml_node in root.findall(".//CpPetR3"):
        pet_r3_obj = PetR3.from_xml(xml_node)
        pet_r3_objects.append(pet_r3_obj)

    return pet_r3_objects


# Sample XML data
xml_data = get_rx_example(r3_tag)

# Timing the parsing
time_taken = timeit.timeit(lambda: parse_pet_r3(xml_data), number=100000)
print(f"Time taken to parse 100000 times: {time_taken} seconds")
