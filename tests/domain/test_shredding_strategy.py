import pytest
from src.domain.processed_data import ProcessedData

from src.domain.shredding_strategies import R3Strategy
from src.services.data_spec_builder import MetaData


def test_r3_shredder(r3_quote_xml):
    spec = MetaData(
        object_id=1,
        xml_src_column="r3",
        xml_src_node="CpPetR3",
        target_table="r3_quote",
        action="quote",
        root_mapping={},
        xml_mapping={"Dat_ProductName": ("My_new_field_name", "str")},
        shredding_strategy=R3Strategy,
    )
    processed_data = ProcessedData()

    processed_data_list = spec.shredding_strategy.shred(
        spec, r3_quote_xml, processed_data
    )

    assert len(processed_data_list) == 6
