from collections import namedtuple
from unittest.mock import Mock
import pytest
from src.domain.shredder import XMLProcessor
from src.domain.processed_data import ProcessedData
from src.services.data_spec_builder import MetaData, ShreddingStrategy
from pyodbc import Row


@pytest.fixture
def mock_shredding_strategy():
    class MockShreddingStrategy(ShreddingStrategy):
        @classmethod
        def shred(cls, spec: MetaData, xml_data, processed_data: ProcessedData):
            return_list = []
            for i in range(3):
                clone = processed_data.clone()
                clone.add_attribute("target_field", f"target_value")
                return_list.append(clone)
            return return_list

    return MockShreddingStrategy


@pytest.fixture
def mock_r3_metadata(mock_shredding_strategy):
    # Assuming MetaData needs these arguments, adjust as necessary.
    return MetaData(
        object_id=1,
        xml_src_column="r3",
        xml_src_node="node",
        target_table="table",
        action="action",
        root_mapping={
            "source_column": ("target_column", "data_type"),
        },
        xml_mapping={
            "source_field": ("target_field", "data_type"),
        },
        shredding_strategy=mock_shredding_strategy,
    )


@pytest.fixture
def mock_row():
    MockRow = namedtuple("MockRow", ("action quote source_column r1 r2 r3"))
    return MockRow(
        action="quote", quote="quote", source_column="1", r1="r1", r2="r2", r3="r3"
    )


@pytest.fixture
def xml_processor(mock_r3_metadata):
    return XMLProcessor(data_spec_classes=[mock_r3_metadata])


def test_init(xml_processor, mock_r3_metadata):
    assert xml_processor.data_spec_classes == [mock_r3_metadata]


def test_process_row_returns_expected_number_of_processed_data(
    xml_processor, mock_r3_metadata, mock_row
):
    expected_dynamic_attrs = {"target_column": "1", "target_field": "target_value"}

    result = xml_processor._process_row(mock_row, mock_r3_metadata)

    assert len(result) == 3
    for processed_data in result:
        assert isinstance(processed_data, ProcessedData)
        assert processed_data.dynamic_attrs == expected_dynamic_attrs
