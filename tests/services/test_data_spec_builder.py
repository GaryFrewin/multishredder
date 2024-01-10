import pytest

from src.services.data_spec_builder import MetaDataBuilder

db_select_string = """
select 'UnstructuredSources -->>'					as Header1

		, us.[UnstructuredSourcesId]				as [UnstructuredSourcesId]			 

		, us.SrcServer								as SrcServer						

		, us.SrcDatabase							as SrcDatabase					

		, us.SrcSchema								as SrcSchema								

		, us.SrcTable								as SrcTable								

		, us.SrcColumn								as SrcColumn							

		, us.SrcCodeType							as SrcCodeType							

		, us.SrcMethodOrAction						as SrcMethodOrAction					

		, us.SrcNode								as SrcNode						

		, us.TgtServer								as TgtServer						

		, us.TgtDatabase							as TgtDatabase					

		, us.TgtSchema								as TgtSchema					

		, us.TgtTable								as TgtTable					

		, 'UnstructuredTables -->>'					as Header2

		, ut.[UnstructuredTablesId]					as [UnstructuredTablesId]	

		, ut.[UnstructuredSourcesId]				as [UnstructuredSourcesId_FK]

		, ut.[SourceFieldName]						as [SourceFieldName]		

		, ut.[OutputFieldName]						as [OutputFieldName]		

		, ut.[DataLevel]							as [DataLevel]			

		, ut.[DataNode1]							as [DataNode1]			

		, ut.[DataNode2]							as [DataNode2]			

		, ut.[DataType]								as [DataType]				

		, ut.[SqlTransformation]					as [SqlTransformation]	

		, 'DerivedColumns -->>'						as Header3


		,COUNT(1) OVER (PARTITION BY us.[UnstructuredSourcesId] ) TotalFieldsInTable

		,COUNT(SourceFieldName) OVER (PARTITION BY us.[UnstructuredSourcesId] ORDER BY ut.UnstructuredTablesId) FieldRowNumberInTable

from UnstructuredSources us
 
left join (

	select [UnstructuredTablesId], [UnstructuredSourcesId], [SourceFieldName], [OutputFieldName], [DataLevel], [DataNode1], [DataNode2], [DataType], [SqlTransformation] 

	from UnstructuredTables ut

	) ut on ut.UnstructuredSourcesId = us.UnstructuredSourcesId;

"""


def test_builder_returns_expected_types(db_connection):
    conn = db_connection
    cursor = conn.cursor()

    cursor.execute(db_select_string)

    data = cursor.fetchall()

    metadata_class_builder = MetaDataBuilder(data)

    metadata_classes = metadata_class_builder.build()

    assert len(metadata_classes) == 7
