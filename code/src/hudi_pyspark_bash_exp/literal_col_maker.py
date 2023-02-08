from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame, StructField, StructType

def literal_col_maker(
    col_name: str,
    size: int,
    literal: any,
    col_type: any
) -> DataFrame:
    output = [
        (literal,) for i in range(size)
    ]

    spark = SparkSession.builder.getOrCreate()

    return spark.createDataFrame (
        data=output, 
        schema=StructType([StructField(col_name, col_type)]),
    )

    