import datetime

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

import unittest

from pyspark_test import assert_pyspark_df_equal

from hudi_pyspark_bash_exp.literal_col_maker import literal_col_maker

spark_session = SparkSession.builder.getOrCreate()

class TestAdd(unittest.TestCase):
    def test_01(self):

        expected_df = spark_session.createDataFrame(
            data = [('h',), ('h',), ('h',), ('h',), ('h',),],
            schema=StructType(
                [
                    StructField('col_a', StringType(), True),
                ]
            ),
        )
        expected_df.show()

        actual_df = literal_col_maker('col_a', 5, 'h', col_type=StringType())
        actual_df.show()

        assert_pyspark_df_equal(expected_df, actual_df)


if __name__ == '__main__':
    unittest.main()