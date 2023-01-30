from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import column
import pyspark.sql as sql
import os

def main(spark: SparkSession):

    loc = f"{os.getcwd()}/.."

    input_table_names = ['user_table', 'user_chocolate_table', 'chocolate_table']


    input_dfs: List[DataFrame] = [
            spark.read.format("csv").options(header=True, inferSchema= True).load(f"{loc}/s3_mock_data/{table_name}/load.csv")
            for table_name in input_table_names
        ]

    # For user_table
    input_dfs[0] = input_dfs[0].withColumnRenamed('id', 'user_id')

    # For user_choc_table
    input_dfs[1] = input_dfs[1].drop('id')

    # For chocolate_table
    input_dfs[2] = input_dfs[2].withColumnRenamed('id', 'chocolate_id')

    for df in input_dfs:
        df.show()
        print(df.schema)

    join_on: List[str] = ['', 'user_id', 'chocolate_id' ]

    output_df: DataFrame = input_dfs[0]


    for ind in range(1, len(input_dfs)):
        output_df = output_df.join(other=input_dfs[ind], on=join_on[ind], how='left')



    output_table_name = 'user_report'

    options = {
        'hoodie.table.name': output_table_name,
        'hoodie.datasource.write.recordkey.field': 'user_id',
        'hoodie.datasource.write.table.name': output_table_name,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'user_id',
        'hoodie.datasource.write.partitionpath.field': 'country',
        'hoodie.cleaner.policy' : 'KEEP_LATEST_COMMITS',
        'hoodie.cleaner.commits.retained':2,
        }

    output_df = output_df.withColumnRenamed('company','chocolate_company')\
        .withColumnRenamed('name', 'chocolcate_name')\
        .sort('user_id')

    output_df.show()

    output_df.write.format("hudi").options(**options).mode("append").save(f"{loc}/hudi_data/{output_table_name}")

    on_disk_df = spark.read.format("hudi").load(f"{loc}/hudi_data/{output_table_name}")

    for col in on_disk_df.columns:
        if col[0] == '_':
            on_disk_df = on_disk_df.drop(col)

    on_disk_df\
        .select(['user_id', 'first_name', 'last_name', 'chocolcate_name', 'chocolate_company'])\
        .sort('user_id').show()


if __name__ == "__main__":
    # main(SparkSession.builder.getOrCreate())
    spark = SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer').enableHiveSupport().getOrCreate()
    main(spark)

