#  Standard way of running this file is 'python main.py' or 'python3 main.py'

from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import column
import pyspark.sql as sql
import os

def main(spark: SparkSession):

    loc = f"{os.getcwd()}/../../.."

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
    
    # To put hudi bundle on path, just download from here:
    # https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark-bundle
    # and make sure it matches the name given in config

    jar_name = 'hudi-spark3.3-bundle_2.12-0.12.2.jar'

    if not os.path.exists(jar_name):

        import urllib.request
        url = f'https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.3-bundle_2.12/0.12.2/{jar_name}'
        urllib.request.urlretrieve(url, jar_name)

    spark = SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')\
    .config('spark.driver.extraClassPath', jar_name) \
    .config('spark.serializer','org.apache.spark.serializer.KryoSerializer') \
    .config('spark.sql.extensions','org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
    .enableHiveSupport().getOrCreate()
    main(spark)

def main2():
        # To put hudi bundle on path, just download from here:
    # https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark-bundle
    # and make sure it matches the name given in config

    jar_name = 'hudi-spark3.3-bundle_2.12-0.12.2.jar'

    if not os.path.exists(jar_name):

        import urllib.request
        url = f'https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.3-bundle_2.12/0.12.2/{jar_name}'
        urllib.request.urlretrieve(url, jar_name)

    spark = SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')\
    .config('spark.driver.extraClassPath', jar_name) \
    .config('spark.serializer','org.apache.spark.serializer.KryoSerializer') \
    .config('spark.sql.extensions','org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
    .enableHiveSupport().getOrCreate()
    main(spark)

