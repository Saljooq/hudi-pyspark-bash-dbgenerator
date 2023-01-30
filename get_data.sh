#!/usr/bin/bash

if [ "$#" == "0" ]; then
    echo "you can give table name as argument too"

    spark-sql \
    --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.0 \
    --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
    --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'\
    --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog'\
    --conf "spark.hadoop.hive.cli.print.header=true"
elif [ "$1" == "clear" ] || [ "$1" == "clean" ]; then
    echo "clearing all the files"
    cd hudi_data
    rm -rf *
    exit
elif [ "$#" == "2" ] && [ "$2" == "raw" ]; then

    echo """from pyspark.sql import SparkSession
spark = SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer').enableHiveSupport().getOrCreate()
spark.read.format('hudi').load('/home/saljooq/Pictures/hudi_exp/hudi_data/$1').show()""" | \
    pyspark \
        --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.0 \
        --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
        --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'
elif [ "$#" == "2" ] && [ "$2" == "schema" ]; then

echo """from pyspark.sql import SparkSession
spark = SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer').enableHiveSupport().getOrCreate()
df = spark.read.format('hudi').load('/home/saljooq/Pictures/hudi_exp/hudi_data/$1')
print('\\n'.join(list(map( lambda a: str(a) , [*df.schema]))))""" | \
    pyspark \
        --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.0 \
        --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
        --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'
else
    echo """from pyspark.sql import SparkSession
spark = SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer').enableHiveSupport().getOrCreate()
df = spark.read.format('hudi').load('/home/saljooq/Pictures/hudi_exp/hudi_data/$1')
for col in df.columns:
    if col[0] == '_':
        df = df.drop(col)
df.show()""" | \
    pyspark \
    --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.0 \
    --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
    --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'
fi