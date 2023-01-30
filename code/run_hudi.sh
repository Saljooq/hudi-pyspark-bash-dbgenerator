#!/usr/bin/bash
if [ "$#" != "1" ]; then
    pyspark \
    --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.0 \
    --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
    --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'
else
    cat "$1" | \
    pyspark \
        --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.0 \
        --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
        --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'

        # --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' &

    # PID="$!"

    # echo "$PID"
    

    # echo sleeping now...

    # sleep 10


    # cat $1 > "/proc/$PID/fd/0"

    # fg % "$PID"

fi
