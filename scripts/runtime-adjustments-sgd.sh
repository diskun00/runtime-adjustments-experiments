#!/usr/bin/env bash

HADOOP_HOME=${HADOOP_HOME:-/home/ilya/hadoop-2.7.3}
SPARK_HOME=${SPARK_HOME:-/home/ilya/spark-2.1.0-bin-hadoop2.7}

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

#$HADOOP_HOME/bin/hadoop fs -copyFromLocal /data/ilya/datasets/sgd.txt /

for (( i = 0; i < 50; i++ )); do

    echo "Executing run $i..."

    RUN_NAME=run$(printf %02d $i)

    $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --executor-cores 8 \
    --properties-file conf/spark-defaults.conf \
    --class de.tuberlin.dos.jobs.SGD \
    ../target/runtime-adjustments-experiments-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --min-containers 4 \
    --max-containers 50 \
    --max-runtime 300000 \
    --iterations 20 \
    --adaptive \
    --db "tcp://130.149.249.30:9092/~/bell" \
    hdfs://wally020:45010//sgd.txt \
    > logs/${RUN_NAME}.out 2> logs/${RUN_NAME}.log

done
