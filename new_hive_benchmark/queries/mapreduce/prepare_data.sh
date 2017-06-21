#!/usr/bin/env bash

# set up the pathes
source benchmark.conf;

# Here, I assume that all the html data are put in a local directory ./data/. 
# If not, you need to copy them over.

# Also, we assume that the benchmark jars are copied to a local directory ./hadoop-jars/.
# If not, you need to copy them over.

DATA_DIR="$BASE_DIR/data"
JARS_DIR="$BASE_DIR/hadoop-jars"

# prepare data for hive and pig
$HADOOP_CMD fs -copyFromLocal "$DATA_DIR/Rankings_0.dat" /data/rankings/Rankings_0.dat
$HADOOP_CMD fs -copyFromLocal "$DATA_DIR/Rankings_1.dat" /data/rankings/Rankings_1.dat
$HADOOP_CMD fs -copyFromLocal "$DATA_DIR/Rankings_2.dat" /data/rankings/Rankings_2.dat
$HADOOP_CMD fs -copyFromLocal "$DATA_DIR/Rankings_3.dat" /data/rankings/Rankings_3.dat
$HADOOP_CMD fs -copyFromLocal "$DATA_DIR/Rankings_4.dat" /data/rankings/Rankings_4.dat
$HADOOP_CMD fs -copyFromLocal "$DATA_DIR/Rankings_5.dat" /data/rankings/Rankings_5.dat
$HADOOP_CMD fs -copyFromLocal "$DATA_DIR/Rankings_6.dat" /data/rankings/Rankings_6.dat
$HADOOP_CMD fs -copyFromLocal "$DATA_DIR/Rankings_7.dat" /data/rankings/Rankings_7.dat
$HADOOP_CMD fs -copyFromLocal "$DATA_DIR/Rankings_8.dat" /data/rankings/Rankings_8.dat
$HADOOP_CMD fs -copyFromLocal "$DATA_DIR/Rankings_9.dat" /data/rankings/Rankings_9.dat

$HADOOP_CMD fs -copyFromLocal "$DATA_DIR/UserVisits_0.dat" /data/uservisits/UserVisits_0.dat
$HADOOP_CMD fs -copyFromLocal "$DATA_DIR/UserVisits_1.dat" /data/uservisits/UserVisits_1.dat
$HADOOP_CMD fs -copyFromLocal "$DATA_DIR/UserVisits_2.dat" /data/uservisits/UserVisits_2.dat

# prepare data for hadoop
$HADOOP_CMD jar "$JARS_DIR/dataloader.jar" rankings "$DATA_DIR/Rankings_0.dat" /data/hadoop/rankings/Rankings_0.dat
$HADOOP_CMD jar "$JARS_DIR/dataloader.jar" rankings "$DATA_DIR/Rankings_1.dat" /data/hadoop/rankings/Rankings_1.dat
$HADOOP_CMD jar "$JARS_DIR/dataloader.jar" rankings "$DATA_DIR/Rankings_2.dat" /data/hadoop/rankings/Rankings_2.dat
$HADOOP_CMD jar "$JARS_DIR/dataloader.jar" rankings "$DATA_DIR/Rankings_3.dat" /data/hadoop/rankings/Rankings_3.dat
$HADOOP_CMD jar "$JARS_DIR/dataloader.jar" rankings "$DATA_DIR/Rankings_4.dat" /data/hadoop/rankings/Rankings_4.dat
$HADOOP_CMD jar "$JARS_DIR/dataloader.jar" rankings "$DATA_DIR/Rankings_5.dat" /data/hadoop/rankings/Rankings_5.dat
$HADOOP_CMD jar "$JARS_DIR/dataloader.jar" rankings "$DATA_DIR/Rankings_6.dat" /data/hadoop/rankings/Rankings_6.dat
$HADOOP_CMD jar "$JARS_DIR/dataloader.jar" rankings "$DATA_DIR/Rankings_7.dat" /data/hadoop/rankings/Rankings_7.dat
$HADOOP_CMD jar "$JARS_DIR/dataloader.jar" rankings "$DATA_DIR/Rankings_8.dat" /data/hadoop/rankings/Rankings_8.dat
$HADOOP_CMD jar "$JARS_DIR/dataloader.jar" rankings "$DATA_DIR/Rankings_9.dat" /data/hadoop/rankings/Rankings_9.dat

$HADOOP_CMD jar "$JARS_DIR/dataloader.jar" uservisits "$DATA_DIR/UserVisits_0.dat" /data/hadoop/uservisits/UserVisits_0.dat
$HADOOP_CMD jar "$JARS_DIR/dataloader.jar" uservisits "$DATA_DIR/UserVisits_1.dat" /data/hadoop/uservisits/UserVisits_1.dat
$HADOOP_CMD jar "$JARS_DIR/dataloader.jar" uservisits "$DATA_DIR/UserVisits_2.dat" /data/hadoop/uservisits/UserVisits_2.dat

