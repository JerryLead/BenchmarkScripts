#!/bin/bash
fileSize=2048
nrFiles=32

while [ $nrFiles -le 128 ]
do
  bufferSize=1024
  while [ $bufferSize -le 67108864 ]
  do
    hadoop jar $HADOOP_HOME/hadoop-0.20.2-test.jar TestDFSIO -write -nrFiles $nrFiles -fileSize $fileSize -bufferSize $bufferSize
    sleep 10
    sync
    echo 3> /proc/sys/vm/drop_caches
    hadoop jar $HADOOP_HOME/hadoop-0.20.2-test.jar TestDFSIO -read -nrFiles $nrFiles -fileSize $fileSize -bufferSize $bufferSize 
    hadoop jar $HADOOP_HOME/hadoop-0.20.2-test.jar TestDFSIO -clean
    bufferSize=$[$bufferSize*4]
    sleep 15
  done
  
  nrFiles=$[$nrFiles*2]
done
