#!/usr/bin/env bash

if [ $# -lt 1 ]; then
  echo "Usage: $0 benchmark.conf"
  echo ""
  exit 1
fi

if [ ! -e $1 ]
then
    echo "Cannot find $1, exiting"
    exit
fi

# set up configurations
source $1;

if [ -e "$LOG_FILE" ]; then
	timestamp=`date "+%F-%R" --reference=$LOG_FILE`
	backupFile="$LOG_FILE.$timestamp"
	mv $LOG_FILE $LOG_DIR/$backupFile
fi

echo ""
echo "***********************************************"
echo "*           TPC-H benchmark on Hive           *"
echo "***********************************************"
echo "                                               " 
echo "Running Hive from $HIVE_HOME" | tee -a $LOG_FILE
echo "Running Hadoop from $HADOOP_HOME" | tee -a $LOG_FILE
echo "See $LOG_FILE for more details of query errors."
echo ""

trial=0
while [ $trial -lt $NUM_OF_TRIALS ]; do
	trial=`expr $trial + 1`
	echo "Executing Trial #$trial of $NUM_OF_TRIALS trial(s)..."

	for query in ${HIVE_TPCH_QUERIES_ALL[@]}; do
		echo "Running Hive CMD: $HIVE_CMD -f $BASE_DIR/$query" | tee -a $LOG_FILE
		start=$(date +%s)
		$HIVE_CMD -f $BASE_DIR/$query 2>&1 | tee -a $LOG_FILE
		end=$(date +%s)
		timemsg=$(( $end - $start ))
		echo "Time taken for $query (sec): $timemsg"
	done

done # TRIAL
echo "***********************************************"
echo ""
