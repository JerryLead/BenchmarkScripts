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

# set up the execution log file
if [ -e "$LOG_FILE" ]; then
	timestamp=`date "+%F-%R" --reference=$LOG_FILE`
	backupFile="$LOG_FILE.$timestamp"
	mv $LOG_FILE $backupFile
fi

# set up the timing log file
if [ -e "$TIMING_FILE" ]; then
	timestamp=`date "+%F-%R" --reference=$TIMING_FILE`
	backupFile="$TIMING_FILE.$timestamp"
	mv $TIMING_FILE $backupFile
fi

# output the timing headers
echo 'Timings, grep select, rankings select, uservisits aggregation, uservisits-rankings join' >> $TIMING_FILE

trial=0
while [ $trial -lt $NUM_OF_TRIALS ]; do
	trial=`expr $trial + 1`
	echo "Executing Trial #$trial of $NUM_OF_TRIALS trial(s)..."
   	echo "Trial $trial" >> $TIMING_FILE
    
	# Hive queries
	if [ "$RUN_HIVE" == "1" ]; then
	echo -n "Hive," >> $TIMING_FILE
	for query in ${HIVE_BENCHMARKS[@]}; do
		echo "Running Hive CMD: $HIVE_CMD -f $BASE_DIR/$query" | tee -a $LOG_FILE
		start=$(date +%s)
		$HIVE_CMD -f $BASE_DIR/$query 2>&1 | tee -a $LOG_FILE
		end=$(date +%s)
		timemsg=$(( $end - $start ))
		echo $timemsg
		echo -n "${timemsg}," >> $TIMING_FILE 
	done
	echo  " " >> $TIMING_FILE	
	fi
	
	# PIG queries
	if [ "$RUN_PIG" == "1" ]; then
	echo -n "PIG," >> $TIMING_FILE
	for query in ${PIG_BENCHMARKS[@]}; do
		echo "Running Pig CMD: $PIG_CMD $BASE_DIR/$query" | tee -a $LOG_FILE
		start=$(date +%s)
		$PIG_CMD -f $BASE_DIR/$query 2>&1 | tee -a $LOG_FILE
		end=$(date +%s)
		timemsg=$(( $end - $start ))
		echo $timemsg
		echo -n "${timemsg}," >> $TIMING_FILE 
	done
	echo " " >> $TIMING_FILE
	fi

	# hadoop queries
	if [ "$RUN_HADOOP" == "1" ]; then
	echo -n "Hadoop," >> $TIMING_FILE
	for query in ${!HADOOP_BENCHMARKS[*]}; do
		$HADOOP_CMD ${HADOOP_DATA_PREPARE[$query]} 2>&1 | tee -a $LOG_FILE > /dev/null 
		echo "Running Hadoop CMD: $HADOOP_CMD ${HADOOP_BENCHMARKS[$query]}" | tee -a $LOG_FILE
		start=$(date +%s)
		$HADOOP_CMD ${HADOOP_BENCHMARKS[$query]} 2>&1 | tee -a $LOG_FILE
		end=$(date +%s)
		timemsg=$(( $end - $start ))
		echo $timemsg
		echo -n "${timemsg}," >> $TIMING_FILE 
	done
	echo " " >> $TIMING_FILE	
	fi

done # TRIAL

