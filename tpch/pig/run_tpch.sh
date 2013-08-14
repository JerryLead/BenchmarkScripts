#!/usr/bin/env bash

# Show usage if we go less than 3 arguments
if [ $# -lt 3 ]; then
  echo "Usage: $0 input_dir output_dir reducers [hadoop_opts]"
  echo ""
  echo "   input_dir:   The input directory on HDFS"
  echo "   output_dir:  The output directory on HDFS"
  echo "   reducers:    The number of reduce tasks to use per MapReduce job"
  echo "   hadoop_opts: Optional Hadoop options (-conf or -D)"
  echo ""
  exit 1
fi

# Get the input parameters
INPUT_DIR=$1
OUTPUT_DIR=$2
REDUCERS=$3
shift; shift; shift;
HADOOP_OPTS="$@"

# Check environment variables
if [ "$PIG_HOME" == "" ]; then
    echo "Cannot find PIG_HOME, set PIG_HOME environment variable to directory above bin/pig."
    exit
fi

if [ "$HADOOP_HOME" == "" ]; then
    echo "Cannot find HADOOP_HOME, set HADOOP_HOME environment variable to directory above bin/hadoop."
    exit
fi

###########################################################
# EXECUTE THE BENCHMARK
declare cmd=""
declare total_pig_times=0

for ((  i = 1 ;  i <= 22;  i++  ))
do
    declare pig_times=0
   
    # Prepare the run
    echo "Running Pig Query Q$i"
    ${HADOOP_HOME}/bin/hadoop fs -rmr "$OUTPUT_DIR/Q${i}out"
    cmd="${PIG_HOME}/bin/pig $HADOOP_OPTS -param input=$INPUT_DIR -param output=$OUTPUT_DIR -param reducers=$REDUCERS -f queries/Q$i.pig"
    echo "Going to run:"
    echo $cmd
    echo ""

    # Run the pig script      
    start=$(date +%s)
    $cmd
    end=$(date +%s)
    pig_times=$(( $end - $start ))
    total_pig_times=$(( $total_pig_times + $pig_times ))
  
    echo "Q$i times (sec):	$pig_times"
    echo ""

done

# Done
echo "Total times (sec):	$total_pig_times"
echo ""

