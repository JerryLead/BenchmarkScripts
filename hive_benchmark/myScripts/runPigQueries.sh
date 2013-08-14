#!/bin/bash
NUM_OF_TRIALS=1 

# variables 
reducers=9
io_sort_mb=200
xmx=1000
xms=

# pig all benchmark queries

USER_AGGR="pig/uservisits_aggre.pig" 
RANK_USER_JOIN="pig/rankings_uservisits_join.pig"
									
while [ $xmx -le 4000 ]
do
  io_sort_mb=200
  while [ $io_sort_mb -le 800 ]      
  do
    reducers=9
    while [ $reducers -le 18 ]
	do
      
	  mapred_child_java_opts=\'"-Xmx"$xmx"m"\'
      echo mapred_child_java_opts=$mapred_child_java_opts > jvm.param
      PIG_CMD="$PIG_HOME/bin/pig -param name=$USER_AGGR -param_file jvm.param -param io_sort_mb=$io_sort_mb -param reducers=$reducers"
      $PIG_CMD $USER_AGGR
	  sleep 15
	  echo $PIG_CMD

	  
	  mapred_child_java_opts=\'"-Xmx"$xmx"m -Xms"$xmx"m"\'
      PIG_CMD="$PIG_HOME/bin/pig -param reducers=$reducers -param io_sort_mb=$io_sort_mb -param mapred_child_java_opts=$mapred_child_java_opts"
      PIG_CMD="$PIG_HOME/bin/pig -param reducers=$reducers -param io_sort_mb=$io_sort_mb -param xmx=$xmx"m" -param xms=$xmx"m"" 
      echo mapred_child_java_opts=$mapred_child_java_opts > jvm.param
      PIG_CMD="$PIG_HOME/bin/pig -param name=$USER_AGGR -param_file jvm.param -param io_sort_mb=$io_sort_mb -param reducers=$reducers"
	  
      $PIG_CMD $USER_AGGR
	  sleep 15
	  echo $PIG_CMD

	  reducers=$[$reducers+9]
    done	  
  
  io_sort_mb=$[$io_sort_mb+200]
  done


  xmx=$[$xmx+1000]
done
