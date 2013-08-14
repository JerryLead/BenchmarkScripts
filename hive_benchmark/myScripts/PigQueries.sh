#!/bin/bash
NUM_OF_TRIALS=1 

# variables 
reducers=9
io_sort_mb=400
xmx=2000
xms=0

# pig all benchmark queries

USER_AGGR="pig/uservisits_aggre.pig" 
RANK_USER_JOIN="pig/rankings_uservisits_join.pig"
RANK_REDUCE_JOIN="pig/my_rankings_join.pig"
									
      
#	  mapred_child_java_opts=\'"-Xmx"$xmx"m"\'
#      echo mapred_child_java_opts=$mapred_child_java_opts > jvm.param
#      PIG_CMD="$PIG_HOME/bin/pig -param name=$USER_AGGR -param_file jvm.param -param io_sort_mb=$io_sort_mb -param reducers=$reducers"
#      $PIG_CMD $USER_AGGR
#	  sleep 15
#	  echo $PIG_CMD

	  
#mapred_child_java_opts=\'"-Xmx1000m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -verbose:gc -XX:-PrintGCDetails -XX:+PrintGCTimeStamps"\'
	  mapred_child_java_opts=\'"-Xmx1000m -verbose:gc -XX:-PrintGCDetails -XX:+PrintGCTimeStamps"\'
      echo mapred_child_java_opts=$mapred_child_java_opts > jvm.param

#      PIG_CMD="$PIG_HOME/bin/pig -param name=$USER_AGGR -param_file jvm.param -param io_sort_mb=$io_sort_mb -param reducers=$reducers"
#      $PIG_CMD $USER_AGGR 
#	  echo $PIG_CMD $USER_AGGR

#     PIG_CMD="$PIG_HOME/bin/pig -param name=$RANK_USER_JOIN -param_file jvm.param -param io_sort_mb=$io_sort_mb -param reducers=$reducers"
#      $PIG_CMD $RANK_USER_JOIN
#	  echo $PIG_CMD $RANK_USER_JOIN

      PIG_CMD="$PIG_HOME/bin/pig -param name=$RANK_REDUCE_JOIN -param_file jvm.param -param io_sort_mb=$io_sort_mb -param reducers=$reducers"
      $PIG_CMD $RANK_REDUCE_JOIN
	  echo $PIG_CMD $RANK_REDUCE_JOIN

