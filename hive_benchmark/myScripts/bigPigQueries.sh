#!/bin/bash
NUM_OF_TRIALS=1 

# variables 
split=64
reducers=9
io_sort_mb=200
xmx=1000
xms=

# pig all benchmark queries

USER_AGGR="bigpig/uservisits_aggre.pig" 
RANK_USER_JOIN="bigpig/rankings_uservisits_join.pig"

									
while [ $split -le 256 ]
do
  reducers=9
  while [ $reducers -le 18 ]
  do
      xmx=1000
      while [ $xmx -le 4000 ]
	  do
          io_sort_mb=200
          while [ $io_sort_mb -le 800 ]      
          do 
		      splitBytes=$[$split*1024*1024]

	          mapred_child_java_opts=\'"-Xmx"$xmx"m"\'
      		  echo mapred_child_java_opts=$mapred_child_java_opts > jvm.param
              PIG_CMD="$PIG_HOME/bin/pig -param name=$USER_AGGR -param_file jvm.param -param io_sort_mb=$io_sort_mb -param reducers=$reducers \
					   -param split=$splitBytes"
              $PIG_CMD $USER_AGGR
	          sleep 15
	          echo $PIG_CMD

	  
	          mapred_child_java_opts=\'"-Xmx"$xmx"m -Xms"$xmx"m"\'
              echo mapred_child_java_opts=$mapred_child_java_opts > jvm.param
              PIG_CMD="$PIG_HOME/bin/pig -param name=$USER_AGGR -param_file jvm.param -param io_sort_mb=$io_sort_mb -param reducers=$reducers \
					   -param split=$splitBytes"
              $PIG_CMD $USER_AGGR
	          sleep 15
	          echo $PIG_CMD
		  
           io_sort_mb=$[$io_sort_mb+200] 
		   done

      xmx=$[$xmx+1000]
      done
  reducers=$[$reducers+9]
  done
split=$[$split*2]	 
done
