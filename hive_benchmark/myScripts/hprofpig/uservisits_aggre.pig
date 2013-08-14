SET default_parallel $reducers 
SET pig.splitCombination false
--SET io.sort.mb 105
--SET io.sort.record.percent 0.32241842
--SET io.sort.spill.percent 1.0
--SET mapred.child.java.opts -Xmx1500m

SET child.monitor.metrics.seconds 2
SET child.monitor.jvm.seconds 2
SET child.monitor.jstat.seconds 2

SET io.sort.mb $io_sort_mb
SET mapred.child.java.opts '$mapred_child_java_opts' 
--SET mapred.child.java.opts '-Xmx$xmx -Xms$xms'
--SET mapred.child.java.opts '$xmx $xms'

SET mapred.job.reuse.jvm.num.tasks 1
--SET job.name '$name $xmx $xms'
SET job.name '$name $split B $mapred_child_java_opts ismb=$io_sort_mb, RN=$reducers'


SET mapred.task.profile true
SET mapred.task.profile.params -agentlib:hprof=heap=sites,verbose=n,file=%s
SET mapred.task.profile.maps 0-2
SET mapred.task.profile.reduces 0-2
--SET io.sort.record.percent 0.12
--SET mapred.inmem.merge.threshold 54
--SET mapred.reduce.parallel.copies 27
--SET mapred.job.shuffle.merge.percent 0.9

SET mapred.min.split.size $split
SET mapred.max.split.size $split 
SET dfs.block.size $split
--SET mapred.task.profile true
--SET mapred.task.profile.params -agentlib:hprof=cpu=samples,heap=sites,depth=3,force=n,thread=y,verbose=n,file=%s

rmf output/pig_bench/uservisits_aggre;
a = load '/data2/uservisits' using PigStorage('|') as (sourceIP,destURL,visitDate,adRevenue,userAgent,countryCode,languageCode,searchWord,duration);
a1 = foreach a generate sourceIP, adRevenue;
b = group a1 by sourceIP;
c = FOREACH b GENERATE group, SUM(a1. adRevenue);
store c into 'output/pig_bench/uservisits_aggre';

