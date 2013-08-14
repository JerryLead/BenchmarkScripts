SET default_parallel $reducers

SET io.sort.mb 200
--SET io.sort.record.percent 0.15
--SET child.monitor.metrics.seconds 2
--SET child.monitor.counters true


--SET child.monitor.metrics.seconds 2
--SET child.monitor.counters true
--SET child.monitor.jvm.seconds 2
SET child.monitor.jstat.seconds 2

SET io.sort.mb $io_sort_mb
SET mapred.child.java.opts '$mapred_child_java_opts' 

SET mapred.job.reuse.jvm.num.tasks 1
SET job.name '$name $mapred_child_java_opts $io_sort_mb $reducers'
--SET job.name '$name $io_sort_mb $reducers'

--SET mapred.task.profile true
--SET mapred.task.profile.params -agentlib:hprof=cpu=samples,heap=sites,depth=10,force=n,thread=y,verbose=n,file=%s
--SET mapred.task.profile.maps 0
--SET mapred.task.profile.reduces 0
--SET io.sort.record.percent 0.12
--SET mapred.inmem.merge.threshold 54
--SET mapred.reduce.parallel.copies 27
SET mapred.job.shuffle.merge.percent 0.9
SET mapred.job.shuffle.input.buffer.percent 0.9

--SET mapred.min.split.size 134217728
--SET mapred.task.profile true
--SET mapred.task.profile.params -agentlib:hprof=cpu=samples,heap=sites,depth=3,force=n,thread=y,verbose=n,file=%s



rmf output/pig_bench/html_join;
a = load '/data/uservisits' using PigStorage('|') as (sourceIP,destURL,visitDate,adRevenue,userAgent,countryCode,languageCode,searchWord,duration);
b = load '/data/rankings' using PigStorage('|') as (pagerank:int,pageurl,aveduration);
b1 = foreach b generate pagerank, pageurl;
c = filter a by visitDate > '1999-01-01' AND visitDate < '2000-01-01';
c1 = foreach c generate sourceIP, destURL, adRevenue;
d = JOIN c1 by destURL, b1 by pageurl;
d1 = foreach d generate sourceIP, pagerank, adRevenue;
e = group d1 by sourceIP;
f = FOREACH e GENERATE group, AVG(d1.pagerank), SUM(d1.adRevenue);
g = order f by $2 desc;
store g into 'output/pig_bench/html_join';

