SET default_parallel 8
--SET mapred.child.java.opts -Xmx1500m
--SET io.sort.mb 100
--SET child.monitor.metrics.seconds 2
--SET child.monitor.counters true

--SET io.sort.record.percent 0.12
--SET mapred.inmem.merge.threshold 54
--SET mapred.reduce.parallel.copies 27
--SET mapred.job.shuffle.merge.percent 0.9

--SET mapred.min.split.size 134217728
--SET mapred.task.profile true
--SET mapred.task.profile.params -agentlib:hprof=cpu=samples,heap=sites,depth=3,force=n,thread=y,verbose=n,file=%s

rmf output/pig_bench/uservisits_aggre;
a = load '/data/uservisits' using PigStorage('|') as (sourceIP,destURL,visitDate,adRevenue,userAgent,countryCode,languageCode,searchWord,duration);
a1 = foreach a generate sourceIP, adRevenue;
b = group a1 by sourceIP;
c = FOREACH b GENERATE group, SUM(a1. adRevenue);
store c into 'output/pig_bench/uservisits_aggre';

