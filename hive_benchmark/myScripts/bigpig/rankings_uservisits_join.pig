SET default_parallel $reducers

SET io.sort.mb 200
--SET io.sort.record.percent 0.15
--SET child.monitor.metrics.seconds 2
--SET child.monitor.counters true

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

