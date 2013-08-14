rmf output/pig_bench/rankings_select;
a = load '/data/rankings' using PigStorage('|') as (pagerank:int,pageurl,aveduration);
b = filter a by pagerank > 10;
store b into 'output/pig_bench/rankings_select';
