rmf output/pig_bench/grep_select;
a = load '/data/grep' using PigStorage as (key, field);
b = filter a by field matches '.*XYZ.*';
store b into 'output/pig_bench/grep_select';
