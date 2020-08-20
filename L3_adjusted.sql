

DROP TABLE IF EXISTS pps.adj_baseline;
CREATE TABLE pps.adj_baseline
(base_item string,
qty int,
qty_reg int,
baseline_sse int,
uplift int,
price float,
comp_uplift float,
sub_uplift float,
adj_baseline int,
true_adj_baseline int,
adj_uplift int,
flag string)
PARTITIONED BY (
prm_end_dat date)
row format delimited
fields terminated by ','
stored as textfile
location 'hdfs://HDPSAPRODHA/tmp/adjusted_baseline/'
TBLPROPERTIES (
'skip.header.line.count'='1');


DROP TABLE IF EXISTS pps.adj_revenue;
CREATE TABLE pps.adj_revenue
(base_item string,
qty int,
baseline_sse int,
uplift int,
price float,
comp_uplift float,
sub_uplift float,
revenue float,
adj_revenue float,
true_adj_revenue float,
flag string)
PARTITIONED BY (
prm_end_dat date)
row format delimited
fields terminated by ','
stored as textfile
location 'hdfs://HDPSAPRODHA/tmp/adjusted_revenue/'
TBLPROPERTIES (
'skip.header.line.count'='1');


