

DROP TABLE IF EXISTS pps.item_comp_sub_head;
CREATE TABLE pps.item_comp_sub_head
(key_term string,
value string,
rank int,
type string)
PARTITIONED BY (
prm_end_dat date);



DROP TABLE IF EXISTS pps.promo_item_pairs_sale;
CREATE TABLE pps.promo_item_pairs_sale 
(base_item string,
itm_nbr string, 
type string,
promo_week string 
)
PARTITIONED BY (
prm_end_dat date) 
row format delimited 
fields terminated by ',' 
stored as textfile 
location 'hdfs://HDPSAPRODHA/tmp/promo_item_pairs_sale/'
TBLPROPERTIES (
'skip.header.line.count'='1');
