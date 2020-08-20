# usage : ./run_L1.sh <windows_user_id> <password> <promo_week_end_date>
#promo_week_end_date='2019-10-11'
uid=$1
passwrd=$2
prm_end_dat=$3

rm -f item_sales_*.csv
rm -f comp_sub_sales_*.csv
rm -f price_*.csv
rm -f item_comp_sub_*.csv
rm -f affinity_index_*.csv
rm -f similarity_index_*.csv
rm -f promo_item_pair_*.csv
rm -f preprocess_*.csv
rm -f adj_baseline_*.csv
rm -f revenue_*.csv
rm -f base_cs_sale_*.csv
rm -f coefficients_*.csv
rm -f sales_*.csv



#....call L1_1_adjusted.sql :creating 2 tables pps.item_comp_sub_head and pps.promo_item_pairs_sale
        #pps.item_comp_sub_head : containing list of base_item and its substitute and complementary items
        #pps.promo_item_pairs_sale : containing list of base_item and its substitute and complementary items promoted together in given week
#beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' -n ${uid} -p ${passwrd}  -f L1_1_adjusted.sql

#....call L1_2_adjusted.sql : will enter those base_items with its  subs and comp items having both comp and sub list available and are in head or core items list per promo-week
beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' -n ${uid} -p ${passwrd} --hivevar prm_end_dat=$prm_end_dat -f L1_2_adjusted.sql

#....create item_comp_sub_${prm_end_dat}.csv : by selecting data from pps.item_comp_sub_head for current promo-week
beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' --hiveconf  hive.resultset.use.unique.column.names=false --outputformat=csv2 -e "select distinct key_term,value as itm_nbr,rank,type from pps.item_comp_sub_head where prm_end_dat='$prm_end_dat'" -n ${uid} -p ${passwrd} >> item_comp_sub_${prm_end_dat}.csv

#....create item_sales_${prm_end_dat}.csv : by selecting data from pps.pema_l2_out_res for base_items we have selected in pps.item_comp_sub_head table (to get baseline,quantity sold, price etc. of selected base_items)
beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' --hiveconf  hive.resultset.use.unique.column.names=false  --outputformat=csv2 -e "select * from pps.pema_l2_out_res where promo_week='$prm_end_dat' and itm_nbr in (select distinct key_term from pps.item_comp_sub_head where prm_end_dat='$prm_end_dat')" -n ${uid} -p ${passwrd} >> item_sales_${prm_end_dat}.csv

#....create comp_sub_sales_${prm_end_dat}.csv : having 4 weeks sales data from PPS.item_lct_day_level_history_with_promo_flag table i.e of selected promo-week and 3 weeks before that
beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' --hiveconf  hive.resultset.use.unique.column.names=false --outputformat=csv2 -e --color=true "select itm_nbr,fsc_wk_END_DT as week, sum(tot_sal_amt) as tot_sal_amt, sum(tot_itm_qty) as tot_itm_qty, sum(tpr) as tpr,sum(nlp) as nlp,sum(legacy) as legacy, sum(mdo_auto) as mdo_auto  from PPS.item_lct_day_level_history_with_promo_flag A INNER JOIN LOWES_TABLES.I0036_FSC_CAL_CNV B ON A.BUS_DT=B.CAL_DT where A.itm_nbr in(select value from pps.item_comp_sub_head where prm_end_dat='$prm_end_dat') and fsc_wk_END_DT>=date_sub('$prm_end_dat',21) and fsc_wk_END_DT<='$prm_end_dat' group by itm_nbr,fsc_wk_END_DT" -n ${uid} -p ${passwrd} >>comp_sub_sales_${prm_end_dat}.csv
 

      
#....create price_${prm_end_dat}.csv : by selecting price information for each comp/sub item
echo "collect price info of  items"

beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' -n ${uid} -p ${passwrd} --hivevar prm_end_dat=$prm_end_dat -f price_adjusted.sql

beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' --outputformat=csv2 -e --color=true "select itm_nbr, price from pps.ITM_PRC_WK_MODE_TABLE4" -n ${uid} -p ${passwrd} >> price_${prm_end_dat}.csv


#....affinity_index_${prm_end_dat}.csv : select confidence for each base_item and its comp pair
beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' --hiveconf  hive.resultset.use.unique.column.names=false --outputformat=csv2 -e --color=true "select itm_nbr_a,itm_nbr_b,confidence_a_b from pps.cmp_itm_55 where itm_nbr_a in (select distinct key_term from pps.item_comp_sub_head where prm_end_dat='$prm_end_dat')" -n ${uid} -p ${passwrd} >> affinity_index_${prm_end_dat}.csv

#....similarity_index_${prm_end_dat}.csv : select weight for each base_item and its subs pair
beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' --outputformat=csv2 -e --color=true "select itm_nbr_1,itm_nbr_2,sum_of_grpby_weighted_mean_value,rank from PPS.ITEM_SUBSTITUE_ENSEMBLE_ALL_5_DATASOURCE_ACTUAL_RANK_15_VIEW  where itm_nbr_1 in (select distinct key_term from pps.item_comp_sub_head where prm_end_dat='$prm_end_dat')" -n ${uid} -p ${passwrd} >> similarity_index_${prm_end_dat}.csv


python3 L2.1_adjusted.py ${prm_end_dat}


#insert promoted item pairs into the table to get sales ratio 

hdfs dfs -rm -r /tmp/promo_item_pairs_sale/prm_end_dat=${prm_end_dat}
hdfs dfs -mkdir -p /tmp/promo_item_pairs_sale/prm_end_dat=${prm_end_dat}
hadoop fs -put ./promo_item_pair_${prm_end_dat}.csv /tmp/promo_item_pairs_sale/prm_end_dat=${prm_end_dat}/
beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' -n ${uid} -p ${passwrd} -e "MSCK REPAIR TABLE pps.promo_item_pairs_sale;"


sh sales_ratio_adjusted.sh ${uid} ${passwrd} ${prm_end_dat}
python3 sales_ratio_adjusted.py ${prm_end_dat}
python3 L2.2_adjusted.py ${prm_end_dat}



sh run_L3_adjusted.sh ${uid} ${passwrd} ${prm_end_dat}
