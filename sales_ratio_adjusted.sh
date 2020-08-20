uid=$1
passwrd=$2
prm_end_dat=$3

rm -f sales_${prm_end_dat}.csv

#collecting 6 months of sales history of anchor items,com/sub items to calculate sales_ratio


beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' --hiveconf  hive.resultset.use.unique.column.names=false --outputformat=csv2 -e --color=true "select itm_nbr,fsc_wk_END_DT as week, sum(tot_sal_amt) as tot_sal_amt, sum(tot_itm_qty) as tot_itm_qty, sum(tpr) as tpr from PPS.item_lct_day_level_history_with_promo_flag A INNER JOIN LOWES_TABLES.I0036_FSC_CAL_CNV B ON A.BUS_DT=B.CAL_DT where A.itm_nbr in(select distinct base_item as items from pps.promo_item_pairs_sale where prm_end_dat='$prm_end_dat' union all select distinct itm_nbr as items from pps.promo_item_pairs_sale where prm_end_dat='$prm_end_dat') and fsc_wk_END_DT>=date_sub('$prm_end_dat',182) and fsc_wk_END_DT<'$prm_end_dat' group by itm_nbr,fsc_wk_END_DT" -n ${uid} -p ${passwrd} >>sales_${prm_end_dat}.csv



