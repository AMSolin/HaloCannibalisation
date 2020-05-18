dt='2019-11-22'

#rm -f item_comps.csv
#rm -f item_subs.csv
rm -f item_sales.csv
rm -f comp_sub_sales.csv
rm -f price.csv

beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' --showDbInPrompt=true --color=true -n nkatyal -p Apr@2020 -e "drop table if exists pps.comp_items;"

beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' --showDbInPrompt=true --color=true -n nkatyal -p Apr@2020 -e "create table pps.comp_items as select t1.key_term,t2.rank,t2.value from pps.compl_itm_nbr t1 lateral view explode (map(1,reco1,2,reco2,3,reco3,4,reco4,5,reco5,6,reco6,7,reco7,8,reco8,9,reco9,10,reco10,11,reco11,12,reco12)) t2 as rank,value;"


beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' --showDbInPrompt=true --color=true -n nkatyal -p Apr@2020 -e "drop table if exists pps.item_comp_sub_head; create table pps.item_comp_sub_head as select key_term,value,rank,'c' as type from pps.comp_items where key_term in (select distinct itm_nbr from pps.pema_l2_out_res d inner join (select key_term from pps.comp_items a inner join (select itm_nbr_1 from daci_sandbox.item_SUBSTITUE_3_SRC_PROMOlist_base_table_Rank15_PG_Final) b on a.key_term=b.itm_nbr_1 inner join (select item_num from pps.head where kvi='Head') c on a.key_term=c.item_num) e on d.itm_nbr=e.key_term where d.promo_week ='$dt') union all select itm_nbr_1 as key_term,itm_nbr_2 as value,rank,'s' as type from daci_sandbox.item_SUBSTITUE_3_SRC_PROMOlist_base_table_Rank15_PG_Final where itm_nbr_1 in (select distinct itm_nbr from pps.pema_l2_out_res d inner join (select key_term from pps.comp_items a inner join (select itm_nbr_1 from daci_sandbox.item_SUBSTITUE_3_SRC_PROMOlist_base_table_Rank15_PG_Final) b on a.key_term=b.itm_nbr_1 inner join (select item_num from pps.head where kvi='Head') c on a.key_term=c.item_num) e on d.itm_nbr=e.key_term where d.promo_week ='$dt');"


#beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' --showDbInPrompt=true --color=true -n nkatyal -p Apr@2020 -e "drop table if exists pps.item_comp_head; create table pps.item_comp_head as select key_term,value from pps.comp_items where key_term in (select distinct itm_nbr from pps.pema_l2_out_res d inner join (select key_term from pps.comp_items a inner join (select itm_nbr_1 from daci_sandbox.item_SUBSTITUE_3_SRC_PROMOlist_base_table_Rank15_PG_Final) b on a.key_term=b.itm_nbr_1 inner join (select item_num from pps.head where kvi='Head') c on a.key_term=c.item_num) e on d.itm_nbr=e.key_term where d.promo_week in ('$dt','$dt1'));"

#beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' --showHeader=false --outputformat=csv2 -e "select * from pps.item_comp_head" -n nkatyal -p Apr@2020 >> item_comps.csv

#beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' --showDbInPrompt=true --color=true -n nkatyal -p Apr@2020 -e "drop table if exists pps.item_sub_head;create table pps.item_sub_head as select itm_nbr_1,itm_nbr_2 from daci_sandbox.item_SUBSTITUE_3_SRC_PROMOlist_base_table_Rank15_PG_Final where itm_nbr_1 in (select distinct itm_nbr from pps.pema_l2_out_res d inner join (select key_term from pps.comp_items a inner join (select itm_nbr_1 from daci_sandbox.item_SUBSTITUE_3_SRC_PROMOlist_base_table_Rank15_PG_Final) b on a.key_term=b.itm_nbr_1 inner join (select item_num from pps.head where kvi='Head') c on a.key_term=c.item_num) e on d.itm_nbr=e.key_term where d.promo_week in ('$dt','$dt1'));"
 
#beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' --showHeader=false --outputformat=csv2 -e --color=true "select * from pps.item_sub_head" -n nkatyal -p Apr@2020 >> item_subs.csv


beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' --outputformat=csv2 -e --color=true "select * from pps.pema_l2_out_res where promo_week='$dt' and itm_nbr in (select distinct key_term from pps.item_comp_sub_head)" -n nkatyal -p Apr@2020 >> item_sales1.csv

sed -e 's/pema_l2_out_res.//g' < item_sales1.csv > item_sales.csv
rm -f item_sales1.csv

week_dt=$dt
week_st=$(date -d "$dt -21 days" +"%Y-%m-%d")

#fetching items sales data



en=$week_dt

st=$week_st
dt_new=$st
while [ "$en" != "$dt_new" ]
do
        st=$dt_new
        dt_new=$(date -d "$st +7 days" +"%Y-%m-%d")
        echo $st
	beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' --outputformat=csv2 -e --color=true "select itm_nbr,'$st' as week,sum(tot_sal_amt) as tot_sal_amt,sum(tot_cst_amt) as tot_cst_amt,sum(tot_itm_qty) as tot_itm_qty,sum(mkd_ibm_sap) as mkd_ibm_sap,sum(tpr) as tpr,sum(nlp) as nlp,sum(legacy) as legacy, sum(mdo_auto) as mdo_auto from PPS.item_lct_day_level_history_with_promo_flag where itm_nbr in (select value from pps.item_comp_sub_head) and bus_dt>='$st' and bus_dt<'$dt_new' group by itm_nbr" -n nkatyal -p Apr@2020 >>comp_sub_sales.csv
done
        
echo "collect price info of  items"

beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' --outputformat=csv2 -e --color=true "select itm_nbr, max(PRC_MRT_SET_WK_ITM_PRC_AMT) as price from lowes_tables.i0776_lct_itm_prc_wk where itm_nbr in (select value from pps.item_comp_sub_head) and (PRC_MRT_SET_CD = 3 OR PRC_LCT_SET_CD = 99) group by itm_nbr" -n nkatyal -p Apr@2020 >> price.csv






