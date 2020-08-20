

uid=$1
passwrd=$2
prm_end_dat=$3

#beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' -n ${uid} -p ${passwrd}  -f L3_adjusted.sql


             
hdfs dfs -rm -r /tmp/adjusted_baseline/prm_end_dat=${prm_end_dat}
hdfs dfs -mkdir -p /tmp/adjusted_baseline/prm_end_dat=${prm_end_dat}
#insert adj_baseline output file 
hadoop fs -put ./adj_baseline_${prm_end_dat}.csv /tmp/adjusted_baseline/prm_end_dat=${prm_end_dat}/
beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' -n ${uid} -p ${passwrd} -e "MSCK REPAIR TABLE pps.adj_baseline;"
    
hdfs dfs -rm -r /tmp/adjusted_revenue/prm_end_dat=${prm_end_dat}
hdfs dfs -mkdir -p /tmp/adjusted_revenue/prm_end_dat=${prm_end_dat}
#insert adj_revenue output file 
hadoop fs -put ./revenue_${prm_end_dat}.csv /tmp/adjusted_revenue/prm_end_dat=${prm_end_dat}/
beeline -u 'jdbc:hive2://hadoop-sa:8443/default;transportMode=http;httpPath=gateway/default/hive?tez.queue.name=pricing' -n ${uid} -p ${passwrd} -e "MSCK REPAIR TABLE pps.adj_revenue;"
