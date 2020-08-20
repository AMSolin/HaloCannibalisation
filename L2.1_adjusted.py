import pandas as pd


def preprocess(promo_week):
    
    df_base = pd.read_csv("item_sales_"+str(promo_week)+".csv")
   
    itm_list = df_base["itm_nbr"]
    df=pd.read_csv("comp_sub_sales_"+str(promo_week)+".csv")
    df.drop_duplicates()
    df_cs=pd.read_csv("item_comp_sub_"+str(promo_week)+".csv")
    df_cs=df_cs.drop_duplicates(subset=['key_term', 'itm_nbr'], keep='first')
    df_cs = df_cs.dropna()

    df_base['uplift'] = df_base['qty'] - df_base['baseline_sse']
    df_base=df_base.rename({'itm_nbr': 'base_item'}, axis=1)

    df_base_new=df_base[['base_item','qty','baseline_sse','uplift','price']]
    df['week'] = pd.to_datetime(df['week'], format='%Y-%m-%d')

    # price dataframe
    df_price = pd.read_csv("price_"+str(promo_week)+".csv")
    df_price=df_price.rename({'price': 'item_price'}, axis=1)

    df2 = df.pivot_table(index=['itm_nbr'], columns='week', values='tot_itm_qty') \
        .reset_index().rename_axis(None, axis=1)

    #df2["cs_baseline"] = df2.iloc[:, [1, 2, 3]].mean(axis=1).abs()
    df2["cs_baseline"]=((0.2*df2.iloc[:,1])+(0.3*df2.iloc[:,2])+(0.5*df2.iloc[:,3]))
    df2.fillna(0, inplace=True)
    df2["cs_baseline"]=df2["cs_baseline"].astype(int)
   # df2["cs_baseline"]=((0.2*df2.iloc[:,1])+(0.3*df2.iloc[:,2])+(0.5*df2.iloc[:,3])).astype(int)
    df2["promo_week_sum"] = df2.iloc[:,4]

    #if any substitute/complementary is on TPR promotion on promo week this column will be 1 else 0
    df["promo_flag"]=df['tpr'].where(df["tpr"] == 0, 1)

    df_promo=df[['itm_nbr','week','tpr',"promo_flag"]]
    df_promo=df_promo.loc[df_promo['week'] == promo_week]
    df_promo=df_promo[['itm_nbr','week','promo_flag']]


    df_final = df2

    df_final["cs_uplift"] = df_final["promo_week_sum"] - df_final["cs_baseline"]
    df_final=pd.merge(df_final,df_promo,on='itm_nbr')
    # df_final["cs_uplift"] = df_final["cs_uplift1"].where(df_final["cs_uplift1"] > 0, 0)
    df_final = pd.merge(df_price,df_final, on='itm_nbr')
    df_final=pd.merge(df_cs,df_final, on='itm_nbr')

    df_final=df_final.rename({'key_term': 'base_item'}, axis=1)
    df_final=pd.merge(df_base_new,df_final,on='base_item')

    #removing those rows where base_item is same as its comp/sub item
    df_final.drop(df_final[(df_final['base_item'] == df_final['itm_nbr']) ].index, inplace=True)
    
    #get pairs of base item with its comp/cub item if both are promoted, quantity >0, price>0
    df_final=df_final[(df_final['qty'] >0) & (df_final['price'] >0) & (df_final['item_price'] >0)]
    df_itm_pairs=df_final[(df_final['promo_flag'] == 1)]
    df_itm_pairs=df_itm_pairs[['base_item','itm_nbr','type','week']]
    
    df_itm_pairs['itm_nbr']=df_itm_pairs['itm_nbr'].round(0).astype(int)
    df_itm_pairs=df_itm_pairs.drop_duplicates(subset=['base_item', 'itm_nbr'], keep='first')
    df_itm_pairs.to_csv("promo_item_pair_"+str(promo_week)+".csv", index=False)

    ####...........insert the above generated csv into a hive table from where these item pairs will be picked to calculate cross price elasticity.........

    df_final.fillna(0, inplace=True)


    #similarity_index
    df_sub = pd.read_csv("similarity_index_"+str(promo_week)+".csv")
    df_sub['type']='s'
    df_sub=df_sub.drop_duplicates()
    df_sub_weighted_sum=df_sub.groupby('itm_nbr_1',as_index=False)['sum_of_grpby_weighted_mean_value'].sum()
    df_sub_weighted_sum=df_sub_weighted_sum.rename(columns={'sum_of_grpby_weighted_mean_value':'weighted_sum'})

    df_sub=pd.merge(df_sub,df_sub_weighted_sum,on='itm_nbr_1')
    df_sub=df_sub.rename({'itm_nbr_1': 'base_item','itm_nbr_2': 'itm_nbr'}, axis=1)
    df_sub['similarity']=df_sub['sum_of_grpby_weighted_mean_value']/df_sub['weighted_sum']
    df_sub=df_sub[['base_item','itm_nbr','type','similarity']]


    #affinity_index
    df_com = pd.read_csv("affinity_index_"+str(promo_week)+".csv")
    df_com['type']='c'
    df_com=df_com.drop_duplicates()
    df_com_weighted_sum=df_com.groupby('itm_nbr_a',as_index=False)['confidence_a_b'].sum()
    df_com_weighted_sum=df_com_weighted_sum.rename(columns={'confidence_a_b':'weighted_sum'})

    df_com=pd.merge(df_com,df_com_weighted_sum,on='itm_nbr_a')
    df_com=df_com.rename({'itm_nbr_a': 'base_item','itm_nbr_b': 'itm_nbr'}, axis=1)
    df_com['similarity']=df_com['confidence_a_b']/df_com['weighted_sum']
    df_com=df_com[['base_item','itm_nbr','type','similarity']]

    #concat affinity and similarity dataframe vertically
    df_cs=pd.concat([df_sub, df_com], ignore_index=True)

    df_final=pd.merge(df_final,df_cs,on=['base_item','itm_nbr','type'])
    df_final.to_csv('preprocess_'+str(promo_week)+".csv", index=False)


import sys

def main():
    # print command line arguments
    for arg in sys.argv[1:]:
        print(sys.argv[1])
    preprocess(sys.argv[1])
if __name__ == "__main__":
    main()
