
import pandas as pd
def postprocess(promo_week):
    df_final=pd.read_csv("preprocess_"+str(promo_week)+".csv")
    df_base = pd.read_csv("item_sales_"+str(promo_week)+".csv")
    itm_list = df_base["itm_nbr"]
    df = pd.read_csv("comp_sub_sales_"+str(promo_week)+".csv")
    df.drop_duplicates()
    df_cs = pd.read_csv("item_comp_sub_"+str(promo_week)+".csv")
    df_cs = df_cs.drop_duplicates(subset=['key_term', 'itm_nbr'], keep='first')
    df_cs = df_cs.dropna()

    df_base['uplift'] = df_base['qty'] - df_base['baseline_sse']
    df_base = df_base.rename({'itm_nbr': 'base_item'}, axis=1)

    df_base_new = df_base[['base_item', 'qty', 'qty_reg', 'baseline_sse', 'uplift', 'price']]
    df_base_new=df_base_new[(df_base_new['price'] >0) & (df_base_new['qty'] >0) ]
    #df_base_new.to_csv('df_base_new.csv', index=False)
    # sum of uplifts of promoted products only
    df_final_promo= df_final[(df_final['promo_flag'] == 1)]
    #if promoted comp/sub items shows negative uplift in their sale nullyfy the impact
    df_final_promo['cs_uplift']=df_final_promo['cs_uplift'].where(df_final_promo['cs_uplift'] > 0, 0)
    #taking only those complementary items having price greater than base item, as we need to find uplift on those items to which base item is complemntary
    df_final_promo_a = df_final_promo[df_final_promo['type'] == 'c']
    df_final_promo_a['cs_uplift'] = df_final_promo_a['cs_uplift'].where(df_final_promo_a['item_price'] > df_final_promo_a['price'], 0)
    df_final_promo_b = df_final_promo[df_final_promo['type'] == 's']

    df_final_promo = pd.concat([df_final_promo_a, df_final_promo_b], ignore_index=True)

    #..........fetch from table (sales_ratio)
    df_sales_ratio=pd.read_csv("coefficients_"+str(promo_week)+".csv")
    df_sales_ratio=df_sales_ratio.rename({'key_term': 'base_item'}, axis=1)


    df_final_promo=pd.merge(df_final_promo,df_sales_ratio,on=['base_item','itm_nbr','type'])



    df_final_promo["adj_cs_uplift"]=df_final_promo["cs_uplift"]*(df_final_promo["coefficient"].abs())*df_final_promo["similarity"]

    df_cs_promo = df_final_promo.groupby(['base_item','type'], as_index=False)['adj_cs_uplift'].sum()

    #adjusted baseline calculation
    df_adj_base = df_cs_promo.pivot_table(index=['base_item'], columns='type', values='adj_cs_uplift') \
        .reset_index().rename_axis(None, axis=1)

    #df_adj_base=pd.merge(df_base_new,df_adj_base,on='base_item')
    #df_adj_base.fillna(0, inplace=True)
    df_adj_base=pd.merge(df_base_new,df_adj_base, on='base_item', how='left').fillna(0)
    df_adj_base['adj_baseline']=df_adj_base['baseline_sse']-df_adj_base['s']+df_adj_base['c']
    #df_adj_base['adj_baseline']=df_adj_base['adj_baseline'].astype(int)
    #check if adjusted baseline > qty  or adjusted_baseline<0 ...make it equal to baseline calculated 
    df_adj_base['true_adj_baseline'] = df_adj_base['adj_baseline'].where(df_adj_base["adj_baseline"]< df_adj_base['qty'], df_adj_base["baseline_sse"])
    df_adj_base['true_adj_baseline'] = df_adj_base['true_adj_baseline'].where(df_adj_base["true_adj_baseline"] >0, df_adj_base["baseline_sse"])
    df_adj_base['true_adj_baseline'] = df_adj_base['true_adj_baseline'].where(df_adj_base["true_adj_baseline"] > df_adj_base["qty_reg"], df_adj_base["baseline_sse"])
    df_adj_base['adj_uplift']=df_adj_base['qty'] - df_adj_base['true_adj_baseline']
    df_adj_base.loc[df_adj_base['baseline_sse'] == 0, 'adj_baseline'] = 0
    df_adj_base['flag'] = 0
    df_adj_base['flag'] = df_adj_base['flag'].where(df_adj_base["true_adj_baseline"] >= df_adj_base["adj_baseline"], 'upper_bound_check')
    df_adj_base['flag'] = df_adj_base['flag'].where(df_adj_base["true_adj_baseline"] <= df_adj_base["adj_baseline"], 'lower_bound_check')
    #df_adj_base = df_adj_base.drop('qty_reg', 1)
    #print adj_baseline only for items having  baseline >0
    df_adj_base=df_adj_base[ (df_adj_base['qty'] >0) ]
    df_adj_base=df_adj_base.rename({'c': 'comp','s':'sub'}, axis=1)
    df_adj_base.to_csv("adj_baseline_"+str(promo_week)+".csv", index=False)







    #........................................Revenue Uplift............................
    #sum uplifts of all non-promoted complementary/substitute items of each base_item
    df_final_nonpromo= df_final[(df_final['promo_flag'] == 0)]
    # nullify the impact of complementary items showing negative uplift and substitute showing positive uplift
    df_final_nonpromo["adj_cs_uplift"]=df_final_nonpromo['cs_uplift']
    df_final_nonpromo_a=df_final_nonpromo[df_final_nonpromo['type'] == 'c']
    df_final_nonpromo_a['adj_cs_uplift']=df_final_nonpromo_a['adj_cs_uplift'].where(df_final_nonpromo_a["adj_cs_uplift"] >= 0, 0)
    df_final_nonpromo_b=df_final_nonpromo[df_final_nonpromo['type'] == 's']
    df_final_nonpromo_b['adj_cs_uplift']=df_final_nonpromo_b['adj_cs_uplift'].where(df_final_nonpromo_b["cs_uplift"] <= 0, 0)
    df_final_nonpromo=pd.concat([df_final_nonpromo_a,df_final_nonpromo_b], ignore_index=True)

    df_final_nonpromo["cs_revenue"]=df_final_nonpromo["adj_cs_uplift"]*df_final_nonpromo["similarity"]*df_final_nonpromo['item_price']
    df_cs_nonpromo = df_final_nonpromo.groupby(['base_item','type'], as_index=False)['cs_revenue'].sum()

    df_revenue = df_cs_nonpromo.pivot_table(index=['base_item'], columns='type', values='cs_revenue') \
         .reset_index().rename_axis(None, axis=1)


   # df_revenue=pd.merge(df_base_new,df_revenue,on='base_item')
    df_revenue=pd.merge(df_base_new,df_revenue,on='base_item', how='left').fillna(0)
    df_revenue.fillna(0, inplace=True)
    df_revenue['revenue']=df_revenue['qty']*df_revenue["price"]
    df_revenue['adj_revenue']=df_revenue['qty']*df_revenue["price"] + df_revenue['s']+df_revenue['c']
    #if baseline =0 dont calculate its revenue############
    #df_revenue.loc[df_revenue['baseline_sse'] == 0, 'revenue'] = 0
    #df_revenue.loc[df_revenue['baseline_sse'] == 0, 'adj_revenue'] = 0
    df_revenue = df_revenue.drop('qty_reg', 1)

    #flag=1
    df_revenue['true_adj_revenue'] = df_revenue['adj_revenue'].where(df_revenue['qty'] > df_revenue['baseline_sse'],df_revenue['revenue'])

    #flag=2
    df_revenue['true_adj_revenue'] = df_revenue['true_adj_revenue'].where(df_revenue['true_adj_revenue']>0,df_revenue['revenue'])
    

    df_revenue['flag'] = 0
    df_revenue['flag'] = df_revenue['flag'].where(df_revenue['qty'] > df_revenue['baseline_sse'], 'baseline equals to qty')
    df_revenue['flag'] = df_revenue['flag'].where(df_revenue['adj_revenue']>0, 'negative revenue check')

    #flag=3 outlier detection using standard deviation
    df_revenue['diff_per']=0
    df_revenue['diff_per']= df_revenue['diff_per'].where(df_revenue['flag']!=0,(df_revenue['revenue']-df_revenue['true_adj_revenue'])/df_revenue['revenue'])
    df_new = df_revenue[(df_revenue['flag'] == 0)]
    std=df_new.loc[:,'diff_per'].std()
    df_revenue['flag'] = df_revenue['flag'].where(df_revenue['diff_per'] < (df_revenue['diff_per'].mean() + 2*std),'outlier')
    df_revenue['true_adj_revenue']=df_revenue['true_adj_revenue'].where(df_revenue['flag']!='outlier',df_revenue['revenue'])

    df_revenue = df_revenue.drop('diff_per', 1)
    #print revenue only for items having quantity sold greater than 0
    df_revenue=df_revenue[ (df_revenue['qty'] >0)]
    df_revenue=df_revenue.rename({'c': 'comp','s':'sub'}, axis=1)
    df_revenue.to_csv("revenue_"+str(promo_week)+".csv", index=False)

import sys

def main():
    # print command line arguments
    for arg in sys.argv[1:]:
        print(arg)
    postprocess(sys.argv[1])
if __name__ == "__main__":
    main()
