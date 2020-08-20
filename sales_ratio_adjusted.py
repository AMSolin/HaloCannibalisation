import pandas as pd

def sales(promo_week):

    df_cs = pd.read_csv("item_comp_sub_"+str(promo_week)+".csv")
    df_cs = df_cs.drop_duplicates()
    df_cs = df_cs.dropna(how='any', axis=0)
    # df_cs_base = pd.unique(df_cs['key_term'])

    df_sale = pd.read_csv("sales_"+str(promo_week)+".csv")
    df_sale = df_sale.rename({'itm_nbr': 'key_term'}, axis=1)
    df_sale['key_term']=df_sale['key_term'].astype(str)
    df_cs['key_term'] = df_cs['key_term'].astype(str)
    df_cs['itm_nbr'] = df_cs['itm_nbr'].astype(int)

    df_b=pd.merge(df_cs,df_sale,on='key_term')

    df_sale = df_sale.rename({'key_term': 'itm_nbr'}, axis=1)

    df_b['itm_nbr'] = df_b['itm_nbr'].astype(str)
    df_sale['itm_nbr'] = df_sale['itm_nbr'].astype(str)
    df_b = df_b.rename({'tot_sal_amt': 'key_term_tot_sal','tot_itm_qty': 'key_term_itm_qty','tpr':'key_tpr'}, axis=1)

    # print(df_sale)
    df_b = pd.merge(df_b, df_sale, on=['itm_nbr','week'])

    df_b = df_b.rename({'tot_sal_amt': 'itm_nbr_tot_sal', 'tot_itm_qty': 'itm_nbr_itm_qty','tpr':'itm_nbr_tpr'}, axis=1)
   #adding average sales_ratio logic instead of trainig
    #df_b=df_b[(df_b['key_term_itm_qty'] >0) & (df_b['itm_nbr_itm_qty'] >0)  & (df_b['key_tpr'] >0) & (df_b['itm_nbr_tpr'] >0)]
    #df_b['sales_ratio']= df_b['key_term_itm_qty']/df_b['itm_nbr_itm_qty']
    df_b.to_csv("base_cs_sale_"+str(promo_week)+".csv")

    #df_b = df_b.groupby(['key_term','itm_nbr','type'], as_index=False)['sales_ratio'].mean()
    #df_b.to_csv("coefficients_mean_"+str(promo_week)+".csv")



def sales_ratio(promo_week):
    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LinearRegression
    from sklearn import metrics

    dataset=pd.read_csv("base_cs_sale_"+str(promo_week)+".csv")
    dataset=dataset.dropna(how='any', axis=0)

    df_grouped = dataset.groupby(['key_term','itm_nbr','type'], as_index=False)['rank'].sum()


    df_ratio = pd.DataFrame(
        columns=['key_term', 'itm_nbr', 'type', 'intercept', 'coefficient'])

    for row in df_grouped.itertuples():
        dataset_new=dataset[(dataset['key_term'] == row.key_term) & (dataset['itm_nbr'] == row.itm_nbr) & (dataset['type'] == row.type)]

        intercept=0
        coefficient=0
        if(dataset_new['key_term'].count() > 4):
            X = dataset_new['itm_nbr_itm_qty'].values.reshape(-1,1)
            y = dataset_new['key_term_itm_qty'].values.reshape(-1,1)

            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=0)
            regressor = LinearRegression()
            regressor.fit(X_train, y_train) #training the algorithm

            #To retrieve the intercept:
            intercept=regressor.intercept_
            intercept=intercept[-1]
            #For retrieving the slope:
            coefficient=regressor.coef_
            coefficient=coefficient[-1][-1]

        row = {'key_term':row.key_term,'itm_nbr': row.itm_nbr, 'type': row.type, 'intercept': intercept, 'coefficient': coefficient}
        df_ratio = df_ratio.append(row, ignore_index=True)



    df_ratio.to_csv("coefficients_"+str(promo_week)+".csv")

import sys

def main():
    # print command line arguments
    for arg in sys.argv[1:]:
        print(arg)
    sales(sys.argv[1])
    sales_ratio(sys.argv[1])
if __name__ == "__main__":
    main()
