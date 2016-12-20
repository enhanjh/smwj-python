# -*- coding: utf-8 -*-
"""
Created on Sun Nov 27 19:49:10 2016

@author: jihwan
"""
import pandas as pd
import numpy as np
import mysql.connector as conn
from pandas import DataFrame


cnx = conn.connect(host='221.151.62.212',user='smwjwas',password='qwer0802',database='smwj')
cursor = cnx.cursor()

item = '^KQ11' # kosdaq
inv = '251340' # kodex kosdaq 150 inverse
basic = '229200' # kodex kosdaq 150
date = '20161001'

ind_q  = (" SELECT C.TRAN_DAY AS DATE"
          "      , ROUND((C.END-A.END)/A.END*100,2) AS RATE"
          "   FROM MARKET_INDEX A"
          "      , TRAN_DAY_CAL B"
          "      , MARKET_INDEX C"
          "  WHERE A.TRAN_DAY = B.POOL_DAY"
          "    AND B.TRAN_DAY = C.TRAN_DAY"
          "    AND A.ITEM = C.ITEM"
          "    AND A.ITEM = '" + item + "'"          
          "    AND C.TRAN_DAY >= '" + date + "'"          
)

inv_q = (" SELECT B.TRAN_DAY AS DATE"
          "     , B.RATE AS RATE"
          "   FROM PRICE_SMMRY B"
          "  WHERE B.ITEM = '" + inv + "'"
          "    AND B.TRAN_DAY >= '" + date + "'"
)    

basic_q = (" SELECT B.TRAN_DAY AS DATE"
           "      , B.RATE AS RATE"
           "   FROM PRICE_SMMRY B"
           "  WHERE B.ITEM = '" + basic + "'"
           "    AND B.TRAN_DAY >= '" + date + "'"
)    

#df_all = DataFrame([df_ind_q[1][i] for i in range(len(df_ind_q))], columns=['kosdaq'], index=[df_ind_q[0]])

df_all = pd.read_sql(ind_q,cnx,'DATE')
df_inv_q = pd.read_sql(inv_q,cnx,'DATE')
df_basic_q = pd.read_sql(basic_q,cnx,'DATE')

df_all['inv'] = df_inv_q
df_all['idx'] = df_basic_q

values = df_all.inv + df_all.idx

df_all['sum'] = values

print df_all

df_all.cumsum().plot(figsize=(10,5))

"""
np_array = df_all.values

m = np_array[:,0]
i = np_array[:,1]
b = np_array[:,2]


covariance_i = np.cov(i,m)
covariance_b = np.cov(b,m)

print covariance_i
print covariance_b

beta_i = covariance_i[0,1]/covariance_i[1,1]
beta_b = covariance_b[0,1]/covariance_b[1,1]

print beta_i
print beta_b
"""


