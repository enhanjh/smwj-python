# -*- coding: utf-8 -*-
"""
Created on Sun Feb  5 19:29:14 2017

@author: jihwan
"""

import numpy as np
from stat import db_config
import mysql.connector as conn

# 2. 배열 합치기
# 3. 통계값 계산

cnx = conn.connect(**db_config.config)
cursor = cnx.cursor()

# 0. 특정구간 선택
dateList = (" SELECT TRAN_DAY"
            "   FROM TRAN_DAY_CAL"
            "  WHERE TRAN_DAY BETWEEN '20170101' AND '20170131'"
)    

cursor.execute(dateList)
    
rows = cursor.fetchall()

avgList = list()

# 1. 특정일의 30개 종목 정보 조회
for row in rows :

    date = row[0]

    daily  = (" SELECT B.TRAN_DAY "
              "      , AVG(B.RATE) AS RATE "
              "   FROM PRICE A "
              "      , PRICE_SMMRY B "
              "      , TRAN_DAY_CAL C "
              "  WHERE A.TRAN_DAY = C.POOL_DAY "
              "    AND B.TRAN_DAY = C.TRAN_DAY "
              "    AND A.ITEM = B.ITEM "
              "    AND B.TRAN_DAY = %(date)s "
              "    AND A.END < 100000 "
              "    AND B.STATE NOT LIKE '%관리종목%' "
              "  GROUP BY B.TRAN_DAY "
              "  ORDER BY A.TRAN_TOT DESC "
              "  LIMIT 30 "
    )

    var_daily = {
        'date' : date
    }      

    cursor.execute(daily, var_daily)
        
    rslt = cursor.fetchall()
    
    if len(rslt) > 0 :
        #print rslt[0][1]
        avgList.append(rslt[0][1])
    
print  '평균 : ', np.average(avgList)




"""


#df_all = DataFrame([df_ind_q[1][i] for i in range(len(df_ind_q))], columns=['kosdaq'], index=[df_ind_q[0]])


df_inv_q = pd.read_sql(inv_q,cnx,'DATE')
df_basic_q = pd.read_sql(basic_q,cnx,'DATE')

df_all['inv'] = df_inv_q
df_all['idx'] = df_basic_q

values = df_all.inv + df_all.idx

df_all['sum'] = values

print df_all

df_all.cumsum().plot(figsize=(10,5))


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


