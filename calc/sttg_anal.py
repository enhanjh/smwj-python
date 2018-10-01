# -*- coding: utf-8 -*-
"""
Created on Tue May 30 22:54:55 2017

@author: jihwan
"""

import mysql.connector as conn
from stat import db_config
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

plt.show()
pd.set_option('display.max_rows', None)

# db connection
cnx = conn.connect(**db_config.config)
cursor = cnx.cursor()

bm_q  = (' SELECT A.TRAN_DAY'
         '      , A.END'
         '   FROM MARKET_INDEX A'
         '  WHERE A.TRAN_DAY BETWEEN \'20160601\' AND \'20170530\''
         '    AND A.ITEM = \'001\''
         )


df_bm = pd.read_sql(bm_q,cnx)
df_bm.index = df_bm['TRAN_DAY']
del df_bm['TRAN_DAY']
df_bm['ret'] = (df_bm['END'] - df_bm['END'].shift(1) )/ df_bm['END'].shift(1) * 100

inv_q  = (' SELECT A.TRAN_DAY'
          '      , A.END'
          '   FROM PRICE A'
          '  WHERE A.TRAN_DAY BETWEEN \'20160601\' AND \'20170530\''
          '    AND A.ITEM = \'114800\''
          )


df_inv = pd.read_sql(inv_q,cnx)
df_inv.index = df_inv['TRAN_DAY']
del df_inv['TRAN_DAY']
df_inv['ret'] = (df_inv['END'] - df_inv['END'].shift(1)) / df_inv['END'].shift(1) * 100


ups_q = ('  SELECT A.TRAN_DAY'
	     '       , SUM(D.END) AS price1'
         '       , SUM(A.END) AS price2'
         '       , SUM(E.END) AS price3'
         '       , COUNT(*) AS cnt'
         '    FROM PRICE A'
         '	     , TRAN_DAY_CAL B'
         '       , TRAN_DAY_CAL C'
         '	     , PRICE D'
         '       , PRICE E'
         '	     , INVESTOR F'
         '   WHERE A.TRAN_DAY = B.TRAN_DAY'
         '	   AND A.TRAN_DAY = C.POOL_DAY'
         '	   AND A.ITEM     = D.ITEM'
         '	   AND A.ITEM     = E.ITEM'
         '	   AND B.POOL_DAY = D.TRAN_DAY'
         '     AND C.TRAN_DAY = E.TRAN_DAY'
         '     AND A.ITEM     = F.ITEM'
         '     AND A.TRAN_DAY = F.TRAN_DAY'
         '	   AND A.TRAN_DAY BETWEEN \'20160601\' AND \'20170530\''
         '     AND (A.END - D.END)/D.END*100 BETWEEN 0.5 AND 10'
         '     AND A.AVG_5 > A.END'
         '     AND A.AVG_10 > A.END'
         '     AND F.FORE > 0'
         '     AND F.INS > 0'
         '   GROUP BY A.TRAN_DAY'
        )

df_ups = pd.read_sql(ups_q,cnx)
df_ups['ret'] = (df_ups['price3'] - df_ups['price2']) / df_ups['price2'] * 100

df_ups.index = df_ups['TRAN_DAY']
del df_ups['TRAN_DAY']


df_bm['bm'] = df_bm['ret']
df_bm['inv'] = df_inv['ret']
df_bm['ups'] = df_ups['ret']

del df_bm['END']
del df_bm['ret']

pastRateSm1 = np.array([0.0, 0.0, 0.0])
pastRateSm2 = np.array([0.0, 0.0, 0.0])
pastColSm1 = ''
pastColSm2 = ''
nextInvest1 = 1
nextInvest2 = 1
df_temp = df_bm.copy()
df_bm['sm1'] = np.zeros(len(df_bm.index))
df_bm['sm2'] = np.zeros(len(df_bm.index))
df_bm['invest1'] = np.zeros(len(df_bm.index))
df_bm['invest2'] = np.zeros(len(df_bm.index))


for index, row in df_temp.iterrows() :
    ups = row['ups']
    bm = row['bm']
    inv = row['inv']

    #if pastColSm1 != '' :
        #print 'before ni1 : ' + str(nextInvest1) + ' ni2 : ' + str(nextInvest2)

    df_bm['invest1'][index] = int(nextInvest1)
    df_bm['invest2'][index] = int(nextInvest2)

    #print 'ni1 : ' + str(df_bm['invest1'][index]) + ' ni2 : ' + str(df_bm['invest2'][index])

    if index == '20160913':
        df_bm['sm1'][index] = ups
        df_bm['sm2'][index] = ups
        pastColSm1 = 'ups'
        pastColSm2 = 'ups'
    else :
        df_bm['sm1'][index] = row[pastColSm1]
        df_bm['sm2'][index] = row[pastColSm2]

        if row[pastColSm1] < 0.4 :
            nextInvest1 = nextInvest1 * 2
        else :
            nextInvest1 = 1

        if row[pastColSm2] < 0.4 :
            nextInvest2 = nextInvest2 * 2
        else :
            nextInvest2 = 1

        pastRateSm1[0] = pastRateSm1[1]
        pastRateSm1[1] = pastRateSm1[2]
        pastRateSm1[2] = row[pastColSm1]

        pastRateSm2[0] = pastRateSm2[1]
        pastRateSm2[1] = pastRateSm2[2]
        pastRateSm2[2] = row[pastColSm2]

        #if row[pastCol] < max([ups, bm, inv]) and (pastRate.sum() / 3) < -0.5:
        if (pastRateSm1.sum() / 3) < -0.5 :
            if bm > inv :
                if bm > ups :
                    pastColSm1 = 'bm'
                else :
                    pastColSm1 = 'ups'
            else :
                if inv > ups :
                    pastColSm1 = 'inv'
                else :
                    pastColSm1 = 'ups'

        if (pastRateSm2.sum() / 3) < -0.5 :
            if bm > inv :
                if inv > ups :
                    pastColSm2 = 'inv'
                else :
                    if bm > ups :
                        pastColSm2 = 'ups'
                    else :
                        pastColSm2 = 'bm'
            else :
                if bm > ups :
                    pastColSm2 = 'bm'
                else :
                    if inv > ups :
                        pastColSm2 = 'ups'
                    else :
                        pastColSm2 = 'inv'

    #print 'after ni1 : ' + str(nextInvest1) + ' ni2 : ' + str(nextInvest2)



df_bm['ret1'] = df_bm['invest1'] * (df_bm['sm1'] - 0.33)
df_bm['ret2'] = df_bm['invest2'] * (df_bm['sm2'] - 0.33)

print df_bm
print df_bm.describe()
print df_bm['ret1'].sum()
print df_bm['ret2'].sum()
#df_bm.plot()