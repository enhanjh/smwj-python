# -*- coding: utf-8 -*-
"""
Created on Mon May 29 22:07:57 2017

@author: jihwan
"""

import mysql.connector as conn
from stat import db_config
import pandas as pd
import numpy as np


# db connection
cnx = conn.connect(**db_config.config)
cursor = cnx.cursor()

item_q  = (" SELECT ITEM"
          "    FROM ITEM"
          "   WHERE ITEM_SP IN ('P','D')"
          "     AND USE_YN = 'Y'"
          #" LIMIT 1"
)


df_item = pd.read_sql(item_q,cnx)
arr_date = ['20161212']

for date in arr_date :
    for index, row in df_item.iterrows():

        item = row['ITEM']
        price_q  = (" SELECT TRAN_DAY"
                    "      , END"
                    "   FROM PRICE A"
                    "  WHERE A.ITEM = '" + item + "'"
                    "    AND A.TRAN_DAY <= '" + date + "'"
                    "  ORDER BY A.TRAN_DAY DESC"                
                    "  LIMIT 120"
                    )

        df_price = pd.read_sql(price_q,cnx)
        dfCnt = df_price.count()

        if dfCnt['END'] < 120 :
            continue

        cumsum = np.cumsum(np.insert(df_price["END"].tolist(),0,0))
        avg5 = cumsum[5] / 5
        avg10 = cumsum[10] / 10
        avg20 = cumsum[20] / 20
        avg60 = cumsum[60]/ 60
        avg90 = cumsum[90] / 90
        avg120 = cumsum[120] / 120
        tranDay = df_price["TRAN_DAY"][0]
        price = df_price["END"][0]

        insert_avg_dsp = (" UPDATE PRICE "
                          "    SET AVG_5   = %(avg5)s "
                          "      , AVG_10  = %(avg10)s "
                          "      , AVG_20  = %(avg20)s "
                          "      , AVG_60  = %(avg60)s "
                          "      , AVG_90  = %(avg90)s "
                          "      , AVG_120 = %(avg120)s "
                          "      , DSP_5   = ROUND(%(avg5)s/%(price)s*100,2) "
                          "      , DSP_10  = ROUND(%(avg10)s/%(price)s*100,2) "
                          "      , DSP_20  = ROUND(%(avg20)s/%(price)s*100,2) "
                          "      , DSP_60  = ROUND(%(avg60)s/%(price)s*100,2) "
                          "      , DSP_90  = ROUND(%(avg90)s/%(price)s*100,2) "
                          "      , DSP_120 = ROUND(%(avg120)s/%(price)s*100,2) "
                          "  WHERE ITEM     = %(item)s "
                          "    AND TRAN_DAY = %(tranDay)s "
                              )

        data_avg_dsp = {
            'avg5' : int(avg5),
            'avg10' : int(avg10),
            'avg20' : int(avg20),
            'avg60' : int(avg60),
            'avg90' : int(avg90),
            'avg120' : int(avg120),
            'price' : int(price),
            'item' : item,
            'tranDay' : tranDay
            }

        cursor.execute(insert_avg_dsp, data_avg_dsp)
        cnx.commit()