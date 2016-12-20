# -*- coding: utf-8 -*-
"""
Created on Sat Nov 19 21:16:20 2016

@author: jihwan
"""


import numpy as np
import pandas_datareader.data as web
import mysql.connector as conn
import pytz
import matplotlib as mpl
import matplotlib.pyplot as plt
from datetime import datetime
from tzlocal import get_localzone

local_tz = get_localzone()
local_dt = local_tz.localize(datetime.now())
utc_dt = local_dt.astimezone(pytz.UTC)
edt_dt = utc_dt.astimezone(pytz.timezone('US/Eastern'))

"""
start = datetime(2016, 8, 22)
"""
start = edt_dt
end = edt_dt

"""

kospi = web.DataReader('^KS11', 'yahoo', start, end)
kospi['Close'].plot(style='--')
"""
sToday = datetime.today().strftime('%Y%m%d')

cnx = conn.connect(host='221.151.62.212',user='smwjwas',password='qwer0802',database='smwj')
cursor = cnx.cursor()

select_pool = (" SELECT A.ITEM"
               "      , A.TRAN_DAY"
               "      , A.PRICE"
               "      , A.CNT"
               "   FROM PRICE A"
               "  WHERE A.ITEM = '" + sToday + "'"
               "    AND A.TRAN_DAY BETWEEN '" + startDate + "' AND '" + endDate + "'"
               "                     )"
               #"    AND A.BUY_INVOLVE_ITEM_YN = 'Y'"
               "    AND A.MODE = 2"
)    

cursor.execute(select_pool)

rows = cursor.fetchall()

for row in rows:
    """
    for index, row in rslt.iterrows():
        data_market_index = {
            'item' : itemCd,
            'tranDay' : index.strftime('%Y%m%d'),
            'begin' : np.round(row['Open'],2),
            'low' : np.round(row['Low'],2),
            'high' : np.round(row['High'],2),
            'end' : np.round(row['Close'],2),
            }      
    """
    print row

cursor.close()
cnx.close()