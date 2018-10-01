# -*- coding: utf-8 -*-
"""
Spyder Editor

기본 패키지
1. anaconda
-----------
추가 설치 패키지
1. pandas_datareader
2. mysql connector
"""

import pandas_datareader.data as web
import mysql.connector as conn
from datetime import datetime, timedelta

"""
start = datetime(2016, 8, 22)
"""

end = datetime.now()
start = end - timedelta(days=7)

cnx = conn.connect(host='10.0.1.7', user='smwjwas', password='qwer0802', database='smwj')
cursor = cnx.cursor()

add_market_index = (" INSERT INTO MARKET_INDEX "
                    "      ( ITEM"
                    "      , TRAN_DAY"
                    "      , BEGIN"
                    "      , LOW"
                    "      , HIGH"
                    "      , END"
                    "      , WORK_MAN"
                    "      , WORK_TIME"
                    "      )"
                    " VALUES "
                    "      ( %(item)s"
                    "      , %(tranDay)s"
                    "      , %(begin)s"
                    "      , %(low)s"
                    "      , %(high)s"
                    "      , %(end)s"
                    "      , 'P'"
                    "      , NOW()"
                    "      ) "
                    "     ON DUPLICATE KEY "
                    " UPDATE BEGIN     = %(begin)s"
                    "      , LOW       = %(low)s"
                    "      , HIGH      = %(high)s"
                    "      , END       = %(end)s"
                    "      , WORK_MAN  = 'P'"
                    "      , WORK_TIME = NOW()"
                    )

itemList = ['^KS11', '^KQ11', '^DJI', '^IXIC', '^GSPC', '^N225', '^GDAXI', 'KRW=X']

for itemCd in itemList:
    print itemCd
    #rslt = web.DataReader(itemCd, 'yahoo', start, end)
    rslt = web.DataReader(itemCd, 'yahoo', '20170510', '20170512')
    print rslt
    row = rslt.tail(1)
    data_market_index = {
        'item': itemCd,
        'tranDay': row.index[0].strftime('%Y%m%d'),
        'begin': round(row['Open'], 2),
        'low': round(row['Low'], 2),
        'high': round(row['High'], 2),
        'end': round(row['Close'], 2),
    }

    print data_market_index
    cursor.execute(add_market_index, data_market_index)
    cnx.commit()

cursor.close()
cnx.close()