# -*- coding: utf-8 -*-
"""
Created on Sun May 28 10:01:12 2017

@author: jihwan
"""

import mysql.connector as conn
import unicodecsv
from stat import db_config
import os 

def get_csv_reader(filename): 
    reader = [] 
    
    if not os.path.isfile(filename): 
        csvfile = open(filename, "w") 
    else: 
        csvfile = open(filename, "rb") 
        
    reader = unicodecsv.reader(csvfile, encoding='utf-8') 
    
    return list(reader)


# db connection
cnx = conn.connect(**db_config.config)
cursor = cnx.cursor()

add_market_cap = (" INSERT INTO PRICE_SMMRY "
                  "      ( ITEM"
                  "      , TRAN_DAY"
                  "      , ITEM_NM"
                  "      , PRICE"
                  "      , PRC_DIFF"
                  "      , RATE"
                  "      , TRAN_AMT"
                  "      , MARKET_CAP"
                  "      , SHARE_NUM"
                  "      )"
                  " VALUES "
                  "      ( %(item)s"
                  "      , %(tranDay)s"
                  "      , %(itemNm)s"
                  "      , REPLACE(%(price)s, ',', '')"
                  "      , REPLACE(%(prcDiff)s, ',', '')"
                  "      , %(rate)s"
                  "      , REPLACE(%(tranAmt)s, ',', '')"
                  "      , ROUND(REPLACE(%(marketCap)s, ',', '') / 100000000, 0)"
                  "      , ROUND(REPLACE(%(shareNum)s, ',', '') / 1000, 0)"
                  "      ) "
                    )                    

ymd = '20170526'

rslt = get_csv_reader('./' + ymd + '.csv')

cnt = 0

for line in rslt:
    
    if cnt == 0 :
        cnt = cnt + 1
        continue
    
    data_market_cap = {
        'item'      : line[1],
        'tranDay'   : ymd,
        'itemNm'    : line[2],
        'price'     : line[3],
        'prcDiff'   : line[4],
        'rate'      : line[5],
        'tranAmt'   : line[6],
        'marketCap' : line[8],
        'shareNum'  : line[10],
        }      

    print data_market_cap
    
    cursor.execute(add_market_cap, data_market_cap)
    cnx.commit()

cursor.close()
cnx.close()
