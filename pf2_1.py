# -*- coding: utf-8 -*-
"""
Created on Mon Nov 14 21:07:20 2016

@name  : pf2_1.py
@author: jihwan
@pf2   : 시가 매수 후 5영업일 후 매도(수익률에 따라 매도 시기 변경)
"""

import exTimer
import sys
import logging
import logging.handlers
import math
import datetime
import urllib2
import json
import dbConfig
import mysql.connector as conn
from time import localtime, strftime
from logging.handlers import TimedRotatingFileHandler

_today = datetime.date.today()
_url = "http://10.0.1.8:8082/"

# 실행여부
_updateTranSimul = False
_insertBuyingItem = False
_retDbInterestItem = False

# DB connection
_cnx = conn.connect(**dbConfig.config)
_cursor = _cnx.cursor()

# logger instance
_logger = logging.getLogger("myLogger")
_formatter = logging.Formatter('[%(levelname)s:%(lineno)s] %(asctime)s > %(message)s')
_fh = TimedRotatingFileHandler("C:\SMWJ_SIMUL_LOG\pf2_1", when="midnight")
_fh.setFormatter(_formatter)
_fh.suffix = "_%Y%m%d.log"
_logger.addHandler(_fh)
_logger.setLevel(logging.DEBUG)

"""
_cursor.close()
_cnx.close()
"""
# 타이머 핸들러
def watchHandler():
    
    lTime = int(strftime('%H%M%S', localtime()))
    sToday = _today.strftime('%Y%m%d')
    
    global _updateTranSimul
    global _insertBuyingItem
    global _retDbInterestItem
    
    # 미청산종목 업데이트
    if lTime >= 85500 and lTime < 90000 :
        if _updateTranSimul == False :
            updateTranSimul(sToday)
            
            _updateTranSimul = True            
    # 매수가정
    elif lTime >= 90000 and lTime < 90200 :
        if _insertBuyingItem == False :
            insertBuyingItem(sToday)

            _insertBuyingItem = True
    # 오전1매도판단
    elif lTime >= 90200 and lTime < 90700 :
        retDbInterestItem(sToday, lTime)
    # 오전2매도판단
    elif lTime >= 90700 and lTime <= 93000 :
        retDbInterestItem(sToday, lTime)
    # 오후매도판단
    elif lTime >= 151700 and lTime <= 152000 :
        if _retDbInterestItem == False :
            retDbInterestItem(sToday, lTime)
            
            _retDbInterestItem = True
    # 프로그램종료
    elif lTime >= 160000 :
        sys.exit()
        
        
# 미청산종목 일자 업데이트
def updateTranSimul(sToday) :
    
    _logger.info("updateTranSimul")
    
    update_tran = (" UPDATE TRAN_SIMUL "
                   "    SET TRAN_DAY = %(today)s"
                   "  WHERE SIMUL    = 3"
                   "    AND TRAN_SP  = 2"
                   "    AND TRAN_DAY = (SELECT MAX(A.TRAN_DAY)"
                   "                      FROM (SELECT * FROM TRAN_SIMUL) A"
                   "                     WHERE A.TRAN_DAY < %(today)s"
                   "                    )"               
                  )                    
    
    update_tran_data = {
        'today' : sToday,
        }      
    
    _cursor.execute(update_tran, update_tran_data)

    _logger.debug(_cursor.statement)
    
    _cnx.commit()


# 매수대상 종목 DB 인서트
def insertBuyingItem(sToday) :
    
    _logger.info("insertBuyingItem")
        
    select_pool = (" SELECT A.ITEM"
                   "      , A.BEGIN_PRICE"
                   "      , FLOOR(150000 / A.BEGIN_PRICE) AS BUY_CNT"
                   "   FROM POOL A"
                   "      , PRICE B"                 
                   "  WHERE A.ITEM = B.ITEM"
                   "    AND A.TRAN_DAY = B.TRAN_DAY"
                   "    AND A.ITEM NOT IN (SELECT AA.ITEM"
                   "                         FROM TRAN_SIMUL AA"
                   "                        WHERE AA.TRAN_DAY = '" + sToday + "'"
                   "                      )"
                   "    AND A.TRAN_DAY = (SELECT MAX(BB.TRAN_DAY)"
                   "                        FROM POOL BB"
                   "                       WHERE BB.TRAN_DAY < '" + sToday + "'"
                   "                     )"
                   "    AND A.BUY_INVOLVE_ITEM_YN = 'Y'"
                   "    AND A.MODE = 2"
                   "    AND B.AVG_5 > B.AVG_10"
                   "    AND B.AVG_10 > B.AVG_20"
                   "    AND A.BEGIN_PRICE <= 150000"
                   "    AND A.BEGIN_PRICE > 0"                   
    )    
    
    _cursor.execute(select_pool)
    
    rows = _cursor.fetchall()
    
    insert_buying_item = (" INSERT INTO TRAN_SIMUL "
                          "      ( SIMUL"
                          "      , ITEM"
                          "      , TRAN_DAY"
                          "      , TRAN_ID"
                          "      , MODE"
                          "      , TRAN_SP"
                          "      , BUY_PRICE"
                          "      , BUY_CNT"
                          "      , BUY_FEE"
                          "      , BUY_AMT"
                          "      , TIME_2"
                          "      )"
                          " VALUES "
                          "      ( 3"
                          "      , %(item)s"
                          "      , %(tranDay)s"
                          "      , %(tranId)s"
                          "      , 2"
                          "      , 2"
                          "      , %(buyPrice)s"
                          "      , %(buyCnt)s"
                          "      , %(buyFee)s"
                          "      , %(buyAmt)s"
                          "      , NOW()"
                          "      ) "
                          )       
    
    for row in rows :
        item = row[0]
        beginPrice = int(row[1])
        buyCnt = int(row[2])
        buyTot = beginPrice * buyCnt
        
        data_buying_item = {
            'item' : item,
            'tranDay' : sToday,
            'tranId' : sToday + "-" + item,
            'buyPrice' : beginPrice,
            'buyCnt' : buyCnt,
            'buyFee' : (math.floor(buyTot * 0.000015) * 10),
            'buyAmt' : buyTot + (math.floor(buyTot * 0.000015) * 10)
            }
        _cursor.execute(insert_buying_item, data_buying_item)
        
        _logger.debug(_cursor.statement)
            
        _cnx.commit()


# DB 매도대상 조회
def retDbInterestItem(sToday, lTime) :
    
    _logger.info("retDbInterestItem")
    
    select_tran = (" SELECT A.ITEM"
                   "      , A.BUY_CNT"
                   "      , A.BUY_AMT"
                   "      , B.SEQ - C.SEQ AS SEQ"
                   "      , A.BUY_PRICE"
                   "   FROM TRAN_SIMUL A"
                   "      , TRAN_DAY_CAL B"
                   "      , TRAN_DAY_CAL C"
                   "  WHERE A.TRAN_DAY = B.TRAN_DAY"
                   "    AND SUBSTR(A.TRAN_ID,1,8) = C.TRAN_DAY"
                   "    AND A.TRAN_DAY = '" + sToday + "'"
                   "    AND A.SIMUL    = 3"
                   "    AND A.MODE     = 2"
                   "    AND A.TRAN_SP  = 2"
    )
    
    _cursor.execute(select_tran)

    _logger.debug(_cursor.statement)
    
    rows = _cursor.fetchall()

    if _cursor.rowcount > 0 :
        retInterestItem(rows, sToday, lTime)


# 매도대상 가격 조회
def retInterestItem(params, sToday, lTime) :

    _logger.info("retInterestItem")    
    
    items = ""
    itemDict = dict()

    
    for row in params :

        info = {
            'buyCnt' : row[1],
            'buyAmt' : row[2],
            'seq' : row[3],
            'buyPrc' : row[4]
        }
     
        itemDict[row[0]] = info
        
        _logger.debug(row[0] + " : " + str(row[3]) + " , " + str(row[4]))
                
        items += row[0]
        items += ";"
        
    query = "1005"

    content = urllib2.urlopen(_url + query + "&" + items).read()
    
    rows = json.loads(content)
    
    _logger.debug(rows)

    if len(rows) > 0 :
        retSellingItem(rows,itemDict,sToday,lTime)    


# 매도여부 판단
def retSellingItem(rows,itemDict,sToday,lTime) :
    
    _logger.info("retSellingItem")    

    for row in rows :
        item = row.get('item')
        price = row.get('price')
        seq = itemDict[item]['seq']
        buyCnt = itemDict[item]['buyCnt']
        buyAmt = itemDict[item]['buyAmt']        
        buyPrc = itemDict[item]['buyPrc']
        
        rate = round(int(price)/int(buyPrc),4)
        
        if lTime > 151700 :         
            if int(seq) == 5 :
                updateSellingItem(item,price,buyCnt,buyAmt,sToday)
            else :
                if rate > 1.04 :
                    updateSellingItem(item,price,buyCnt,buyAmt,sToday)
                elif rate <= 1.02 and rate > 1.0033 :
                    updateSellingItem(item,price,buyCnt,buyAmt,sToday)
        elif lTime > 90000 and lTime <= 90700 :
            if rate > 1.04 :
                updateSellingItem(item,price,buyCnt,buyAmt,sToday)
        elif lTime > 90700 and lTime <= 93000 :
            if rate <= 1.02 and rate > 1.0033 :
                updateSellingItem(item,price,buyCnt,buyAmt,sToday)
    
    
# 매도
def updateSellingItem(item,price,buyCnt,buyAmt,sToday) :
    
    _logger.info("updateSellingItem")    
    
    insert_selling_item = (" UPDATE TRAN_SIMUL "
                           "    SET TRAN_SP    = 4"
                           "      , SELL_PRICE = %(sellPrice)s"
                           "      , SELL_CNT   = BUY_CNT"
                           "      , SELL_FEE   = %(sellFee)s"
                           "      , SELL_TAX   = %(sellTax)s"
                           "      , SELL_AMT   = %(sellAmt)s"
                           "      , INCOME     = %(income)s"
                           "      , FINAL_RATE = %(finalRate)s"
                           "      , TIME_4     = NOW()"
                           "  WHERE SIMUL    = 3"
                           "    AND ITEM     = %(item)s"
                           "    AND TRAN_DAY = %(tranDay)s"
                           "    AND TRAN_SP  = 2"
                          ) 
                          
    sellTot = price * buyCnt
    sellFee = math.floor(sellTot * 0.00015 / 10) * 10
    sellTax = math.floor(sellTot * 0.003)
    sellAmt = sellTot - sellFee - sellTax
    finalRate = round((sellAmt-buyAmt)/buyAmt*100,2)
    
    data_selling_item = {
        'sellPrice' : price,
        'sellFee' : sellFee,
        'sellTax' : sellTax,
        'sellAmt' : sellAmt,
        'income' : sellAmt - buyAmt,
        'finalRate' : finalRate,
        'item' : item,
        'tranDay' : sToday
        }

    _cursor.execute(insert_selling_item, data_selling_item)
    
    _logger.debug(_cursor.statement)

    _cnx.commit()
    

#updateTranSimul("20161104")
#insertBuyingItem("20161121")
#retDbInterestItem("20170106")

# 타이머 스타터
th = exTimer.exTimer()
th.setHandler(watchHandler)
th.setDelay(30)
th.start()