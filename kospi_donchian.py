import mysql.connector as conn
import dbConfig
import pandas as pd
import numpy as np

# db connection
cnx = conn.connect(**dbConfig.config)
cursor = cnx.cursor()


# 0. WHAT TO BUY

date = '20180102'
item_cd = '122630'
# 122630 kodex leverage
# 252670 kodex kospi 200 future inverse 2x
# 234790 kodex kosdaq 150 leverage

qs_end_avg = (' SELECT A.TRAN_DAY'
              '      , A.ITEM'
              '      , LAG(A.END) OVER(PARTITION BY A.ITEM, A.TRAN_DAY ORDER BY A.TRAN_DAY) AS YDAY_END'
              '      , A.HIGH'
              '      , A.LOW'
              '      , A.END'
              '      , A.AVG_20'
              '      , A.AVG_120'
              '   FROM PRICE A'
              '  WHERE A.TRAN_DAY IN '
              '                      SELECT POOL_DAY'
              '                        FROM TRAN_DAY_CAL'
              '                       WHERE TRAN_DAY = \''.join(date) + '\''
              '                       UNION ALL '                                                                        
              '                      SELECT TRAN_DAY'
              '                        FROM TRAN_DAY_CAL'
              '                       WHERE TRAN_DAY = \''.join(date) + '\''
              '    AND A.ITEM = \''.join(item_cd) + '\''
              )

print qs_end_avg

df_end_avg = pd.read_sql(qs_end_avg, cnx)

df_end_avg.head(5)
