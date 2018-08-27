import mysql.connector as conn
import db_config
import pandas as pd
import numpy as np
pd.core.common.is_list_like = pd.api.types.is_list_like
from pandas_datareader import data, wb
from datetime import datetime

# installed package
# 1. mysql connector
# 2. pandas
# 3. pandas datareader

# db connection
# cnx = conn.connect(**db_config.config)
# cursor = cnx.cursor()


# 0. WHAT TO BUY

date = '20180201'
item_cd = '122630'
# 122630 kodex leverage
# 252670 kodex kospi 200 future inverse 2x
# 234790 kodex kosdaq 150 leverage

# qs_end_avg = (' select tran_day'
#               '      , item'
#               '      , yday_end'
#               '      , high'
#               '      , low'
#               '      , end'
#               '      , avg_20'
#               '      , avg_120'
#               '   from ('
#               '         select a.tran_day'
#               '              , a.item'
#               '              , lag(a.end) over(partition by a.item order by a.tran_day) as yday_end'
#               '              , a.high'
#               '              , a.low'
#               '              , a.end'
#               '              , a.avg_20'
#               '              , a.avg_120'
#               '           from price a'
#               '          where a.tran_day in ('
#               '                               select pool_day'
#               '                                 from tran_day_cal'
#               '                                where tran_day = \'' + date + '\''
#               '                                union all'
#               '                               select tran_day'
#               '                                 from tran_day_cal'
#               '                                where tran_day = \'' + date + '\''
#               '                              )'
#               '           and a.item = \'' + item_cd + '\''
#               '        ) mst'
#               '  where tran_day = \'' + date + '\''
#               )
#
# print(qs_end_avg)
#
# df_end_avg = pd.read_sql(qs_end_avg, cnx)

df_end_avg = data.DataReader(
    "KRX:KOSPI" 
    ,"google"
    , datetime(2018, 7, 1)
    , datetime(2018, 8, 25)
)

print(df_end_avg)