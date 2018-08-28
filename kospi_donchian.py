import mysql.connector as conn
import db_config
import pandas as pd
import numpy as np
from datetime import datetime

# installed package
# 1. mysql connector
# 2. pandas
# 3. pandas datareader

# db connection
cnx = conn.connect(**db_config.config)
cursor = cnx.cursor()


# 0. WHAT TO BUY

date = '20180201'
item_cd = '122630'
# 122630 kodex leverage
# 252670 kodex kospi 200 future inverse 2x
# 234790 kodex kosdaq 150 leverage

qs_end_avg = (' select a.tran_day'
              '      , a.item'
              '      , lag(a.end) over(partition by a.item order by a.tran_day) as yday_end'
              '      , a.high'
              '      , a.low'
              '      , a.end'
#              '      , a.avg_20'
#              '      , a.avg_120'
              '   from price a'
              '  where a.tran_day between date_format(date_sub(\'' + date + '\', interval 1 month),\'%Y%m%d\') and \'' + date + '\''
              '   and a.item = \'' + item_cd + '\''
              )

#print(qs_end_avg)

df_end_avg = pd.read_sql(qs_end_avg, cnx)

df_end_avg["tr1"] = df_end_avg["high"]-df_end_avg["low"]
df_end_avg["tr2"] = df_end_avg["high"]-df_end_avg["yday_end"]
df_end_avg["tr3"] = df_end_avg["yday_end"]-df_end_avg["low"]
df_end_avg["true_range"] = df_end_avg[["tr1","tr2","tr3"]].max(axis=1)
df_end_avg["N"] = (19 * df_end_avg["yday_end"] + df_end_avg["true_range"]) / 20

print(df_end_avg)

#nda_end_avg = df_end_avg.values

