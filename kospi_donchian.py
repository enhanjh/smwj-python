import mysql.connector as conn
import db_config
import pandas as pd
import numpy as np
from statistics import mean
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
              '      , a.begin'
              '      , a.high'
              '      , a.low'
              '      , a.end'
              '      , a.avg_60'
              '   from price a'
              '  where a.tran_day between date_format(date_sub(\'' + date + '\', interval 2 month),\'%Y%m%d\') and \'' + date + '\''
              '   and a.item = \'' + item_cd + '\''
              )

#print(qs_end_avg)

df_end_avg = pd.read_sql(qs_end_avg, cnx)


# 1. volatility
df_end_avg["tr1"] = df_end_avg["high"]-df_end_avg["low"]
df_end_avg["tr2"] = df_end_avg["high"]-df_end_avg["yday_end"]
df_end_avg["tr3"] = df_end_avg["yday_end"]-df_end_avg["low"]
df_end_avg["true_range"] = df_end_avg[["tr1","tr2","tr3"]].max(axis=1)

arr_tr = df_end_avg["true_range"].values
arr_n = list()
i = 0
pdn = 0
for temp in arr_tr:

    if i == 0 :
        arr_n.append(temp)
    else :
        arr_n.append((19 * pdn + temp) / 20)

    i += 1
    pdn = arr_n[-1]     # updatae latest pdn


#print(arr_n)
df_end_avg["N"] = arr_n
#print(df_end_avg)


# 2. position size

account_size = 1000000
df_end_avg["unit_size"] = round((0.01 * account_size) / df_end_avg["N"])
#print(df_end_avg)


# 3. entry signal
# using 60-day break-out entry (need to be adjusted to 55-day)

df_entry = df_end_avg.tail(2)
map_prev = df_entry.iloc[0]
map_next = df_entry.iloc[1]
#print(df_entry)
#print(map_prev)
#print(map_next)

buy_simul = (" insert into tran_simul "
             "      ( simul"
             "      , tran_day"
             "      , tran_id"
             "      , mode"
             "      , item"
             "      , tran_sp"
             "      , buy_price"
             "      , buy_cnt"
             "      , buy_tot"
             "      , buy_fee"
             "      , buy_amt"
             "      , time_2"
             "      )"
             " VALUES "
             "      ( 4"
             "      , %(tran_day)s"
             "      , %(tran_id)s"
             "      , 8"
             "      , %(item)s"
             "      , 2"
             "      , %(price)s"
             "      , %(unit_size)s"
             "      , %(price)s * %(unit_size)s"
             "      , floor(%(price)s * %(unit_size)s * 0.00015 / 10) * 10"
             "      , %(price)s * %(unit_size)s + floor(%(price)s * %(unit_size)s * 0.00015 / 10) * 10"
             "      , now()"
             "      ) "
             )

if int(map_prev["end"]) > int(map_prev["avg_60"]) :
    data_buy_simul = {
        'item': map_next["item"],
        'tran_id': map_next["tran_day"] + '-' + map_next["item"],
        'tran_day': map_next["tran_day"],
        'price': int(map_next["begin"]),
        'unit_size': int(map_next["unit_size"]),
    }

    print(data_buy_simul)

    cursor.execute(buy_simul, data_buy_simul)
    cnx.commit()

cursor.close()
cnx.close()

