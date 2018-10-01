import mysql.connector as conn
from stat import db_config
import pandas as pd

# installed package
# 1. mysql connector
# 2. pandas
# 3. pandas datareader

pd.set_option('display.max_columns', None)

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

df_entry = df_end_avg.tail(21)
map_prev = df_entry.iloc[19]
map_next = df_entry.iloc[20]
#print(df_entry)
print(map_prev)
#print(map_next)

# 3~4. previous transaction

qs_tran_yn = (" select tran_id"
              "   from tran_simul"
              "  where simul   = 4"
              "    and mode    = 8"
              "    and tran_sp = 2"
              "    and item    = '" + map_prev["item"] + "'"
              )

df_tran_yn = pd.read_sql(qs_tran_yn, cnx)
if df_tran_yn.empty :
    tran_id = ""
else :
    tran_id = df_tran_yn.iloc[0]["tran_id"]

#print(df_tran_yn)


if int(map_prev["end"]) > int(map_prev["avg_60"]) :

    if len(tran_id) <= 0 :

        qs_buy_simul = (" insert into tran_simul "
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

        data_buy_simul = {
            'item': map_next["item"],
            'tran_id': map_next["tran_day"] + '-' + map_next["item"],
            'tran_day': map_next["tran_day"],
            'price': int(map_next["begin"]),
            'unit_size': int(map_next["unit_size"]),
        }

        #print(data_buy_simul)

        cursor.execute(qs_buy_simul, data_buy_simul)
        cnx.commit()


# 4. stop or profit sell

qs_sell_simul = (" update tran_simul "
                 "    set tran_sp    = 4"
                 "      , sell_price = %(price)s"
                 "      , sell_cnt   = buy_cnt"
                 "      , sell_tot   = %(price)s * buy_cnt"
                 "      , sell_fee   = floor(%(price)s * buy_cnt * 0.00015 / 10) * 10"
                 "      , sell_amt   = %(price)s * buy_cnt + floor(%(price)s * buy_cnt * 0.00015 / 10) * 10"
                 "      , time_4     = now()"
                 "  where simul    = 4"
                 "    and tran_day = substr(%(tran_id)s, 1, 8)"
                 "    and tran_id  = %(tran_id)s"
                 "    and item     = substr(%(tran_id)s, 10, 6)"
                 "    and mode     = 8"
                 "    and tran_sp  = 2"
                 )

if len(tran_id) > 0 :

    diff = int(map_next["yday_end"]) - int(map_next["end"])
    df_sell = df_entry[:20]
    nn = int(round(map_prev["N"])) * -2
    end = int(map_prev["end"])
    min = df_sell["end"].min()

    # print(diff)
    # print(df_sell)
    # print(nn)
    # print(end)
    # print(min)

    # stop order
    if diff < nn :

        data_sell_simul = {
            'tran_id': tran_id,
            'tran_day': map_next["tran_day"],
            'price': int(map_next["begin"]),
        }

        # print(data_sell_simul)

        cursor.execute(qs_sell_simul, data_sell_simul)
        cnx.commit()

    # profit order
    elif min > end :

        data_sell_simul = {
            'tran_id': tran_id,
            'tran_day': map_next["tran_day"],
            'price': int(map_next["begin"]),
        }

        # print(data_sell_simul)

        cursor.execute(qs_sell_simul, data_sell_simul)
        cnx.commit()


cursor.close()
cnx.close()

