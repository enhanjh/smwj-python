import smwjsql.query as query
import pandas as pd


fore_tr_amt_param = {
    'item': 'kospi',
    'sdate': '20100101',
    'edate': '20181111'
}

result = pd.read_sql(query.fore_tr_amt.format(**fore_tr_amt_param), query.engine, index_col='tran_day')

anal = result.copy()

anal['fore_15'] = anal['fore'].ewm(com=15).mean()
anal['fore_30'] = anal['fore'].ewm(com=30).mean()
anal['fore_60'] = anal['fore'].ewm(com=60).mean()

anal.plot()

