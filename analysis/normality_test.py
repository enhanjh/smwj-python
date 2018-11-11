import smwjsql.query as query
import pandas as pd
from scipy import stats


def item_normality(item, sdate, edate):
    item_price_param = {
        'item': item,
        'sdate': sdate,
        'edate': edate
    }

    result = pd.read_sql(query.item_price.format(**item_price_param), query.engine)
    result['y_close'] = result['close'].shift(1)
    result['rate'] = round(result['close'] / result['y_close'] - 1, 3)
    result = result[1:]

    print("1. basic item stats")
    result.describe()
    result['rate'].hist(bins=70)

    print("2. sharpiro normaility test")
    stats.shapiro(result['rate'].fillna(0).values)

    sharpe_ratio = result['rate'].mean() / result['rate'].std() * 16
    print("3. annualized sharpe ratio")
    print(sharpe_ratio)


# kodex leverage
item_normality('122630', '20181109')
item_normality('122630', '20181109')
item_normality('122630', '20181109')
item_normality('122630', '20181109')
item_normality('122630', '20181109')