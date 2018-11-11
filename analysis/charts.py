import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import smwjsql.query as query
import numpy as np
import pandas as pd


def basic_chart(item, sdate, edate):
    item_price_param = {
        'item': item,
        'sdate': sdate,
        'edate': edate
    }

    result = pd.read_sql(query.item_price.format(**item_price_param), query.engine, index_col='tran_day')
    result = result.drop('item', 1)
    result.head(5)

    tick_spacing = 10
    fig, ax = plt.subplots(1, 1)
    ax.plot(result.index.values, result['close'])
    ax.xaxis.set_major_locator(ticker.MultipleLocator(tick_spacing))
    plt.xticks(rotation=70)
    plt.show()


def multi_item_chart(items, sdate, edate):
    items_price = pd.DataFrame()
    for item in items:
        item_price_param = {
            'item': item,
            'sdate': sdate,
            'edate': edate
        }

        temp = pd.read_sql(query.multi_item_price.format(**item_price_param), query.engine, index_col='tran_day')
        # using logarithm rate for better comparison
        temp[item] = np.log(temp[item]) - np.log(temp[item].shift(1))

        if len(items_price) <= 0:
            items_price = temp
        else:
            items_price = items_price.join(temp, on='tran_day')

    items_price = items_price[1:]
    items_price = items_price.cumsum()
    #items_price.head(5)

    tick_spacing = 10
    fig, ax = plt.subplots(1, 1)
    lineobject = ax.plot(items_price)
    plt.xticks(items_price.index.values, rotation=70)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(tick_spacing))
    plt.legend(iter(lineobject), items_price.columns.values, loc='upper right')
    plt.show()

basic_chart('122630', '20171110', '20181109')
multi_item_chart(['122630', '176950'], '20171110', '20181109')


