import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import smwjsql.query as query
import pandas as pd


def market_index_tr_amt_chart(item, sdate, edate):
    market_index_tr_amt_param = {
        'item': item,
        'sdate': sdate,
        'edate': edate
    }

    result = pd.read_sql(query.market_index_tr_amt.format(**market_index_tr_amt_param), query.engine, index_col='tran_day')

    kospi = result['kospi_close'].values
    kospi = kospi[61:]

    tr_amt = result.drop('kospi_close', 1)
    tr_amt = tr_amt.rolling(20).sum()
    tr_amt = tr_amt[61:]

    #tr_amt.head(10)

    tick_spacing = 30
    fig, ax1 = plt.subplots()
    lineobject = ax1.plot(tr_amt)
    ax1.set_ylabel('tr_amt')
    plt.xticks(rotation=70)
    ax1.xaxis.set_major_locator(ticker.MultipleLocator(tick_spacing))

    ax2 = ax1.twinx()
    ax2.plot(kospi, 'r')
    ax2.set_ylabel('kospi')

    fig.tight_layout()
    plt.legend(iter(lineobject), tr_amt.columns.values, loc='upper right')
    plt.show()

market_index_tr_amt_chart('kospi', '20171110', '20181111')
