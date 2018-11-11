import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import smwjsql.query as query
import pandas as pd


def money_chart(sdate, edate):
    money_param = {
        'sdate': sdate,
        'edate': edate
    }

    result = pd.read_sql(query.money.format(**money_param), query.engine, index_col='tran_day')
    result.head(5)

    kospi = result['kospi_close'].values
    result = result.drop('kospi_close', 1)

    tick_spacing = 10
    fig, ax1 = plt.subplots()
    lineobject = ax1.plot(result)
    ax1.set_ylabel('money')
    plt.xticks(rotation=70)
    ax1.xaxis.set_major_locator(ticker.MultipleLocator(tick_spacing))

    ax2 = ax1.twinx()
    ax2.plot(kospi)
    ax2.set_ylabel('kospi')

    fig.tight_layout()
    plt.legend(iter(lineobject), result.columns.values, loc='upper right')
    plt.show()

money_chart('20171110', '20181109')