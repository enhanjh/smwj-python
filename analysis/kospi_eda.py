# python 3.6 32bit
# installed package
# 1. mysql-connector-python
# 2. sqlalchemy
# 3. odo and [datapipelines, networkx 1.11, cassiopeia]
# 4. pandas
# 5. matplotlib
# 6. sklearn

import logging
import sys
import time
import smwjsql.query as qu
import pandas as pd
import const.db_config as ic
import matplotlib.pyplot as plt
from logging.handlers import TimedRotatingFileHandler
from sqlalchemy import create_engine
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


def logger_start():
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    formatter = logging.Formatter('[%(levelname)s:%(lineno)s] %(asctime)s > %(message)s')
    logger = logging.getLogger()

    fh = TimedRotatingFileHandler("/Users/enhanjh/Documents/smwj_log/analyze", when="midnight")
    fh.setFormatter(formatter)
    fh.suffix = "_%Y%m%d.log"

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.setLevel(logging.DEBUG)

    return logger


def orm_init():
    scott = ic.config["user"]
    tiger = ic.config["password"]
    host = ic.config["host"]
    bind = 'mysql+mysqlconnector://' + scott + ':' + tiger + '@' + host + ':3306/smwj'

    # DBSession = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    # db_session = DBSession()

    return create_engine(bind)


def kospi_data_load():
    # price, tr volume, f/x rate
    result = pd.read_sql(qu.kospi_tr_amt, engine)

    return result


# some env setting
pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)

# variable init
logger = logger_start()
engine = orm_init()
today = time.strftime("%Y%m%d")

# data load
df = kospi_data_load()

# data copy
anal = df.copy()

# data info
anal.info()
anal.describe()
anal.hist(bins=50, figsize=(20, 15))
plt.show()

# data transform
# column select
# kospi, futures, put, bond,
anal_tmp_sort = anal.sort_values(["tran_day"], ascending=False)
   nnmbnbmnn bm
anal_tmp_sort["lahjn bel"] = anal_tmp_sort["diff_rate"].shift(1).rolling(window=5).max()
# anal = anal.drop("inst_1da", axis=1)
anal = anal.drop("tbill", axis=1)
anal = anal.drop("diff", axis=1)
anal.head(5)

# correlation of diff_rate
corr_mat = anal.corr()
corr_mat["diff_rate"].sort_values(ascending=False)

# stardardized scaling
anal_pipeline = Pipeline([('std_scaler', StandardScaler())])
anal_ss = anal_pipeline.fit_transform(anal)
anal_ss.shape

# max drawdown
add = df.copy()

window = 20
roll_max = df["close"].rolling(window, min_periods=1).max()
daily_drawdown = df["close"] / roll_max - 1.0
max_daily_drawdown = daily_drawdown.rolling(window, min_periods=1).min()

daily_drawdown.plot()
max_daily_drawdown.plot()
plt.xticks(add['tran_day'].values)
plt.show()
