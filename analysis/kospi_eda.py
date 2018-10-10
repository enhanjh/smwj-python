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

    fh = TimedRotatingFileHandler("./analyze", when="midnight")
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


def query(sql):
    result = pd.read_sql(sql, engine)

    return result


# some env setting
pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)

# variable init
logger = logger_start()
engine = orm_init()
today = time.strftime("%Y%m%d")

# data load
# df = kospi_data_load()
df = query(qu.magic)

# data copy
anal = df.copy()

# data info
anal.info()
anal.describe()
anal.hist(bins=50, figsize=(20, 15))
# correlation of diff_rate
corr_mat = anal.corr()
corr_mat["diff_rate"].sort_values(ascending=False)

# data transform
# 1. label insert
anal_tmp = pd.DataFrame(anal['kospi_close'].shift(-1))
anal_tmp['org'] = anal['kospi_close']
anal_tmp['max'] = anal_tmp['kospi_close'][::-1].rolling(window=5, center=False).max()[::-1]
anal_tmp['max_rate'] = ((anal_tmp['max'] / anal_tmp['org'] - 1) * 100).round(2)
anal_tmp.loc[anal_tmp['max_rate'] > 1.0, 'label'] = 1
anal_tmp['label'] = anal_tmp['label'].fillna(0)
# len(anal_tmp.loc[anal_tmp['label'] == 1])  # 10days: 499, 5days: 339, 3days: 238
# anal_tmp.head(30)
anal['label'] = anal_tmp['label']
del anal_tmp

# 2. data scaling
# stardardized scaling
anal_pipeline = Pipeline([('std_scaler', StandardScaler())])
anal_ss = anal_pipeline.fit_transform(anal)
anal_scaled = pd.DataFrame(anal_ss, columns=anal.columns)
# 2.5 column cleaning
anal_scaled = anal_scaled.loc[:, ['volume', 'mmf', 'kospi_fore', 'kospi_inst', 'futures_fore', 'futures_inst']]
# anal_scaled.describe()
# anal_scaled = anal_scaled.drop('label', axis=1)

# 3. 1, 5, 10, 20, 40, 60days before data insert
anal_tmp = anal_scaled.copy()
days = [1, 5, 10, 20, 40, 60]
for col in anal_tmp.columns:
    for day in days:
        anal_scaled[col + str(day)] = anal_scaled[col].shift(day)

# anal_scaled.head(70)
# anal_scaled.describe()
anal_scaled['label'] = anal['label']
anal_scaled = anal_scaled[81:]










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
