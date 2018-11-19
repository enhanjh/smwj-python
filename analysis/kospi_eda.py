# python 3.6 32bit
# installed package
# 1. mysql-connector-python
# 2. sqlalchemy
# 3. odo and [datapipelines, networkx 1.11, cassiopeia]
# 4. pandas
# 5. numpy
# 6. matplotlib
# 7. sklearn

import logging
import sys
import time
import smwjsql.query as qu
import pandas as pd
import numpy as np
import const.const as ic
import matplotlib.pyplot as plt
from logging.handlers import TimedRotatingFileHandler
from sqlalchemy import create_engine
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score


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


def add_label(df):
    result_df = pd.DataFrame(df['kospi_close'].shift(-1))
    result_df['org'] = df['kospi_close']
    result_df['max'] = result_df['kospi_close'][::-1].rolling(window=5, center=False).max()[::-1]
    result_df['max_rate'] = ((result_df['max'] / result_df['org'] - 1) * 100).round(2)
    result_df.loc[result_df['max_rate'] > 0.6, 'label'] = 1
    result_df['label'] = result_df['label'].fillna(0)

    return result_df


def add_plain_variable(df):
    result_df = df.copy()
    anal_temp = result_df.copy()
    days = [1, 5, 10, 20, 40, 60]
    for col in anal_temp.columns:
        for day in days:
            result_df[col + str(day)] = ((result_df[col] / result_df[col].shift(day) - 1) * 100).round(2)

    del anal_temp

    return result_df


def add_cumsum_variable(df):
    result_df = df.copy()
    anal_temp = result_df.copy()
    days = [2, 5, 10, 20, 40, 60]
    for col in anal_temp.columns:
        for day in days:
            result_df[col + str(day)] = result_df[col].rolling(day).sum()

    del anal_temp

    return result_df


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
# 1. preparing label
anal_tmp = add_label(anal)
anal_label_train = anal_tmp['label'][81:981]
anal_label_test = anal_tmp['label'][981:]
anal_label_train.shape
anal_label_test.shape
del anal_tmp
# len(anal_tmp.loc[anal_tmp['label'] == 1])  # 10days: 499, 5days: 339, 3days: 238
# anal_tmp.head(30)


# 2 dropping useless column
anal_plain_pp = anal.loc[:, ['volume', 'fx_close', 'mmf']]
anal_cumsum_pp = anal.loc[:, ['kospi_fore', 'kospi_inst', 'futures_fore', 'futures_inst']]
# anal_scaled.describe()
# anal_scaled = anal_scaled.drop('label', axis=1)

# 3. adding diff rate between present and 1, 5, 10, 20, 40, 60days ago
anal_plain_pp = add_plain_variable(anal_plain_pp)
anal_cumsum_pp = add_cumsum_variable(anal_cumsum_pp)
anal_pp = anal_plain_pp.merge(anal_cumsum_pp, left_index=True, right_index=True)
anal_pp_train = anal_pp[81:981]
anal_pp_test = anal_pp[981:]
anal_pp_train.head(10)
anal_pp_train.shape
anal_pp_test.shape
del anal_pp

# 4. data scaling
# stardardized scaling
anal_pipeline = Pipeline([('std_scaler', StandardScaler())])
anal_train = anal_pipeline.fit_transform(anal_pp_train)
anal_test = anal_pipeline.fit_transform(anal_pp_test)
# anal_train = pd.DataFrame(anal_ss, columns=anal_pp.columns)
# anal_train.head(70)
# anal_train.describe()

# 5. Random Forest
rf_clf = RandomForestClassifier()
rf_clf.fit(anal_train, anal_label_train)
rf_score = cross_val_score(rf_clf, anal_train, anal_label_train, scoring='neg_mean_squared_error', cv=10)
rf_rmse_score = np.sqrt(-rf_score)
rf_rmse_score

# 6. preparing test data
predict = rf_clf.predict(anal_test)
result = pd.DataFrame([predict, anal_label_test.values], columns=['pred', 'actual'])
round(len(result.loc[result['actual'] == result['pred']]) / len(result) * 100, 2)

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
