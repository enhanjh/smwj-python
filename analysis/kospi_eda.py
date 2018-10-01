# python 3.6 32bit
# installed package
# 1. mysql-connector-python
# 2. sqlalchemy
# 3. odo and [datapipelines, networkx 1.11, cassiopeia]
# 4. pandas

import logging
import time
import pandas as pd
from stat import db_config as ic
from logging.handlers import TimedRotatingFileHandler
from sqlalchemy import create_engine


def logger_start():
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    logger = logging.getLogger("myLogger")
    formatter = logging.Formatter('[%(levelname)s:%(lineno)s] %(asctime)s > %(message)s')
    fh = TimedRotatingFileHandler("C:\SMWJ_LOG\\analyze", when="midnight")
    fh.setFormatter(formatter)
    fh.suffix = "_%Y%m%d.log"
    logger.addHandler(fh)
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


def kodex_lev_data_load():
    # price, tr volume, f/x rate
    sql = "select a.tran_day, a.open, a.high, a.low, a.close, b.diff_rate, b.regi_fore, b.inst, b.ant, c.close as usdkrw " \
          "  from price a" \
          "  join investor b" \
          "    on a.item     = b.item" \
          "   and a.tran_day = b.tran_day" \
          "  left" \
          "  join market_index c" \
          "    on a.tran_day = c.tran_day" \
          " where a.item     = '122630' " \
          "   and a.tran_day between '20150101' and '20171231'" \
          "   and c.item     = 'USDKRWSMBS';"
    result = pd.read_sql(sql, engine)

    return result


# variable init
logger = logger_start()
engine = orm_init()
today = time.strftime("%Y%m%d")

# data load
df = kodex_lev_data_load()

# data copy
anal = df.copy()

# correlation of diff_rate
corr_mat = anal.corr()
corr_mat["diff_rate"].sort_values(ascending=False)
