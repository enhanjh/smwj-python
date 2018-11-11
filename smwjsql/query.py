import const.db_config as ic
from sqlalchemy import create_engine


scott = ic.config["user"]
tiger = ic.config["password"]
host = ic.config["host"]
bind = 'mysql+mysqlconnector://' + scott + ':' + tiger + '@' + host + ':3306/smwj'
engine = create_engine(bind)


item_price = "select a.item" \
             "     , a.tran_day" \
             "     , a.close" \
             "  from price a" \
             " where a.item = '{item}'" \
             "   and a.tran_day between '{sdate}' and '{edate}';"

multi_item_price = "select a.tran_day" \
                   "     , a.close as '{item}'" \
                   "  from price a" \
                   " where a.item = '{item}'" \
                   "   and a.tran_day between '{sdate}' and '{edate}';"

money = "select tran_day" \
        "     , kospi_close" \
        "     , volume" \
        "     , deposit" \
        "     , credit_bal" \
        "     , depo_futures" \
        "     , stock" \
        "     , bond" \
        "     , tbill" \
        "     , mmf" \
        "  from market_liquidity" \
        " where tran_day between '{sdate}' and '{edate}'"

kospi_liq = "select a.tran_day" \
            "     , a.kospi_close" \
            "     , case when a.diff_rate < 0 then a.diff * -1" \
            "            else a.diff" \
            "         end as diff" \
            "     , a.diff_rate" \
            "     , a.volume" \
            "     , a.deposit" \
            "     , a.deposit_diff" \
            "     , a.roll_rate" \
            "     , a.credit_bal" \
            "     , a.credit_rest" \
            "     , a.depo_futures" \
            "     , a.stock" \
            "     , a.mix_stock" \
            "     , a.mix_bond" \
            "     , a.bond" \
            "     , a.mmf" \
            "  from market_liquidity a" \
            " where tran_day > '20140101';"


magic = "select a.tran_day" \
        "     , a.kospi_close" \
        "     , case when a.diff_rate < 0 then a.diff * -1" \
        "            else a.diff" \
        "         end as diff" \
        "     , a.diff_rate" \
        "     , a.volume" \
        "     , a.mmf" \
        "     , b.kospi_fore" \
        "     , b.kospi_inst" \
        "     , c.futures_fore" \
        "     , c.futures_inst" \
        "     , d.fx_close" \
        "  from market_liquidity a" \
        "  join (" \
        "       select aa.tran_day" \
        "            , aa.fore as kospi_fore" \
        "            , aa.inst as kospi_inst" \
        "         from market_index_tr_amt aa" \
        "        where item = 'kospi'" \
        "       ) b" \
        "    on a.tran_day = b.tran_day" \
        "  join (" \
        "       select bb.tran_day" \
        "            , bb.fore as futures_fore" \
        "            , bb.inst as futures_inst" \
        "         from market_index_tr_amt bb" \
        "        where item = 'futures'" \
        "       ) c" \
        "    on a.tran_day = c.tran_day" \
        "  join (" \
        "       select cc.tran_day" \
        "            , cc.close as fx_close" \
        "         from market_index cc" \
        "        where cc.item = 'USDKRWSMBS'" \
        "       ) d" \
        "    on a.tran_day = d.tran_day" \
        " where a.tran_day between '20140101' and '20171231';"
