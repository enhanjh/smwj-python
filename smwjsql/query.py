magic = "select a.tran_day                    " \
       "     , a.kospi_close                 " \
       "     , a.diff_rate                   " \
       "     , a.volume                      " \
       "     , a.mmf                         " \
       "     , b.fx_close                    " \
       "     , c.kospi_fore                  " \
       "     , c.kospi_inst                  " \
       "     , d.futures_fore                " \
       "     , d.futures_inst                " \
       "  from market_liquidity a            " \
       "  join (                             " \
       "       select aa.tran_day            " \
       "            , aa.close as fx_close   " \
       "         from market_index aa        " \
       "       where aa.item = 'USDKRWSMBS'  " \
       "       ) b                           " \
       "    on a.tran_day = b.tran_day       " \
       "  join (                             " \
       "       select bb.tran_day            " \
       "            , bb.fore as kospi_fore  " \
       "            , bb.inst as kospi_inst  " \
       "         from market_index_tr_amt bb " \
       "       where bb.item = 'kospi'       " \
       "       ) c                           " \
       "    on a.tran_day = c.tran_day       " \
       "  join (                             " \
       "       select cc.tran_day            " \
       "            , cc.fore as futures_fore " \
       "            , cc.inst as futures_inst " \
       "         from market_index_tr_amt cc " \
       "       where cc.item = 'futures'     " \
       "       ) d                           " \
       "    on a.tran_day = d.tran_day       " \
       " where a.tran_day between '20140101' and '20180630';"


kospi_tr_amt = "select a.tran_day                    " \
               "     , a.kospi_close                 " \
               "     , a.diff                        " \
               "     , a.diff_rate                   " \
               "     , a.volume                      " \
               "     , a.deposit                     " \
               "     , a.deposit_diff                " \
               "     , a.roll_rate                   " \
               "     , a.credit_bal                  " \
               "     , a.credit_rest                 " \
               "     , a.depo_futures                " \
               "     , a.stock                       " \
               "     , a.mix_stock                   " \
               "     , a.mix_bond                    " \
               "     , a.bond                        " \
               "     , a.tbill                       " \
               "     , a.mmf                         " \
               "     , b.fx_close                    " \
               "     , c.kospi_fore                  " \
               "     , c.kospi_inst                  " \
               "     , d.futures_fore                " \
               "     , d.futures_inst                " \
               "     , e.call_fore                   " \
               "     , e.call_inst                   " \
               "     , f.put_fore                    " \
               "     , f.put_inst                    " \
               "  from market_liquidity a            " \
               "  join (                             " \
               "       select aa.tran_day            " \
               "            , aa.close as fx_close   " \
               "         from market_index aa        " \
               "       where aa.item = 'USDKRWSMBS'  " \
               "       ) b                           " \
               "    on a.tran_day = b.tran_day       " \
               "  join (                             " \
               "       select bb.tran_day            " \
               "            , bb.fore as kospi_fore  " \
               "            , bb.inst as kospi_inst  " \
               "         from market_index_tr_amt bb " \
               "       where bb.item = 'kospi'       " \
               "       ) c                           " \
               "    on a.tran_day = c.tran_day       " \
               "  join (                             " \
               "       select cc.tran_day            " \
               "            , cc.fore as futures_fore " \
               "            , cc.inst as futures_inst " \
               "         from market_index_tr_amt cc " \
               "       where cc.item = 'futures'     " \
               "       ) d                           " \
               "    on a.tran_day = d.tran_day       " \
               "  join (                             " \
               "       select dd.tran_day            " \
               "            , dd.fore as call_fore   " \
               "            , dd.inst as call_inst   " \
               "         from market_index_tr_amt dd " \
               "       where dd.item = 'call'        " \
               "       ) e                           " \
               "    on a.tran_day = e.tran_day       " \
               "  join (                             " \
               "       select ee.tran_day            " \
               "            , ee.fore as put_fore    " \
               "            , ee.inst as put_inst    " \
               "         from market_index_tr_amt ee " \
               "       where ee.item = 'put'         " \
               "       ) f                           " \
               "    on a.tran_day = f.tran_day       " \
               " where a.tran_day > '20140101';       "
