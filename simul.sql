-- 1. 일, 월평균 요약

select substr(tran_id,1,8)
     , sum(buy_amt)
     , sum(income)
     , avg(final_rate)
  from tran_simul
 where simul = 2
 group by substr(tran_id,1,6), substr(tran_id,1,8) with rollup;
 
-- 2. 실제와 시뮬레이션 비교

SELECT A.TRAN_ID AS TRAN
     , B.TRAN_ID AS TRAN_SIM
     , A.BUY_AMT AS BUY
     , B.BUY_AMT AS BUB_SIM
     , A.INCOME  AS INC
     , B.INCOME  AS INC_SIM
     , A.FINAL_RATE AS RATE
     , B.FINAL_RATE AS RATE_SIM
  FROM TRAN A
 RIGHT
  JOIN TRAN_SIMUL B
    ON A.TRAN_ID = B.TRAN_ID
 WHERE A.TRAN_DAY > '20170110'
   AND B.TRAN_DAY > '20170110'
   AND A.MODE = 7
   AND B.SIMUL = 2   
   ;

-- 3. 특정일자 매도가 업데이트

UPDATE TRAN_SIMUL A
  JOIN PRICE B
    ON A.ITEM = B.ITEM
   SET A.SELL_PRICE = B.END
     , A.TRAN_DAY = '20170123'
 WHERE A.SIMUL = 2
   AND A.TRAN_SP = 2
   AND B.TRAN_DAY = '20170123'
   AND A.TRAN_ID IN (SELECT * FROM (SELECT A.TRAN_ID
					  FROM TRAN_SIMUL A      
					     , TRAN_DAY_CAL B      
					     , TRAN_DAY_CAL C 
					 WHERE A.TRAN_DAY = B.TRAN_DAY    
					   AND SUBSTR(A.TRAN_ID,1,8) = C.TRAN_DAY   
					   AND A.TRAN_DAY = '20170123'   
					   AND A.SIMUL    = 2    
					   AND A.MODE     = 2    
					   AND A.TRAN_SP  = 2 
					   AND B.SEQ - C.SEQ = 5) AA) ;
-- 4. 매도 수량 업데이트

update tran_simul
   set sell_cnt = buy_cnt
     , sell_tot = sell_price * sell_cnt
 where simul = 2
   and tran_sp = 2
   and substr(tran_id,1,8) > '20170111'
   and sell_price > 0;

-- 5. 세금 및 수수료 업데이트


update tran_simul
   set sell_fee = floor(sell_tot * 0.00015 / 10) * 10
     , sell_tax = floor(sell_tot * 0.003)
 where simul = 2
   and tran_sp = 2
   and substr(tran_id,1,8) > '20170111'
   and sell_price > 0;
   
-- 6. 이익 및 이익률 업데이트

update tran_simul
   set sell_amt = sell_tot - sell_fee - sell_tax
     , income = sell_tot - sell_fee - sell_tax - buy_amt
     , final_rate = round((sell_amt - buy_amt) / buy_amt * 100, 2)
 where simul = 2
   and tran_sp = 2
   and substr(tran_id,1,8) > '20170111'
   and sell_price > 0;

-- 7. 일정기간 경과 후 이익률

SELECT A.ITEM
     , (SELECT ITEM_NM FROM ITEM WHERE ITEM = A.ITEM) AS ITEM_NM
     , C.BEGIN
     , C.END AS 'END-0'
     , ROUND((C.END - C.BEGIN)/C.BEGIN*100,2) AS 'END-0-R'
     , E.END AS 'END-1'
     , ROUND((E.END - C.BEGIN)/C.BEGIN*100,2) AS 'END-1-R'
     , G.END AS 'END-2'
     , ROUND((G.END - C.BEGIN)/C.BEGIN*100,2) AS 'END-2-R'
     , I.END AS 'END-3'
     , ROUND((I.END - C.BEGIN)/C.BEGIN*100,2) AS 'END-3-R'
     , K.END AS 'END-4'
     , ROUND((K.END - C.BEGIN)/C.BEGIN*100,2) AS 'END-4-R'
     , M.END AS 'END-5'
     , ROUND((M.END - C.BEGIN)/C.BEGIN*100,2) AS 'END-5-R'
     , O.END AS 'END-6'
     , ROUND((O.END - C.BEGIN)/C.BEGIN*100,2) AS 'END-6-R'
     , Q.END AS 'END-7'
     , ROUND((Q.END - C.BEGIN)/C.BEGIN*100,2) AS 'END-7-R'
     , S.END AS 'END-8'
     , ROUND((S.END - C.BEGIN)/C.BEGIN*100,2) AS 'END-8-R'
     , U.END AS 'END-9'
     , ROUND((U.END - C.BEGIN)/C.BEGIN*100,2) AS 'END-9-R'
  FROM POOL A
     , TRAN_DAY_CAL B
     , PRICE C
     , TRAN_DAY_CAL D
     , PRICE E
     , TRAN_DAY_CAL F
     , PRICE G
     , TRAN_DAY_CAL H
     , PRICE I
     , TRAN_DAY_CAL J
     , PRICE K
     , TRAN_DAY_CAL L 
     , PRICE M
     , TRAN_DAY_CAL N
     , PRICE O
     , TRAN_DAY_CAL P
     , PRICE Q
     , TRAN_DAY_CAL R
     , PRICE S
     , TRAN_DAY_CAL T
     , PRICE U     
     , TRAN_DAY_CAL V
 WHERE A.TRAN_DAY = B.TRAN_DAY
   AND B.SEQ + 1 = D.SEQ
   AND A.ITEM = C.ITEM
   AND C.TRAN_DAY = D.TRAN_DAY
   AND B.SEQ + 2 = F.SEQ
   AND A.ITEM = E.ITEM
   AND E.TRAN_DAY = F.TRAN_DAY
   AND B.SEQ + 3 = H.SEQ
   AND A.ITEM = G.ITEM
   AND G.TRAN_DAY = H.TRAN_DAY
   AND B.SEQ + 4 = J.SEQ
   AND A.ITEM = I.ITEM
   AND I.TRAN_DAY = J.TRAN_DAY
   AND B.SEQ + 5 = L.SEQ
   AND A.ITEM = K.ITEM
   AND K.TRAN_DAY = L.TRAN_DAY
   AND B.SEQ + 6 = N.SEQ
   AND A.ITEM = M.ITEM
   AND M.TRAN_DAY = N.TRAN_DAY
   AND B.SEQ + 7 = P.SEQ
   AND A.ITEM = O.ITEM
   AND O.TRAN_DAY = P.TRAN_DAY
   AND B.SEQ + 8 = R.SEQ
   AND A.ITEM = Q.ITEM
   AND Q.TRAN_DAY = R.TRAN_DAY
   AND B.SEQ + 9 = T.SEQ
   AND A.ITEM = S.ITEM
   AND S.TRAN_DAY = T.TRAN_DAY
   AND B.SEQ + 10 = V.SEQ
   AND A.ITEM = U.ITEM
   AND U.TRAN_DAY = V.TRAN_DAY
   AND A.TRAN_DAY > '20161005'
   AND A.TRAN_DAY < '20161101'
   AND A.MODE = 6;

--

SELECT *
  FROM POOL
 WHERE MODE = 6
   AND TRAN_DAY > '20160101';

--

SELECT *
  FROM POOL
 WHERE TRAN_DAY = '20170123';
--

SELECT *
  FROM TRAN_SIMUL
 WHERE SIMUL = 3;
  