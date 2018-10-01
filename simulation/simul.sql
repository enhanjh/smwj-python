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
 WHERE A.TRAN_DAY > 20170110
   AND B.TRAN_DAY > 20170110
   AND A.MODE = 7
   AND B.SIMUL = 2   
   ;

-- 3. 특정일자 매도가 업데이트

UPDATE TRAN_SIMUL A
  JOIN PRICE B
    ON A.ITEM = B.ITEM
   SET A.SELL_PRICE = B.END
     , A.TRAN_DAY = 20170123
 WHERE A.SIMUL = 2
   AND A.TRAN_SP = 2
   AND B.TRAN_DAY = 20170123
   AND A.TRAN_ID IN (SELECT * FROM (SELECT A.TRAN_ID
					  FROM TRAN_SIMUL A      
					     , TRAN_DAY_CAL B      
					     , TRAN_DAY_CAL C 
					 WHERE A.TRAN_DAY = B.TRAN_DAY    
					   AND SUBSTR(A.TRAN_ID,1,8) = C.TRAN_DAY   
					   AND A.TRAN_DAY = 20170123   
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
   and substr(tran_id,1,8) > 20170111
   and sell_price > 0;

-- 5. 세금 및 수수료 업데이트


update tran_simul
   set sell_fee = floor(sell_tot * 0.00015 / 10) * 10
     , sell_tax = floor(sell_tot * 0.003)
 where simul = 2
   and tran_sp = 2
   and substr(tran_id,1,8) > 20170111
   and sell_price > 0;
   
-- 6. 이익 및 이익률 업데이트

update tran_simul
   set sell_amt = sell_tot - sell_fee - sell_tax
     , income = sell_tot - sell_fee - sell_tax - buy_amt
     , final_rate = round((sell_amt - buy_amt) / buy_amt * 100, 2)
 where simul = 2
   and tran_sp = 2
   and substr(tran_id,1,8) > 20170111
   and sell_price > 0;

-- 7. 일정기간 경과 후 이익률

SELECT A.ITEM
     , (SELECT ITEM_NM FROM ITEM WHERE ITEM = A.ITEM) AS ITEM_NM
     , A.TRAN_DAY
     , C.BEGIN
     , C.END AS END-0
     , ROUND((C.END - C.BEGIN)/C.BEGIN*100,2) AS END-0-R
     , E.END AS END-1
     , ROUND((E.END - C.BEGIN)/C.BEGIN*100,2) AS END-1-R
     , G.END AS END-2
     , ROUND((G.END - C.BEGIN)/C.BEGIN*100,2) AS END-2-R
     , I.END AS END-3
     , ROUND((I.END - C.BEGIN)/C.BEGIN*100,2) AS END-3-R
     , K.END AS END-4
     , ROUND((K.END - C.BEGIN)/C.BEGIN*100,2) AS END-4-R
     , M.END AS END-5
     , ROUND((M.END - C.BEGIN)/C.BEGIN*100,2) AS END-5-R
     , O.END AS END-6
     , ROUND((O.END - C.BEGIN)/C.BEGIN*100,2) AS END-6-R
     , Q.END AS END-7
     , ROUND((Q.END - C.BEGIN)/C.BEGIN*100,2) AS END-7-R
     , S.END AS END-8
     , ROUND((S.END - C.BEGIN)/C.BEGIN*100,2) AS END-8-R
     , U.END AS END-9
     , ROUND((U.END - C.BEGIN)/C.BEGIN*100,2) AS END-9-R
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
   AND A.TRAN_DAY > 20160701
   AND A.TRAN_DAY < 20160930
   AND A.MODE = 2
   AND A.BEGIN_PRICE > 0
   AND A.BUY_INVOLVE_ITEM_YN = Y;

--

SELECT *
  FROM POOL
 WHERE MODE = 6
   AND TRAN_DAY > 20160101;

--

SELECT *
  FROM POOL
 WHERE TRAN_DAY = 20170123;
--

SELECT *
  FROM TRAN_SIMUL
 WHERE SIMUL = 2;
 
--
select *
  from tran_day_cal;
--
select *
  from tran_simul a
     , tran_day_cal b
     , tran_day_cal c
     , price d
 where substr(a.tran_id,1,8) = b.tran_day
   and b.seq + 5 = c.seq
   and c.tran_day = substr(d.tran_day,1,8)
   -- and substr(a.tran_day,1,8) = 20170420
   and a.simul = 2;
 
 --
 
SELECT *
  FROM PRICE
 WHERE TRAN_DAY = 20170510;
 
-- 매일 거래량 많은 30개 종목의 다음날 수익률
-- there is always a marker.
-- 시장이 무너지는 지표는 무엇인가?

SELECT TRAN_DAY
     , AVG(RATE)
  FROM (
SELECT B.TRAN_DAY
     , B.RATE
  FROM INVESTOR A
     , PRICE_SMMRY B
     , TRAN_DAY_CAL C
 WHERE A.TRAN_DAY = C.POOL_DAY
   AND B.TRAN_DAY = C.TRAN_DAY
   AND A.ITEM = B.ITEM
   AND B.TRAN_DAY BETWEEN 20170101 AND 20170202
   AND A.INS > 0
   AND A.FORE > 0
   AND B.PRICE < 100000
   AND B.STATE NOT LIKE %관리종목%
       ) MST
 GROUP BY TRAN_DAY
; 

-- 매일 시가총액 하위 30개 종목의 다음날 수익률

SELECT *
  FROM MARKET_INDEX
 WHERE TRAN_DAY BETWEEN 20170501 AND 20170606;
  
--

 SELECT MAX(A.SEQ)+1
            , (SELECT MAX(AA.TRAN_DAY) FROM TRAN_DAY_CAL AA WHERE AA.TRAN_DAY < 20170606)
            , 20170606
            , M
            , NOW()
 FROM TRAN_DAY_CAL A;
 
 -- 전날 오른종목의 다음날
 -- 조건 1 : 외인, 기관 순매수
 -- 조건 2 : 5일 이동평균이 종가보다 큼
 
SELECT *
  FROM (
  SELECT A.TRAN_DAY
       , A.END
	FROM MARKET_INDEX A
   WHERE A.ITEM = '001'
	   ) AA
     , (
  SELECT A.TRAN_DAY
       , A.END
	FROM PRICE A
   WHERE A.ITEM = '114800'
	   ) BB
     , ( 
  SELECT C.TRAN_DAY
	   , SUM(A.END) AS price1
       , SUM(C.END) AS price2
       , SUM(F.END) AS price3
       , COUNT(*) AS cnt
	FROM PRICE A
	   , TRAN_DAY_CAL B
	   , PRICE C
	   , INVESTOR D
       , TRAN_DAY_CAL E
       , PRICE F
   WHERE A.TRAN_DAY = B.POOL_DAY
	 AND B.TRAN_DAY = C.TRAN_DAY
	 AND A.ITEM     = C.ITEM
	 AND C.ITEM     = D.ITEM
	 AND C.TRAN_DAY = D.TRAN_DAY
     AND C.TRAN_DAY = E.POOL_DAY
     AND F.TRAN_DAY = E.TRAN_DAY
	 AND C.TRAN_DAY BETWEEN '20160601' AND '20170530'
	 AND (C.END - A.END)/A.END*100 BETWEEN 0.5 AND 10
	 AND C.AVG_5 > C.END
     AND C.AVG_10 > C.END
	 AND D.FORE > 0
	 AND D.INS > 0
   GROUP BY A.TRAN_DAY
       ) CC
  WHERE AA.TRAN_DAY = BB.TRAN_DAY
    AND AA.TRAN_DAY = CC.TRAN_DAY
    AND AA.TRAN_DAY BETWEEN '20160601' AND '20170530';
    
--

  SELECT A.TRAN_DAY
	   , SUM(D.END) AS price1
       , SUM(A.END) AS price2
       , SUM(E.END) AS price3
       , COUNT(*) AS cnt
	FROM PRICE A
	   , TRAN_DAY_CAL B
       , TRAN_DAY_CAL C
	   , PRICE D
       , PRICE E
	   , INVESTOR F
   WHERE A.TRAN_DAY = B.TRAN_DAY
	 AND A.TRAN_DAY = C.POOL_DAY
	 AND A.ITEM     = D.ITEM
	 AND A.ITEM     = E.ITEM
	 AND B.POOL_DAY = D.TRAN_DAY
     AND C.TRAN_DAY = E.TRAN_DAY
     AND A.ITEM     = F.ITEM
     AND A.TRAN_DAY = F.TRAN_DAY
	 AND A.TRAN_DAY BETWEEN '20160601' AND '20170530'
	 AND (A.END - D.END)/D.END*100 BETWEEN 0.5 AND 10
	 AND A.AVG_5 > A.END
     AND A.AVG_10 > A.END
	 AND F.FORE > 0
	 AND F.INS > 0
   GROUP BY A.TRAN_DAY;