sql = ("SELECT plan.ptype,CASE WHEN plan.npln IS NULL THEN 0 ELSE plan.npln END npln,CASE WHEN plan.recctn IS NULL THEN 0 ELSE plan.recctn END recctn,0 diff,sysdate FROM \n" +
       "(\n" +
       "	select \n" +
       "		'REC' PROTYPE, sysdate ,P1.PTYPE,P1.PLANCTN,P2.RMPLANCTN,(\n" +
       "		(CASE WHEN P2.RMPLANCTN IS NULL THEN 0 ELSE P2.RMPLANCTN end) + (CASE WHEN P1.PLANCTN IS NULL THEN 0 ELSE P1.PLANCTN end)) npln,P3.RECCTN, sysdate ,sysdate\n" +
       "	FROM (\n" +
       "	select CASE E.RECISSTYPE  WHEN '01'  THEN 'OVERSEA' WHEN '02'  THEN 'OVERSEA' ELSE  'DOMESTIC' END PTYPE,SUM(round(B.QUANTITY/L.OUTERPCS)) PLANCTN\n" +
       "	from TBT_RECTRANSBODY B\n" +
       "		inner join  TBT_RECTRANSENT E on B.RECEIVINGKEY = E.RECEIVINGKEY\n" +
       "		inner join  TBT_LEDGER L on B.TAGRP = L.TAGRP AND B.PARTNO = L.PARTNO\n" +
       "		inner join  TBM_PART P on B.TAGRP = P.TAGRP AND B.PARTNO = P.PARTNO \n" +
       "	WHERE P.CD <> '10'  AND to_char(E.RECEIVINGDTE,'yyyymmdd')  = to_char(sysdate,'yyyymmdd')\n" +
       "	GROUP BY  CASE E.RECISSTYPE  WHEN '01'  THEN 'OVERSEA' WHEN '02'  THEN 'OVERSEA' ELSE  'DOMESTIC' END                                                 \n" +
       "	UNION\n" +
       "		select  'WIRE' PTYPE,SUM(round(B.QUANTITY/L.OUTERPCS)) PLANCTN\n" +
       "		from TBT_RECTRANSBODY B\n" +
       "			inner join  TBT_RECTRANSENT E on B.RECEIVINGKEY = E.RECEIVINGKEY\n" +
       "			inner join  TBT_LEDGER L on B.TAGRP = L.TAGRP AND B.PARTNO = L.PARTNO\n" +
       "			inner join  TBM_PART P on B.TAGRP = P.TAGRP AND B.PARTNO = P.PARTNO \n" +
       "		WHERE P.CD = '10'  AND to_char(E.RECEIVINGDTE,'yyyymmdd')  =   to_char(sysdate,'yyyymmdd')\n" +
       "	) P1\n" +
       "	LEFT JOIN(   --- REMAIN PLAN\n" +
       "		select CASE E.RECISSTYPE  WHEN '01'  THEN 'OVERSEA' WHEN '02'  THEN 'OVERSEA' ELSE  'DOMESTIC' END PTYPE,SUM(round(B.QUANTITY/L.OUTERPCS)) RMPLANCTN\n" +
       "		from TBT_RECTRANSBODY B\n" +
       "		inner join  TBT_RECTRANSENT E on B.RECEIVINGKEY = E.RECEIVINGKEY\n" +
       "		inner join  TBT_LEDGER L on B.TAGRP = L.TAGRP AND B.PARTNO = L.PARTNO\n" +
       "		inner join  TBM_PART P on B.TAGRP = P.TAGRP AND B.PARTNO = P.PARTNO \n" +
       "		WHERE B.RVMANAGINGNO IS NULL \n" +
       "			AND P.CD <> '10'\n" +
       "			AND to_char(E.RECEIVINGDTE,'yyyymmdd')  > to_char(sysdate,'yyyymmdd') - 5\n" +
       "			and to_char(E.RECEIVINGDTE,'yyyymmdd')  < to_char(sysdate,'yyyymmdd')\n" +
       "		GROUP BY  CASE E.RECISSTYPE  WHEN '01'  THEN 'OVERSEA' WHEN '02'  THEN 'OVERSEA' ELSE  'DOMESTIC' END                                                 \n" +
       "		UNION\n" +
       "			select  'WIRE' PTYPE,SUM(round(B.QUANTITY/L.OUTERPCS)) PLANCTN\n" +
       "			from TBT_RECTRANSBODY B\n" +
       "			inner join  TBT_RECTRANSENT E on B.RECEIVINGKEY = E.RECEIVINGKEY\n" +
       "			inner join  TBT_LEDGER L on B.TAGRP = L.TAGRP AND B.PARTNO = L.PARTNO\n" +
       "			inner join  TBM_PART P on B.TAGRP = P.TAGRP AND B.PARTNO = P.PARTNO \n" +
       "			WHERE B.RVMANAGINGNO IS NULL \n" +
       "				AND P.CD = '10'\n" +
       "				AND to_char(E.RECEIVINGDTE,'yyyymmdd')  > to_char(sysdate,'yyyymmdd') - 5\n" +
       "				and to_char(E.RECEIVINGDTE,'yyyymmdd')  < to_char(sysdate,'yyyymmdd')\n" +
       "	) P2 ON P1.PTYPE= P2.PTYPE\n" +
       "	LEFT JOIN(   --- RECEIVE\n" +
       "		select CASE s.recisstype  WHEN '01'  THEN 'OVERSEA' WHEN '02'  THEN 'OVERSEA' ELSE  'DOMESTIC' END PTYPE,count(*) RECCTN\n" +
       "		from TBT_CARTONDETAILS C\n" +
       "			inner join  tbt_stockdetails s on c.RVMANAGINGNO = s.RVMANAGINGNO\n" +
       "			inner join  TBM_PART P on c.TAGRP = P.TAGRP AND c.PARTNO = P.PARTNO \n" +
       "		WHERE P.CD <> '10'  AND to_char(c.sysdte,'yyyymmdd') =  to_char(sysdate,'yyyymmdd')\n" +
       "		GROUP BY  CASE s.recisstype  WHEN '01'  THEN 'OVERSEA' WHEN '02'  THEN 'OVERSEA' ELSE  'DOMESTIC' END \n" +
       "		UNION\n" +
       "			select 'WIRE' PTYPE,count(*) RECCTN\n" +
       "			from TBT_CARTONDETAILS C\n" +
       "				inner join  tbt_stockdetails s on c.RVMANAGINGNO = s.RVMANAGINGNO\n" +
       "				inner join  TBM_PART P on c.TAGRP = P.TAGRP AND c.PARTNO = P.PARTNO \n" +
       "			WHERE P.CD = '10'  AND to_char(c.sysdte,'yyyymmdd') =  to_char(sysdate,'yyyymmdd')\n" +
       "	) P3 ON P1.PTYPE= P3.PTYPE    \n" +
       ") plan")

print(sql)
