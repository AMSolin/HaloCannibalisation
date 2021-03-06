DROP TABLE IF EXISTS pps.ITM_PRC_WK_MODE_TABLE1;
CREATE TABLE IF NOT EXISTS pps.ITM_PRC_WK_MODE_TABLE1
as
SELECT ITM_NBR,
       LCT_NBR,
       FSC_WK_END_DT,
       (CASE PRC_MRT_SET_CD
            WHEN 3 THEN PRC_MRT_SET_WK_ITM_PRC_AMT
            ELSE PRC_LCT_SET_WK_ITM_PRC_AMT
        END) AS ITM_PRC_AMT
FROM LOWES_TABLES.I0776_LCT_ITM_PRC_WK
WHERE (PRC_MRT_SET_CD = 3
       OR PRC_LCT_SET_CD = 99)
      AND FSC_WK_END_DT='${prm_end_dat}'
      AND ITM_NBR in (select value from pps.item_comp_sub_head where prm_end_dat='${prm_end_dat}');

DROP TABLE IF EXISTS pps.ITM_PRC_WK_MODE_TABLE2;
CREATE TABLE IF NOT EXISTS pps.ITM_PRC_WK_MODE_TABLE2
as
SELECT  ITM_NBR,
        ITM_PRC_AMT,
        COUNT(*) as occurrence
FROM pps.ITM_PRC_WK_MODE_TABLE1
GROUP BY ITM_NBR,ITM_PRC_AMT;

DROP TABLE IF EXISTS pps.ITM_PRC_WK_MODE_TABLE3;
CREATE TABLE IF NOT EXISTS pps.ITM_PRC_WK_MODE_TABLE3
as
SELECT A.ITM_NBR,
MAX(A.occurrence) AS max_occurrence
FROM pps.ITM_PRC_WK_MODE_TABLE2 as A
WHERE A.itm_prc_amt > 0
GROUP BY A.ITM_NBR;

DROP TABLE IF EXISTS pps.ITM_PRC_WK_MODE_TABLE4;
CREATE TABLE IF NOT EXISTS pps.ITM_PRC_WK_MODE_TABLE4
as
SELECT a.ITM_NBR,
a.max_occurrence,
b.ITM_PRC_AMT as price
FROM pps.ITM_PRC_WK_MODE_TABLE3 as a
join pps.ITM_PRC_WK_MODE_TABLE2 as b
on a.ITM_NBR = b.ITM_NBR and a.max_occurrence = b.occurrence
GROUP BY a.ITM_NBR,a.max_occurrence,b.ITM_PRC_AMT;
