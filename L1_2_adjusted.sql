--using following tables: pps.pema_l2_out_res,pps.cmp_itm_55,PPS.ITEM_SUBSTITUE_ENSEMBLE_ALL_5_DATASOURCE_ACTUAL_RANK_15_VIEW and pps.head

INSERT OVERWRITE TABLE pps.item_comp_sub_head PARTITION (prm_end_dat)
SELECT a.key_term,a.value,a.rank,a.type,b.promo_week as prm_end_dat FROM
(SELECT
   DISTINCT
   itm_nbr_a as key_term,
   itm_nbr_b as value,
   rank as rank,
   'c' AS type
FROM
   pps.cmp_itm_55 where rank <=15) a 
   INNER JOIN
      (
         SELECT
            itm_nbr , promo_week
         FROM
            pps.pema_l2_out_res 
         WHERE
            promo_week = '${prm_end_dat}'
      )
      b 
      ON ( a.key_term = b.itm_nbr) 
   INNER JOIN
      (SELECT DISTINCT itm_nbr_1 FROM PPS.ITEM_SUBSTITUE_ENSEMBLE_ALL_5_DATASOURCE_ACTUAL_RANK_15_VIEW) c 
      ON ( a.key_term = c.itm_nbr_1) 
   INNER JOIN
      (
         SELECT
            item_num 
         FROM
            pps.head 
         WHERE
            kvi = 'Head' 
            OR kvi = 'Core'
      )
      d 
      ON ( a.key_term = d.item_num) 
   UNION ALL
   SELECT a.key_term,a.value,a.rank,a.type,b.promo_week as prm_end_dat FROM
      (SELECT DISTINCT
      itm_nbr_1 AS key_term,
      itm_nbr_2 AS value,
      rank as rank,
      's' AS type
      
   FROM
      PPS.ITEM_SUBSTITUE_ENSEMBLE_ALL_5_DATASOURCE_ACTUAL_RANK_15_VIEW where rank <=15) a 
      INNER JOIN
         (
            SELECT
               itm_nbr , promo_week 
            FROM
               pps.pema_l2_out_res 
            WHERE
               promo_week = '${prm_end_dat}'
         )
         b 
         ON ( a.key_term = b.itm_nbr) 
      INNER JOIN
         (SELECT DISTINCT itm_nbr_a from pps.cmp_itm_55)c 
         ON ( a.key_term = c.itm_nbr_a) 
      INNER JOIN
         (
            SELECT
               item_num 
            FROM
               pps.head 
            WHERE
               kvi = 'Head' 
               OR kvi = 'Core'
         )
         d 
         ON ( a.key_term = d.item_num);

