# nw acc delay ----
## day ----
query_nw_acc_delay_day_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
WITH stat_aua_list
    AS
        (SELECT stat_aua_id      AS unit_code,
                aua_name         AS unit_name,
                aua_country_code AS unit_country,
                aua_type         AS unit_type_2
           FROM aru_syn.stat_aua f
          WHERE stat_aua_id IN ('EBBUACC',
                                'EDGGALL',
                                'EDMMACC',
                                'EDUUUAC',
                                'EDWWACC',
                                'EDYYUAC',
                                'EETTACC',
                                'EFINACC',
                               -- 'EFESACC', -- code change on 22-02-2021
                                'EGGXOCA',
                                'EGPXALL',
                                'EGTTACC',
                                'EGTTTC',
                                'EHAAACC',
                                'EIDWACC',
                                'EISNACC',
                                'EKDKACC',
                                'ENBDACC',
                                'ENOSACC',
                                'ENSVACC',
                                'EPWWACC',
                                'ESMMACC',
                                'ESOSACC',
                                'EVRRACC',
                                'EYVCACC',
                                'GCCCACC',
                                'GMMMACC',
                                'GMACACC',
                                'LAAAACC',
                                'LBSRACC',
                                'LCCCACC',
                                'LDZOACC',
                                'LECBACC',
                                'LECMALL',
                                'LECPACC',
                                'LECSACC',
                                'LFBBALL',
                                'LFEEACC',
                                'LFFFALL',
                                'LFMMACC',
                                'LFRRACC',
                                'LGGGACC',
                                'LGMDACC',
                                'LHCCACC',
                                'LIBBACC',
                                'LIMMACC',
                                'LIPPACC',
                                'LIRRACC',
                                'LJLAACC',
                                'LKAAACC',
                                'LLLLACC',
                                'LMMMACC',
                                'LOVVACC',
                                'LPPCACC',
                                'LPPOOAC',
                                'LRBBACC',
                                'LSAGACC',
                                'LSAZACC',
                                'LTAAACC',
                                'LTBBACC',
                                'LUUUACC',
                                'LWSSACC',
                                'LYBAACC',
                                'LZBBACC',
                                'UDDDACC',
                                'UGGGACC',
                                'UKBVACC',
                                'UKDVACC',
                                'UKLVACC',
                                'UKOVACC',
                                'BIRDACC'
                                ,'LQSBACC')

  ),


STAT_AUA_DAY AS (
SELECT a.unit_code,
       a.unit_name,
       a.unit_country,
       a.unit_type_2,
        t.day_date,
        t.month,
        t.week,
        t.week_nb_year,
        t.day_type,
        t.day_of_week_nb AS day_of_week,
        t.year
FROM stat_aua_list a, pru_time_references t
WHERE
 (t.day_date >= ", mydate, "-1  AND t.day_date < ", mydate, ")
  or
 (t.day_date >= ", mydate, " -1 - greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0)
    AND t.day_date <  ", mydate, " - greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0)
 )
  or
 (t.day_date >= ", mydate, " - 1 - 364    AND t.day_date <  ", mydate, " - 364)
  or
 (t.day_date >= ", mydate, " - 1 - 7    AND t.day_date <  ", mydate, " - 7)

)   ,

DATA_SOURCE as
(SELECT
      trunc(agg_asp_entry_date)  AS entry_day,
      agg_asp_id          AS unit_code,
      agg_asp_ty          AS unit_type,
      agg_asp_name        AS unit_name,
      nvl (a.agg_asp_a_traffic_asp,0)  AS syn_traffic,
      nvl (a.agg_asp_delay_tvs,0)  AS syn_delay,
      nvl (a.agg_asp_delay_tvs,0) - nvl(a.agg_asp_delay_airport_tvs,0)  AS syn_er_delay

  FROM v_aiu_agg_asp a
  WHERE
 (   (
       a.AGG_ASP_ENTRY_DATE >=  ", mydate, " -  1   AND  a.AGG_ASP_ENTRY_DATE <  ", mydate, "
     )
    OR
     (  a.AGG_ASP_ENTRY_DATE >=   ", mydate, " -1 - greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0)
       AND a.AGG_ASP_ENTRY_DATE <    ", mydate, " - greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0)
      )

    OR
     (  a.AGG_ASP_ENTRY_DATE >=   ", mydate, " - 364 - 1  AND a.AGG_ASP_ENTRY_DATE <   ", mydate, " - 364
     )
    OR
     (  a.AGG_ASP_ENTRY_DATE >=   ", mydate, " - 7 -  1  AND  a.AGG_ASP_ENTRY_DATE <   ", mydate, " - 7
     )
)
     AND A.agg_asp_ty = 'AUA_STAT'
     AND A.agg_asp_unit_ty <> 'REGION'
),


STAT_AUA_DATA as (
SELECT n.YEAR,
       n.MONTH,
       n.day_date    AS entry_date,
       nvl(f.UNIT_TYPE,'AUA_STAT')   AS UNIT_KIND,
       n.unit_code,
       n.UNIT_NAME,
       nvl(f.SYN_TRAFFIC,0) AS flight,
       nvl(f.SYN_delay,0) AS dly,
       nvl(f.syn_er_delay,0) AS dly_er,
       n.WEEK,
       n.WEEK_NB_YEAR,
       n.DAY_TYPE,
       n.day_of_week
  FROM stat_aua_day  N
       LEFT JOIN DATA_SOURCE F
           ON n.unit_code = f.unit_code AND f.entry_day = n.day_date
),


STAT_AUA_DATA_2 as
(select YEAR,
       MONTH,
       ENTRY_DATE,
       unit_name,
       unit_code,
      flight,
      dly,
      dly_er,
       WEEK,
       WEEK_NB_YEAR,
       day_of_week,
       sum(flight) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(364, 'day')  PRECEDING and  NUMTODSINTERVAL( 364, 'day') PRECEDING )flight_PREV_YEAR,
       sum(flight) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day')  PRECEDING and  NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day') PRECEDING )flight_2019,
       sum(flight) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(7, 'day')  PRECEDING and  NUMTODSINTERVAL(7, 'day') PRECEDING )flight_7DAY,
       
       min(ENTRY_DATE) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(364, 'day')  PRECEDING and  NUMTODSINTERVAL( 364, 'day') PRECEDING ) ENTRY_DATE_PREV_YEAR,
       min(ENTRY_DATE) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day')  PRECEDING and  NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day') PRECEDING ) ENTRY_DATE_2019,
       min(ENTRY_DATE) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(7, 'day')  PRECEDING and  NUMTODSINTERVAL(7, 'day') PRECEDING ) ENTRY_DATE_7DAY,

       sum(dly) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(364, 'day')  PRECEDING and  NUMTODSINTERVAL( 364, 'day') PRECEDING )dly_PREV_YEAR,
       sum(dly) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day')  PRECEDING and  NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day') PRECEDING )dly_2019,
       sum(dly) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(7, 'day')  PRECEDING and  NUMTODSINTERVAL(7, 'day') PRECEDING )dly_7DAY,

       sum(dly_er) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(364, 'day')  PRECEDING and  NUMTODSINTERVAL( 364, 'day') PRECEDING ) dly_er_PREV_YEAR,
       sum(dly_er) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day')  PRECEDING and  NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day') PRECEDING ) dly_er_2019,
       sum(dly_er) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(7, 'day')  PRECEDING and  NUMTODSINTERVAL(7, 'day') PRECEDING ) dly_er_7DAY


      FROM STAT_AUA_DATA
 )  ,


  STAT_AUA_DATA_3 as
   (select YEAR,
       MONTH,
       WEEK,
       WEEK_NB_YEAR,
       day_of_week,
       unit_name,
       unit_code,
       ENTRY_DATE,
       ENTRY_DATE_PREV_YEAR,
       ENTRY_DATE_2019,
       ENTRY_DATE_7DAY,
      flight,
      flight_PREV_YEAR,
      flight_2019,
      flight_7DAY,
      dly,
      dly_PREV_YEAR,
      dly_2019,
      dly_7DAY,
      dly_er,
      dly_er_PREV_YEAR,
      dly_er_2019,
      dly_er_7DAY,

     flight - flight_PREV_YEAR  as flight_PREV_YEAR_DIFF,
     flight - flight_2019  as flight_2019_DIFF,
     flight - flight_7DAY  as flight_7DAY_DIFF,
      CASE WHEN flight_PREV_YEAR <>0
           THEN flight/flight_PREV_YEAR -1
      ELSE NULL
      END flight_PREV_YEAR_DIFF_PERC,
      CASE WHEN flight_2019 <>0
           THEN flight/flight_2019 -1
      ELSE NULL
      END flight_2019_DIFF_PERC,
      CASE WHEN flight_7DAY <>0
           THEN flight/flight_7DAY -1
      ELSE NULL
      END flight_7DAY_DIFF_PERC,

      CASE WHEN dly_PREV_YEAR <>0
           THEN dly/dly_PREV_YEAR -1
      ELSE NULL
      END dly_PREV_YEAR_DIFF_PERC,
      CASE WHEN dly_2019 <>0
           THEN dly/dly_2019 -1
      ELSE NULL
      END dly_2019_DIFF_PERC,
      CASE WHEN dly_7DAY <>0
           THEN dly/dly_7DAY -1
      ELSE NULL
      END dly_7DAY_DIFF_PERC

      FROM STAT_AUA_DATA_2
      where ENTRY_DATE= ", mydate, " -1 --'01-mar-2020'
      )


   SELECT
        unit_name,
        unit_code,
        ENTRY_DATE,
        ENTRY_DATE_2019,
        ENTRY_DATE_PREV_YEAR,
        ENTRY_DATE_7DAY,
      flight,
      flight_2019,
      flight_PREV_YEAR,
      flight_7DAY,
      flight_2019_DIFF,
      flight_PREV_YEAR_DIFF,
      flight_7DAY_DIFF,
      flight_2019_DIFF_PERC,
      flight_PREV_YEAR_DIFF_PERC,
      flight_7DAY_DIFF_PERC,

      dly,
      dly_2019,
      dly_PREV_YEAR,
      dly_7DAY,
      dly_2019_DIFF_PERC,
      dly_PREV_YEAR_DIFF_PERC,
      dly_7DAY_DIFF_PERC,

      dly_er,
      dly_er_2019,
      dly_er_PREV_YEAR,
      dly_er_7DAY

       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (flight) DESC)  r_rank_flt_day
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (flight_PREV_YEAR) desc)  r_rank_flt_day_PREV_YY
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (flight_2019) desc)  r_rank_flt_day_2019
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (flight_7DAY) desc)  r_rank_flt_day_7DAY

       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (dly) DESC)  r_rank_dly_day
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (dly_PREV_YEAR) desc)  r_rank_dly_day_PREV_YY
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (dly_2019) desc)  r_rank_dly_day_2019
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (dly_7DAY) desc)  r_rank_dly_day_7DAY

       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (dly_er) DESC)  r_rank_dly_er_day
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (dly_er_PREV_YEAR) desc)  r_rank_dly_er_day_PREV_YY
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (dly_er_2019) desc)  r_rank_dly_er_day_2019
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (dly_er_7DAY) desc)  r_rank_dly_er_day_7DAY

   FROM STAT_AUA_DATA_3
  --    order by  flight_DIFF_2019_PERC desc"
  )
}

## week ----
query_nw_acc_delay_week_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
  WITH stat_aua_list
    AS
        (SELECT stat_aua_id      AS unit_code,
                aua_name         AS unit_name,
                aua_country_code AS unit_country,
                aua_type         AS unit_type_2
           FROM aru_syn.stat_aua f
          WHERE stat_aua_id IN ('EBBUACC',
                                'EDGGALL',
                                'EDMMACC',
                                'EDUUUAC',
                                'EDWWACC',
                                'EDYYUAC',
                                'EETTACC',
                                'EFINACC',
                               -- 'EFESACC', -- code change on 22-02-2021
                                'EGGXOCA',
                                'EGPXALL',
                                'EGTTACC',
                                'EGTTTC',
                                'EHAAACC',
                                'EIDWACC',
                                'EISNACC',
                                'EKDKACC',
                                'ENBDACC',
                                'ENOSACC',
                                'ENSVACC',
                                'EPWWACC',
                                'ESMMACC',
                                'ESOSACC',
                                'EVRRACC',
                                'EYVCACC',
                                'GCCCACC',
                                'GMMMACC',
                                'GMACACC',
                                'LAAAACC',
                                'LBSRACC',
                                'LCCCACC',
                                'LDZOACC',
                                'LECBACC',
                                'LECMALL',
                                'LECPACC',
                                'LECSACC',
                                'LFBBALL',
                                'LFEEACC',
                                'LFFFALL',
                                'LFMMACC',
                                'LFRRACC',
                                'LGGGACC',
                                'LGMDACC',
                                'LHCCACC',
                                'LIBBACC',
                                'LIMMACC',
                                'LIPPACC',
                                'LIRRACC',
                                'LJLAACC',
                                'LKAAACC',
                                'LLLLACC',
                                'LMMMACC',
                                'LOVVACC',
                                'LPPCACC',
                                'LPPOOAC',
                                'LRBBACC',
                                'LSAGACC',
                                'LSAZACC',
                                'LTAAACC',
                                'LTBBACC',
                                'LUUUACC',
                                'LWSSACC',
                                'LYBAACC',
                                'LZBBACC',
                                'UDDDACC',
                                'UGGGACC',
                                'UKBVACC',
                                'UKDVACC',
                                'UKLVACC',
                                'UKOVACC',
                                'BIRDACC'
                                ,'LQSBACC')

  ),


STAT_AUA_DAY AS (
SELECT a.unit_code,
       a.unit_name,
       a.unit_country,
       a.unit_type_2,
        t.day_date,
        t.month,
        t.week,
        t.week_nb_year,
        t.day_type,
        t.day_of_week_nb AS day_of_week,
        t.year
FROM stat_aua_list a, pru_time_references t
WHERE
 (t.day_date >= ", mydate, "-7  AND t.day_date < ", mydate, ")
  or
 (t.day_date >= ", mydate, " -7 - greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0)
    AND t.day_date <  ", mydate, " - greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0)
 )  or
 (t.day_date >= ", mydate, " - 7 - 364    AND t.day_date <  ", mydate, " - 364)
  or
 (t.day_date >= ", mydate, " - 7 - 14    AND t.day_date <  ", mydate, " - 14)
 or
 (t.day_date >= ", mydate, " - 7 - 7    AND t.day_date <  ", mydate, " - 7)

)   ,

DATA_SOURCE as
(SELECT
      trunc(agg_asp_entry_date)  AS entry_day,
      agg_asp_id          AS unit_code,
      agg_asp_ty          AS unit_type,
      agg_asp_name        AS unit_name,
      nvl (a.agg_asp_a_traffic_asp,0)  AS syn_traffic,
      nvl (a.agg_asp_delay_tvs,0)  AS syn_delay,
      nvl (a.agg_asp_delay_tvs,0) - nvl(a.agg_asp_delay_airport_tvs,0)  AS syn_er_delay
  FROM v_aiu_agg_asp a
  WHERE
 (   (
       a.AGG_ASP_ENTRY_DATE >=  ", mydate, " -  7   AND  a.AGG_ASP_ENTRY_DATE <  ", mydate, "
     )
    OR
     (  a.AGG_ASP_ENTRY_DATE >=  ", mydate, " -7 - greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0)
       AND a.AGG_ASP_ENTRY_DATE <   ", mydate, " - greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0)
     )

    OR
     (  a.AGG_ASP_ENTRY_DATE >=   ", mydate, " - 364 - 7  AND a.AGG_ASP_ENTRY_DATE <   ", mydate, " - 364
     )
    OR
     (  a.AGG_ASP_ENTRY_DATE >=   ", mydate, " - 14 -  7  AND  a.AGG_ASP_ENTRY_DATE <   ", mydate, " - 14
     )
     OR
     (  a.AGG_ASP_ENTRY_DATE >=   ", mydate, " - 7 -  7  AND  a.AGG_ASP_ENTRY_DATE <   ", mydate, " - 7
     )
)
     AND A.agg_asp_ty = 'AUA_STAT'
     AND A.agg_asp_unit_ty <> 'REGION'
),


STAT_AUA_DATA as (
SELECT n.YEAR,
       n.MONTH,
       n.day_date    AS entry_date,
       nvl(f.UNIT_TYPE,'AUA_STAT')   AS UNIT_KIND,
       n.unit_code,
       n.UNIT_NAME,
       nvl(f.SYN_TRAFFIC,0) AS flight,
       nvl(f.SYN_delay,0) AS dly,
       nvl(f.syn_er_delay,0) AS dly_er,
       n.WEEK,
       n.WEEK_NB_YEAR,
       n.DAY_TYPE,
       n.day_of_week
  FROM stat_aua_day  N
       LEFT JOIN DATA_SOURCE F
           ON n.unit_code = f.unit_code AND f.entry_day = n.day_date
),


STAT_AUA_DATA_2 as
(select YEAR,
       MONTH,
       ENTRY_DATE,
       unit_name,
       unit_code,
      flight,
      dly,
      dly_er,
       WEEK,
       WEEK_NB_YEAR,
       day_of_week,
       sum(flight) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(364, 'day')  PRECEDING and  NUMTODSINTERVAL( 364, 'day') PRECEDING )flight_PREV_YEAR,
       min(ENTRY_DATE) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(364, 'day')  PRECEDING and  NUMTODSINTERVAL( 364, 'day') PRECEDING ) ENTRY_DATE_PREV_YEAR,
       sum(flight) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day')  PRECEDING
                                                                              and  NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day') PRECEDING )flight_2019,
       min(ENTRY_DATE) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day')  PRECEDING
                                                                                   and NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day') PRECEDING ) ENTRY_DATE_2019,
       sum(flight) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(7, 'day')  PRECEDING and  NUMTODSINTERVAL(7, 'day') PRECEDING )flight_7DAY,
       min(ENTRY_DATE) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(7, 'day')  PRECEDING and  NUMTODSINTERVAL(7, 'day') PRECEDING ) ENTRY_DATE_7DAY,

       sum(dly) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(364, 'day')  PRECEDING and  NUMTODSINTERVAL( 364, 'day') PRECEDING )dly_PREV_YEAR,
       sum(dly) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day')  PRECEDING
                                                                           and  NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day') PRECEDING )dly_2019,
       sum(dly) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(7, 'day')  PRECEDING and  NUMTODSINTERVAL(7, 'day') PRECEDING )dly_7DAY,

       sum(dly_er) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(364, 'day')  PRECEDING and  NUMTODSINTERVAL( 364, 'day') PRECEDING )dly_er_PREV_YEAR,
       sum(dly_er) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day')  PRECEDING
                                                                           and  NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day') PRECEDING )dly_er_2019,
       sum(dly_er) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(7, 'day')  PRECEDING and  NUMTODSINTERVAL(7, 'day') PRECEDING )dly_er_7DAY

  FROM STAT_AUA_DATA
 )  ,


 STAT_AUA_DATA_3  as
  (
      SELECT
      UNIT_NAME,
      unit_code,
      to_char(min(ENTRY_DATE), 'dd-mm-yyyy') || ' -> ' || to_char(max(entry_date),'DD-MM-YYYY') as entry_date,
      min(ENTRY_DATE) as min_entry_date,
      max(ENTRY_DATE) as max_entry_date,
      sum(flight) as flight,
      avg(flight) as daily_flight,
      sum(flight_prev_year) as flight_prev_year,
      avg(flight_prev_year) as daily_flight_prev_year,
      min(ENTRY_DATE_PREV_YEAR) as min_ENTRY_DATE_PREV_YEAR,
      max (ENTRY_DATE_PREV_YEAR) as max_ENTRY_DATE_PREV_YEAR,
      to_char(min(entry_date_prev_year), 'dd-mm-yyyy') || ' -> ' || to_char(max(entry_date_prev_year),'DD-MM-YYYY')as entry_date_prev_year,
      sum(flight_2019) as flight_2019,
      avg(flight_2019) as daily_flight_2019,
      min(ENTRY_DATE_2019) as min_ENTRY_DATE_2019,
      max (ENTRY_DATE_2019) as max_ENTRY_DATE_2019,
      to_char(min(entry_date_2019), 'dd-mm-yyyy') || ' -> ' || to_char(max(entry_date_2019),'DD-MM-YYYY') as entry_date_2019,
      sum(flight_7DAY) as flight_7DAY,
      avg(flight_7DAY) as DAILY_flight_7DAY,
      min(ENTRY_DATE_7DAY) as MIN_ENTRY_DATE_7DAY,
      max (ENTRY_DATE_7DAY) as max_ENTRY_DATE_7DAY,
      to_char(min(entry_date_7DAY), 'dd-mm-yyyy') || ' -> ' || to_char(max(entry_date_7DAY),'DD-MM-YYYY') as entry_date_7DAY,

      sum(dly) as dly,
      avg(dly) as daily_dly,
      sum(dly_prev_year) as dly_prev_year,
      avg(dly_prev_year) as daily_dly_prev_year,
      sum(dly_2019) as dly_2019,
      avg(dly_2019) as daily_dly_2019,
      sum(dly_7DAY) as dly_7DAY,
      avg(dly_7DAY) as DAILY_dly_7DAY,

      sum(dly_er) as dly_er,
      avg(dly_er) as daily_dly_er,
      sum(dly_er_prev_year) as dly_er_prev_year,
      avg(dly_er_prev_year) as daily_dly_er_prev_year,
      sum(dly_er_2019) as dly_er_2019,
      avg(dly_er_2019) as daily_dly_er_2019,
      sum(dly_er_7DAY) as dly_er_7DAY,
      avg(dly_er_7DAY) as DAILY_dly_er_7DAY

      FROM STAT_AUA_DATA_2
      WHERE entry_date >= ", mydate, " -7 and entry_date <", mydate, "
      GROUP BY UNIT_NAME,Unit_code
  )

   SELECT
       UNIT_NAME,
       unit_code,
       ENTRY_DATE,
       ENTRY_DATE_PREV_YEAR,
       ENTRY_DATE_2019,
       ENTRY_DATE_7DAY,
       MIN_ENTRY_DATE,
       MIN_ENTRY_DATE_PREV_YEAR,
       MIN_ENTRY_DATE_2019,
       MIN_ENTRY_DATE_7DAY,
       MAX_ENTRY_DATE,
       MAX_ENTRY_DATE_PREV_YEAR,
       MAX_ENTRY_DATE_2019,
       MAX_ENTRY_DATE_7DAY,
       flight,
       flight_PREV_YEAR,
       flight_2019,
        flight_7DAY,
       DAILY_flight,
       DAILY_flight_PREV_YEAR,
       DAILY_flight_2019,
       DAILY_flight_7DAY,
      CASE WHEN flight_PREV_YEAR <>0
           THEN flight/flight_PREV_YEAR -1
      ELSE NULL
      END   FLIGHT_PREV_YEAR_DIFF_PERC,
      CASE WHEN flight_2019 <>0
           THEN flight/flight_2019 -1
      ELSE NULL
      END  flight_2019_DIFF_PERC,
      CASE WHEN flight_7DAY <>0
           THEN flight/flight_7DAY -1
      ELSE NULL
      END  flight_7DAY_DIFF_PERC,

      dly,
      daily_dly,
      dly_prev_year,
      daily_dly_prev_year,
      dly_2019,
      daily_dly_2019,
      dly_7DAY,
      DAILY_dly_7DAY,

      CASE WHEN dly_PREV_YEAR <>0
           THEN dly/dly_PREV_YEAR -1
      ELSE NULL
      END dly_PREV_YEAR_DIFF_PERC,
      CASE WHEN dly_2019 <>0
           THEN dly/dly_2019 -1
      ELSE NULL
      END dly_2019_DIFF_PERC,
      CASE WHEN dly_7DAY <>0
           THEN dly/dly_7DAY -1
      ELSE NULL
      END dly_7DAY_DIFF_PERC,

      dly_er,
      daily_dly_er,
      dly_er_prev_year,
      daily_dly_er_prev_year,
      dly_er_2019,
      daily_dly_er_2019,
      dly_er_7DAY,
      DAILY_dly_er_7DAY

       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (flight) DESC)  r_rank_flt_wk
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (flight_PREV_YEAR) desc)  r_rank_flt_wk_PREV_YY
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (flight_2019) desc)  r_rank_flt_wk_2019
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (flight_7DAY) desc)  r_rank_flt_wk_7DAY

       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (dly) DESC)  r_rank_dly_wk
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (dly_PREV_YEAR) desc)  r_rank_dly_wk_PREV_YY
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (dly_2019) desc)  r_rank_dly_wk_2019
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (dly_7DAY) desc)  r_rank_dly_wk_7DAY

       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (dly_er) DESC)  r_rank_dly_er_wk
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (dly_er_PREV_YEAR) desc)  r_rank_dly_er_wk_PREV_YY
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (dly_er_2019) desc)  r_rank_dly_er_wk_2019
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (dly_er_7DAY) desc)  r_rank_dly_er_wk_7DAY

      FROM STAT_AUA_DATA_3
--      WHERE (flight <> 0 or flight_prev_year <> 0 or flight_2019 <>  0 or flight_14day <> 0 or flight_7day <>0)
")
}

## y2d ----
query_nw_acc_delay_y2d_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
WITH stat_aua_list
    AS
        (
	SELECT
		stat_aua_id AS unit_code,
		aua_name AS unit_name,
		aua_country_code AS unit_country,
		aua_type AS unit_type_2
	FROM
		aru_syn.stat_aua f
	WHERE
		stat_aua_id IN (
			'EBBUACC',
                                'EDGGALL',
                                'EDMMACC',
                                'EDUUUAC',
                                'EDWWACC',
                                'EDYYUAC',
                                'EETTACC',
                                'EFINACC',
			-- 'EFESACC', -- code change on 22-02-2021
                                'EGGXOCA',
                                'EGPXALL',
                                'EGTTACC',
                                'EGTTTC',
                                'EHAAACC',
                                'EIDWACC',
                                'EISNACC',
                                'EKDKACC',
                                'ENBDACC',
                                'ENOSACC',
                                'ENSVACC',
                                'EPWWACC',
                                'ESMMACC',
                                'ESOSACC',
                                'EVRRACC',
                                'EYVCACC',
                                'GCCCACC',
                                'GMMMACC',
                                'GMACACC',
                                'LAAAACC',
                                'LBSRACC',
                                'LCCCACC',
                                'LDZOACC',
                                'LECBACC',
                                'LECMALL',
                                'LECPACC',
                                'LECSACC',
                                'LFBBALL',
                                'LFEEACC',
                                'LFFFALL',
                                'LFMMACC',
                                'LFRRACC',
                                'LGGGACC',
                                'LGMDACC',
                                'LHCCACC',
                                'LIBBACC',
                                'LIMMACC',
                                'LIPPACC',
                                'LIRRACC',
                                'LJLAACC',
                                'LKAAACC',
                                'LLLLACC',
                                'LMMMACC',
                                'LOVVACC',
                                'LPPCACC',
                                'LPPOOAC',
                                'LRBBACC',
                                'LSAGACC',
                                'LSAZACC',
                                'LTAAACC',
                                'LTBBACC',
                                'LUUUACC',
                                'LWSSACC',
                                'LYBAACC',
                                'LZBBACC',
                                'UDDDACC',
                                'UGGGACC',
                                'UKBVACC',
                                'UKDVACC',
                                'UKLVACC',
                                'UKOVACC',
                                'BIRDACC'
                                , 'LQSBACC'
		)
)
  
,
STAT_AUA_DAY AS (
	SELECT
		a.unit_code,
		a.unit_name,
		a.unit_country,
		a.unit_type_2,
		t.day_date,
		t.month,
		t.week,
		t.week_nb_year,
		t.day_type,
		t.day_of_week_nb AS day_of_week,
		t.year
	FROM
		stat_aua_list a,
		prudev.pru_time_references t
	WHERE
		t.day_date >= TO_DATE (
			'01-01-2019',
			'dd-mm-yyyy'
		)
			AND TO_NUMBER (
				TO_CHAR (
					TRUNC (t.day_date),
					'mmdd'
				)
			) <= TO_NUMBER (
				TO_CHAR (
					(
						", mydate, "-1
					),
					'mmdd'
				)
			)
				AND YEAR <= EXTRACT(YEAR FROM (", mydate, " - 1))
				--order by t.day_date desc
) ,    
 
DATA_SOURCE AS
(
	SELECT
		trunc(agg_asp_entry_date) AS entry_day,
		agg_asp_id AS unit_code,
		agg_asp_ty AS unit_type,
		agg_asp_name AS unit_name,
		nvl (
			a.agg_asp_a_traffic_asp,
			0
		) AS syn_traffic,
		nvl (
			a.agg_asp_delay_tvs,
			0
		) AS syn_delay,
		nvl (
			a.agg_asp_delay_tvs,
			0
		) - nvl(a.agg_asp_delay_airport_tvs, 0) AS syn_er_delay
	FROM
		prudev.v_aiu_agg_asp a
	WHERE
		a.AGG_ASP_ENTRY_DATE >= TO_DATE (
			'01-01-2019',
			'dd-mm-yyyy'
		)
			AND TO_NUMBER (
				TO_CHAR (
					TRUNC (a.AGG_ASP_ENTRY_DATE),
					'mmdd'
				)
			) <= TO_NUMBER (
				TO_CHAR (
					(
						", mydate, "-1
					),
					'mmdd'
				)
			)
				AND A.agg_asp_ty = 'AUA_STAT'
				AND A.agg_asp_unit_ty <> 'REGION'
),


STAT_AUA_DATA AS (
	SELECT
		n.YEAR,
		n.MONTH,
		n.day_date AS entry_date,
		nvl(f.UNIT_TYPE, 'AUA_STAT') AS UNIT_KIND,
		n.unit_code,
		n.UNIT_NAME,
		nvl(f.SYN_TRAFFIC, 0) AS flight,
		nvl(f.SYN_delay, 0) AS dly,
		nvl(f.syn_er_delay, 0) AS dly_er,
		n.WEEK,
		n.WEEK_NB_YEAR,
		n.DAY_TYPE,
		n.day_of_week
	FROM
		stat_aua_day N
	LEFT JOIN DATA_SOURCE F
           ON
		n.unit_code = f.unit_code
		AND f.entry_day = n.day_date
),

STAT_AUA_CALC AS(
      SELECT
      UNIT_NAME, 
      unit_code,
      entry_date,
      YEAR,
      min(ENTRY_DATE) OVER (PARTITION BY unit_name, YEAR) AS min_date,
      max(ENTRY_DATE) OVER (PARTITION BY unit_name, YEAR ) AS max_date,
      flight,
      dly,
      dly_er,
      sum(dly) OVER (PARTITION BY unit_name, YEAR ORDER BY ENTRY_DATE RANGE BETWEEN NUMTODSINTERVAL(to_number(to_char(ENTRY_DATE, 'DDD')), 'day') 
        PRECEDING AND NUMTODSINTERVAL( 0, 'day') PRECEDING ) AS y2d_dly,
      avg(dly) OVER (PARTITION BY unit_name, YEAR ORDER BY ENTRY_DATE RANGE BETWEEN NUMTODSINTERVAL(to_number(to_char(ENTRY_DATE, 'DDD')), 'day') 
        PRECEDING AND NUMTODSINTERVAL( 0, 'day') PRECEDING ) AS y2d_avg_dly,
      sum(dly_er) OVER (PARTITION BY unit_name, YEAR ORDER BY ENTRY_DATE RANGE BETWEEN NUMTODSINTERVAL(to_number(to_char(ENTRY_DATE, 'DDD')), 'day') 
        PRECEDING AND NUMTODSINTERVAL( 0, 'day') PRECEDING ) AS y2d_dly_er,
      avg(dly_er) OVER (PARTITION BY unit_name, YEAR ORDER BY ENTRY_DATE RANGE BETWEEN NUMTODSINTERVAL(to_number(to_char(ENTRY_DATE, 'DDD')), 'day') 
        PRECEDING AND NUMTODSINTERVAL( 0, 'day') PRECEDING ) AS y2d_avg_dly_er,
      sum(flight) OVER (PARTITION BY unit_name, YEAR ORDER BY ENTRY_DATE RANGE BETWEEN NUMTODSINTERVAL(to_number(to_char(ENTRY_DATE, 'DDD')), 'day') 
        PRECEDING AND NUMTODSINTERVAL( 0, 'day') PRECEDING ) AS y2d_flight,
      avg(flight) OVER (PARTITION BY unit_name, YEAR ORDER BY ENTRY_DATE RANGE BETWEEN NUMTODSINTERVAL(to_number(to_char(ENTRY_DATE, 'DDD')), 'day') 
        PRECEDING AND NUMTODSINTERVAL( 0, 'day') PRECEDING ) AS y2d_avg_flight
 
      FROM STAT_AUA_DATA
      WHERE entry_date < ", mydate, "
--      order by UNIT_NAME, entry_date 
),

data_last_day AS (
	SELECT
		UNIT_NAME,
		unit_code,
		entry_date,
		YEAR,
		min_date,
		max_date,
		flight,
		dly,
		y2d_dly,
		y2d_avg_dly,
		y2d_dly_er,
		y2d_avg_dly_er,
		y2d_flight,
		y2d_avg_flight
		--    case when ENTRY_DATE = max_date
		----    case when ENTRY_DATE = ", mydate, "-1
		--        then 'yes'
		--        else '-'
		--    end flag_last_day
	FROM
		STAT_AUA_CALC
	WHERE
		entry_date = max_date
)

SELECT
	a.*,
	b.flight AS flight_py,
	b.dly AS dly_py,
	b.y2d_dly AS y2d_dly_py,
	b.y2d_avg_dly AS y2d_avg_dly_py,
	b.y2d_dly_er AS y2d_dly_er_py,
	b.y2d_avg_dly_er AS y2d_avg_dly_er_py,
	b.y2d_flight AS y2d_flight_py,
	b.y2d_avg_flight AS y2d_avg_flight_py,
	
	c.flight AS flight_2019,
	c.dly AS dly_2019,
	c.y2d_dly AS y2d_dly_2019,
	c.y2d_avg_dly AS y2d_avg_dly_2019,
	c.y2d_dly_er AS y2d_dly_er_2019,
	c.y2d_avg_dly_er AS y2d_avg_dly_er_2019,
	c.y2d_flight AS y2d_flight_2019,
	c.y2d_avg_flight AS y2d_avg_flight_2019
FROM
	data_last_day a
LEFT JOIN data_last_day b ON
	a.YEAR -1 = b.YEAR
	AND a.unit_code = b.unit_code
LEFT JOIN data_last_day c ON
	2019 = c.YEAR
	AND a.unit_code = c.unit_code
WHERE a.YEAR = EXTRACT (YEAR FROM (", mydate, " -1))
  ")
}

# nw state delay ----
## day ----
query_nw_st_delay_day_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
with DATA_CALC as (
SELECT     agg_asp_entry_date as DY_TO_DATE,
             agg_asp_id DY_CTRY_DLY_CODE,
              CASE
             WHEN agg_asp_id = 'LY' then 'Serbia/Montenegro'
                WHEN agg_asp_id = 'LT' THEN 'Türkiye'
                WHEN agg_asp_id = 'LQ' THEN 'Bosnia and Herzegovina'
                ELSE agg_asp_name
             END DY_CTRY_DLY_NAME,
             SUM (coalesce(a.agg_asp_a_traffic_asp,0)) as DY_CTRY_FLT,
             SUM(coalesce(a.agg_asp_delay_tvs,0)) as DY_CTRY_DLY,
              (SUM (coalesce(a.agg_asp_delay_tvs,0)) - SUM (coalesce(a.agg_asp_delay_airport_tvs,0)))  AS DY_CTRY_DLY_ERT,
             SUM (coalesce(a.agg_asp_delay_airport_tvs,0))  AS DY_CTRY_DLY_APT,
             SUM (coalesce(a.agg_asp_delayed_traffic_tvs,0)) as DY_CTRY_DLYED_FLT
       FROM v_aiu_agg_asp a
       WHERE
             a.AGG_ASP_ENTRY_DATE = ", mydate, "-1
             and agg_asp_ty = 'COUNTRY_AUA'  AND A.agg_asp_unit_ty <> 'REGION'
             AND (SUBSTR(a.agg_asp_id,1,1) IN ('E','L')
         OR SUBSTR(a.agg_asp_id,1,2) IN ('GC','GM','GE','UD','UG','UK','YY'))
    GROUP BY
             agg_asp_entry_date,
             agg_asp_id,
             agg_asp_ty,
             agg_asp_name
)

select a.*,
      case when DY_CTRY_FLT = 0
          then 0
          else DY_CTRY_DLY/DY_CTRY_FLT
      end DY_CTRY_DLY_PER_FLT
from DATA_CALC a
where DY_CTRY_DLY_NAME <> 'Ukraine'
"
  )
}

## week ----
query_nw_st_delay_week_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
WITH
    DATA_DAY
    AS
 (
 SELECT     agg_asp_entry_date as entry_date,
             agg_asp_id COUNTRY_CODE,
             agg_asp_ty as TYPE,
              CASE
                WHEN agg_asp_id = 'LY' then 'Serbia/Montenegro'
                WHEN agg_asp_id = 'LT' THEN 'Türkiye'
                WHEN agg_asp_id = 'LQ' THEN 'Bosnia and Herzegovina'
                ELSE agg_asp_name
             END COUNTRY_NAME,
             SUM (coalesce(a.agg_asp_a_traffic_asp,0)) as flight,
             SUM(coalesce(a.agg_asp_delay_tvs,0)) as delay,
             (SUM (coalesce(a.agg_asp_delay_tvs,0)) - SUM (coalesce(a.agg_asp_delay_airport_tvs,0)))  AS ERT_DELAY,
             SUM (coalesce(a.agg_asp_delay_airport_tvs,0))  AS ARP_DELAY,
             SUM (coalesce(a.agg_asp_delayed_traffic_tvs,0)) as DELAY_FLIGHT,
             (SUM (coalesce(a.agg_asp_delayed_traffic_tvs,0))  - SUM (coalesce(  a.agg_asp_delayed_traffic_ad_tvs,0))) as ERT_DELAY_FLIGHT,
              SUM (coalesce(a.agg_asp_delayed_traffic_ad_tvs,0))  AS ARP_DELAY_FLIGHT
       FROM v_aiu_agg_asp a
       WHERE
             agg_asp_entry_date >= '24-dec-2018' AND a.AGG_ASP_ENTRY_DATE < ", mydate, "
             and agg_asp_ty = 'COUNTRY_AUA'  AND A.agg_asp_unit_ty <> 'REGION'
             AND (SUBSTR(a.agg_asp_id,1,1) IN ('E','L')
         OR SUBSTR(a.agg_asp_id,1,2) IN ('GC','GM','GE','UD','UG','UK','YY'))
    GROUP BY
             agg_asp_entry_date,
             agg_asp_id,
             agg_asp_ty,
             agg_asp_name
  ),

  LIST_COUNTRY as
  (select distinct country_code, country_name from DATA_DAY
  ),

  ALL_DAY
    AS
        (SELECT
                 a.country_code,
                 a.country_name,
                 t.day_date,
                 case
                     when (t.day_date >= ", mydate, "-7  AND t.day_date < ", mydate, ") then '1_ROL_WEEK'
                     when  (t.day_date >= ", mydate, " -7 - ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                           AND t.day_date <  ", mydate, " - ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 )
                        then '5_ROL_WEEK_2019'
                     when  (t.day_date >= ", mydate, " - 7 - 364    AND t.day_date <  ", mydate, " - 364) then '4_ROL_WEEK_PREV_YEAR'
                     when  (t.day_date >= ", mydate, " - 7 - 14    AND t.day_date <  ", mydate, " - 14) then '3_ROL_WEEK_14_DAY'
                     when   (t.day_date >= ", mydate, " - 7 - 7    AND t.day_date <  ", mydate, " - 7) then  '2_ROL_WEEK_7_DAY'
                 end PERIOD_TYPE
           FROM pru_time_references t,
                LIST_COUNTRY a
            WHERE
             (  (t.day_date >= ", mydate, "-7  AND t.day_date < ", mydate, ")
            or(t.day_date >= ", mydate, " -7 - ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                AND t.day_date <  ", mydate, " - ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 )
            or (t.day_date >= ", mydate, " - 7 - 364    AND t.day_date <  ", mydate, " - 364)
            or (t.day_date >= ", mydate, " - 7 - 14    AND t.day_date <  ", mydate, " - 14)
            or (t.day_date >= ", mydate, " - 7 - 7    AND t.day_date <  ", mydate, " - 7)
             )
       ),

    ALL_DAY_DATA
    AS
        (SELECT
                b.period_type,
                a.country_code,
                a.country_name,
                COALESCE (a.ENTRY_DATE, b.day_date)   AS ENTRY_DATE,
                COALESCE (a.FLIGHT, 0)                AS FLIGHT,
                COALESCE (a.delay, 0)                 AS delay,
                COALESCE (a.ARP_DELAY, 0)             AS ARP_DELAY,
                COALESCE (a.ERT_DELAY, 0)             AS ERT_DELAY,
                COALESCE (a.delay_FLIGHT, 0)          AS delay_FLIGHT,
                COALESCE (a.ARP_DELAY, 0)             AS ARP_DELAY_FLIGHT,
                COALESCE (a.ERT_DELAY, 0)             AS ERT_DELAY_FLIGHT
           FROM ALL_DAY b LEFT JOIN DATA_DAY a ON  b.day_date =a.ENTRY_DATE  and b.country_code = a.country_code  and b.country_name = a.country_name
         )

      SELECT period_type,
            country_code,
            country_name,
         MIN (entry_date)          FROM_DATE,
         MAX (entry_date)          TO_DATE,
          SUM (flight)              flight,
         SUM (delay)               DELAY,
         SUM (ERT_DELAY)           ERT_DELAY,
         SUM (ARP_DELAY)           ARP_DELAY,
         SUM (delay_FLIGHT)        DELAY_FLIGHT,
         SUM (ERT_DELAY_FLIGHT)    ERT_DELAY_FLIGHT,
         SUM (ARP_DELAY_FLIGHT)    ARP_DELAY_FLIGHT,
         AVG (delay)               avg_DELAY,
         AVG (flight)              avg_flight,
         AVG (ERT_DELAY)           AVG_ERT_DELAY,
         AVG (ARP_DELAY)           AVG_ARP_DELAY,
         AVG (delay_FLIGHT)        AVG_DELAY_FLIGHT,
         AVG (ERT_DELAY_FLIGHT)    AVG_ERT_DELAY_FLIGHT,
         AVG (ARP_DELAY_FLIGHT)    AVG_ARP_DELAY_FLIGHT
    FROM ALL_DAY_DATA
GROUP BY period_type, country_code, country_name
order by period_type, country_name
"
  )
}

## y2d ----
query_nw_st_delay_y2d_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
WITH
    DATA_DAY
    AS
 (
 SELECT     agg_asp_entry_date as entry_date,
             agg_asp_id COUNTRY_CODE,
             agg_asp_ty as TYPE,
              CASE
                WHEN agg_asp_id = 'LT' THEN 'Türkiye'
             WHEN agg_asp_id = 'LY' then 'Serbia/Montenegro'
                WHEN agg_asp_id = 'LQ' THEN 'Bosnia and Herzegovina'
                ELSE agg_asp_name
             END COUNTRY_NAME,
             SUM (coalesce(a.agg_asp_a_traffic_asp,0)) as flight,
             SUM(coalesce(a.agg_asp_delay_tvs,0)) as delay,
             (SUM (coalesce(a.agg_asp_delay_tvs,0)) - SUM (coalesce(a.agg_asp_delay_airport_tvs,0)))  AS ERT_DELAY,
             SUM (coalesce(a.agg_asp_delay_airport_tvs,0))  AS ARP_DELAY,
             SUM (coalesce(a.agg_asp_delayed_traffic_tvs,0)) as DELAY_FLIGHT,
             (SUM (coalesce(a.agg_asp_delayed_traffic_tvs,0))  - SUM (coalesce(  a.agg_asp_delayed_traffic_ad_tvs,0))) as ERT_DELAY_FLIGHT,
              SUM (coalesce(a.agg_asp_delayed_traffic_ad_tvs,0))  AS ARP_DELAY_FLIGHT
       FROM v_aiu_agg_asp a
       WHERE
             agg_asp_entry_date >= '01-JAN-2019' AND a.AGG_ASP_ENTRY_DATE < ", mydate, "
             and agg_asp_ty = 'COUNTRY_AUA'  AND A.agg_asp_unit_ty <> 'REGION'
             AND (SUBSTR(a.agg_asp_id,1,1) IN ('E','L')
         OR SUBSTR(a.agg_asp_id,1,2) IN ('GC','GM','GE','UD','UG','UK', 'YY'))
    GROUP BY
             agg_asp_entry_date,
             agg_asp_id,
             agg_asp_ty,
             agg_asp_name
  ),

  LIST_COUNTRY as
  (select distinct country_code, country_name from DATA_DAY
  ),

  ALL_DAY
    AS
        (SELECT
                 a.country_code,
                 a.country_name,
                 t.day_date,
                 t.year
           FROM pru_time_references t,
                LIST_COUNTRY  a
           WHERE
               day_date >= TO_DATE ('01-01-2019', 'dd-mm-yyyy')   AND day_date < ", mydate, "
                AND TO_NUMBER (TO_CHAR (t.day_date, 'mmdd')) <=   TO_NUMBER (TO_CHAR (", mydate, "-1, 'mmdd'))
       ),

    ALL_DAY_DATA
    AS
        (SELECT
                b.year,
                a.country_code,
                a.country_name,
                COALESCE (a.ENTRY_DATE, b.day_date)   AS ENTRY_DATE,
                COALESCE (a.FLIGHT, 0)                AS FLIGHT,
                COALESCE (a.delay, 0)                 AS delay,
                COALESCE (a.ARP_DELAY, 0)             AS ARP_DELAY,
                COALESCE (a.ERT_DELAY, 0)             AS ERT_DELAY,
                COALESCE (a.delay_FLIGHT, 0)          AS delay_FLIGHT,
                COALESCE (a.ARP_DELAY, 0)             AS ARP_DELAY_FLIGHT,
                COALESCE (a.ERT_DELAY, 0)             AS ERT_DELAY_FLIGHT
           FROM ALL_DAY b LEFT JOIN DATA_DAY a ON  b.day_date =a.ENTRY_DATE  and b.country_code = a.country_code  and b.country_name = a.country_name
         )

      SELECT year,
         MIN (entry_date)          FROM_DATE,
         MAX (entry_date)          TO_DATE,
         country_code,
         country_name,
         SUM (flight)              flight,
         SUM (delay)               DELAY,
         SUM (ERT_DELAY)           ERT_DELAY,
         SUM (ARP_DELAY)           ARP_DELAY,
         SUM (delay_FLIGHT)        DELAY_FLIGHT,
         SUM (ERT_DELAY_FLIGHT)    ERT_DELAY_FLIGHT,
         SUM (ARP_DELAY_FLIGHT)    ARP_DELAY_FLIGHT,
         AVG (delay)               avg_DELAY,
         AVG (flight)              avg_flight,
         AVG (ERT_DELAY)           AVG_ERT_DELAY,
         AVG (ARP_DELAY)           AVG_ARP_DELAY,
         AVG (delay_FLIGHT)        AVG_DELAY_FLIGHT,
         AVG (ERT_DELAY_FLIGHT)    AVG_ERT_DELAY_FLIGHT,
         AVG (ARP_DELAY_FLIGHT)    AVG_ARP_DELAY_FLIGHT
    FROM ALL_DAY_DATA
GROUP BY year, country_code, country_name
order by year desc, country_name
")
}

# airport punctuality ----
query_ap_punct <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0(
    "WITH
  --Getting the list of airports
LIST_AIRPORT  as
(select * from pruread.v_aiu_app_list_airport ),

--creating a table with the airport codes and dates since 2019
  AP_DAY AS
  (SELECT
              a.ec_ap_code as arp_code,
              a.ec_ap_name as arp_name,
              t.year,
              t.month,
              t.week,
              t.week_nb_year,
              t.day_type,
              t.day_of_week_nb AS day_of_week,
              t.day_date
      FROM LIST_AIRPORT a, prudev.pru_time_references t
      WHERE
         t.day_date >= to_date('24-12-2018','DD-MM-YYYY')
         AND t.day_date < ", mydate, "
         )
    --Joining the airport dates table with the aiport punctuality table
    SELECT
              a.*,
              AP_DESC,
              DEP_PUNCTUALITY_PERCENTAGE ,
              DEPARTURE_FLIGHTS ,
              AVG_DEPARTURE_SCHEDULE_DELAY ,
              ARR_PUNCTUALITY_PERCENTAGE ,
              ARRIVAL_FLIGHTS ,
              AVG_ARRIVAL_SCHEDULE_DELAY ,
              MISSING_SCHEDULES_PERCENTAGE ,
              DEP_PUNCTUAL_FLIGHTS,
              ARR_PUNCTUAL_FLIGHTS ,
              DEP_SCHED_DELAY ,
              ARR_SCHED_DELAY ,
              MISSING_SCHED_FLIGHTS ,
              DEP_SCHEDULE_FLIGHT ,
              ARR_SCHEDULE_FLIGHT ,
              DEP_FLIGHTS_NO_OVERFLIGHTS
      FROM AP_DAY a
      left join LDW_VDM.VIEW_FAC_PUNCTUALITY_AP_DAY b
      on a.arp_code = b.\"ICAO_CODE\" AND a.day_date = b.\"DATE\"
"
  )
}
