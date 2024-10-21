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
                                'UKOVACC'
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
       min(ENTRY_DATE) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(364, 'day')  PRECEDING and  NUMTODSINTERVAL( 364, 'day') PRECEDING ) ENTRY_DATE_PREV_YEAR,
       sum(flight) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day')  PRECEDING and  NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day') PRECEDING )flight_2019,
       min(ENTRY_DATE) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day')  PRECEDING and  NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day') PRECEDING ) ENTRY_DATE_2019,
       sum(flight) over (PARTITION BY unit_name ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(7, 'day')  PRECEDING and  NUMTODSINTERVAL(7, 'day') PRECEDING )flight_7DAY,
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
                                'UKOVACC'
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
                                'UKOVACC'
                                ,'LQSBACC')

  )

, STAT_AUA_DAY AS (
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
 t.day_date >= TO_DATE ('01-01-2019', 'dd-mm-yyyy')
 AND TO_NUMBER (TO_CHAR (TRUNC (t.day_date), 'mmdd')) <=   TO_NUMBER (TO_CHAR ((", mydate, "-1), 'mmdd'))
and year <= extract(year from (", mydate, " - 1))
--order by t.day_date desc
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
     a.AGG_ASP_ENTRY_DATE >= TO_DATE ('01-01-2019', 'dd-mm-yyyy')
    AND TO_NUMBER (TO_CHAR (TRUNC (a.AGG_ASP_ENTRY_DATE), 'mmdd')) <=   TO_NUMBER (TO_CHAR ((", mydate, "-1), 'mmdd'))
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

STAT_AUA_CALC as(
      SELECT
      UNIT_NAME,
      unit_code,
      entry_date,
      year,
      min(ENTRY_DATE) over (PARTITION BY unit_name, year) as min_date,
      max(ENTRY_DATE) over (PARTITION BY unit_name, year ) as max_date,
      flight,
      dly,
      dly_er,
      sum(dly) over (PARTITION BY unit_name, year ORDER BY ENTRY_DATE range between NUMTODSINTERVAL(to_number(to_char(ENTRY_DATE, 'DDD')), 'day')
        PRECEDING and  NUMTODSINTERVAL( 0, 'day') PRECEDING ) as y2d_dly,
      avg(dly) over (PARTITION BY unit_name, year ORDER BY ENTRY_DATE range between NUMTODSINTERVAL(to_number(to_char(ENTRY_DATE, 'DDD')), 'day')
        PRECEDING and  NUMTODSINTERVAL( 0, 'day') PRECEDING ) as y2d_avg_dly,
      sum(dly_er) over (PARTITION BY unit_name, year ORDER BY ENTRY_DATE range between NUMTODSINTERVAL(to_number(to_char(ENTRY_DATE, 'DDD')), 'day')
        PRECEDING and  NUMTODSINTERVAL( 0, 'day') PRECEDING ) as y2d_dly_er,
      avg(dly_er) over (PARTITION BY unit_name, year ORDER BY ENTRY_DATE range between NUMTODSINTERVAL(to_number(to_char(ENTRY_DATE, 'DDD')), 'day')
        PRECEDING and  NUMTODSINTERVAL( 0, 'day') PRECEDING ) as y2d_avg_dly_er,
      sum(flight) over (PARTITION BY unit_name, year ORDER BY ENTRY_DATE range between NUMTODSINTERVAL(to_number(to_char(ENTRY_DATE, 'DDD')), 'day')
        PRECEDING and  NUMTODSINTERVAL( 0, 'day') PRECEDING ) as y2d_flight,
      avg(flight) over (PARTITION BY unit_name, year ORDER BY ENTRY_DATE range between NUMTODSINTERVAL(to_number(to_char(ENTRY_DATE, 'DDD')), 'day')
        PRECEDING and  NUMTODSINTERVAL( 0, 'day') PRECEDING ) as y2d_avg_flight

      FROM STAT_AUA_DATA
      where entry_date < ", mydate, "
--      order by UNIT_NAME, entry_date
)

Select
      UNIT_NAME,
      unit_code,
      entry_date,
      year,
      min_date,
      max_date,
      flight,
      dly,
      y2d_dly,
      y2d_avg_dly,
      y2d_dly_er,
      y2d_avg_dly_er,
      y2d_flight,
      y2d_avg_flight,
    case when ENTRY_DATE = ", mydate, "-1
        then 'yes'
        else '-'
    end flag_last_day
from STAT_AUA_CALC
where flag_last_day = 'yes'
")
}

# nw airport delay all periods ----
query_nw_apt_delay_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
  with

LIST_AIRPORT as
(select a.code as arp_code,
        a.id as arp_id ,
    a.dashboard_name as arp_name
 from prudev.pru_airport a
 where a.code in (
   'EBLG',
'EBBR','EDDF','EDDL','EDDM','EFHK','EGCC','EGKK','EGLL','EGSS','EHAM','EIDW','EKCH','ENGM','EPWA','ESSA','LEBL','LEMD','LEPA','LFMN','LFPG','LFPO','LGAV',
'LIMC','LIRF','LLBG','LOWW','LPPT','LSGG','LSZH','EYVI','LBSF','LIME','LTBJ','EDDP'
,'EGLC','EGPD','LFBO','LGTS','LICC','LTAC','LTFJ','UKBB','EBCI',
'EDDK','EDDN','EGAA','EGPF','GCLP','LCLK','LGIR','LICJ','LKPR','LMML','LTAI','EDDH','EGGD','EPKK','ESSB','EVRA','GCXO','LFBD','LFLL','LROP','EGNX','EGPH',
'GMMX','LEVC','LEZL','LYBE','LPPR','EDDB','GMMN','LIRA','LPFR','EGBB','ELLX','LEAL','LHBP','LIPE','ENZV','LEMG','LIML','EGGW','ENVA','ENBR','EDDS','GCTS','LFSB',
'EDDV','ESGG','LFRS','LEIB','LFML','LEBB','LFPB','LIPZ','LIRN','GCRR','LTFM','LTBA','LATI', 'UDYZ', 'LDZA', 'EETN', 'LJLJ', 'LZIB', 'LUKK', 'LWSK','LYPG', 'LQSA','UGTB')
 )
 ,


 AIRPORT_DAY
as
(select a.arp_id,
       a.arp_code,
       a.arp_name,
       t.day_date,
       t.year,
       t.month
 from LIST_AIRPORT a, pru_time_references t
where
      t.day_date >= to_date('24-12-2018','dd-mm-yyyy')
  AND t.day_date < ", mydate, "
),

ARP_SYN_ARR
    AS
(SELECT SUM (nvl(f.ades_day_all_trf,0)) AS flt_arr,
                 trunc(f.ades_DAY_FLT_DATE) as entry_date,
                 f.ades_day_ades_ctfm as arp_code
     from  v_aiu_agg_arr_day f
     where f.ades_DAY_FLT_DATE >=to_date('24-12-2018','dd-mm-yyyy')
 GROUP BY
         trunc(f.ades_DAY_FLT_DATE), f.ades_day_ades_ctfm
  ),

ARP_SYN_DEP
    AS
        (  SELECT SUM (nvl(adep_day_all_trf,0)) AS flt_DEP,
                trunc(f.adep_DAY_FLT_DATE) as entry_date,
                f.adep_day_adep as arp_code
             FROM v_aiu_agg_dep_day f
              where f.adep_DAY_FLT_DATE >=to_date('24-12-2018','dd-mm-yyyy')
            GROUP BY  trunc(f.adep_DAY_FLT_DATE), f.adep_day_adep
 ),

ARP_SYN_DEP_ARR as (
select coalesce(d.entry_date,a.entry_date) as entry_date,
        coalesce(d.arp_code,a.arp_code) as arp_code,
        nvl(d.flt_dep,0) as flt_dep, nvl(a.flt_arr,0) as flt_arr, nvl(d.flt_dep,0)+nvl(a.flt_arr,0) as flt_dep_arr
from arp_syn_dep d full outer join  arp_syn_arr a
 on ( d.arp_code = a.arp_code and d.entry_date = a.entry_date )
),

ARP_DELAY_ARR as (
    SELECT
        trunc(a.agg_flt_a_first_entry_date) as entry_date,
        REF_LOC_ID,
        SUM (NVL(a.agg_flt_total_delay, 0)) AS dly_arr

    from v_aiu_agg_flt_flow a
where a.AGG_FLT_MP_REGU_LOC_TY = 'Airport' and a.MP_REGU_LOC_CAT = 'Arrival'
and a.agg_flt_a_first_entry_date > to_date('24-12-2018','dd-mm-yyyy')
GROUP BY trunc(a.agg_flt_a_first_entry_date), REF_LOC_ID
),

ARP_DELAY_DEP as (
    SELECT
        trunc(a.agg_flt_a_first_entry_date) as entry_date,
        REF_LOC_ID,
        SUM (NVL(a.agg_flt_total_delay, 0)) AS dly_dep

    from v_aiu_agg_flt_flow a
where a.AGG_FLT_MP_REGU_LOC_TY = 'Airport' and a.MP_REGU_LOC_CAT = 'Departure'
and a.agg_flt_a_first_entry_date > to_date('24-12-2018','dd-mm-yyyy')
GROUP BY trunc(a.agg_flt_a_first_entry_date), REF_LOC_ID
),

ARP_JOIN_DLY as (
select
      u.arp_code,
      u.day_date AS FLIGHT_DATE,
      u.arp_name,
      coalesce(a.dly_arr,0) as dly_arr,
      coalesce(d.dly_dep,0) as dly_dep,
      coalesce(a.dly_arr,0) + coalesce(d.dly_dep,0) as dly_dep_arr,
      u.year,
      u.month
 from AIRPORT_DAY u
    left join ARP_DELAY_ARR a
    on u.day_date = a.entry_date and u.arp_code = a.REF_LOC_ID
    left join ARP_DELAY_DEP d
    on u.day_date = d.entry_date and u.arp_code = d.REF_LOC_ID
),

ARP_ALL_DATA as (
select u.*,
     a.flt_arr, flt_dep, a.flt_dep_arr
    from ARP_JOIN_DLY u
    left join ARP_SYN_DEP_ARR a on u.arp_code = a.arp_code and u.FLIGHT_DATE = a.entry_date
),

 ARP_CALC as
(select arp_code,
        arp_name,
       flight_date,
       flt_dep,
       flt_arr,
       flt_dep_arr,
       dly_dep,
       dly_arr,
       dly_dep_arr,
       year,
       month,
       LAG (dly_DEP_ARR, 364) OVER (PARTITION BY arp_code ORDER BY flight_date)  dly_DEP_ARR_PREV_YEAR,
       LAG (flt_DEP_ARR, 364) OVER (PARTITION BY arp_code ORDER BY flight_date)  flt_dep_ARR_PREV_YEAR,
       LAG (dly_ARR, 364) OVER (PARTITION BY arp_code ORDER BY flight_date)  dly_ARR_PREV_YEAR,
       LAG (flt_ARR, 364) OVER (PARTITION BY arp_code ORDER BY flight_date)  flt_ARR_PREV_YEAR,
       LAG (flight_date, 364) OVER (PARTITION BY arp_code ORDER BY flight_date) FLIGHT_DATE_PREV_YEAR,
       ADD_MONTHS(flight_date,-12) as SAME_DATE_PREV_YEAR,
       ADD_MONTHS(flight_date,-12*(extract(year from flight_date)-2019)) as SAME_DATE_2019,
       LAG (dly_DEP_ARR, greatest((extract (year from flight_date)-2019) *364+ floor((extract (year from flight_date)-2019)/4)*7,0) ) OVER (PARTITION BY arp_code ORDER BY flight_date) dly_DEP_ARR_2019,
       LAG (flt_DEP_ARR, greatest((extract (year from flight_date)-2019) *364+ floor((extract (year from flight_date)-2019)/4)*7,0) ) OVER (PARTITION BY arp_code ORDER BY flight_date) flt_DEP_ARR_2019,
       LAG (dly_ARR, greatest((extract (year from flight_date)-2019) *364+ floor((extract (year from flight_date)-2019)/4)*7,0) ) OVER (PARTITION BY arp_code ORDER BY flight_date) dly_ARR_2019,
       LAG (flt_ARR, greatest((extract (year from flight_date)-2019) *364+ floor((extract (year from flight_date)-2019)/4)*7,0) ) OVER (PARTITION BY arp_code ORDER BY flight_date) flt_ARR_2019,
       LAG (flight_date, greatest((extract (year from flight_date)-2019) *364+ floor((extract (year from flight_date)-2019)/4)*7,0) ) OVER (PARTITION BY arp_code ORDER BY flight_date) FLIGHT_DATE_2019,
       AVG (dly_DEP_ARR)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ROLL_WEEK_dly,
       AVG (flt_DEP_ARR)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ROLL_WEEK_flt,
       AVG (dly_ARR)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ROLL_WEEK_dly_arr,
       AVG (flt_ARR)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ROLL_WEEK_arr,
       AVG (flt_DEP_ARR)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN to_number(to_char(flight_date,'DDD'))-1 PRECEDING AND CURRENT ROW) AS Y2d_avg_flt,
       AVG (flt_ARR)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN to_number(to_char(flight_date,'DDD'))-1 PRECEDING AND CURRENT ROW) AS Y2d_avg_arr,
       AVG (dly_DEP_ARR)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN to_number(to_char(flight_date,'DDD'))-1 PRECEDING AND CURRENT ROW) AS Y2d_avg_dly,
       AVG (dly_ARR)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN to_number(to_char(flight_date,'DDD'))-1 PRECEDING AND CURRENT ROW) AS Y2d_avg_dly_arr
FROM ARP_ALL_DATA
),

ARP_CALC_PREV  as
  (
      select arp_code,
        arp_name,
       flight_date,
       flt_dep,
       flt_arr,
       flt_dep_arr,
       dly_dep,
       dly_arr,
       dly_dep_arr,
       year,
       month,
       Y2d_avg_dly,
       Y2d_avg_dly_arr,
       Y2d_avg_flt,
       Y2d_avg_arr,
       dly_DEP_ARR_PREV_YEAR,
       dly_ARR_PREV_YEAR,
       flt_DEP_ARR_PREV_YEAR,
       flt_ARR_PREV_YEAR,
       FLIGHT_DATE_PREV_YEAR,
       dly_ARR_2019,
       dly_DEP_ARR_2019,
       flt_DEP_ARR_2019,
       flt_ARR_2019,
       FLIGHT_DATE_2019,
       ROLL_WEEK_dly,
       ROLL_WEEK_dly_arr,
       ROLL_WEEK_flt,
       ROLL_WEEK_arr,
       AVG (dly_DEP_ARR_PREV_YEAR)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ROLL_WEEK_dly_PREV_YEAR,
       AVG (dly_ARR_PREV_YEAR)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ROLL_WEEK_dly_arr_PREV_YEAR,
       AVG (flt_DEP_ARR_PREV_YEAR)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ROLL_WEEK_flt_PREV_YEAR,
       AVG (flt_ARR_PREV_YEAR)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ROLL_WEEK_arr_PREV_YEAR,
       AVG (dly_DEP_ARR_2019)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ROLL_WEEK_dly_2019,
       AVG (dly_ARR_2019)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ROLL_WEEK_dly_arr_2019,
       AVG (flt_DEP_ARR_2019)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ROLL_WEEK_flt_2019,
       AVG (flt_ARR_2019)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ROLL_WEEK_arr_2019,
       LAG (Y2d_avg_flt, flight_date-SAME_DATE_PREV_YEAR) OVER (PARTITION BY arp_code ORDER BY flight_date)  Y2d_avg_flt_prev_year,
       LAG (Y2d_avg_arr, flight_date-SAME_DATE_PREV_YEAR) OVER (PARTITION BY arp_code ORDER BY flight_date)  Y2d_avg_arr_prev_year,
       LAG (Y2d_avg_dly, flight_date-SAME_DATE_PREV_YEAR) OVER (PARTITION BY arp_code ORDER BY flight_date)  Y2d_avg_dly_prev_year,
       LAG (Y2d_avg_dly_arr, flight_date-SAME_DATE_PREV_YEAR) OVER (PARTITION BY arp_code ORDER BY flight_date)  Y2d_avg_dly_arr_prev_year,
       LAG (Y2d_avg_flt, flight_date-SAME_DATE_2019) OVER (PARTITION BY arp_code ORDER BY flight_date)  Y2d_avg_flt_2019,
       LAG (Y2d_avg_arr, flight_date-SAME_DATE_2019) OVER (PARTITION BY arp_code ORDER BY flight_date)  Y2d_avg_arr_2019,
       LAG (Y2d_avg_dly, flight_date-SAME_DATE_2019) OVER (PARTITION BY arp_code ORDER BY flight_date)  Y2d_avg_dly_2019,
       LAG (Y2d_avg_dly_arr, flight_date-SAME_DATE_2019) OVER (PARTITION BY arp_code ORDER BY flight_date)  Y2d_avg_dly_arr_2019

      FROM ARP_CALC
      where flight_date >= '01-jan-2019'
  )
  select arp_code,
         arp_name,
           month,
       flt_dep,
       flt_arr,
       flt_dep_arr,
       dly_dep,
       dly_arr,
       dly_dep_arr,
       Y2d_avg_dly,
       Y2d_avg_dly_arr,
       Y2d_avg_dly_prev_year,
       Y2d_avg_dly_arr_prev_year,
       Y2d_avg_dly_2019,
       Y2d_avg_dly_arr_2019,
       Y2d_avg_flt,
       Y2d_avg_arr,
       Y2d_avg_flt_prev_year,
       Y2d_avg_arr_prev_year,
       Y2d_avg_flt_2019,
       Y2d_avg_arr_2019,
       flight_date,
       FLIGHT_DATE_PREV_YEAR,
       FLIGHT_DATE_2019,
       dly_DEP_ARR_PREV_YEAR,
       dly_ARR_PREV_YEAR,
       dly_DEP_ARR_2019,
       dly_ARR_2019,
       flt_DEP_ARR_PREV_YEAR,
       flt_ARR_PREV_YEAR,
       flt_DEP_ARR_2019,
       flt_ARR_2019,
       ROLL_WEEK_dly,
       ROLL_WEEK_dly_arr,
       ROLL_WEEK_flt,
       ROLL_WEEK_arr,
       ROLL_WEEK_dly_PREV_YEAR,
       ROLL_WEEK_dly_arr_PREV_YEAR,
       ROLL_WEEK_dly_2019,
       ROLL_WEEK_dly_arr_2019,
       ROLL_WEEK_flt_PREV_YEAR,
       ROLL_WEEK_arr_PREV_YEAR,
       ROLL_WEEK_flt_2019,
       ROLL_WEEK_arr_2019,
        CASE WHEN ROLL_WEEK_dly_PREV_YEAR <> 0
            THEN ROLL_WEEK_dly/ROLL_WEEK_dly_PREV_YEAR -1
            ELSE NULL
       END ROLL_WEEK_dly_PREV_YEAR_PERC,
       CASE WHEN ROLL_WEEK_dly_2019 <> 0
            THEN ROLL_WEEK_dly/ROLL_WEEK_dly_2019 -1
            ELSE NULL
       END ROLL_WEEK_dly_2019_PERC,

       CASE WHEN ROLL_WEEK_dly_arr_PREV_YEAR <> 0
            THEN ROLL_WEEK_dly_arr/ROLL_WEEK_dly_Arr_PREV_YEAR -1
            ELSE NULL
       END ROLL_WEEK_dly_arr_PY_PERC,
       CASE WHEN ROLL_WEEK_dly_arr_2019 <> 0
            THEN ROLL_WEEK_dly_arr/ROLL_WEEK_dly_arr_2019 -1
            ELSE NULL
       END ROLL_WEEK_dly_arr_2019_PERC,

      CASE WHEN dly_DEP_ARR_PREV_YEAR <>0
           THEN dly_DEP_ARR/dly_DEP_ARR_PREV_YEAR -1
      ELSE NULL
      END  dly_DEP_ARR_DIFF_PERC,
      CASE WHEN dly_DEP_ARR_2019 <>0
           THEN dly_DEP_ARR/dly_DEP_ARR_2019 -1
      ELSE NULL
      END  dly_DEP_ARR_DIFF_2019_PERC,

      CASE WHEN dly_ARR_PREV_YEAR <>0
           THEN dly_ARR/dly_DEP_ARR_PREV_YEAR -1
      ELSE NULL
      END  dly_ARR_DIFF_PERC,
      CASE WHEN dly_ARR_2019 <>0
           THEN dly_ARR/dly_DEP_ARR_2019 -1
      ELSE NULL
      END  dly_ARR_DIFF_2019_PERC,

      CASE WHEN Y2d_avg_dly_prev_year <> 0
            THEN Y2d_avg_dly/Y2d_avg_dly_prev_year -1
            ELSE NULL
       END Y2D_dly_PREV_YEAR_PERC,
       CASE WHEN Y2d_avg_dly_2019 <> 0
            THEN Y2d_avg_dly/Y2d_avg_dly_2019 -1
            ELSE NULL
       END Y2D_dly_2019_PERC,

      CASE WHEN Y2d_avg_dly_arr_prev_year <> 0
            THEN Y2d_avg_dly_arr/Y2d_avg_dly_arr_prev_year -1
            ELSE NULL
       END Y2D_dly_arr_PREV_YEAR_PERC,
       CASE WHEN Y2d_avg_dly_arr_2019 <> 0
            THEN Y2d_avg_dly_arr/Y2d_avg_dly_arr_2019 -1
            ELSE NULL
       END Y2D_dly_arr_2019_PERC


      FROM ARP_CALC_PREV
      where flight_date = ", mydate, " -1
      order by Y2d_avg_flt desc
  ")
  }

