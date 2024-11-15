
# airport dimension table (lists the airports and their ICAO codes)
if (exists("apt_icao") == FALSE) {
  apt_icao <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(ap_base_file),
      start = ap_base_dir),
    sheet = "lists",
    range = cell_limits(c(1, 1), c(NA, NA))) %>%
    as_tibble()
}



## traffic ----
query_ap_traffic <- paste0("with LIST_AIRPORT as
(select code as arp_code,
        id as arp_id ,
       dashboard_name as arp_name,
       SUBSTR (CODE, 1, 2) AS ICAO2LETTER
 from pru_airport where code in ('",paste0(apt_icao$apt_icao_code, collapse = "','"),"'
          )
 ),


AIRPORT_DAY
as
(select a.arp_id,
       a.arp_code,
       a.arp_name,
       a.icao2letter,
       t.day_date,
       t.year,
       t.month,
       t.WEEK_NB_YEAR,
       t.DAY_TYPE,
       t.day_name,
       t.WEEK,
       TO_CHAR ( t.day_date, 'd')  AS day_of_week,
       TO_CHAR ( t.day_date, 'mmdd') AS MMDD
 from LIST_AIRPORT a, pru_time_references t
where
      t.day_date >= to_date('24-12-2018','dd-mm-yyyy')
 -- AND t.day_date < TRUNC (SYSDATE)
),



 ttf_dep as(
SELECT adep_day_adep, sum(coalesce(adep_day_all_trf,0)) AS DEP,f.adep_DAY_FLT_DATE
              FROM  v_aiu_agg_dep_day f
              where f.adep_DAY_FLT_DATE >=to_date('24-12-2018','dd-mm-yyyy')
              and adep_day_adep in (select arp_code from list_airport)
              group by  adep_day_adep ,f.adep_DAY_FLT_DATE
   ),


 ttf_arr as (
                  SELECT ades_day_ades_ctfm, sum(coalesce(ades_day_all_trf,0)) AS ARR,ades_DAY_FLT_DATE
              FROM  v_aiu_agg_arr_day
              where ades_DAY_FLT_DATE >=to_date('24-12-2018','dd-mm-yyyy')
           and ades_day_ades_ctfm in (select arp_code from list_airport)
           group by ades_day_ades_ctfm,ades_DAY_FLT_DATE
),



 ARP_SYN_DEP
 as (
select
c.arp_code, c.arp_id, c.arp_name, c.icao2letter, c.month, c.year, c.WEEK_NB_YEAR,c.DAY_TYPE,c.day_name,c.WEEK, c.day_of_week, c.day_date ,
 coalesce(d.dep,0) as dep
from airport_day c
     left join ttf_dep d  on ( c.day_date= d.adep_DAY_FLT_DATE  and c.arp_code=d.adep_day_adep  )

) ,

ARP_SYN_DEP_ARR
 as (
select c.arp_code, c.arp_id, c.arp_name, c.icao2letter,c.month, c.year, c.WEEK_NB_YEAR,c.DAY_TYPE,c.day_name,c.WEEK, c.day_of_week, c.day_date as flight_date,
 coalesce(c.dep,0) as day_dep, coalesce(a.arr, 0) as day_arr,  coalesce(c.dep,0) + coalesce(a.arr, 0) as day_dep_arr
from  ARP_SYN_DEP c
     left join ttf_arr a on (c.day_date=a.ades_DAY_FLT_DATE  and c.arp_code=a.ades_day_ades_ctfm )
) ,


DATA_arp_Y2D as
(select
        flight_date ,
        arp_code,
       SUM (day_dep) OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) Y2D_DEP_YEAR,
       SUM (day_dep) OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW)/
                Count (flight_date) OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) Y2D_AVG_DEP_YEAR,

       SUM (day_arr) OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) Y2D_ARR_YEAR,
       SUM (day_arr) OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW)/
                Count (flight_date) OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) Y2D_AVG_ARR_YEAR,

       SUM (day_dep_arr) OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) Y2D_DEP_ARR_YEAR,
       SUM (day_dep_arr) OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW)/
                Count (flight_date) OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) Y2D_AVG_DEP_ARR_YEAR

FROM ARP_SYN_DEP_ARR
),


AIRP_CALC as
(select a.arp_code,
        a.arp_name,
       a.icao2letter,
       a.year,
       a.month,
       a.WEEK_NB_YEAR,
       a.DAY_TYPE,
       a.day_name,
       a.WEEK,
       a.day_of_week,

       a.flight_date,
       a.day_dep,
       a.day_arr,
       a.day_dep_arr,

       LAG (a.day_dep, 7) OVER (PARTITION BY a.arp_code ORDER BY a.flight_date)  DAY_DEP_PREV_WEEK,
       LAG (a.day_arr, 7) OVER (PARTITION BY a.arp_code ORDER BY a.flight_date)  DAY_ARR_PREV_WEEK,
       LAG (a.day_dep_arr, 7) OVER (PARTITION BY a.arp_code ORDER BY a.flight_date)  DAY_DEP_ARR_PREV_WEEK,
       LAG (a.flight_date, 7) OVER (PARTITION BY a.arp_code ORDER BY a.flight_date)  FLIGHT_DATE_PREV_WEEK,

       LAG (a.day_dep, 364) OVER (PARTITION BY a.arp_code ORDER BY a.flight_date)  DAY_DEP_PREV_YEAR,
       LAG (a.day_arr, 364) OVER (PARTITION BY a.arp_code ORDER BY a.flight_date)  DAY_ARR_PREV_YEAR,
       LAG (a.day_dep_arr, 364) OVER (PARTITION BY a.arp_code ORDER BY a.flight_date)  DAY_DEP_ARR_PREV_YEAR,
       LAG (a.flight_date, 364) OVER (PARTITION BY a.arp_code ORDER BY a.flight_date)  FLIGHT_DATE_PREV_YEAR,

       LAG (a.day_dep, greatest(((((extract (year from a.flight_date)-2020) *364)+ floor((extract (year from a.flight_date)-2019)/4)*7)),0)) OVER (PARTITION BY a.arp_code ORDER BY a.flight_date) DAY_DEP_2020,
       LAG (a.day_arr, greatest(((((extract (year from a.flight_date)-2020) *364)+ floor((extract (year from a.flight_date)-2019)/4)*7)),0)) OVER (PARTITION BY a.arp_code ORDER BY a.flight_date) DAY_ARR_2020,
       LAG (a.day_dep_arr, greatest(((((extract (year from a.flight_date)-2020) *364)+ floor((extract (year from a.flight_date)-2019)/4)*7)),0)) OVER (PARTITION BY a.arp_code ORDER BY a.flight_date) DAY_DEP_ARR_2020,
       LAG (a.flight_date, greatest(((((extract (year from a.flight_date)-2020) *364)+ floor((extract (year from a.flight_date)-2019)/4)*7)),0)) OVER (PARTITION BY a.arp_code ORDER BY a.flight_date) FLIGHT_DATE_2020,

       LAG (a.day_dep, greatest(((((extract (year from a.flight_date)-2019) *364)+ floor((extract (year from a.flight_date)-2019)/4)*7)),0)) OVER (PARTITION BY a.arp_code ORDER BY a.flight_date) DAY_DEP_2019,
       LAG (a.day_arr, greatest(((((extract (year from a.flight_date)-2019) *364)+ floor((extract (year from a.flight_date)-2019)/4)*7)),0)) OVER (PARTITION BY a.arp_code ORDER BY a.flight_date) DAY_ARR_2019,
       LAG (a.day_dep_arr, greatest(((((extract (year from a.flight_date)-2019) *364)+ floor((extract (year from a.flight_date)-2019)/4)*7)),0)) OVER (PARTITION BY a.arp_code ORDER BY a.flight_date) DAY_DEP_ARR_2019,
       LAG (a.flight_date, greatest(((((extract (year from a.flight_date)-2019) *364)+ floor((extract (year from a.flight_date)-2019)/4)*7)),0)) OVER (PARTITION BY a.arp_code ORDER BY a.flight_date) FLIGHT_DATE_2019,

       AVG (a.day_dep)  OVER (PARTITION BY a.arp_code ORDER BY a.flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS RWK_AVG_DEP,
       AVG (a.day_arr)  OVER (PARTITION BY a.arp_code ORDER BY a.flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS RWK_AVG_ARR,
       AVG (a.day_dep_arr)  OVER (PARTITION BY a.arp_code ORDER BY a.flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS RWK_AVG_DEP_ARR,
--       AVG (DEP_ARR)  OVER (PARTITION BY arp_code, week_nb_year,week  ORDER BY week)AS avg_week

       --year to date
       b.Y2D_DEP_YEAR,
       b.Y2D_AVG_DEP_YEAR,
       c.Y2D_DEP_YEAR as Y2D_DEP_PREV_YEAR,
       c.Y2D_AVG_DEP_YEAR as Y2D_AVG_DEP_PREV_YEAR,
       d.Y2D_DEP_YEAR as Y2D_DEP_2019,
       d.Y2D_AVG_DEP_YEAR as Y2D_AVG_DEP_2019,

       b.Y2D_ARR_YEAR,
       b.Y2D_AVG_ARR_YEAR,
       c.Y2D_ARR_YEAR as Y2D_ARR_PREV_YEAR,
       c.Y2D_AVG_ARR_YEAR as Y2D_AVG_ARR_PREV_YEAR,
       d.Y2D_ARR_YEAR as Y2D_ARR_2019,
       d.Y2D_AVG_ARR_YEAR as Y2D_AVG_ARR_2019,

       b.Y2D_DEP_ARR_YEAR,
       b.Y2D_AVG_DEP_ARR_YEAR,
       c.Y2D_DEP_ARR_YEAR as Y2D_DEP_ARR_PREV_YEAR,
       c.Y2D_AVG_DEP_ARR_YEAR as Y2D_AVG_DEP_ARR_PREV_YEAR,
       d.Y2D_DEP_ARR_YEAR as Y2D_DEP_ARR_2019,
       d.Y2D_AVG_DEP_ARR_YEAR as Y2D_AVG_DEP_ARR_2019

      FROM ARP_SYN_DEP_ARR a
      left join DATA_arp_Y2D b on a.flight_DATE = b.flight_DATE and a.arp_code = b.arp_code
      left join DATA_arp_Y2D c on add_months(a.flight_DATE,-12) = c.flight_DATE and a.arp_code = c.arp_code
      left join DATA_arp_Y2D d on add_months(a.flight_DATE,-12*(extract (year from a.flight_DATE)-2019)) = d.flight_DATE and a.arp_code = d.arp_code


      )  ,

   AIRP_CACL_PREV  as
  (
      select
      arp_code,
       arp_name,
       icao2letter,
       year,
       month,
       WEEK_NB_YEAR,
       DAY_TYPE,
       day_name,
       WEEK,
       day_of_week,

-- day
       flight_date,
       FLIGHT_DATE_PREV_WEEK,
       FLIGHT_DATE_PREV_YEAR,
       FLIGHT_DATE_2019,

       day_dep,
       day_DEP_PREV_WEEK,
       day_DEP_PREV_YEAR,
       day_DEP_2019,

       day_arr,
       day_ARR_PREV_WEEK,
       day_ARR_PREV_YEAR,
       day_ARR_2019,

       day_dep_arr,
       day_DEP_ARR_PREV_WEEK,
       day_DEP_ARR_PREV_YEAR,
       day_DEP_ARR_2019,

-- rolling week
       RWK_AVG_DEP,
       RWK_AVG_ARR,
       RWK_AVG_DEP_ARR,

       AVG (day_DEP_PREV_WEEK)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS RWK_AVG_DEP_PREV_WEEK,
       AVG (day_ARR_PREV_WEEK)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS RWK_AVG_ARR_PREV_WEEK,
       AVG (day_DEP_ARR_PREV_WEEK)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS RWK_AVG_DEP_ARR_PREV_WEEK,

       AVG (day_DEP_PREV_YEAR)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS RWK_AVG_DEP_PREV_YEAR,
       AVG (day_ARR_PREV_YEAR)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS RWK_AVG_ARR_PREV_YEAR,
       AVG (day_DEP_ARR_PREV_YEAR)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS RWK_AVG_DEP_ARR_PREV_YEAR,

       AVG (day_DEP_2020)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS RWK_AVG_DEP_2020,
       AVG (day_ARR_2020)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS RWK_AVG_ARR_2020,
       AVG (day_DEP_ARR_2020)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS RWK_AVG_DEP_ARR_2020,

       AVG (day_DEP_2019)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS RWK_AVG_DEP_2019,
       AVG (day_ARR_2019)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS RWK_AVG_ARR_2019,
       AVG (day_DEP_ARR_2019)  OVER (PARTITION BY arp_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS RWK_AVG_DEP_ARR_2019,

-- y2d
       Y2D_DEP_YEAR,
       Y2D_AVG_DEP_YEAR,
       Y2D_DEP_PREV_YEAR,
       Y2D_AVG_DEP_PREV_YEAR,
       Y2D_DEP_2019,
       Y2D_AVG_DEP_2019,

       Y2D_ARR_YEAR,
       Y2D_AVG_ARR_YEAR,
       Y2D_ARR_PREV_YEAR,
       Y2D_AVG_ARR_PREV_YEAR,
       Y2D_ARR_2019,
       Y2D_AVG_ARR_2019,

       Y2D_DEP_ARR_YEAR,
       Y2D_AVG_DEP_ARR_YEAR,
       Y2D_DEP_ARR_PREV_YEAR,
       Y2D_AVG_DEP_ARR_PREV_YEAR,
       Y2D_DEP_ARR_2019,
       Y2D_AVG_DEP_ARR_2019

--       AVG (DEP_ARR_PREV_YEAR)  OVER (PARTITION BY arp_code,week_nb_year,week  ORDER BY week)AS avg_week_PREV_YEAR,
--       AVG (DEP_ARR_2019)  OVER (PARTITION BY arp_code,week_nb_year,week  ORDER BY week)AS avg_week_2019
      FROM AIRP_CALC
  )

  select
       arp_code,
       arp_name,
       icao2letter,
       year,
       month,
       WEEK_NB_YEAR,
       DAY_TYPE,
       day_name,
       WEEK,
       day_of_week,

-- day
       flight_date,
       FLIGHT_DATE_PREV_WEEK,
       FLIGHT_DATE_PREV_YEAR,
       FLIGHT_DATE_2019,

       day_dep,
       day_DEP_PREV_WEEK,
       day_DEP_PREV_YEAR,
       day_DEP_2019,

       day_arr,
       day_ARR_PREV_WEEK,
       day_ARR_PREV_YEAR,
       day_ARR_2019,

       day_dep_arr,
       day_DEP_ARR_PREV_WEEK,
       day_DEP_ARR_PREV_YEAR,
       day_DEP_ARR_2019,

       day_dep_arr - day_DEP_ARR_PREV_WEEK as DAY_DEP_ARR_DIF_PREV_WEEK,
       day_dep_arr - day_DEP_ARR_PREV_YEAR as DAY_DEP_ARR_DIF_PREV_YEAR,
       day_dep_arr - day_DEP_ARR_2019 as DAY_DEP_ARR_DIF_2019,

       CASE WHEN day_DEP_ARR_PREV_WEEK <> 0
            THEN day_dep_arr / day_DEP_ARR_PREV_WEEK -1
            ELSE NULL
       END DAY_DEP_ARR_DIF_PREV_WEEK_PERC,

       CASE WHEN day_DEP_ARR_PREV_YEAR <> 0
            THEN day_dep_arr / day_DEP_ARR_PREV_YEAR -1
            ELSE NULL
       END DAY_DEP_ARR_DIF_PREV_YEAR_PERC,

       CASE WHEN day_DEP_ARR_2019 <> 0
            THEN day_dep_arr / day_DEP_ARR_2019 -1
            ELSE NULL
       END DAY_DEP_ARR_DIF_2019_PERC,

-- rolling week
       RWK_AVG_DEP,
       RWK_AVG_DEP_PREV_WEEK,
       RWK_AVG_DEP_PREV_YEAR,
       RWK_AVG_DEP_2020,
       RWK_AVG_DEP_2019,

       RWK_AVG_ARR,
       RWK_AVG_ARR_PREV_WEEK,
       RWK_AVG_ARR_PREV_YEAR,
       RWK_AVG_ARR_2020,
       RWK_AVG_ARR_2019,

       RWK_AVG_DEP_ARR,
       RWK_AVG_DEP_ARR_PREV_WEEK,
       RWK_AVG_DEP_ARR_PREV_YEAR,
       RWK_AVG_DEP_ARR_2020,
       RWK_AVG_DEP_ARR_2019,

       CASE WHEN RWK_AVG_DEP_ARR_PREV_YEAR <> 0
            THEN RWK_AVG_DEP_ARR / RWK_AVG_DEP_ARR_PREV_YEAR -1
            ELSE NULL
       END RWK_DEP_ARR_DIF_PREV_YEAR_PERC,

       CASE WHEN RWK_AVG_DEP_ARR_2019 <> 0
            THEN RWK_AVG_DEP_ARR / RWK_AVG_DEP_ARR_2019 -1
            ELSE NULL
       END RWK_DEP_ARR_DIF_2019_PERC,

-- y2d
       Y2D_DEP_YEAR,
       Y2D_AVG_DEP_YEAR,
       Y2D_DEP_PREV_YEAR,
       Y2D_AVG_DEP_PREV_YEAR,
       Y2D_DEP_2019,
       Y2D_AVG_DEP_2019,

       Y2D_ARR_YEAR,
       Y2D_AVG_ARR_YEAR,
       Y2D_ARR_PREV_YEAR,
       Y2D_AVG_ARR_PREV_YEAR,
       Y2D_ARR_2019,
       Y2D_AVG_ARR_2019,

       Y2D_DEP_ARR_YEAR,
       Y2D_AVG_DEP_ARR_YEAR,
       Y2D_DEP_ARR_PREV_YEAR,
       Y2D_AVG_DEP_ARR_PREV_YEAR,
       Y2D_DEP_ARR_2019,
       Y2D_AVG_DEP_ARR_2019,

       CASE WHEN Y2D_AVG_DEP_ARR_PREV_YEAR <> 0
            THEN Y2D_AVG_DEP_ARR_YEAR / Y2D_AVG_DEP_ARR_PREV_YEAR -1
            ELSE NULL
       END Y2D_DEP_ARR_DIF_PREV_YEAR_PERC,

       CASE WHEN Y2D_AVG_DEP_ARR_2019 <> 0
            THEN Y2D_AVG_DEP_ARR_YEAR / Y2D_AVG_DEP_ARR_2019 -1
            ELSE NULL
       END Y2D_DEP_ARR_DIF_2019_PERC,
       trunc(sysdate) - 1 as LAST_DATA_DAY


     FROM AIRP_CACL_PREV
--      where flight_date >=to_date('30-12-2018','DD-MM-YYYY')
     where flight_DATE >=to_date('01-01-'|| extract(year from (trunc(sysdate)-1)),'dd-mm-yyyy') AND
     flight_DATE <=to_date('31-12-'|| extract(year from (trunc(sysdate)-1)),'dd-mm-yyyy')")



## delay ----
query_ap_delay <- paste0("with LIST_AIRPORT as
(select code as arp_code,
  id as arp_id ,
  dashboard_name as arp_name,
  SUBSTR (CODE, 1, 2) AS ICAO2LETTER
  from pru_airport where code in ('",paste0(apt_icao$apt_icao_code, collapse = "','"),"'
  )
),


AIRPORT_DAY
as
(select a.arp_id,
  a.arp_code,
  a.arp_name,
  a.icao2letter,
  t.day_date,
  t.year,
  t.month,
  t.WEEK_NB_YEAR,
  t.DAY_TYPE,
  t.day_name,
  t.WEEK,
  TO_CHAR ( t.day_date, 'd')  AS day_of_week,
  TO_CHAR ( t.day_date, 'mmdd') AS MMDD
  from LIST_AIRPORT a, pru_time_references t
  where
  t.day_date >= to_date('24-12-2018','dd-mm-yyyy')
--  AND t.day_date < TRUNC (SYSDATE)
),

arp_SYN_ARR
AS
(SELECT SUM (nvl(f.ades_day_all_trf,0)) AS day_arr,
  u.arp_code,
  u.day_date  AS FLIGHT_DATE,
  u.arp_id,
  u.arp_name, u.ICAO2LETTER,
  u.day_date,
  u.year,
  u.month,
  u.WEEK_NB_YEAR,
  u.DAY_TYPE,
  u.day_name,
  u.WEEK,
  u.day_of_week
  FROM AIRPORT_DAY u left join v_aiu_agg_arr_day f
  on trunc(f.ades_DAY_FLT_DATE) = u.day_date
  and f.ades_day_ades_ctfm = u.arp_code
  GROUP BY
  u.arp_code, u.day_date,u.arp_id, u.arp_name, u.ICAO2LETTER,u.day_date,
  u.year,
  u.month,
  u.WEEK_NB_YEAR,
  u.DAY_TYPE,
  u.day_name,
  u.WEEK,
  u.day_of_week
),

arp_DELAY
AS
(  SELECT
  a.ref_loc_id as arp_code,
  a.agg_flt_a_first_entry_date AS entry_date,

  SUM (NVL(a.agg_flt_total_delay, 0))
  tdm_arp,
  SUM (NVL(agg_flt_delayed_traffic, 0))
  tdf_arp,
  SUM (NVL(a.agg_flt_regulated_traffic, 0))
  trf_arp,

  SUM (NVL((CASE WHEN a.agg_flt_delay_interval IN  (']15,30]', ']30,60]', '> 60') THEN a.agg_flt_total_delay END),0))
  tdm_15_arp,
  SUM (NVL((CASE WHEN a.agg_flt_delay_interval IN  (']15,30]', ']30,60]', '> 60') THEN a.agg_flt_delayed_traffic END),0))
  tdf_15_arp,
  SUM (NVL((CASE WHEN a.agg_flt_delay_interval IN  (']15,30]', ']30,60]', '> 60') THEN NVL (a.agg_flt_regulated_traffic, 0) END),0))
  trf_15_arp,

  SUM (NVL((CASE WHEN a.agg_flt_delay_interval IN  (']0,5]', ']5,10]', ']10,15]') THEN  a.agg_flt_total_delay END),0))
  TDM_01_15_arp,
  SUM (NVL((CASE WHEN a.agg_flt_delay_interval IN (']15,30]', ']30,60]') THEN  a.agg_flt_total_delay END),0))
  TDM_15_60_arp,
  SUM (NVL((CASE WHEN a.agg_flt_delay_interval = '> 60' THEN  a.agg_flt_total_delay END),0))
  TDM_60_arp,

  SUM (NVL((CASE WHEN a.agg_flt_delay_interval IN (']0,5]', ']5,10]', ']10,15]') THEN  a.agg_flt_delayed_traffic END),0))
  TDF_01_15_arp,
  SUM (NVL((CASE WHEN a.agg_flt_delay_interval IN (']15,30]', ']30,60]') THEN  a.agg_flt_delayed_traffic END),0))
  TDF_15_60_arp,
  SUM (NVL((CASE WHEN a.agg_flt_delay_interval = '> 60' THEN  a.agg_flt_delayed_traffic END),0))
  TDF_60_arp,

  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('C', 'S', 'G')   THEN a.agg_flt_total_delay END),0))
  tdm_arp_csg,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('V','I', 'R','T')   THEN a.agg_flt_total_delay END),0))
  tdm_arp_virt,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('W', 'D')   THEN a.agg_flt_total_delay END),0))
  tdm_arp_wd,
  SUM (NVL((CASE WHEN agg_flt_regu_reas NOT IN ('C','S','G','V','I','R', 'T','W','D')   THEN a.agg_flt_total_delay END),0))
  tdm_arp_no_csgvirtwd,

  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('A')   THEN a.agg_flt_total_delay END),0))
  tdm_arp_a,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('C')   THEN a.agg_flt_total_delay END),0))
  tdm_arp_c,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('D')   THEN a.agg_flt_total_delay END),0))
  tdm_arp_d,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('E')   THEN a.agg_flt_total_delay END),0))
  tdm_arp_e,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('G')   THEN a.agg_flt_total_delay END),0))
  tdm_arp_g,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('I')   THEN a.agg_flt_total_delay END),0))
  tdm_arp_i,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('M')   THEN a.agg_flt_total_delay END),0))
  tdm_arp_m,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('N')   THEN a.agg_flt_total_delay END),0))
  tdm_arp_n,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('O')   THEN a.agg_flt_total_delay END),0))
  tdm_arp_o,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('P')   THEN a.agg_flt_total_delay END),0))
  tdm_arp_p,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('R')   THEN a.agg_flt_total_delay END),0))
  tdm_arp_r,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('S')   THEN a.agg_flt_total_delay END),0))
  tdm_arp_s,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('T')   THEN a.agg_flt_total_delay END),0))
  tdm_arp_t,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('V')   THEN a.agg_flt_total_delay END),0))
  tdm_arp_v,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('W')   THEN a.agg_flt_total_delay END),0))
  tdm_arp_w,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('NA')   THEN a.agg_flt_total_delay END),0))
  tdm_arp_na,

  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('A')    AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_a,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('C')    AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_c,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('D')    AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_d,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('E')    AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_e,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('G')    AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_g,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('I')    AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_i,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('M')    AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_m,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('N')    AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_n,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('O')    AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_o,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('P')    AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_p,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('R')    AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_r,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('S')    AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_s,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('T')    AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_t,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('V')    AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_v,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('W')    AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_w,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('NA')    AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_na,



  SUM (NVL((CASE WHEN a.MP_REGU_LOC_CAT = 'Arrival' THEN  a.agg_flt_total_delay END),0))
  tdm_arp_arr,
  SUM (NVL((CASE WHEN a.MP_REGU_LOC_CAT = 'Arrival' THEN  a.agg_flt_delayed_traffic END),0))
  tdf_arp_arr,
  SUM (NVL((CASE WHEN a.MP_REGU_LOC_CAT = 'Arrival' THEN  NVL (a.agg_flt_regulated_traffic, 0) END),0))
  trf_arp_arr,

  SUM (NVL((CASE WHEN a.MP_REGU_LOC_CAT = 'Arrival'  AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_arr,
  SUM (NVL((CASE WHEN a.MP_REGU_LOC_CAT = 'Arrival'  AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60')  THEN  a.agg_flt_delayed_traffic END),0))
  tdf_15_arp_arr,
  SUM (NVL((CASE WHEN a.MP_REGU_LOC_CAT = 'Arrival'  AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60')THEN  NVL (a.agg_flt_regulated_traffic, 0) END),0))
  trf_15_arp_arr,

  SUM (NVL((CASE WHEN a.agg_flt_delay_interval IN (']0,5]', ']5,10]', ']10,15]') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  TDM_01_15_arp_arr,
  SUM (NVL((CASE WHEN a.agg_flt_delay_interval IN (']15,30]', ']30,60]') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  TDM_15_60_arp_arr,
  SUM (NVL((CASE WHEN a.agg_flt_delay_interval = '> 60' AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  TDM_60_arp_arr,

  SUM (NVL((CASE WHEN a.agg_flt_delay_interval IN (']0,5]', ']5,10]', ']10,15]') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN  a.agg_flt_delayed_traffic END),0))
  TDF_01_15_arp_arr,
  SUM (NVL((CASE WHEN a.agg_flt_delay_interval IN (']15,30]', ']30,60]') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN  a.agg_flt_delayed_traffic END),0))
  TDF_15_60_arp_arr,
  SUM (NVL((CASE WHEN a.agg_flt_delay_interval =   '> 60' AND  a.MP_REGU_LOC_CAT = 'Arrival' THEN  a.agg_flt_delayed_traffic END),0))
  TDF_60_arp_arr,

  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('C', 'S', 'G') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_csg,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('V','I', 'R','T') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_virt,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('W', 'D') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_wd,
  SUM (NVL((CASE WHEN agg_flt_regu_reas NOT IN ('C','S','G','V','I','R', 'T','W','D') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_no_csgvirtwd,

  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('A') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_a,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('C') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_c,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('D') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_d,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('E') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_e,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('G') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_g,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('I') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_i,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('M') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_m,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('N') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_n,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('O') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_o,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('P') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_p,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('R') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_r,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('S') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_s,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('T') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_t,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('V') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_v,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('W') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_w,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('NA') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_na,

  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('A') AND a.MP_REGU_LOC_CAT = 'Arrival'  AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_arr_a,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('C') AND a.MP_REGU_LOC_CAT = 'Arrival'  AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_arr_c,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('D') AND a.MP_REGU_LOC_CAT = 'Arrival'  AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_arr_d,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('E') AND a.MP_REGU_LOC_CAT = 'Arrival'  AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_arr_e,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('G') AND a.MP_REGU_LOC_CAT = 'Arrival'  AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_arr_g,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('I') AND a.MP_REGU_LOC_CAT = 'Arrival'  AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_arr_i,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('M') AND a.MP_REGU_LOC_CAT = 'Arrival'  AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_arr_m,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('N') AND a.MP_REGU_LOC_CAT = 'Arrival'  AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_arr_n,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('O') AND a.MP_REGU_LOC_CAT = 'Arrival'  AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_arr_o,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('P') AND a.MP_REGU_LOC_CAT = 'Arrival'  AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_arr_p,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('R') AND a.MP_REGU_LOC_CAT = 'Arrival'  AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_arr_r,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('S') AND a.MP_REGU_LOC_CAT = 'Arrival'  AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_arr_s,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('T') AND a.MP_REGU_LOC_CAT = 'Arrival'  AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_arr_t,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('V') AND a.MP_REGU_LOC_CAT = 'Arrival'  AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_arr_v,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('W') AND a.MP_REGU_LOC_CAT = 'Arrival'  AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_arr_w,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('NA') AND a.MP_REGU_LOC_CAT = 'Arrival'  AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_arr_na,


  SUM (NVL((CASE WHEN a.MP_REGU_LOC_CAT = 'Departure'  THEN a.agg_flt_total_delay END),0))
  tdm_arp_dep,
  SUM (NVL((CASE WHEN a.MP_REGU_LOC_CAT = 'Departure'  THEN a.agg_flt_delayed_traffic END),0))
  tdf_arp_dep,
  SUM (NVL((CASE WHEN a.MP_REGU_LOC_CAT = 'Departure'  THEN NVL (a.agg_flt_regulated_traffic, 0) END),0))
  trf_arp_dep,

  SUM (NVL((CASE WHEN a.MP_REGU_LOC_CAT = 'Departure' AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN a.agg_flt_total_delay END),0))
  tdm_15_arp_dep,
  SUM (NVL((CASE WHEN a.MP_REGU_LOC_CAT = 'Departure' AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN a.agg_flt_delayed_traffic END),0))
  tdf_15_arp_dep,
  SUM (NVL((CASE WHEN a.MP_REGU_LOC_CAT = 'Departure' AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  NVL (a.agg_flt_regulated_traffic, 0) END),0))
  trf_15_arp_dep,

  SUM (NVL((CASE WHEN a.agg_flt_delay_interval IN (']0,5]', ']5,10]', ']10,15]') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  TDM_01_15_arp_dep,
  SUM (NVL((CASE WHEN a.agg_flt_delay_interval IN (']15,30]', ']30,60]') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  TDM_15_60_arp_dep,
  SUM (NVL((CASE WHEN a.agg_flt_delay_interval = '> 60' AND a.MP_REGU_LOC_CAT = 'Departure' THEN  a.agg_flt_total_delay END),0))
  TDM_60_arp_dep,

  SUM (NVL((CASE WHEN a.agg_flt_delay_interval IN (']0,5]', ']5,10]', ']10,15]') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_delayed_traffic END),0))
  TDF_01_15_arp_dep,
  SUM (NVL((CASE WHEN a.agg_flt_delay_interval IN (']15,30]', ']30,60]') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_delayed_traffic END),0))
  TDF_15_60_arp_dep,
  SUM (NVL((CASE WHEN a.agg_flt_delay_interval =   '> 60' AND  a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_delayed_traffic END),0))
  TDF_60_arp_dep,

  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('C', 'S', 'G') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_csg,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('V','I', 'R','T')  AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_virt,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('W', 'D') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_wd,
  SUM (NVL((CASE WHEN agg_flt_regu_reas NOT IN ('C','S','G','V','I','R', 'T','W','D') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_no_csgvirtwd,

  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('C', 'S') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_cs,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('I', 'T') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_it,
  SUM (NVL((CASE WHEN agg_flt_regu_reas NOT IN ('C','S','I','T','W','D') AND a.MP_REGU_LOC_CAT = 'Arrival' THEN a.agg_flt_total_delay END),0))
  tdm_arp_arr_no_csitwd,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('I', 'T')  AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_it,
  SUM (NVL((CASE WHEN agg_flt_regu_reas NOT IN ('C','S','G','I', 'T','W','D') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_no_csgitwd,

  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('A') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_a,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('C') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_c,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('D') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_d,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('E') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_e,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('G') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_g,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('I') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_i,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('M') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_m,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('N') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_n,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('O') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_o,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('P') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_p,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('R') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_r,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('S') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_s,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('T') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_t,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('V') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_v,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('W') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_w,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('NA') AND a.MP_REGU_LOC_CAT = 'Departure' THEN   a.agg_flt_total_delay END),0))
  tdm_arp_dep_na,

  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('A') AND a.MP_REGU_LOC_CAT = 'Departure' AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_dep_a,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('C') AND a.MP_REGU_LOC_CAT = 'Departure' AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_dep_c,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('D') AND a.MP_REGU_LOC_CAT = 'Departure' AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_dep_d,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('E') AND a.MP_REGU_LOC_CAT = 'Departure' AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_dep_e,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('G') AND a.MP_REGU_LOC_CAT = 'Departure' AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_dep_g,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('I') AND a.MP_REGU_LOC_CAT = 'Departure' AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_dep_i,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('M') AND a.MP_REGU_LOC_CAT = 'Departure' AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_dep_m,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('N') AND a.MP_REGU_LOC_CAT = 'Departure' AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_dep_n,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('O') AND a.MP_REGU_LOC_CAT = 'Departure' AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_dep_o,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('P') AND a.MP_REGU_LOC_CAT = 'Departure' AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_dep_p,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('R') AND a.MP_REGU_LOC_CAT = 'Departure' AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_dep_r,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('S') AND a.MP_REGU_LOC_CAT = 'Departure' AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_dep_s,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('T') AND a.MP_REGU_LOC_CAT = 'Departure' AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_dep_t,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('V') AND a.MP_REGU_LOC_CAT = 'Departure' AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_dep_v,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('W') AND a.MP_REGU_LOC_CAT = 'Departure' AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_dep_w,
  SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('NA') AND a.MP_REGU_LOC_CAT = 'Departure' AND a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60') THEN  a.agg_flt_total_delay END),0))
  tdm_15_arp_dep_na
  FROM v_aiu_agg_flt_flow a
  WHERE
  a.agg_flt_a_first_entry_date >= to_date('24-12-2018','dd-mm-yyyy')
  and a.AGG_FLT_MP_REGU_LOC_TY = 'Airport'
  GROUP BY
  a.agg_flt_a_first_entry_date,
  a.ref_loc_id
),

ALL_DAY_DATA as (
  select
  a.arp_code,
  a.arp_name,
  a.flight_date,
  a.icao2letter,
  a.day_arr,
  coalesce(TDM_ARP,0) as TDM_ARP,
  coalesce(TDF_ARP,0) as TDF_ARP,
  coalesce(TRF_ARP,0) as TRF_ARP,
  coalesce(TDM_15_ARP,0) as TDM_15_ARP,
  coalesce(TDF_15_ARP,0) as TDF_15_ARP,
  coalesce(TRF_15_ARP,0) as TRF_15_ARP,
  coalesce(TDM_01_15_ARP,0) as TDM_01_15_ARP,
  coalesce(TDM_15_60_ARP,0) as TDM_15_60_ARP,
  coalesce(TDM_60_ARP,0) as TDM_60_ARP,
  coalesce(TDF_01_15_ARP,0) as TDF_01_15_ARP,
  coalesce(TDF_15_60_ARP,0) as TDF_15_60_ARP,
  coalesce(TDF_60_ARP,0) as TDF_60_ARP,
  coalesce(TDM_ARP_CSG,0) as TDM_ARP_CSG,
  coalesce(TDM_ARP_VIRT,0) as TDM_ARP_VIRT,
  coalesce(TDM_ARP_WD,0) as TDM_ARP_WD,
  coalesce(TDM_ARP_NO_CSGVIRTWD,0) as TDM_ARP_NO_CSGVIRTWD,
  coalesce(TDM_ARP_A,0) as TDM_ARP_A,
  coalesce(TDM_ARP_C,0) as TDM_ARP_C,
  coalesce(TDM_ARP_D,0) as TDM_ARP_D,
  coalesce(TDM_ARP_E,0) as TDM_ARP_E,
  coalesce(TDM_ARP_G,0) as TDM_ARP_G,
  coalesce(TDM_ARP_I,0) as TDM_ARP_I,
  coalesce(TDM_ARP_M,0) as TDM_ARP_M,
  coalesce(TDM_ARP_N,0) as TDM_ARP_N,
  coalesce(TDM_ARP_O,0) as TDM_ARP_O,
  coalesce(TDM_ARP_P,0) as TDM_ARP_P,
  coalesce(TDM_ARP_R,0) as TDM_ARP_R,
  coalesce(TDM_ARP_S,0) as TDM_ARP_S,
  coalesce(TDM_ARP_T,0) as TDM_ARP_T,
  coalesce(TDM_ARP_V,0) as TDM_ARP_V,
  coalesce(TDM_ARP_W,0) as TDM_ARP_W,
  coalesce(TDM_ARP_NA,0) as TDM_ARP_NA,
  coalesce(TDM_15_ARP_A,0) as TDM_15_ARP_A,
  coalesce(TDM_15_ARP_C,0) as TDM_15_ARP_C,
  coalesce(TDM_15_ARP_D,0) as TDM_15_ARP_D,
  coalesce(TDM_15_ARP_E,0) as TDM_15_ARP_E,
  coalesce(TDM_15_ARP_G,0) as TDM_15_ARP_G,
  coalesce(TDM_15_ARP_I,0) as TDM_15_ARP_I,
  coalesce(TDM_15_ARP_M,0) as TDM_15_ARP_M,
  coalesce(TDM_15_ARP_N,0) as TDM_15_ARP_N,
  coalesce(TDM_15_ARP_O,0) as TDM_15_ARP_O,
  coalesce(TDM_15_ARP_P,0) as TDM_15_ARP_P,
  coalesce(TDM_15_ARP_R,0) as TDM_15_ARP_R,
  coalesce(TDM_15_ARP_S,0) as TDM_15_ARP_S,
  coalesce(TDM_15_ARP_T,0) as TDM_15_ARP_T,
  coalesce(TDM_15_ARP_V,0) as TDM_15_ARP_V,
  coalesce(TDM_15_ARP_W,0) as TDM_15_ARP_W,
  coalesce(TDM_15_ARP_NA,0) as TDM_15_ARP_NA,
  coalesce(TDM_ARP_ARR,0) as TDM_ARP_ARR,
  coalesce(TDF_ARP_ARR,0) as TDF_ARP_ARR,
  coalesce(TRF_ARP_ARR,0) as TRF_ARP_ARR,
  coalesce(TDM_15_ARP_ARR,0) as TDM_15_ARP_ARR,
  coalesce(TDF_15_ARP_ARR,0) as TDF_15_ARP_ARR,
  coalesce(TRF_15_ARP_ARR,0) as TRF_15_ARP_ARR,
  coalesce(TDM_01_15_ARP_ARR,0) as TDM_01_15_ARP_ARR,
  coalesce(TDM_15_60_ARP_ARR,0) as TDM_15_60_ARP_ARR,
  coalesce(TDM_60_ARP_ARR,0) as TDM_60_ARP_ARR,
  coalesce(TDF_01_15_ARP_ARR,0) as TDF_01_15_ARP_ARR,
  coalesce(TDF_15_60_ARP_ARR,0) as TDF_15_60_ARP_ARR,
  coalesce(TDF_60_ARP_ARR,0) as TDF_60_ARP_ARR,
  coalesce(TDM_ARP_ARR_CSG,0) as TDM_ARP_ARR_CSG,
  coalesce(TDM_ARP_ARR_VIRT,0) as TDM_ARP_ARR_VIRT,
  coalesce(TDM_ARP_ARR_WD,0) as TDM_ARP_ARR_WD,
  coalesce(TDM_ARP_ARR_NO_CSGVIRTWD,0) as TDM_ARP_ARR_NO_CSGVIRTWD,
  coalesce(TDM_ARP_ARR_A,0) as TDM_ARP_ARR_A,
  coalesce(TDM_ARP_ARR_C,0) as TDM_ARP_ARR_C,
  coalesce(TDM_ARP_ARR_D,0) as TDM_ARP_ARR_D,
  coalesce(TDM_ARP_ARR_E,0) as TDM_ARP_ARR_E,
  coalesce(TDM_ARP_ARR_G,0) as TDM_ARP_ARR_G,
  coalesce(TDM_ARP_ARR_I,0) as TDM_ARP_ARR_I,
  coalesce(TDM_ARP_ARR_M,0) as TDM_ARP_ARR_M,
  coalesce(TDM_ARP_ARR_N,0) as TDM_ARP_ARR_N,
  coalesce(TDM_ARP_ARR_O,0) as TDM_ARP_ARR_O,
  coalesce(TDM_ARP_ARR_P,0) as TDM_ARP_ARR_P,
  coalesce(TDM_ARP_ARR_R,0) as TDM_ARP_ARR_R,
  coalesce(TDM_ARP_ARR_S,0) as TDM_ARP_ARR_S,
  coalesce(TDM_ARP_ARR_T,0) as TDM_ARP_ARR_T,
  coalesce(TDM_ARP_ARR_V,0) as TDM_ARP_ARR_V,
  coalesce(TDM_ARP_ARR_W,0) as TDM_ARP_ARR_W,
  coalesce(TDM_ARP_ARR_NA,0) as TDM_ARP_ARR_NA,
  coalesce(TDM_15_ARP_ARR_A,0) as TDM_15_ARP_ARR_A,
  coalesce(TDM_15_ARP_ARR_C,0) as TDM_15_ARP_ARR_C,
  coalesce(TDM_15_ARP_ARR_D,0) as TDM_15_ARP_ARR_D,
  coalesce(TDM_15_ARP_ARR_E,0) as TDM_15_ARP_ARR_E,
  coalesce(TDM_15_ARP_ARR_G,0) as TDM_15_ARP_ARR_G,
  coalesce(TDM_15_ARP_ARR_I,0) as TDM_15_ARP_ARR_I,
  coalesce(TDM_15_ARP_ARR_M,0) as TDM_15_ARP_ARR_M,
  coalesce(TDM_15_ARP_ARR_N,0) as TDM_15_ARP_ARR_N,
  coalesce(TDM_15_ARP_ARR_O,0) as TDM_15_ARP_ARR_O,
  coalesce(TDM_15_ARP_ARR_P,0) as TDM_15_ARP_ARR_P,
  coalesce(TDM_15_ARP_ARR_R,0) as TDM_15_ARP_ARR_R,
  coalesce(TDM_15_ARP_ARR_S,0) as TDM_15_ARP_ARR_S,
  coalesce(TDM_15_ARP_ARR_T,0) as TDM_15_ARP_ARR_T,
  coalesce(TDM_15_ARP_ARR_V,0) as TDM_15_ARP_ARR_V,
  coalesce(TDM_15_ARP_ARR_W,0) as TDM_15_ARP_ARR_W,
  coalesce(TDM_15_ARP_ARR_NA,0) as TDM_15_ARP_ARR_NA,
  coalesce(TDM_ARP_DEP,0) as TDM_ARP_DEP,
  coalesce(TDF_ARP_DEP,0) as TDF_ARP_DEP,
  coalesce(TRF_ARP_DEP,0) as TRF_ARP_DEP,
  coalesce(TDM_15_ARP_DEP,0) as TDM_15_ARP_DEP,
  coalesce(TDF_15_ARP_DEP,0) as TDF_15_ARP_DEP,
  coalesce(TRF_15_ARP_DEP,0) as TRF_15_ARP_DEP,
  coalesce(TDM_01_15_ARP_DEP,0) as TDM_01_15_ARP_DEP,
  coalesce(TDM_15_60_ARP_DEP,0) as TDM_15_60_ARP_DEP,
  coalesce(TDM_60_ARP_DEP,0) as TDM_60_ARP_DEP,
  coalesce(TDF_01_15_ARP_DEP,0) as TDF_01_15_ARP_DEP,
  coalesce(TDF_15_60_ARP_DEP,0) as TDF_15_60_ARP_DEP,
  coalesce(TDF_60_ARP_DEP,0) as TDF_60_ARP_DEP,
  coalesce(TDM_ARP_DEP_CSG,0) as TDM_ARP_DEP_CSG,
  coalesce(TDM_ARP_DEP_VIRT,0) as TDM_ARP_DEP_VIRT,
  coalesce(TDM_ARP_DEP_WD,0) as TDM_ARP_DEP_WD,
  coalesce(TDM_ARP_DEP_NO_CSGVIRTWD,0) as TDM_ARP_DEP_NO_CSGVIRTWD,
  coalesce(TDM_ARP_ARR_CS,0) as TDM_ARP_ARR_CS,
  coalesce(TDM_ARP_ARR_IT,0) as TDM_ARP_ARR_IT,
  coalesce(TDM_ARP_ARR_NO_CSITWD,0) as TDM_ARP_ARR_NO_CSITWD,
  coalesce(TDM_ARP_DEP_IT,0) as TDM_ARP_DEP_IT,
  coalesce(TDM_ARP_DEP_NO_CSGITWD,0) as TDM_ARP_DEP_NO_CSGITWD,
  coalesce(TDM_ARP_DEP_A,0) as TDM_ARP_DEP_A,
  coalesce(TDM_ARP_DEP_C,0) as TDM_ARP_DEP_C,
  coalesce(TDM_ARP_DEP_D,0) as TDM_ARP_DEP_D,
  coalesce(TDM_ARP_DEP_E,0) as TDM_ARP_DEP_E,
  coalesce(TDM_ARP_DEP_G,0) as TDM_ARP_DEP_G,
  coalesce(TDM_ARP_DEP_I,0) as TDM_ARP_DEP_I,
  coalesce(TDM_ARP_DEP_M,0) as TDM_ARP_DEP_M,
  coalesce(TDM_ARP_DEP_N,0) as TDM_ARP_DEP_N,
  coalesce(TDM_ARP_DEP_O,0) as TDM_ARP_DEP_O,
  coalesce(TDM_ARP_DEP_P,0) as TDM_ARP_DEP_P,
  coalesce(TDM_ARP_DEP_R,0) as TDM_ARP_DEP_R,
  coalesce(TDM_ARP_DEP_S,0) as TDM_ARP_DEP_S,
  coalesce(TDM_ARP_DEP_T,0) as TDM_ARP_DEP_T,
  coalesce(TDM_ARP_DEP_V,0) as TDM_ARP_DEP_V,
  coalesce(TDM_ARP_DEP_W,0) as TDM_ARP_DEP_W,
  coalesce(TDM_ARP_DEP_NA,0) as TDM_ARP_DEP_NA,
  coalesce(TDM_15_ARP_DEP_A,0) as TDM_15_ARP_DEP_A,
  coalesce(TDM_15_ARP_DEP_C,0) as TDM_15_ARP_DEP_C,
  coalesce(TDM_15_ARP_DEP_D,0) as TDM_15_ARP_DEP_D,
  coalesce(TDM_15_ARP_DEP_E,0) as TDM_15_ARP_DEP_E,
  coalesce(TDM_15_ARP_DEP_G,0) as TDM_15_ARP_DEP_G,
  coalesce(TDM_15_ARP_DEP_I,0) as TDM_15_ARP_DEP_I,
  coalesce(TDM_15_ARP_DEP_M,0) as TDM_15_ARP_DEP_M,
  coalesce(TDM_15_ARP_DEP_N,0) as TDM_15_ARP_DEP_N,
  coalesce(TDM_15_ARP_DEP_O,0) as TDM_15_ARP_DEP_O,
  coalesce(TDM_15_ARP_DEP_P,0) as TDM_15_ARP_DEP_P,
  coalesce(TDM_15_ARP_DEP_R,0) as TDM_15_ARP_DEP_R,
  coalesce(TDM_15_ARP_DEP_S,0) as TDM_15_ARP_DEP_S,
  coalesce(TDM_15_ARP_DEP_T,0) as TDM_15_ARP_DEP_T,
  coalesce(TDM_15_ARP_DEP_V,0) as TDM_15_ARP_DEP_V,
  coalesce(TDM_15_ARP_DEP_W,0) as TDM_15_ARP_DEP_W,
  coalesce(TDM_15_ARP_DEP_NA,0) as TDM_15_ARP_DEP_NA

  FROM arp_SYN_ARR A
  LEFT JOIN ARP_DELAY b on a.arp_code  = B.arp_code  and a.flight_date = b.entry_date

)

SELECT
arp_code, arp_name, icao2letter,
flight_date,
sum(day_arr) as day_arr,
--           avg(day_arr) as avg_day_arr,
sum(TDM_ARP) as TDM_ARP,
sum(TDF_ARP) as TDF_ARP,
sum(TRF_ARP) as TRF_ARP,
sum(TDM_15_ARP) as TDM_15_ARP,
sum(TDF_15_ARP) as TDF_15_ARP,
sum(TRF_15_ARP) as TRF_15_ARP,
sum(TDM_01_15_ARP) as TDM_01_15_ARP,
sum(TDM_15_60_ARP) as TDM_15_60_ARP,
sum(TDM_60_ARP) as TDM_60_ARP,
sum(TDF_01_15_ARP) as TDF_01_15_ARP,
sum(TDF_15_60_ARP) as TDF_15_60_ARP,
sum(TDF_60_ARP) as TDF_60_ARP,
sum(TDM_ARP_CSG) as TDM_ARP_CSG,
sum(TDM_ARP_VIRT) as TDM_ARP_VIRT,
sum(TDM_ARP_WD) as TDM_ARP_WD,
sum(TDM_ARP_NO_CSGVIRTWD) as TDM_ARP_NO_CSGVIRTWD,
sum(TDM_ARP_A) as TDM_ARP_A,
sum(TDM_ARP_C) as TDM_ARP_C,
sum(TDM_ARP_D) as TDM_ARP_D,
sum(TDM_ARP_E) as TDM_ARP_E,
sum(TDM_ARP_G) as TDM_ARP_G,
sum(TDM_ARP_I) as TDM_ARP_I,
sum(TDM_ARP_M) as TDM_ARP_M,
sum(TDM_ARP_N) as TDM_ARP_N,
sum(TDM_ARP_O) as TDM_ARP_O,
sum(TDM_ARP_P) as TDM_ARP_P,
sum(TDM_ARP_R) as TDM_ARP_R,
sum(TDM_ARP_S) as TDM_ARP_S,
sum(TDM_ARP_T) as TDM_ARP_T,
sum(TDM_ARP_V) as TDM_ARP_V,
sum(TDM_ARP_W) as TDM_ARP_W,
sum(TDM_ARP_NA) as TDM_ARP_NA,
sum(TDM_15_ARP_A) as TDM_15_ARP_A,
sum(TDM_15_ARP_C) as TDM_15_ARP_C,
sum(TDM_15_ARP_D) as TDM_15_ARP_D,
sum(TDM_15_ARP_E) as TDM_15_ARP_E,
sum(TDM_15_ARP_G) as TDM_15_ARP_G,
sum(TDM_15_ARP_I) as TDM_15_ARP_I,
sum(TDM_15_ARP_M) as TDM_15_ARP_M,
sum(TDM_15_ARP_N) as TDM_15_ARP_N,
sum(TDM_15_ARP_O) as TDM_15_ARP_O,
sum(TDM_15_ARP_P) as TDM_15_ARP_P,
sum(TDM_15_ARP_R) as TDM_15_ARP_R,
sum(TDM_15_ARP_S) as TDM_15_ARP_S,
sum(TDM_15_ARP_T) as TDM_15_ARP_T,
sum(TDM_15_ARP_V) as TDM_15_ARP_V,
sum(TDM_15_ARP_W) as TDM_15_ARP_W,
sum(TDM_15_ARP_NA) as TDM_15_ARP_NA,
sum(TDM_ARP_ARR) as TDM_ARP_ARR,
sum(TDF_ARP_ARR) as TDF_ARP_ARR,
sum(TRF_ARP_ARR) as TRF_ARP_ARR,
sum(TDM_15_ARP_ARR) as TDM_15_ARP_ARR,
sum(TDF_15_ARP_ARR) as TDF_15_ARP_ARR,
sum(TRF_15_ARP_ARR) as TRF_15_ARP_ARR,
sum(TDM_01_15_ARP_ARR) as TDM_01_15_ARP_ARR,
sum(TDM_15_60_ARP_ARR) as TDM_15_60_ARP_ARR,
sum(TDM_60_ARP_ARR) as TDM_60_ARP_ARR,
sum(TDF_01_15_ARP_ARR) as TDF_01_15_ARP_ARR,
sum(TDF_15_60_ARP_ARR) as TDF_15_60_ARP_ARR,
sum(TDF_60_ARP_ARR) as TDF_60_ARP_ARR,
sum(TDM_ARP_ARR_CSG) as TDM_ARP_ARR_CSG,
sum(TDM_ARP_ARR_VIRT) as TDM_ARP_ARR_VIRT,
sum(TDM_ARP_ARR_WD) as TDM_ARP_ARR_WD,
sum(TDM_ARP_ARR_NO_CSGVIRTWD) as TDM_ARP_ARR_NO_CSGVIRTWD,
sum(TDM_ARP_ARR_A) as TDM_ARP_ARR_A,
sum(TDM_ARP_ARR_C) as TDM_ARP_ARR_C,
sum(TDM_ARP_ARR_D) as TDM_ARP_ARR_D,
sum(TDM_ARP_ARR_E) as TDM_ARP_ARR_E,
sum(TDM_ARP_ARR_G) as TDM_ARP_ARR_G,
sum(TDM_ARP_ARR_I) as TDM_ARP_ARR_I,
sum(TDM_ARP_ARR_M) as TDM_ARP_ARR_M,
sum(TDM_ARP_ARR_N) as TDM_ARP_ARR_N,
sum(TDM_ARP_ARR_O) as TDM_ARP_ARR_O,
sum(TDM_ARP_ARR_P) as TDM_ARP_ARR_P,
sum(TDM_ARP_ARR_R) as TDM_ARP_ARR_R,
sum(TDM_ARP_ARR_S) as TDM_ARP_ARR_S,
sum(TDM_ARP_ARR_T) as TDM_ARP_ARR_T,
sum(TDM_ARP_ARR_V) as TDM_ARP_ARR_V,
sum(TDM_ARP_ARR_W) as TDM_ARP_ARR_W,
sum(TDM_ARP_ARR_NA) as TDM_ARP_ARR_NA,
sum(TDM_15_ARP_ARR_A) as TDM_15_ARP_ARR_A,
sum(TDM_15_ARP_ARR_C) as TDM_15_ARP_ARR_C,
sum(TDM_15_ARP_ARR_D) as TDM_15_ARP_ARR_D,
sum(TDM_15_ARP_ARR_E) as TDM_15_ARP_ARR_E,
sum(TDM_15_ARP_ARR_G) as TDM_15_ARP_ARR_G,
sum(TDM_15_ARP_ARR_I) as TDM_15_ARP_ARR_I,
sum(TDM_15_ARP_ARR_M) as TDM_15_ARP_ARR_M,
sum(TDM_15_ARP_ARR_N) as TDM_15_ARP_ARR_N,
sum(TDM_15_ARP_ARR_O) as TDM_15_ARP_ARR_O,
sum(TDM_15_ARP_ARR_P) as TDM_15_ARP_ARR_P,
sum(TDM_15_ARP_ARR_R) as TDM_15_ARP_ARR_R,
sum(TDM_15_ARP_ARR_S) as TDM_15_ARP_ARR_S,
sum(TDM_15_ARP_ARR_T) as TDM_15_ARP_ARR_T,
sum(TDM_15_ARP_ARR_V) as TDM_15_ARP_ARR_V,
sum(TDM_15_ARP_ARR_W) as TDM_15_ARP_ARR_W,
sum(TDM_15_ARP_ARR_NA) as TDM_15_ARP_ARR_NA,
sum(TDM_ARP_DEP) as TDM_ARP_DEP,
sum(TDF_ARP_DEP) as TDF_ARP_DEP,
sum(TRF_ARP_DEP) as TRF_ARP_DEP,
sum(TDM_15_ARP_DEP) as TDM_15_ARP_DEP,
sum(TDF_15_ARP_DEP) as TDF_15_ARP_DEP,
sum(TRF_15_ARP_DEP) as TRF_15_ARP_DEP,
sum(TDM_01_15_ARP_DEP) as TDM_01_15_ARP_DEP,
sum(TDM_15_60_ARP_DEP) as TDM_15_60_ARP_DEP,
sum(TDM_60_ARP_DEP) as TDM_60_ARP_DEP,
sum(TDF_01_15_ARP_DEP) as TDF_01_15_ARP_DEP,
sum(TDF_15_60_ARP_DEP) as TDF_15_60_ARP_DEP,
sum(TDF_60_ARP_DEP) as TDF_60_ARP_DEP,
sum(TDM_ARP_DEP_CSG) as TDM_ARP_DEP_CSG,
sum(TDM_ARP_DEP_VIRT) as TDM_ARP_DEP_VIRT,
sum(TDM_ARP_DEP_WD) as TDM_ARP_DEP_WD,
sum(TDM_ARP_DEP_NO_CSGVIRTWD) as TDM_ARP_DEP_NO_CSGVIRTWD,
sum(TDM_ARP_ARR_CS) as TDM_ARP_ARR_CS,
sum(TDM_ARP_ARR_IT) as TDM_ARP_ARR_IT,
sum(TDM_ARP_ARR_NO_CSITWD) as TDM_ARP_ARR_NO_CSITWD,
sum(TDM_ARP_DEP_IT) as TDM_ARP_DEP_IT,
sum(TDM_ARP_DEP_NO_CSGITWD) as TDM_ARP_DEP_NO_CSGITWD,
sum(TDM_ARP_DEP_A) as TDM_ARP_DEP_A,
sum(TDM_ARP_DEP_C) as TDM_ARP_DEP_C,
sum(TDM_ARP_DEP_D) as TDM_ARP_DEP_D,
sum(TDM_ARP_DEP_E) as TDM_ARP_DEP_E,
sum(TDM_ARP_DEP_G) as TDM_ARP_DEP_G,
sum(TDM_ARP_DEP_I) as TDM_ARP_DEP_I,
sum(TDM_ARP_DEP_M) as TDM_ARP_DEP_M,
sum(TDM_ARP_DEP_N) as TDM_ARP_DEP_N,
sum(TDM_ARP_DEP_O) as TDM_ARP_DEP_O,
sum(TDM_ARP_DEP_P) as TDM_ARP_DEP_P,
sum(TDM_ARP_DEP_R) as TDM_ARP_DEP_R,
sum(TDM_ARP_DEP_S) as TDM_ARP_DEP_S,
sum(TDM_ARP_DEP_T) as TDM_ARP_DEP_T,
sum(TDM_ARP_DEP_V) as TDM_ARP_DEP_V,
sum(TDM_ARP_DEP_W) as TDM_ARP_DEP_W,
sum(TDM_ARP_DEP_NA) as TDM_ARP_DEP_NA,
sum(TDM_15_ARP_DEP_A) as TDM_15_ARP_DEP_A,
sum(TDM_15_ARP_DEP_C) as TDM_15_ARP_DEP_C,
sum(TDM_15_ARP_DEP_D) as TDM_15_ARP_DEP_D,
sum(TDM_15_ARP_DEP_E) as TDM_15_ARP_DEP_E,
sum(TDM_15_ARP_DEP_G) as TDM_15_ARP_DEP_G,
sum(TDM_15_ARP_DEP_I) as TDM_15_ARP_DEP_I,
sum(TDM_15_ARP_DEP_M) as TDM_15_ARP_DEP_M,
sum(TDM_15_ARP_DEP_N) as TDM_15_ARP_DEP_N,
sum(TDM_15_ARP_DEP_O) as TDM_15_ARP_DEP_O,
sum(TDM_15_ARP_DEP_P) as TDM_15_ARP_DEP_P,
sum(TDM_15_ARP_DEP_R) as TDM_15_ARP_DEP_R,
sum(TDM_15_ARP_DEP_S) as TDM_15_ARP_DEP_S,
sum(TDM_15_ARP_DEP_T) as TDM_15_ARP_DEP_T,
sum(TDM_15_ARP_DEP_V) as TDM_15_ARP_DEP_V,
sum(TDM_15_ARP_DEP_W) as TDM_15_ARP_DEP_W,
sum(TDM_15_ARP_DEP_NA) as TDM_15_ARP_DEP_NA
FROM ALL_DAY_DATA
GROUP BY flight_date, arp_code, arp_name, icao2letter
order by arp_code,  flight_date")



##punctuality ----
query_ap_punct <- paste0("WITH
  --Getting the list of airports
  LIST_AIRPORT as (
  SELECT code as arp_code,
         id as arp_id ,
         dashboard_name as arp_name,
  SUBSTR (CODE, 1, 2) AS ICAO2LETTER
  from pru_airport where code in ('",paste0(apt_icao$apt_icao_code, collapse = "','"),"'
  )
  ),
  --creating a table with the airport codes and dates since 2019
  AP_DAY AS
  (SELECT
              a.arp_code,
              a.arp_name,
              t.year,
              t.month,
              t.week,
              t.week_nb_year,
              t.day_type,
              t.day_of_week_nb AS day_of_week,
              t.day_date
      FROM LIST_AIRPORT a, pru_time_references t
      WHERE
         t.day_date >= to_date('24-12-2018','DD-MM-YYYY')
         AND t.day_date < trunc(sysdate)
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
")


#check how long the query takes (must have generate_json_file open to install libraries)
#start.time <- Sys.time()
#aptt_punct_raw <- export_query(query_ap_punct) %>%
#  as_tibble() %>%
#  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
#end.time <- Sys.time()
#time.taken <- end.time - start.time
#time.taken

