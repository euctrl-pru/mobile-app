
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
query_ap_traffic <- paste0("
with

LIST_AIRPORT  as
(select * from pruprod.v_aiu_app_dim_airport ),

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
      and t.year <= extract(year from TRUNC(SYSDATE)-1)
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
       TRUNC(SYSDATE) - 1 as LAST_DATA_DAY


     FROM AIRP_CACL_PREV
     where flight_DATE >=to_date('01-01-'|| extract(year from (TRUNC(SYSDATE)-1)),'dd-mm-yyyy') AND
     flight_DATE <=to_date('31-12-'|| extract(year from (TRUNC(SYSDATE)-1)),'dd-mm-yyyy')"

)



## delay ----
query_ap_delay <- paste0("
with

LIST_AIRPORT  as
(select * from pruprod.v_aiu_app_dim_airport ),

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
order by arp_code,  flight_date
"
)

##punctuality ----
query_ap_punct <- paste0(
"WITH
  --Getting the list of airports
LIST_AIRPORT  as
(select * from pruprod.v_aiu_app_dim_airport ),

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
         AND t.day_date < TRUNC(SYSDATE)
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



#check how long the query takes (must have generate_json_file open to install libraries)
#start.time <- Sys.time()
#aptt_punct_raw <- export_query(query_ap_punct) %>%
#  as_tibble() %>%
#  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
#end.time <- Sys.time()
#time.taken <- end.time - start.time
#time.taken

# apt ao ----
### day ----
query_ap_ao_data_day_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 ("
WITH

DIM_AO
 as ( select  ao_code, ao_name, ao_grp_code, ao_grp_name, ao_nm_group_code, ao_nm_group_name  from  prudev.v_covid_dim_ao) ,

DIM_APT as
(select * from pruprod.v_aiu_app_dim_airport )

, DATA_SOURCE as (
SELECT
        A.flt_dep_ad as dep_ARP_CODE
        ,A.flt_ctfm_ades as arr_ARP_CODE,
        b.arp_name  as dep_arp_name,
        c.arp_name as arr_arp_name,
        nvl(d.ao_grp_code,'Unidentified')ao_grp_code, nvl(d.ao_grp_name,'Unidentified') ao_grp_name,
        TRUNC(A.flt_a_asp_prof_time_entry) ENTRY_DATE,
        A.flt_uid
FROM v_aiu_flt A
     left outer  join DIM_APT b ON  ( A.flt_dep_ad=  b.arp_code)
     left outer join DIM_APT C  ON  (A.flt_ctfm_ades = C.arp_code)
     left outer join DIM_AO d  on ( (a.ao_icao_id = d.ao_code ) )
WHERE
     (  (  A.flt_lobt >=  ", mydate, " - 1 -2
    AND A.flt_lobt <  ", mydate, " + 2
    AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -  1
    AND A.flt_a_asp_prof_time_entry <  ", mydate, " )
 OR
 (  A.flt_lobt >=  ", mydate, "-1 - 2 -  ((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
    AND A.flt_lobt <   ", mydate, " +2 - ((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
    AND A.flt_a_asp_prof_time_entry >=   ", mydate, " - 1 - ((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
    AND A.flt_a_asp_prof_time_entry <   ", mydate, " - ((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
 )
OR
 (  A.flt_lobt >=  ", mydate, " -364 -1 -2
    AND A.flt_lobt <   ", mydate, " - 364 + 2
    AND A.flt_a_asp_prof_time_entry >=   ", mydate, " - 364 -  1
    AND A.flt_a_asp_prof_time_entry <   ", mydate, " - 364
  )
  OR
 (  A.flt_lobt >=  ", mydate, " -7 -1 -2
    AND A.flt_lobt <   ", mydate, " - 7 + 2
    AND A.flt_a_asp_prof_time_entry >=   ", mydate, " - 1 -  7
    AND A.flt_a_asp_prof_time_entry <   ", mydate, " - 7
  )

)
    AND A.flt_state IN ('TE','TA','AA')

),



DATA_DEP AS (
(SELECT
        dep_ARP_CODE as ARP_CODE,
        dep_arp_name as arp_name ,
        a.ENTRY_DATE,
        ao_grp_code,ao_grp_name,
        COUNT(a.flt_uid) DEP_ARR
FROM DATA_SOURCE a
GROUP BY
        dep_ARP_CODE,
        dep_arp_name  ,
        ENTRY_DATE,
        ao_grp_code,ao_grp_name
)
),

DATA_ARR AS (
SELECT
       arr_ARP_CODE as ARP_CODE,
        ARR_arp_name as arp_name ,
        ENTRY_DATE,
        ao_grp_code,ao_grp_name,
        COUNT(a.flt_uid) DEP_ARR
FROM DATA_SOURCE a
WHERE ARR_arp_name is not null
GROUP BY  arr_ARP_CODE,
          ARR_arp_name  ,
          ENTRY_DATE,
          ao_grp_code,ao_grp_name
),


DATA_DAY as
(SELECT
          coalesce(a.entry_date, b.entry_date) as entry_date,
          coalesce(a.ARP_CODE,b.ARP_CODE) as ARP_CODE,
          coalesce( a.arp_name,b.arp_name) as arp_name,
          coalesce(  a.ao_grp_code,b.ao_grp_code) as ao_grp_code, coalesce(  a.ao_grp_name,b.ao_grp_name) as ao_grp_name,
          coalesce(a.DEP_ARR,0)  + coalesce( b.DEP_ARR,0)  as DEP_ARR
FROM DATA_DEP a full outer join DATA_ARR b  on (a.ARP_CODE = b.ARP_CODE  and a.entry_date = b.entry_date and a.ao_grp_code = b.ao_grp_code)

),

DATA_PERIOD as
 (
 SELECT
      entry_date,
      ao_grp_code, ao_grp_name,
      ARP_CODE,
      arp_name,
      case
                     when (a.entry_date >= ", mydate, "-1  AND a.entry_date < ", mydate, ") then 'CURRENT_DAY'
                     when  (a.entry_date >= ", mydate, " -1 - ((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
                           AND a.entry_date <  ", mydate, " - ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 )
                        then 'DAY_2019'
                     when  (a.entry_date >= ", mydate, " - 1 - 364    AND a.entry_date <  ", mydate, " - 364) then 'DAY_PREV_YEAR'
                     when   (a.entry_date >= ", mydate, " - 1 - 7    AND a.entry_date <  ", mydate, " - 7) then  'DAY_PREV_WEEK'
        end FLAG_DAY,
     DEP_ARR
  FROM DATA_DAY a

  ),

DATA_GRP as
(
SELECT
      a.FLAG_DAY
      ,max(entry_date) as to_date
      , a.ao_grp_code, a.ao_grp_name
      ,a.ARP_CODE
      ,a.arp_name
     ,SUM (DEP_ARR)  DEP_ARR
   --  ,SUM (DEP_ARR) / 7 as avg_DEP_ARR

FROM DATA_PERIOD a
WHERE a.ARP_CODE in (select arp_code from DIM_APT) AND a.ao_grp_name <> 'Unidentified'
GROUP BY a.FLAG_DAY
      , ARP_CODE
      , arp_name
       , a.ao_grp_code, a.ao_grp_name
),

APT_AO_RANK as
(
SELECT
  ARP_CODE, FLAG_DAY, ao_grp_code,
        ROW_NUMBER() OVER (PARTITION BY  ARP_CODE, FLAG_DAY
                ORDER BY DEP_ARR DESC, ao_grp_name) R_RANK,
        RANK() OVER (PARTITION BY  ARP_CODE, FLAG_DAY
                ORDER BY DEP_ARR DESC, ao_grp_name) RANK
FROM DATA_GRP
where FLAG_DAY = 'CURRENT_DAY'
),

APT_AO_RANK_PREV as
(
SELECT
  ARP_CODE, FLAG_DAY, ao_grp_code,
        ROW_NUMBER() OVER (PARTITION BY  ARP_CODE, FLAG_DAY
                ORDER BY DEP_ARR DESC, ao_grp_name) RANK_PREV
 FROM DATA_GRP
where FLAG_DAY = 'DAY_PREV_WEEK'
)

SELECT
      a.ARP_CODE
      ,a.arp_name
      , a.FLAG_DAY
      ,to_date
      ,", mydate, "-1 as last_data_day
      , a.ao_grp_code, a.ao_grp_name
     ,DEP_ARR
     , R_RANK,rank, rank_prev

FROM DATA_GRP a
 left join APT_AO_RANK b on a.ARP_CODE = b.ARP_CODE AND a.ao_grp_code = b.ao_grp_code
 left join APT_AO_RANK_PREV c on a.ARP_CODE = c.ARP_CODE AND a.ao_grp_code = c.ao_grp_code
where R_RANK <=10
order by ARP_CODE, flag_day, R_RANK

")}

### week ----
query_ap_ao_data_week_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 ("
  WITH

DIM_AO
 as ( select  ao_code, ao_name, ao_grp_code, ao_grp_name, ao_nm_group_code, ao_nm_group_name  from  prudev.v_covid_dim_ao) ,

DIM_APT as
(select * from pruprod.v_aiu_app_dim_airport )

, DATA_SOURCE as (
SELECT
        A.flt_dep_ad as dep_ARP_CODE
        ,A.flt_ctfm_ades as arr_ARP_CODE,
        b.arp_name  as dep_arp_name,
        c.arp_name as arr_arp_name,
        nvl(d.ao_grp_code,'Unidentified')ao_grp_code, nvl(d.ao_grp_name,'Unidentified') ao_grp_name,
        TRUNC(A.flt_a_asp_prof_time_entry) ENTRY_DATE,
        A.flt_uid
FROM v_aiu_flt A
     left outer  join DIM_APT b ON  ( A.flt_dep_ad=  b.arp_code)
     left outer join DIM_APT C  ON  (A.flt_ctfm_ades = C.arp_code)
     left outer join DIM_AO d  on ( (a.ao_icao_id = d.ao_code ) )
WHERE
     (  (  A.flt_lobt >=  ", mydate, " - 7 -2
    AND A.flt_lobt <  ", mydate, " + 2
    AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -  7
    AND A.flt_a_asp_prof_time_entry <  ", mydate, " )
 OR
 (  A.flt_lobt >=  ", mydate, "-7 - 2 -  ((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
    AND A.flt_lobt <   ", mydate, " +2 - ((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
    AND A.flt_a_asp_prof_time_entry >=   ", mydate, " - 7 - ((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
    AND A.flt_a_asp_prof_time_entry <   ", mydate, " - ((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
 )
OR
 (  A.flt_lobt >=  ", mydate, " -364 -7 -2
    AND A.flt_lobt <   ", mydate, " - 364 + 2
    AND A.flt_a_asp_prof_time_entry >=   ", mydate, " - 364 -  7
    AND A.flt_a_asp_prof_time_entry <   ", mydate, " - 364
  )
OR
 (  A.flt_lobt >=  ", mydate, " -7 -7 -2
    AND A.flt_lobt <   ", mydate, " - 7 + 2
    AND A.flt_a_asp_prof_time_entry >=   ", mydate, " - 7 -  7
    AND A.flt_a_asp_prof_time_entry <   ", mydate, " - 7
  )

)
    AND A.flt_state IN ('TE','TA','AA')

),




DATA_DEP AS (
(SELECT
        dep_ARP_CODE as ARP_CODE,
        dep_arp_name as arp_name ,
        a.ENTRY_DATE,
        ao_grp_code,ao_grp_name,
        COUNT(a.flt_uid) DEP_ARR
FROM DATA_SOURCE a
GROUP BY
        dep_ARP_CODE,
        dep_arp_name  ,
        ENTRY_DATE,
        ao_grp_code,ao_grp_name
)
),

DATA_ARR AS (
SELECT
       arr_ARP_CODE as ARP_CODE,
        ARR_arp_name as arp_name ,
        ENTRY_DATE,
        ao_grp_code,ao_grp_name,
        COUNT(a.flt_uid) DEP_ARR
FROM DATA_SOURCE a
WHERE ARR_arp_name is not null
GROUP BY  arr_ARP_CODE,
          ARR_arp_name  ,
          ENTRY_DATE,
          ao_grp_code,ao_grp_name
),


DATA_DAY as
(SELECT
          coalesce(a.entry_date, b.entry_date) as entry_date,
          coalesce(a.ARP_CODE,b.ARP_CODE) as ARP_CODE,
          coalesce( a.arp_name,b.arp_name) as arp_name,
          coalesce(  a.ao_grp_code,b.ao_grp_code) as ao_grp_code, coalesce(  a.ao_grp_name,b.ao_grp_name) as ao_grp_name,
          coalesce(a.DEP_ARR,0)  + coalesce( b.DEP_ARR,0)  as DEP_ARR
FROM DATA_DEP a full outer join DATA_ARR b  on (a.ARP_CODE = b.ARP_CODE  and a.entry_date = b.entry_date and a.ao_grp_code = b.ao_grp_code)

),

DATA_PERIOD as
 (
 SELECT
      entry_date,
      ao_grp_code, ao_grp_name,
      ARP_CODE,
      arp_name,
      case
                     when (a.entry_date >= ", mydate, "-7  AND a.entry_date < ", mydate, ") then 'CURRENT_ROLLING_WEEK'
                     when  (a.entry_date >= ", mydate, " -7 - ((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
                           AND a.entry_date <  ", mydate, " - ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 )
                        then 'ROLLING_WEEK_2019'
                     when  (a.entry_date >= ", mydate, " - 7 - 364    AND a.entry_date <  ", mydate, " - 364) then 'ROLLING_WEEK_PREV_YEAR'
                     when   (a.entry_date >= ", mydate, " - 7 - 7    AND a.entry_date <  ", mydate, " - 7) then  'PREV_ROLLING_WEEK'
        end PERIOD_TYPE,
     DEP_ARR
  FROM DATA_DAY a

  ),

DATA_GRP as
(
SELECT
      a.period_type
      ,min(entry_date) as from_date
      ,max(entry_date) as to_date
      , a.ao_grp_code, a.ao_grp_name
      ,a.ARP_CODE
      ,a.arp_name
     ,SUM (DEP_ARR)  DEP_ARR
   --  ,SUM (DEP_ARR) / 7 as avg_DEP_ARR

FROM DATA_PERIOD a
WHERE a.ARP_CODE in (select arp_code from DIM_APT) AND a.ao_grp_name <> 'Unidentified'
GROUP BY a.period_type
      , ARP_CODE
      , arp_name
       , a.ao_grp_code, a.ao_grp_name
),

APT_AO_RANK as
(
SELECT
  ARP_CODE, period_type, ao_grp_code,
        ROW_NUMBER() OVER (PARTITION BY  ARP_CODE, period_type
                ORDER BY DEP_ARR DESC, ao_grp_name) R_RANK,
        RANK() OVER (PARTITION BY  ARP_CODE, period_type
                ORDER BY DEP_ARR DESC, ao_grp_name) RANK
FROM DATA_GRP
where period_type = 'CURRENT_ROLLING_WEEK'
),

APT_AO_RANK_PREV as
(
SELECT
  ARP_CODE, period_type, ao_grp_code,
        RANK() OVER (PARTITION BY  ARP_CODE, period_type
                ORDER BY DEP_ARR DESC, ao_grp_name) RANK_PREV
 FROM DATA_GRP
where period_type = 'PREV_ROLLING_WEEK'
)

SELECT
      a.ARP_CODE
      ,a.arp_name
      , a.period_type
      ,from_date
      ,to_date
      ,", mydate, "-1 as last_data_day
      , a.ao_grp_code, a.ao_grp_name
     ,DEP_ARR
     , R_RANK, rank, rank_prev

FROM DATA_GRP a
 left join APT_AO_RANK b on a.ARP_CODE = b.ARP_CODE AND a.ao_grp_code = b.ao_grp_code
 left join APT_AO_RANK_PREV c on a.ARP_CODE = c.ARP_CODE AND a.ao_grp_code = c.ao_grp_code
where R_RANK <=10
order by ARP_CODE, period_type, R_RANK
"
  )
  }

### y2d ----
query_ap_ao_data_y2d_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 ("
  WITH

DIM_AO
 as ( select  ao_code, ao_name, ao_grp_code, ao_grp_name, ao_nm_group_code, ao_nm_group_name  from  prudev.v_covid_dim_ao) ,

DIM_APT as
(select * from pruprod.v_aiu_app_dim_airport ),


DATA_DAY as
(SELECT
        A.flt_dep_ad as dep_arp_code
        ,A.flt_ctfm_ades as arr_arp_code,
        b.arp_name  as dep_arp_name,
        c.arp_name as arr_arp_name,
        nvl(d.ao_grp_code,'UNKNOWN')ao_grp_code, nvl(d.ao_grp_name,'UNKNOWN') ao_grp_name,
        TRUNC(A.flt_a_asp_prof_time_entry) ENTRY_DATE,
        A.flt_uid
FROM v_aiu_flt A
     left outer  join DIM_APT b ON  ( A.flt_dep_ad=  b.arp_code)
     left outer join DIM_APT C  ON  (A.flt_ctfm_ades = C.arp_code)
     left outer join DIM_AO d  on ( (a.ao_icao_id = d.ao_code ) )
WHERE
     A.flt_lobt>= TO_DATE ('01-01-2019', 'dd-mm-yyyy') -1   AND A.flt_lobt < ", mydate, "
    --  AND TO_NUMBER (TO_CHAR (TRUNC ( A.flt_lobt), 'mmdd')) <   TO_NUMBER (TO_CHAR (TRUNC (SYSDATE+1), 'mmdd'))
     AND flt_a_asp_prof_time_entry >= TO_DATE ('01-01-2019', 'dd-mm-yyyy')
     AND TO_NUMBER (TO_CHAR (TRUNC (flt_a_asp_prof_time_entry), 'mmdd')) <=   TO_NUMBER (TO_CHAR (", mydate, "-1, 'mmdd'))
       and extract (year from flt_a_asp_prof_time_entry) <= extract(year from (", mydate, "-1))
     AND A.flt_state IN ('TE', 'TA', 'AA')
),


DATA_DEP AS (
(SELECT
        dep_arp_code as arp_code,
        dep_arp_name as arp_name ,
        a.ENTRY_DATE,
        ao_grp_code,ao_grp_name,
        COUNT(a.flt_uid) DEP_ARR
FROM DATA_DAY a
GROUP BY
        dep_arp_code,
        dep_arp_name  ,
        ENTRY_DATE,
        ao_grp_code,ao_grp_name
)
),

DATA_ARR AS (
SELECT
       arr_arp_code as arp_code,
        ARR_arp_name as arp_name ,
        ENTRY_DATE,
        ao_grp_code,ao_grp_name,
        COUNT(a.flt_uid) DEP_ARR
FROM DATA_DAY a
WHERE ARR_arp_name is not null
GROUP BY  arr_arp_code,
          ARR_arp_name  ,
          ENTRY_DATE,
          ao_grp_code,ao_grp_name
),


ALL_DAY_DATA as
(SELECT
          coalesce(extract(year from a.entry_date),extract (year from b.entry_date)) as year,
          coalesce(a.entry_date, b.entry_date) as entry_date,
          coalesce(a.arp_code,b.arp_code) as arp_code,
          coalesce( a.arp_name,b.arp_name) as arp_name,
          coalesce(  a.ao_grp_code,b.ao_grp_code) as ao_grp_code, coalesce(  a.ao_grp_name,b.ao_grp_name) as ao_grp_name,
          coalesce(a.DEP_ARR,0)  + coalesce( b.DEP_ARR,0)  as DEP_ARR
FROM DATA_DEP a full outer join DATA_ARR b  on (a.arp_code = b.arp_code  and a.entry_date = b.entry_date and a.ao_grp_code = b.ao_grp_code)

),

DATA_GRP as
(
SELECT
      year, ao_grp_code,ao_grp_name
    , arp_code, arp_name
    ,SUM (DEP_ARR)  DEP_ARR
    ,min(entry_Date) as from_date
    ,max(entry_date) as to_date
    ,", mydate, " -1 as last_data_date

FROM ALL_DAY_DATA
WHERE arp_code in (select arp_code from  DIM_APT) AND ao_grp_name <> 'UNKNOWN' AND ao_grp_code <> 'ZZZ'
GROUP BY year, arp_code, arp_name, ao_grp_code,ao_grp_name
),

APT_AO_RANK as
(
SELECT
  arp_code, year, ao_grp_code,
        ROW_NUMBER() OVER (PARTITION BY  arp_code, year
                ORDER BY DEP_ARR DESC, ao_grp_name) R_RANK,
        RANK() OVER (PARTITION BY  arp_code, year
                ORDER BY DEP_ARR DESC, ao_grp_name) RANK
FROM DATA_GRP
where year = extract (year from (", mydate, "-1))
),

APT_AO_RANK_PREV as
(
SELECT
  arp_code, year, ao_grp_code,
        ROW_NUMBER() OVER (PARTITION BY  arp_code, year
                ORDER BY DEP_ARR DESC, ao_grp_name) RANK_PY
 FROM DATA_GRP
where year = extract (year from (", mydate, "-1)) - 1
)


SELECT
      a.arp_code, a.arp_name
      ,a.year, a.ao_grp_code, a.ao_grp_name
    ,DEP_ARR
    ,from_date
    ,to_date
    , last_data_date
     , R_RANK, rank , RANK_PY
FROM DATA_GRP a
 left join APT_AO_RANK b on a.arp_code = b.arp_code AND a.ao_grp_code = b.ao_grp_code
 left join APT_AO_RANK_PREV c on a.arp_code = c.arp_code AND a.ao_grp_code = c.ao_grp_code
where R_RANK <=10
order by ARP_CODE, year desc, R_RANK
          "
  )
  }

# apt state des ----
### day ----
query_ap_st_des_data_day_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 ("
          with

DIM_AO
 as ( select  ao_code, ao_name, ao_nm_group_code, ao_nm_group_name  from  prudev.v_covid_dim_ao) ,

DIM_APT
 as ( select code as arp_code, dashboard_name as arp_name,  b.aiu_iso_country_name as iso_ct_name, a.iso_country_code as iso_ct_code
 from  pru_airport a
 left join pru_country_iso b on a.iso_country_code = b.ec_iso_country_code
 ),

LIST_APT as
(select * from pruprod.v_aiu_app_dim_airport ),


AIRP_FLIGHT as (

SELECT flt_uid,
       TRUNC (flt_a_asp_prof_time_entry) AS entry_day,
       a.flt_dep_ad dep_ap ,a.flt_ctfm_ades arr_ap,
       A.ao_icao_id

  FROM v_aiu_flt a
 WHERE       (
                     (     A.flt_lobt >= ", mydate, " -1 -1
                        AND A.flt_lobt < ", mydate, "-0
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -1
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0
                        )
                        or
                     (     A.flt_lobt >= ", mydate, " -1 -1-7
                        AND A.flt_lobt < ", mydate, "-0-7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -1-7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0-7
                        )
                        or
                     (     A.flt_lobt >= ", mydate, " -1 -1-364
                        AND A.flt_lobt < ", mydate, "-0-364
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -1 -364
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0 -364
                        )
                        or
                        (     A.flt_lobt >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)-1 -1- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_lobt < ", mydate, "-((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)-1- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
                         )
                     )
       AND A.flt_state IN ('TE', 'TA', 'AA')
       AND  flt_dep_ad IS NOT NULL
       AND    flt_ctfm_ades  IS NOT NULL
     ),


 AP_PAIR_SELECTION as
 (SELECT  a.dep_ap, a.arr_ap, a.entry_day, a.flt_uid,a.ao_icao_id, b.arp_name as arp_name_dep, b.iso_ct_name as iso_ct_name_dep, b.iso_ct_code as iso_ct_code_dep
  FROM  AIRP_FLIGHT A , DIM_APT b
  WHERE  b.arp_code = a.dep_ap
        and dep_ap in (select arp_code  from LIST_APT))
,


AP_PAIR_AREA as (
SELECT entry_day,
       flt_uid,
        a.dep_ap, a.arp_name_dep, a.iso_ct_name_dep, a.iso_ct_code_dep,
        a.arr_ap,  b.arp_name as arp_name_arr,
        b.iso_ct_name as iso_ct_name_arr,
        b.iso_ct_code as iso_ct_code_arr
 FROM  AP_PAIR_SELECTION A , DIM_APT b
  WHERE  b.arp_code = a.arr_ap),


AO_AIRPORT_FLIGHT as
(
SELECT entry_day,
       flt_uid,
       dep_ap, arp_name_dep, iso_ct_name_Dep, iso_ct_code_Dep,
       arr_ap, arp_name_arr, iso_ct_name_arr, iso_ct_code_arr,
   case
     when entry_day >= ", mydate, " -1 and entry_day < ", mydate, "  then 'CURRENT_DAY'
     when entry_day >= ", mydate, " -1-7 and entry_day < ", mydate, "  then 'DAY_PREV_WEEK'
     when entry_day >= ", mydate, " -1-364 and entry_day < ", mydate, "  then 'DAY_PREV_YEAR'
     when entry_day >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)-1- floor((extract (year from (", mydate, "-1))-2019)/4)*7
            and entry_day < ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
        then 'DAY_2019'
     else '-'
   end  flag_period
FROM  AP_PAIR_AREA a
  ),

DATA_GRP as
(
SELECT
   flag_period,
   entry_day,
   dep_ap, arp_name_dep, iso_ct_name_Dep, iso_ct_code_Dep,
       iso_ct_name_arr, iso_ct_code_arr,
   count(flt_uid) as dep

FROM AO_AIRPORT_FLIGHT a
group by flag_period, dep_ap, arp_name_dep,entry_day, iso_ct_name_Dep, iso_ct_name_arr, iso_ct_code_Dep, iso_ct_code_arr
),

APT_CTRY_RANK as
(
SELECT
  dep_ap, flag_period, iso_ct_name_arr,
        RANK() OVER (PARTITION BY  dep_ap, flag_period
                ORDER BY dep DESC, iso_ct_name_arr) RANK,
        ROW_NUMBER() OVER (PARTITION BY  dep_ap, flag_period
                ORDER BY dep DESC, iso_ct_name_arr) R_RANK
FROM DATA_GRP
where flag_period = 'CURRENT_DAY'
),

APT_CTRY_RANK_PREV as
(
SELECT
  dep_ap, flag_period, iso_ct_name_arr,
        RANK() OVER (PARTITION BY  dep_ap, flag_period
                ORDER BY dep DESC, iso_ct_name_arr) as RANK_PREV
FROM DATA_GRP
where  flag_period = 'DAY_PREV_WEEK'
)


 SELECT
   a.flag_period,
   a.dep_ap as arp_code, arp_name_dep as arp_name, a.iso_ct_name_arr, a.iso_ct_code_arr,
   dep,
   entry_day as to_date,
     to_date(  TO_CHAR (", mydate, "-1, 'dd-mm-yyyy'),'dd-mm-yyyy') as last_data_day,
     r_rank, rank,  rank_prev

FROM DATA_GRP a
left join APT_CTRY_RANK b on a.iso_ct_name_arr = b.iso_ct_name_arr AND a.dep_ap = b.dep_ap
left join APT_CTRY_RANK_PREV c on a.iso_ct_name_arr = c.iso_ct_name_arr AND a.dep_ap = c.dep_ap
where r_rank<= '10'
order by a.dep_ap, flag_period, r_rank
"
  )
}

### week ----
query_ap_st_des_data_week_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 (
"with

DIM_AO
 as ( select  ao_code, ao_name, ao_nm_group_code, ao_nm_group_name  from  prudev.v_covid_dim_ao) ,

DIM_APT
 as ( select code as arp_code, dashboard_name as arp_name,  b.aiu_iso_country_name as iso_ct_name, a.iso_country_code as iso_ct_code
 from  pru_airport a
 left join pru_country_iso b on a.iso_country_code = b.ec_iso_country_code
 ),

LIST_APT as
(select * from pruprod.v_aiu_app_dim_airport ),

AIRP_FLIGHT as (

SELECT flt_uid,
       TRUNC (flt_a_asp_prof_time_entry) AS entry_day,
       a.flt_dep_ad dep_ap ,a.flt_ctfm_ades arr_ap,
       A.ao_icao_id

  FROM v_aiu_flt a
 WHERE       (
                     (     A.flt_lobt >= ", mydate, " -7 -1
                        AND A.flt_lobt < ", mydate, "-0
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0
                        )
                        or
                     (     A.flt_lobt >= ", mydate, " -7 -1-7
                        AND A.flt_lobt < ", mydate, "-0-7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -7-7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0-7
                        )
                        or
                     (     A.flt_lobt >= ", mydate, " -7 -1-364
                        AND A.flt_lobt < ", mydate, "-0-364
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -7 -364
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0 -364
                        )
                        or
                        (     A.flt_lobt >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)-7 -1- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_lobt < ", mydate, "-((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)-7- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
                         )
                     )
       AND A.flt_state IN ('TE', 'TA', 'AA')
       AND  flt_dep_ad IS NOT NULL
       AND    flt_ctfm_ades  IS NOT NULL
     ),


 AP_PAIR_SELECTION as
 (SELECT  a.dep_ap, a.arr_ap, a.entry_day, a.flt_uid,a.ao_icao_id, b.arp_name as arp_name_dep, b.iso_ct_name as iso_ct_name_dep, b.iso_ct_code as iso_ct_code_dep
  FROM  AIRP_FLIGHT A , DIM_APT b
  WHERE  b.arp_code = a.dep_ap
        and dep_ap in (select arp_code  from LIST_APT))
,


AP_PAIR_AREA as (
SELECT entry_day,
       flt_uid,
        a.dep_ap, a.arp_name_dep, a.iso_ct_name_dep, a.iso_ct_code_dep,
        a.arr_ap,  b.arp_name as arp_name_arr,
        b.iso_ct_name as iso_ct_name_arr,
        b.iso_ct_code as iso_ct_code_arr
 FROM  AP_PAIR_SELECTION A , DIM_APT b
  WHERE  b.arp_code = a.arr_ap),


AO_AIRPORT_FLIGHT as
(
SELECT entry_day,
       flt_uid,
       dep_ap, arp_name_dep, iso_ct_name_Dep, iso_ct_code_Dep,
       arr_ap, arp_name_arr, iso_ct_name_arr, iso_ct_code_arr,
   case
     when entry_day >= ", mydate, " -7 and entry_day < ", mydate, "  then 'CURRENT_ROLLING_WEEK'
     when entry_day >= ", mydate, " -7-7 and entry_day < ", mydate, "  then 'PREV_ROLLING_WEEK'
     when entry_day >= ", mydate, " -7-364 and entry_day < ", mydate, "  then 'ROLLING_WEEK_PREV_YEAR'
     when entry_day >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)-7- floor((extract (year from (", mydate, "-1))-2019)/4)*7
            and entry_day < ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
        then 'ROLLING_WEEK_2019'
     else '-'
   end  flag_period
FROM  AP_PAIR_AREA a
  ),

DATA_GRP as
(
SELECT
   flag_period,
   min(entry_day) as from_date,
   max(entry_day) as to_date,
   dep_ap, arp_name_dep, iso_ct_name_Dep, iso_ct_code_Dep,
       iso_ct_name_arr, iso_ct_code_arr,
   count(flt_uid) as dep

FROM AO_AIRPORT_FLIGHT a
group by flag_period, dep_ap, arp_name_dep, iso_ct_name_Dep, iso_ct_name_arr, iso_ct_code_Dep, iso_ct_code_arr
),

APT_CTRY_RANK as
(
SELECT
  dep_ap, flag_period, iso_ct_name_arr,
        RANK() OVER (PARTITION BY  dep_ap, flag_period
                ORDER BY dep DESC, iso_ct_name_arr) RANK,
        ROW_NUMBER() OVER (PARTITION BY  dep_ap, flag_period
                ORDER BY dep DESC, iso_ct_name_arr) R_RANK
FROM DATA_GRP
where flag_period = 'CURRENT_ROLLING_WEEK'
),

APT_CTRY_RANK_PREV as
(
SELECT
  dep_ap, flag_period, iso_ct_name_arr,
        RANK() OVER (PARTITION BY  dep_ap, flag_period
                ORDER BY dep DESC, iso_ct_name_arr) as RANK_PREV
FROM DATA_GRP
where  flag_period = 'PREV_ROLLING_WEEK'
)


 SELECT
   a.flag_period,
   a.dep_ap as arp_code, arp_name_dep as arp_name, a.iso_ct_name_arr, a.iso_ct_code_arr,
   dep,
   from_date,
   to_date,
     to_date(  TO_CHAR (", mydate, "-1, 'dd-mm-yyyy'),'dd-mm-yyyy') as last_data_day,
     r_rank, rank,  rank_prev

FROM DATA_GRP a
left join APT_CTRY_RANK b on a.iso_ct_name_arr = b.iso_ct_name_arr AND a.dep_ap = b.dep_ap
left join APT_CTRY_RANK_PREV c on a.iso_ct_name_arr = c.iso_ct_name_arr AND a.dep_ap = c.dep_ap
where r_rank<= '10'
order by a.dep_ap, flag_period, r_rank
"
  )
}

### y2d ----
query_ap_st_des_data_y2d_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 (
    "
    with

DIM_AO
 as ( select  ao_code, ao_name, ao_nm_group_code, ao_nm_group_name  from  prudev.v_covid_dim_ao) ,

DIM_APT
 as ( select code as arp_code, dashboard_name as arp_name,  b.aiu_iso_country_name as iso_ct_name, a.iso_country_code as iso_ct_code
 from  pru_airport a
 left join pru_country_iso b on a.iso_country_code = b.ec_iso_country_code
 ),

LIST_APT as
(select * from pruprod.v_aiu_app_dim_airport ),


AIRP_FLIGHT as (

SELECT flt_uid,
       TRUNC (flt_a_asp_prof_time_entry) AS entry_day,
       a.flt_dep_ad dep_ap ,a.flt_ctfm_ades arr_ap,
       A.ao_icao_id

  FROM v_aiu_flt a
 WHERE
     A.flt_lobt>= TO_DATE ('01-01-2019', 'dd-mm-yyyy') -1  AND A.flt_lobt < ", mydate, "
     AND a.flt_a_asp_prof_time_entry >= TO_DATE ('01-01-2019', 'dd-mm-yyyy')
     AND TO_NUMBER (TO_CHAR (TRUNC (flt_a_asp_prof_time_entry), 'mmdd')) <=   TO_NUMBER (TO_CHAR (", mydate, "-1, 'mmdd'))
     and extract (year from flt_a_asp_prof_time_entry) <= extract(year from (", mydate, "-1))
       AND A.flt_state IN ('TE', 'TA', 'AA')
       AND  flt_dep_ad IS NOT NULL
       AND    flt_ctfm_ades  IS NOT NULL
     ),


 AP_PAIR_SELECTION as
 (SELECT  a.dep_ap, a.arr_ap, a.entry_day, a.flt_uid,a.ao_icao_id, b.arp_name as arp_name_dep, b.iso_ct_name as iso_ct_name_dep, b.iso_ct_code as iso_ct_code_dep
  FROM  AIRP_FLIGHT A , DIM_APT b
  WHERE  b.arp_code = a.dep_ap
        and dep_ap in (select arp_code  from LIST_APT))
,


AP_PAIR_AREA as (
SELECT entry_day,
       extract (year from entry_day) as year,
       flt_uid,
        a.dep_ap, a.arp_name_dep, a.iso_ct_name_dep, a.iso_ct_code_dep,
        a.arr_ap,  b.arp_name as arp_name_arr,
        b.iso_ct_name as iso_ct_name_arr,
        b.iso_ct_code as iso_ct_code_arr
 FROM  AP_PAIR_SELECTION A , DIM_APT b
  WHERE  b.arp_code = a.arr_ap),



DATA_GRP as
(
SELECT
   year,
   min(entry_day) as from_date,
   max(entry_day) as to_date,
   dep_ap, arp_name_dep, iso_ct_name_Dep, iso_ct_code_Dep,
       iso_ct_name_arr, iso_ct_code_arr,
   count(flt_uid) as dep

FROM AP_PAIR_AREA a
group by year, dep_ap, arp_name_dep, iso_ct_name_Dep, iso_ct_name_arr, iso_ct_code_Dep, iso_ct_code_arr
),

APT_CTRY_RANK as
(
SELECT
  dep_ap, year, iso_ct_name_arr,
        RANK() OVER (PARTITION BY  dep_ap, year
                ORDER BY dep DESC, iso_ct_name_arr) RANK,
        ROW_NUMBER() OVER (PARTITION BY  dep_ap, year
                ORDER BY dep DESC, iso_ct_name_arr) R_RANK
FROM DATA_GRP
where year = extract (year from (", mydate, "-1))
),

APT_CTRY_RANK_PREV as
(
SELECT
  dep_ap, year, iso_ct_name_arr,
        RANK() OVER (PARTITION BY  dep_ap, year
                ORDER BY dep DESC, iso_ct_name_arr) as RANK_PREV
FROM DATA_GRP
where  year = extract (year from (", mydate, "-1)) -1
)


 SELECT
   a.year,
   a.dep_ap as arp_code, arp_name_dep as arp_name, a.iso_ct_name_arr, a.iso_ct_code_arr,
   dep,
   from_date,
   to_date,
     to_date(  TO_CHAR (", mydate, "-1, 'dd-mm-yyyy'),'dd-mm-yyyy') as last_data_day,
     r_rank, rank,  rank_prev

FROM DATA_GRP a
left join APT_CTRY_RANK b on a.iso_ct_name_arr = b.iso_ct_name_arr AND a.dep_ap = b.dep_ap
left join APT_CTRY_RANK_PREV c on a.iso_ct_name_arr = c.iso_ct_name_arr AND a.dep_ap = c.dep_ap
where r_rank<= '10'
order by a.dep_ap, year desc, r_rank
"
  )
}

# apt airport des ----
### day ----
query_ap_ap_des_data_day_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 (
    "with

DIM_AO
 as ( select  ao_code, ao_name, ao_nm_group_code, ao_nm_group_name  from  prudev.v_covid_dim_ao) ,

DIM_APT
 as ( select code as arp_code, dashboard_name as arp_name
 from  pru_airport a
 ),

LIST_APT as
(select * from pruprod.v_aiu_app_dim_airport ),


AIRP_FLIGHT as (

SELECT flt_uid,
       TRUNC (flt_a_asp_prof_time_entry) AS entry_day,
       a.flt_dep_ad dep_ap ,a.flt_ctfm_ades arr_ap,
       A.ao_icao_id

  FROM v_aiu_flt a
 WHERE       (
                     (     A.flt_lobt >= ", mydate, " -1 -1
                        AND A.flt_lobt < ", mydate, "-0
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -1
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0
                        )
                        or
                     (     A.flt_lobt >= ", mydate, " -1 -1-7
                        AND A.flt_lobt < ", mydate, "-0-7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -1-7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0-7
                        )
                        or
                     (     A.flt_lobt >= ", mydate, " -1 -1-364
                        AND A.flt_lobt < ", mydate, "-0-364
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -1 -364
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0 -364
                        )
                        or
                        (     A.flt_lobt >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)-1 -1- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_lobt < ", mydate, "-((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)-1- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
                         )
                     )
       AND A.flt_state IN ('TE', 'TA', 'AA')
       AND  flt_dep_ad IS NOT NULL
       AND    flt_ctfm_ades  IS NOT NULL
     ),


 AP_PAIR_SELECTION as
 (SELECT  a.dep_ap, a.arr_ap, a.entry_day, a.flt_uid,a.ao_icao_id, b.arp_name as arp_name_dep
  FROM  AIRP_FLIGHT A , DIM_APT b
  WHERE  b.arp_code = a.dep_ap
        and dep_ap in (select arp_code  from LIST_APT))
,


AP_PAIR_AREA as (
SELECT entry_day,
       flt_uid,
        a.dep_ap, a.arp_name_dep,
        a.arr_ap,  b.arp_name as arp_name_arr
 FROM  AP_PAIR_SELECTION A , DIM_APT b
  WHERE  b.arp_code = a.arr_ap),


AO_AIRPORT_FLIGHT as
(
SELECT entry_day,
       flt_uid,
       dep_ap, arp_name_dep,
       arr_ap, arp_name_arr,
   case
     when entry_day >= ", mydate, " -1 and entry_day < ", mydate, "  then 'CURRENT_DAY'
     when entry_day >= ", mydate, " -1-7 and entry_day < ", mydate, "  then 'DAY_PREV_WEEK'
     when entry_day >= ", mydate, " -1-364 and entry_day < ", mydate, "  then 'DAY_PREV_YEAR'
     when entry_day >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)-1- floor((extract (year from (", mydate, "-1))-2019)/4)*7
            and entry_day < ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
        then 'DAY_2019'
     else '-'
   end  flag_period
FROM  AP_PAIR_AREA a
  ),

DATA_GRP as
(
SELECT
   flag_period,
   entry_day,
   dep_ap, arp_name_dep,
       arr_ap, arp_name_arr,
   count(flt_uid) as dep

FROM AO_AIRPORT_FLIGHT a
group by flag_period, dep_ap, arp_name_dep,entry_day, arr_ap, arp_name_arr
),

APT_CTRY_RANK as
(
SELECT
  dep_ap, flag_period, arp_name_arr,
        RANK() OVER (PARTITION BY  dep_ap, flag_period
                ORDER BY dep DESC, arp_name_arr) RANK,
        ROW_NUMBER() OVER (PARTITION BY  dep_ap, flag_period
                ORDER BY dep DESC, arp_name_arr) R_RANK
FROM DATA_GRP
where flag_period = 'CURRENT_DAY'
),

APT_CTRY_RANK_PREV as
(
SELECT
  dep_ap, flag_period, arp_name_arr,
        RANK() OVER (PARTITION BY  dep_ap, flag_period
                ORDER BY dep DESC, arp_name_arr) as RANK_PREV
FROM DATA_GRP
where  flag_period = 'DAY_PREV_WEEK'
)


 SELECT
   a.flag_period,
   a.dep_ap as arp_code_dep, a.arp_name_dep,
   a.arr_ap as arp_code_arr, a.arp_name_arr,
   a.dep,
   entry_day as to_date,
     to_date(  TO_CHAR (", mydate, "-1, 'dd-mm-yyyy'),'dd-mm-yyyy') as last_data_day,
     r_rank, rank,  rank_prev

FROM DATA_GRP a
left join APT_CTRY_RANK b on a.arp_name_arr = b.arp_name_arr AND a.dep_ap = b.dep_ap
left join APT_CTRY_RANK_PREV c on a.arp_name_arr = c.arp_name_arr AND a.dep_ap = c.dep_ap
where r_rank<= '10'
order by a.dep_ap, flag_period, r_rank
"
  )}

### week ----
query_ap_ap_des_data_week_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 (
  "
  with

DIM_AO
 as ( select  ao_code, ao_name, ao_nm_group_code, ao_nm_group_name  from  prudev.v_covid_dim_ao) ,

DIM_APT
 as ( select code as arp_code, dashboard_name as arp_name
 from  pru_airport a
 ),

LIST_APT as
(select * from pruprod.v_aiu_app_dim_airport ),


AIRP_FLIGHT as (

SELECT flt_uid,
       TRUNC (flt_a_asp_prof_time_entry) AS entry_day,
       a.flt_dep_ad dep_ap ,a.flt_ctfm_ades arr_ap,
       A.ao_icao_id

  FROM v_aiu_flt a
 WHERE       (
                     (     A.flt_lobt >= ", mydate, " -7 -1
                        AND A.flt_lobt < ", mydate, "-0
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0
                        )
                        or
                     (     A.flt_lobt >= ", mydate, " -7 -1-7
                        AND A.flt_lobt < ", mydate, "-0-7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -7-7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0-7
                        )
                        or
                     (     A.flt_lobt >= ", mydate, " -7 -1-364
                        AND A.flt_lobt < ", mydate, "-0-364
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -7 -364
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0 -364
                        )
                        or
                        (     A.flt_lobt >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)-7 -1- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_lobt < ", mydate, "-((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)-7- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
                         )
                     )
       AND A.flt_state IN ('TE', 'TA', 'AA')
       AND  flt_dep_ad IS NOT NULL
       AND    flt_ctfm_ades  IS NOT NULL
     ),


 AP_PAIR_SELECTION as
 (SELECT  a.dep_ap, a.arr_ap, a.entry_day, a.flt_uid,a.ao_icao_id, b.arp_name as arp_name_dep
  FROM  AIRP_FLIGHT A , DIM_APT b
  WHERE  b.arp_code = a.dep_ap
        and dep_ap in (select arp_code  from LIST_APT))
,


AP_PAIR_AREA as (
SELECT entry_day,
       flt_uid,
        a.dep_ap, a.arp_name_dep,
        a.arr_ap,  b.arp_name as arp_name_arr
 FROM  AP_PAIR_SELECTION A , DIM_APT b
  WHERE  b.arp_code = a.arr_ap),


AO_AIRPORT_FLIGHT as
(
SELECT entry_day,
       flt_uid,
       dep_ap, arp_name_dep,
       arr_ap, arp_name_arr,
   case
     when entry_day >= ", mydate, " -7 and entry_day < ", mydate, "  then 'CURRENT_ROLLING_WEEK'
     when entry_day >= ", mydate, " -7-7 and entry_day < ", mydate, "  then 'PREV_ROLLING_WEEK'
     when entry_day >= ", mydate, " -7-364 and entry_day < ", mydate, "  then 'ROLLING_WEEK_PREV_YEAR'
     when entry_day >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)-7- floor((extract (year from (", mydate, "-1))-2019)/4)*7
            and entry_day < ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
        then 'ROLLING_WEEK_2019'
     else '-'
   end  flag_period
FROM  AP_PAIR_AREA a
  ),

DATA_GRP as
(
SELECT
   flag_period,
  min(entry_day) as from_date,
   max(entry_day) as to_date,
   dep_ap, arp_name_dep,
       arr_ap, arp_name_arr,
   count(flt_uid) as dep

FROM AO_AIRPORT_FLIGHT a
group by flag_period, dep_ap, arp_name_dep, arr_ap, arp_name_arr
),

APT_CTRY_RANK as
(
SELECT
  dep_ap, flag_period, arp_name_arr,
        RANK() OVER (PARTITION BY  dep_ap, flag_period
                ORDER BY dep DESC, arp_name_arr) RANK,
        ROW_NUMBER() OVER (PARTITION BY  dep_ap, flag_period
                ORDER BY dep DESC, arp_name_arr) R_RANK
FROM DATA_GRP
where flag_period = 'CURRENT_ROLLING_WEEK'
),

APT_CTRY_RANK_PREV as
(
SELECT
  dep_ap, flag_period, arp_name_arr,
        RANK() OVER (PARTITION BY  dep_ap, flag_period
                ORDER BY dep DESC, arp_name_arr) as RANK_PREV
FROM DATA_GRP
where  flag_period = 'PREV_ROLLING_WEEK'
)


 SELECT
   a.flag_period,
   a.dep_ap as arp_code_dep, a.arp_name_dep,
   a.arr_ap as arp_code_arr, a.arp_name_arr,
   a.dep,
   from_date,
   to_date,
     to_date(  TO_CHAR (", mydate, "-1, 'dd-mm-yyyy'),'dd-mm-yyyy') as last_data_day,
     r_rank, rank,  rank_prev

FROM DATA_GRP a
left join APT_CTRY_RANK b on a.arp_name_arr = b.arp_name_arr AND a.dep_ap = b.dep_ap
left join APT_CTRY_RANK_PREV c on a.arp_name_arr = c.arp_name_arr AND a.dep_ap = c.dep_ap
where r_rank<= '10'
order by a.dep_ap, flag_period, r_rank
  "
    )
  }


### y2d ----
query_ap_ap_des_data_week_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 (
  "
  with

DIM_AO
 as ( select  ao_code, ao_name, ao_nm_group_code, ao_nm_group_name  from  prudev.v_covid_dim_ao) ,

DIM_APT
 as ( select code as arp_code, dashboard_name as arp_name
 from  pru_airport a
 ),

LIST_APT as
(select * from pruprod.v_aiu_app_dim_airport ),


AIRP_FLIGHT as (

SELECT flt_uid,
       TRUNC (flt_a_asp_prof_time_entry) AS entry_day,
       a.flt_dep_ad dep_ap ,a.flt_ctfm_ades arr_ap,
       A.ao_icao_id

  FROM v_aiu_flt a
 WHERE
     A.flt_lobt>= TO_DATE ('01-01-2019', 'dd-mm-yyyy') -1  AND A.flt_lobt < ", mydate, "
     AND a.flt_a_asp_prof_time_entry >= TO_DATE ('01-01-2019', 'dd-mm-yyyy')
     AND TO_NUMBER (TO_CHAR (TRUNC (flt_a_asp_prof_time_entry), 'mmdd')) <=   TO_NUMBER (TO_CHAR (", mydate, "-1, 'mmdd'))
       and extract (year from flt_a_asp_prof_time_entry) <= extract(year from (", mydate, "-1))
       AND A.flt_state IN ('TE', 'TA', 'AA')
       AND  flt_dep_ad IS NOT NULL
       AND    flt_ctfm_ades  IS NOT NULL
     ),



 AP_PAIR_SELECTION as
 (SELECT  a.dep_ap, a.arr_ap, a.entry_day, a.flt_uid,a.ao_icao_id, b.arp_name as arp_name_dep
  FROM  AIRP_FLIGHT A , DIM_APT b
  WHERE  b.arp_code = a.dep_ap
        and dep_ap in (select arp_code  from LIST_APT))
,


AP_PAIR_AREA as (
SELECT entry_day,
       flt_uid,
        a.dep_ap, a.arp_name_dep,
        a.arr_ap,  b.arp_name as arp_name_arr
 FROM  AP_PAIR_SELECTION A , DIM_APT b
  WHERE  b.arp_code = a.arr_ap),


AO_AIRPORT_FLIGHT as
(
SELECT entry_day,
       flt_uid,
       dep_ap, arp_name_dep,
       arr_ap, arp_name_arr,
       extract (year from entry_day) as year

FROM  AP_PAIR_AREA a
  ),

DATA_GRP as
(
SELECT
   year,
  min(entry_day) as from_date,
   max(entry_day) as to_date,
   dep_ap, arp_name_dep,
       arr_ap, arp_name_arr,
   count(flt_uid) as dep

FROM AO_AIRPORT_FLIGHT a
group by year, dep_ap, arp_name_dep, arr_ap, arp_name_arr
),

APT_CTRY_RANK as
(
SELECT
  dep_ap, year, arp_name_arr,
        RANK() OVER (PARTITION BY  dep_ap, year
                ORDER BY dep DESC, arp_name_arr) RANK,
        ROW_NUMBER() OVER (PARTITION BY  dep_ap, year
                ORDER BY dep DESC, arp_name_arr) R_RANK
FROM DATA_GRP
where year = extract (year from (", mydate, "-1))
),

APT_CTRY_RANK_PREV as
(
SELECT
  dep_ap, year, arp_name_arr,
        RANK() OVER (PARTITION BY  dep_ap, year
                ORDER BY dep DESC, arp_name_arr) as RANK_PREV
FROM DATA_GRP
where  year = extract (year from (", mydate, "-1))-1
)


 SELECT
   a.year,
   a.dep_ap as arp_code_dep, a.arp_name_dep,
   a.arr_ap as arp_code_arr, a.arp_name_arr,
   a.dep,
   from_date,
   to_date,
     to_date(  TO_CHAR (", mydate, "-1, 'dd-mm-yyyy'),'dd-mm-yyyy') as last_data_day,
     r_rank, rank,  rank_prev

FROM DATA_GRP a
left join APT_CTRY_RANK b on a.arp_name_arr = b.arp_name_arr AND a.dep_ap = b.dep_ap
left join APT_CTRY_RANK_PREV c on a.arp_name_arr = c.arp_name_arr AND a.dep_ap = c.dep_ap
where r_rank<= '10'
order by a.dep_ap, year desc, r_rank
  "
    )
  }

# apt market segment ----
### day ----
query_ap_ms_data_day_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 (
    "
    WITH

dim_market_segment as

(select SK_FLT_TYPE_RULE_ID,
 rule_description as market_segment
 from  SWH_FCT.DIM_FLIGHT_TYPE_RULE
where SK_FLT_TYPE_RULE_ID <> 5
 ),

DIM_APT as
(select * from pruprod.v_aiu_app_dim_airport )


, DATA_SOURCE as (
SELECT
        A.flt_dep_ad as dep_ARP_CODE
        ,A.flt_ctfm_ades as arr_ARP_CODE,
        b.arp_name  as dep_arp_name,
        c.arp_name as arr_arp_name,
        TRUNC(A.flt_a_asp_prof_time_entry) ENTRY_DATE,
        coalesce(d.market_segment,'Other Types') market_segment,
        A.flt_uid
FROM v_aiu_flt_mark_seg A
     left outer  join DIM_APT b ON  ( A.flt_dep_ad=  b.arp_code)
     left outer join DIM_APT C  ON  (A.flt_ctfm_ades = C.arp_code)
     left outer join dim_market_segment   d  ON (a.SK_FLT_TYPE_RULE_ID = d.SK_FLT_TYPE_RULE_ID )
WHERE
     (  (  A.flt_lobt >=  ", mydate, " - 1 -2
    AND A.flt_lobt <  ", mydate, " + 2
    AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -  1
    AND A.flt_a_asp_prof_time_entry <  ", mydate, " )
 OR
 (  A.flt_lobt >=  ", mydate, "-1 - 2 -  ((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
    AND A.flt_lobt <   ", mydate, " +2 - ((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
    AND A.flt_a_asp_prof_time_entry >=   ", mydate, " - 1 - ((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
    AND A.flt_a_asp_prof_time_entry <   ", mydate, " - ((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
 )
OR
 (  A.flt_lobt >=  ", mydate, " -364 -1 -2
    AND A.flt_lobt <   ", mydate, " - 364 + 2
    AND A.flt_a_asp_prof_time_entry >=   ", mydate, " - 364 -  1
    AND A.flt_a_asp_prof_time_entry <   ", mydate, " - 364
  )
  OR
 (  A.flt_lobt >=  ", mydate, " -7 -1 -2
    AND A.flt_lobt <   ", mydate, " - 7 + 2
    AND A.flt_a_asp_prof_time_entry >=   ", mydate, " - 1 -  7
    AND A.flt_a_asp_prof_time_entry <   ", mydate, " - 7
  )

)
    AND A.flt_state IN ('TE','TA','AA')

),



DATA_DEP AS (
(SELECT
        dep_ARP_CODE as ARP_CODE,
        dep_arp_name as arp_name ,
        a.ENTRY_DATE,
        market_segment,
        COUNT(a.flt_uid) DEP_ARR
FROM DATA_SOURCE a
GROUP BY
        dep_ARP_CODE,
        dep_arp_name  ,
        ENTRY_DATE,
        market_segment
)
),

DATA_ARR AS (
SELECT
       arr_ARP_CODE as ARP_CODE,
        ARR_arp_name as arp_name ,
        ENTRY_DATE,
        market_segment,
        COUNT(a.flt_uid) DEP_ARR
FROM DATA_SOURCE a
WHERE ARR_arp_name is not null
GROUP BY  arr_ARP_CODE,
          ARR_arp_name  ,
          ENTRY_DATE,
        market_segment
),


DATA_DAY as
(SELECT
          coalesce(a.entry_date, b.entry_date) as entry_date,
          coalesce(a.ARP_CODE,b.ARP_CODE) as ARP_CODE,
          coalesce( a.arp_name,b.arp_name) as arp_name,
          coalesce(  a.market_segment,b.market_segment) as market_segment,
          coalesce(a.DEP_ARR,0)  + coalesce( b.DEP_ARR,0)  as DEP_ARR
FROM DATA_DEP a full outer join DATA_ARR b  on (a.ARP_CODE = b.ARP_CODE  and a.entry_date = b.entry_date and a.market_segment = b.market_segment)

),

DATA_PERIOD as
 (
 SELECT
      entry_date,
      case when market_segment = 'Military' then 'Other Types'
          else market_segment
      end market_segment,
      ARP_CODE,
      arp_name,
      case
                     when (a.entry_date >= ", mydate, "-1  AND a.entry_date < ", mydate, ") then 'CURRENT_DAY'
                     when  (a.entry_date >= ", mydate, " -1 - ((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
                           AND a.entry_date <  ", mydate, " - ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 )
                        then 'DAY_2019'
                     when  (a.entry_date >= ", mydate, " - 1 - 364    AND a.entry_date <  ", mydate, " - 364) then 'DAY_PREV_YEAR'
                     when   (a.entry_date >= ", mydate, " - 1 - 7    AND a.entry_date <  ", mydate, " - 7) then  'DAY_PREV_WEEK'
        end flag_period,
     DEP_ARR
  FROM DATA_DAY a

  ),

DATA_GRP as
(
SELECT
      a.flag_period
      ,max(entry_date) as to_date
      , a.market_segment
      ,a.ARP_CODE
      ,a.arp_name
     ,SUM (DEP_ARR)  DEP_ARR
   --  ,SUM (DEP_ARR) / 7 as avg_DEP_ARR

FROM DATA_PERIOD a
WHERE a.ARP_CODE in (select arp_code from DIM_APT)
GROUP BY a.flag_period
      , ARP_CODE
      , arp_name
       , a.market_segment
),

APT_AO_RANK as
(
SELECT
  ARP_CODE, flag_period, market_segment,
        ROW_NUMBER() OVER (PARTITION BY  ARP_CODE, flag_period
                ORDER BY DEP_ARR DESC, market_segment) R_RANK,
        RANK() OVER (PARTITION BY  ARP_CODE, flag_period
                ORDER BY DEP_ARR DESC, market_segment) RANK
FROM DATA_GRP
where flag_period = 'CURRENT_DAY'
),

APT_AO_RANK_PREV as
(
SELECT
  ARP_CODE, flag_period, market_segment,
        ROW_NUMBER() OVER (PARTITION BY  ARP_CODE, flag_period
                ORDER BY DEP_ARR DESC, market_segment) RANK_PREV
 FROM DATA_GRP
where flag_period = 'DAY_PREV_WEEK'
)

SELECT
      a.ARP_CODE
      ,a.arp_name
      , a.flag_period
      ,to_date
      ,", mydate, "-1 as last_data_day
      , a.market_segment
     ,DEP_ARR
     , R_RANK,rank, rank_prev

FROM DATA_GRP a
 left join APT_AO_RANK b on a.ARP_CODE = b.ARP_CODE AND a.market_segment = b.market_segment
 left join APT_AO_RANK_PREV c on a.ARP_CODE = c.ARP_CODE AND a.market_segment = c.market_segment
order by ARP_CODE, flag_period, R_RANK
"
  )}

### week ----
query_ap_ms_data_week_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 (
    "
    WITH

dim_market_segment as

(select SK_FLT_TYPE_RULE_ID,
 rule_description as market_segment
 from  SWH_FCT.DIM_FLIGHT_TYPE_RULE
where SK_FLT_TYPE_RULE_ID <> 5
 ),

DIM_APT as
(select * from pruprod.v_aiu_app_dim_airport )


, DATA_SOURCE as (
SELECT
        A.flt_dep_ad as dep_ARP_CODE
        ,A.flt_ctfm_ades as arr_ARP_CODE,
        b.arp_name  as dep_arp_name,
        c.arp_name as arr_arp_name,
        TRUNC(A.flt_a_asp_prof_time_entry) ENTRY_DATE,
        coalesce(d.market_segment,'Other Types') market_segment,
        A.flt_uid
FROM v_aiu_flt_mark_seg A
     left outer  join DIM_APT b ON  ( A.flt_dep_ad=  b.arp_code)
     left outer join DIM_APT C  ON  (A.flt_ctfm_ades = C.arp_code)
     left outer join dim_market_segment   d  ON (a.SK_FLT_TYPE_RULE_ID = d.SK_FLT_TYPE_RULE_ID )
 WHERE       (
                     (     A.flt_lobt >= ", mydate, " -7 -1
                        AND A.flt_lobt < ", mydate, "-0
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0
                        )
                        or
                     (     A.flt_lobt >= ", mydate, " -7 -1-7
                        AND A.flt_lobt < ", mydate, "-0-7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -7-7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0-7
                        )
                        or
                     (     A.flt_lobt >= ", mydate, " -7 -1-364
                        AND A.flt_lobt < ", mydate, "-0-364
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -7 -364
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0 -364
                        )
                        or
                        (     A.flt_lobt >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)-7 -1- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_lobt < ", mydate, "-((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)-7- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
                         )
                     )

    AND A.flt_state IN ('TE','TA','AA')

),



DATA_DEP AS (
(SELECT
        dep_ARP_CODE as ARP_CODE,
        dep_arp_name as arp_name ,
        a.ENTRY_DATE,
        market_segment,
        COUNT(a.flt_uid) DEP_ARR
FROM DATA_SOURCE a
GROUP BY
        dep_ARP_CODE,
        dep_arp_name  ,
        ENTRY_DATE,
        market_segment
)
),

DATA_ARR AS (
SELECT
       arr_ARP_CODE as ARP_CODE,
        ARR_arp_name as arp_name ,
        ENTRY_DATE,
        market_segment,
        COUNT(a.flt_uid) DEP_ARR
FROM DATA_SOURCE a
WHERE ARR_arp_name is not null
GROUP BY  arr_ARP_CODE,
          ARR_arp_name  ,
          ENTRY_DATE,
        market_segment
),


DATA_DAY as
(SELECT
          coalesce(a.entry_date, b.entry_date) as entry_date,
          coalesce(a.ARP_CODE,b.ARP_CODE) as ARP_CODE,
          coalesce( a.arp_name,b.arp_name) as arp_name,
          coalesce(  a.market_segment,b.market_segment) as market_segment,
          coalesce(a.DEP_ARR,0)  + coalesce( b.DEP_ARR,0)  as DEP_ARR
FROM DATA_DEP a full outer join DATA_ARR b  on (a.ARP_CODE = b.ARP_CODE  and a.entry_date = b.entry_date and a.market_segment = b.market_segment)

),

DATA_PERIOD as
 (
 SELECT
      entry_date,
      case when market_segment = 'Military' then 'Other Types'
          else market_segment
      end market_segment,
      ARP_CODE,
      arp_name,
   case
     when entry_date >= ", mydate, " -7 and entry_date < ", mydate, "  then 'CURRENT_ROLLING_WEEK'
     when entry_date >= ", mydate, " -7-7 and entry_date < ", mydate, "  then 'PREV_ROLLING_WEEK'
     when entry_date >= ", mydate, " -7-364 and entry_date < ", mydate, "  then 'ROLLING_WEEK_PREV_YEAR'
     when entry_date >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)-7- floor((extract (year from (", mydate, "-1))-2019)/4)*7
            and entry_date < ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
        then 'ROLLING_WEEK_2019'
     else '-'
   end  flag_period,
     DEP_ARR
  FROM DATA_DAY a

  ),

DATA_GRP as
(
SELECT
      a.flag_period
      , min(entry_date) as from_date
      , max(entry_date) as to_date
      , a.market_segment
      ,a.ARP_CODE
      ,a.arp_name
     ,SUM (DEP_ARR) as DEP_ARR
   --  ,SUM (DEP_ARR) / 7 as avg_DEP_ARR

FROM DATA_PERIOD a
WHERE a.ARP_CODE in (select arp_code from DIM_APT)
GROUP BY a.flag_period
      , ARP_CODE
      , arp_name
       , a.market_segment
),

APT_AO_RANK as
(
SELECT
  ARP_CODE, flag_period, market_segment,
        ROW_NUMBER() OVER (PARTITION BY  ARP_CODE, flag_period
                ORDER BY DEP_ARR DESC, market_segment) R_RANK,
        RANK() OVER (PARTITION BY  ARP_CODE, flag_period
                ORDER BY DEP_ARR DESC, market_segment) RANK
FROM DATA_GRP
where flag_period = 'CURRENT_ROLLING_WEEK'
),

APT_AO_RANK_PREV as
(
SELECT
  ARP_CODE, flag_period, market_segment,
        ROW_NUMBER() OVER (PARTITION BY  ARP_CODE, flag_period
                ORDER BY DEP_ARR DESC, market_segment) RANK_PREV
 FROM DATA_GRP
where flag_period = 'PREV_ROLLING_WEEK'
)

SELECT
      a.ARP_CODE
      ,a.arp_name
      , a.flag_period
      , a.from_date
      , a.to_date
      ,", mydate, "-1 as last_data_day
      , a.market_segment
     ,DEP_ARR
     , R_RANK,rank, rank_prev

FROM DATA_GRP a
 left join APT_AO_RANK b on a.ARP_CODE = b.ARP_CODE AND a.market_segment = b.market_segment
 left join APT_AO_RANK_PREV c on a.ARP_CODE = c.ARP_CODE AND a.market_segment = c.market_segment
order by ARP_CODE, flag_period, R_RANK
"
)}

### y2d ----
query_ap_ms_data_y2d_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 (
    "
    WITH

dim_market_segment as

(select SK_FLT_TYPE_RULE_ID,
 rule_description as market_segment
 from  SWH_FCT.DIM_FLIGHT_TYPE_RULE
where SK_FLT_TYPE_RULE_ID <> 5
 ),

DIM_APT as
(select * from pruprod.v_aiu_app_dim_airport ),


, DATA_SOURCE as (
SELECT
        A.flt_dep_ad as dep_ARP_CODE
        ,A.flt_ctfm_ades as arr_ARP_CODE,
        b.arp_name  as dep_arp_name,
        c.arp_name as arr_arp_name,
        TRUNC(A.flt_a_asp_prof_time_entry) ENTRY_DATE,
        coalesce(d.market_segment,'Other Types') market_segment,
        A.flt_uid
FROM v_aiu_flt_mark_seg A
     left outer  join DIM_APT b ON  ( A.flt_dep_ad=  b.arp_code)
     left outer join DIM_APT C  ON  (A.flt_ctfm_ades = C.arp_code)
     left outer join dim_market_segment   d  ON (a.SK_FLT_TYPE_RULE_ID = d.SK_FLT_TYPE_RULE_ID )
 WHERE
         A.flt_lobt>= TO_DATE ('01-01-2019', 'dd-mm-yyyy') -1  AND A.flt_lobt < ", mydate, "
     AND a.flt_a_asp_prof_time_entry >= TO_DATE ('01-01-2019', 'dd-mm-yyyy')
     AND TO_NUMBER (TO_CHAR (TRUNC (flt_a_asp_prof_time_entry), 'mmdd')) <=   TO_NUMBER (TO_CHAR (", mydate, "-1, 'mmdd'))
       and extract (year from flt_a_asp_prof_time_entry) <= extract(year from (", mydate, "-1))

    AND A.flt_state IN ('TE','TA','AA')

),



DATA_DEP AS (
(SELECT
        dep_ARP_CODE as ARP_CODE,
        dep_arp_name as arp_name ,
        a.ENTRY_DATE,
        market_segment,
        COUNT(a.flt_uid) DEP_ARR
FROM DATA_SOURCE a
GROUP BY
        dep_ARP_CODE,
        dep_arp_name  ,
        ENTRY_DATE,
        market_segment
)
),

DATA_ARR AS (
SELECT
       arr_ARP_CODE as ARP_CODE,
        ARR_arp_name as arp_name ,
        ENTRY_DATE,
        market_segment,
        COUNT(a.flt_uid) DEP_ARR
FROM DATA_SOURCE a
WHERE ARR_arp_name is not null
GROUP BY  arr_ARP_CODE,
          ARR_arp_name  ,
          ENTRY_DATE,
        market_segment
),


DATA_DAY as
(SELECT
          coalesce(a.entry_date, b.entry_date) as entry_date,
          coalesce(a.ARP_CODE,b.ARP_CODE) as ARP_CODE,
          coalesce( a.arp_name,b.arp_name) as arp_name,
          coalesce(  a.market_segment,b.market_segment) as market_segment,
          coalesce(a.DEP_ARR,0)  + coalesce( b.DEP_ARR,0)  as DEP_ARR
FROM DATA_DEP a full outer join DATA_ARR b  on (a.ARP_CODE = b.ARP_CODE  and a.entry_date = b.entry_date and a.market_segment = b.market_segment)

),

DATA_PERIOD as
 (
 SELECT
      entry_date,
      case when market_segment = 'Military' then 'Other Types'
          else market_segment
      end market_segment,
      ARP_CODE,
      arp_name,
     extract (year from entry_date) as year,
     DEP_ARR
  FROM DATA_DAY a

  ),

DATA_GRP as
(
SELECT
      a.year
      , min(entry_date) as from_date
      , max(entry_date) as to_date
      , a.market_segment
      ,a.ARP_CODE
      ,a.arp_name
     ,SUM (DEP_ARR) as DEP_ARR
   --  ,SUM (DEP_ARR) / 7 as avg_DEP_ARR

FROM DATA_PERIOD a
WHERE a.ARP_CODE in (select arp_code from DIM_APT)
GROUP BY a.year
      , ARP_CODE
      , arp_name
       , a.market_segment
),

APT_AO_RANK as
(
SELECT
  ARP_CODE, year, market_segment,
        ROW_NUMBER() OVER (PARTITION BY  ARP_CODE, year
                ORDER BY DEP_ARR DESC, market_segment) R_RANK,
        RANK() OVER (PARTITION BY  ARP_CODE, year
                ORDER BY DEP_ARR DESC, market_segment) RANK
FROM DATA_GRP
where year = extract (year from (", mydate, "-1))
),

APT_AO_RANK_PREV as
(
SELECT
  ARP_CODE, year, market_segment,
        ROW_NUMBER() OVER (PARTITION BY  ARP_CODE, year
                ORDER BY DEP_ARR DESC, market_segment) RANK_PREV
 FROM DATA_GRP
where year = extract (year from (", mydate, "-1))-1
)

SELECT
      a.ARP_CODE
      ,a.arp_name
      , a.year
      , a.from_date
      , a.to_date
      ,", mydate, "-1 as last_data_day
      , a.market_segment
     ,DEP_ARR
     , R_RANK,rank, rank_prev

FROM DATA_GRP a
 left join APT_AO_RANK b on a.ARP_CODE = b.ARP_CODE AND a.market_segment = b.market_segment
 left join APT_AO_RANK_PREV c on a.ARP_CODE = c.ARP_CODE AND a.market_segment = c.market_segment
order by ARP_CODE, year desc, R_RANK
    "
    )}
