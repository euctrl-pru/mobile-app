# STATE ----
## st_daio ----
st_daio_day_query <- "
WITH

 DATA_DAY
    AS
 (SELECT     agg_asp_entry_date as flight_date,
             agg_asp_id COUNTRY_CODE,
             agg_asp_ty as TYPE,
             CASE WHEN agg_asp_id = 'LQ' then 'Bosnia and Herzegovina'
                  WHEN agg_asp_id = 'LY' then 'Serbia/Montenegro'
                ELSE agg_asp_name
             END COUNTRY_NAME,
             SUM (coalesce(a.agg_asp_a_traffic_asp,0)) as DAY_TFC,
             SUM(coalesce(a.agg_asp_delay_tvs,0)) as delay,
             (SUM (coalesce(a.agg_asp_delay_tvs,0)) - SUM (coalesce(a.agg_asp_delay_airport_tvs,0)))  AS ERT_DELAY,
             SUM (coalesce(a.agg_asp_delay_airport_tvs,0))  AS ARP_DELAY,
             SUM (coalesce(a.agg_asp_delayed_traffic_tvs,0)) as DELAY_FLIGHT,
             (SUM (coalesce(a.agg_asp_delayed_traffic_tvs,0))  - SUM (coalesce(  a.agg_asp_delayed_traffic_ad_tvs,0))) as ERT_DELAY_FLIGHT,
              SUM (coalesce(a.agg_asp_delayed_traffic_ad_tvs,0))  AS ARP_DELAY_FLIGHT
       FROM prudev.v_aiu_agg_asp a
       WHERE
             agg_asp_entry_date >= '24-DEC-2018' AND a.AGG_ASP_ENTRY_DATE < trunc(sysdate)
             and agg_asp_ty = 'COUNTRY_AUA'  AND A.agg_asp_unit_ty <> 'REGION'
             AND (SUBSTR(a.agg_asp_id,1,1) IN ('E','L')
         OR SUBSTR(a.agg_asp_id,1,2) IN ('GC','GM','GE','UD','UG','UK','YY', 'BI'))
    GROUP BY
             agg_asp_entry_date,
             agg_asp_id,
             agg_asp_ty,
             agg_asp_name
--    ORDER BY agg_asp_name,agg_asp_entry_date
  ),

  LIST_COUNTRY as
  (select distinct country_name from DATA_DAY
  ),



 CTRY_DAY AS (
SELECT a.COUNTRY_NAME,
        t.year,
        t.month,
        t.week,
        t.week_nb_year,
        t.day_type,
        t.day_of_week_nb AS day_of_week,
        t.day_date
FROM LIST_COUNTRY a, prudev.pru_time_references t
WHERE
   t.day_date >= to_date('24-12-2018','DD-MM-YYYY')
   AND t.day_date <= to_date('31-12-'|| extract(year from (trunc(sysdate)-1)),'dd-mm-yyyy')
       ),


DATA_COUNTRY as
(SELECT
          a.country_name,
         a.YEAR,
         a.MONTH,
          a.WEEK,
          a.WEEK_NB_YEAR,
          a.day_type,
          a.day_of_week,
          a.day_date as flight_date,
          coalesce(b.DAY_TFC,0) as DAY_TFC
FROM CTRY_DAY A
LEFT JOIN DATA_DAY b on a.COUNTRY_NAME = B.COUNTRY_NAME and a.day_date = b.flight_date

),

DATA_COUNTRY_Y2D as
(select
        flight_date ,
        country_name,
       SUM (DAY_TFC) OVER (PARTITION BY country_name ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) Y2D_TFC_YEAR,
       SUM (DAY_TFC) OVER (PARTITION BY country_name ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW)/
                Count (DAY_TFC) OVER (PARTITION BY country_name ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) Y2D_AVG_TFC_YEAR

FROM DATA_COUNTRY
),

DATA_COUNTRY_2 as
(select
       a.country_name,
       YEAR,
       MONTH,
       WEEK,
       WEEK_NB_YEAR,
       DAY_TYPE,
       day_of_week,
       a.flight_DATE,
       DAY_TFC,

       LAG (DAY_TFC, 7) OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE)  DAY_TFC_PREV_WEEK,
       LAG (a.flight_DATE, 7) OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE) flight_DATE_PREV_WEEK,

       LAG (DAY_TFC, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE)  DAY_TFC_PREV_YEAR,
       LAG (a.flight_DATE, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE) flight_DATE_PREV_YEAR,
       LAG (DAY_TFC,  greatest((extract (year from a.flight_DATE)-2019) *364+ floor((extract (year from a.flight_DATE)-2019)/4)*7,0))
                OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE)  DAY_TFC_2019,
       LAG (a.flight_DATE,  greatest((extract (year from a.flight_DATE)-2019) *364+ floor((extract (year from a.flight_DATE)-2019)/4)*7,0))
                OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE) flight_DATE_2019,
--       LAG (DAY_TFC,  greatest((extract (year from a.flight_DATE)-2020) *364+ floor((extract (year from a.flight_DATE)-2020)/4)*7,0))
--                OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE)  DAY_TFC_2020,
       LAG (a.flight_DATE,  greatest((extract (year from a.flight_DATE)-2020) *364+ floor((extract (year from a.flight_DATE)-2020)/4)*7,0))
                OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE) flight_DATE_2020,

       AVG (DAY_TFC)  OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_rolling_week,
       AVG (DAY_TFC)  OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) AS AVG_rolling_PREV_WEEK,

       b.Y2D_TFC_YEAR,
       c.Y2D_TFC_YEAR as Y2D_TFC_PREV_YEAR,
       d.Y2D_TFC_YEAR as Y2D_TFC_2019,
       b.Y2D_AVG_TFC_YEAR,
       c.Y2D_AVG_TFC_YEAR as Y2D_AVG_TFC_PREV_YEAR,
       d.Y2D_AVG_TFC_YEAR as Y2D_AVG_TFC_2019

 --      , AVG (DAY_TFC)  OVER (PARTITION BY country_name,week_nb_year,week  ORDER BY week)AS avg_week
      FROM DATA_COUNTRY a
      left join DATA_COUNTRY_Y2D b on a.flight_DATE = b.flight_DATE and a.country_name = b.country_name
      left join DATA_COUNTRY_Y2D c on add_months(a.flight_DATE,-12) = c.flight_DATE and a.country_name = c.country_name
      left join DATA_COUNTRY_Y2D d on add_months(a.flight_DATE,-12*(extract (year from a.flight_DATE)-2019)) = d.flight_DATE and a.country_name = d.country_name

)  ,

  DATA_COUNTRY_3  as
  (
      select
       country_name,
        YEAR,
       MONTH,
       flight_DATE,

       DAY_TFC,
       WEEK,
       WEEK_NB_YEAR,
       DAY_TYPE,
       day_of_week,
       flight_DATE_PREV_WEEK,
       DAY_TFC_PREV_WEEK,
       DAY_TFC_PREV_YEAR,
       flight_DATE_PREV_YEAR,
       DAY_TFC_2019,
       flight_DATE_2019,
        flight_DATE_2020,
       avg_rolling_week,
       AVG_rolling_PREV_WEEK,

       LAG (AVG_rolling_week,364) OVER (PARTITION BY country_name ORDER BY flight_DATE) as AVG_rolling_week_PREV_YEAR,
       LAG (AVG_rolling_week, greatest((extract (year from flight_DATE)-2020) *364+ floor((extract (year from flight_DATE)-2020)/4)*7,0)  ) OVER (PARTITION BY country_name ORDER BY flight_DATE) as AVG_rolling_week_2020,
       LAG (AVG_rolling_week, greatest((extract (year from flight_DATE)-2019) *364+ floor((extract (year from flight_DATE)-2019)/4)*7,0)  ) OVER (PARTITION BY country_name ORDER BY flight_DATE) as AVG_rolling_week_2019,

       Y2D_TFC_YEAR,
       Y2D_TFC_PREV_YEAR,
       Y2D_TFC_2019,
       Y2D_AVG_TFC_YEAR,
       Y2D_AVG_TFC_PREV_YEAR,
       Y2D_AVG_TFC_2019

      FROM DATA_COUNTRY_2
  )


  select
       country_name,
       YEAR,
       MONTH,
       WEEK,
       WEEK_NB_YEAR,
       DAY_TYPE,
       day_of_week,

       flight_DATE,
       flight_DATE_PREV_WEEK,
       flight_DATE_PREV_YEAR,
       flight_DATE_2020,
       flight_DATE_2019,

       DAY_TFC,
       DAY_TFC_PREV_WEEK,
       DAY_TFC_PREV_YEAR,
       DAY_TFC_2019,

       CASE WHEN flight_DATE >= trunc(sysdate) THEN NULL
           ELSE DAY_TFC - DAY_TFC_PREV_WEEK
       END DAY_TFC_DIFF_PREV_WEEK,
       CASE WHEN flight_DATE >= trunc(sysdate) THEN NULL
           ELSE DAY_TFC - DAY_TFC_PREV_YEAR
       END DAY_TFC_DIFF_PREV_YEAR,
       CASE WHEN flight_DATE >= trunc(sysdate) THEN NULL
           ELSE DAY_TFC - DAY_TFC_2019
       END DAY_TFC_DIFF_2019,

       CASE WHEN DAY_TFC_PREV_WEEK  <>0  then
            DAY_TFC/DAY_TFC_PREV_WEEK -1
            ELSE NULL
       END as DAY_TFC_PREV_WEEK_perc,
       CASE WHEN DAY_TFC_PREV_YEAR <>0
           THEN DAY_TFC/DAY_TFC_PREV_YEAR -1
       	   ELSE NULL
       END  DAY_DIFF_PREV_YEAR_PERC,
       CASE WHEN DAY_TFC_2019 <>0
           THEN DAY_TFC/DAY_TFC_2019 -1
       	   ELSE NULL
       END  DAY_TFC_DIFF_2019_PERC,

       CASE WHEN flight_DATE >= trunc(sysdate)
           THEN NULL
           ELSE AVG_rolling_week
       END AVG_rolling_week,
       AVG_rolling_PREV_WEEK,
       AVG_rolling_week_PREV_YEAR,
       AVG_rolling_week_2020,
       AVG_rolling_week_2019,

      CASE WHEN AVG_rolling_week_PREV_YEAR <> 0 and flight_DATE < trunc(sysdate)
           THEN avg_rolling_week/AVG_rolling_week_PREV_YEAR -1
           ELSE NULL
       END  DIF_WEEK_PREV_YEAR_PERC,
      CASE WHEN AVG_rolling_week_2019 <> 0 and flight_DATE < trunc(sysdate)
           THEN avg_rolling_week/AVG_rolling_week_2019 -1
           ELSE NULL
       END  DIF_ROLLING_WEEK_2019_perc,

       Y2D_TFC_YEAR,
       Y2D_TFC_PREV_YEAR,
       Y2D_TFC_2019,
       CASE WHEN flight_DATE >= trunc(sysdate)
           THEN NULL
      	   ELSE Y2D_AVG_TFC_YEAR
       END Y2D_AVG_TFC_YEAR,
       Y2D_AVG_TFC_PREV_YEAR,
       Y2D_AVG_TFC_2019,

       CASE WHEN Y2D_AVG_TFC_PREV_YEAR <> 0 THEN
        Y2D_AVG_TFC_YEAR/Y2D_AVG_TFC_PREV_YEAR - 1
        ELSE NULL
       END Y2D_DIFF_PREV_YEAR_PERC,
       CASE WHEN Y2D_AVG_TFC_2019 <> 0 THEN
           Y2D_AVG_TFC_YEAR/Y2D_AVG_TFC_2019 - 1
           ELSE NULL
       END Y2D_DIFF_2019_PERC,
        trunc(sysdate) -1 as LAST_DATA_DAY

      FROM DATA_COUNTRY_3
      where flight_DATE >=to_date('01-01-2024','dd-mm-yyyy')
      AND country_name not in ('ICELAND', 'Iceland')

UNION ALL

  select
       country_name,
       YEAR,
       MONTH,
       WEEK,
       WEEK_NB_YEAR,
       DAY_TYPE,
       day_of_week,

       flight_DATE,
       flight_DATE_PREV_WEEK,
       flight_DATE_PREV_YEAR,
       flight_DATE_2020,
       flight_DATE_2019,

       case when FLIGHT_DATE >='01-jan-2024' then DAY_TFC else NULL end DAY_TFC,
       case when FLIGHT_DATE >='01-jan-2024' then DAY_TFC_PREV_WEEK else NULL end DAY_TFC_PREV_WEEK,
       case when FLIGHT_DATE >='01-jan-2025' then DAY_TFC_PREV_YEAR else NULL end DAY_TFC_PREV_YEAR,
       NULL as DAY_TFC_2019,

       CASE WHEN flight_DATE >= trunc(sysdate) THEN NULL
           ELSE case when FLIGHT_DATE >='01-jan-2024' then DAY_TFC - DAY_TFC_PREV_WEEK
                     else NULL
                end
       END DAY_TFC_DIFF_PREV_WEEK,
       CASE WHEN flight_DATE >= trunc(sysdate) THEN NULL
           ELSE case when FLIGHT_DATE >='01-jan-2025' then DAY_TFC - DAY_TFC_PREV_YEAR
                     else NULL
                end
       END DAY_TFC_DIFF_PREV_YEAR,
       NULL AS DAY_TFC_DIFF_2019,

       CASE WHEN DAY_TFC_PREV_WEEK  <>0 AND FLIGHT_DATE >='01-jan-2024' then
            DAY_TFC/DAY_TFC_PREV_WEEK -1
            ELSE NULL
       END as DAY_TFC_PREV_WEEK_perc,
       CASE WHEN DAY_TFC_PREV_YEAR <>0 AND FLIGHT_DATE >='01-jan-2025'
           THEN DAY_TFC/DAY_TFC_PREV_YEAR -1
       	   ELSE NULL
       END  DAY_DIFF_PREV_YEAR_PERC,
       NULL as DAY_TFC_DIFF_2019_PERC,

       CASE WHEN flight_DATE >= trunc(sysdate) THEN NULL
           ELSE case when FLIGHT_DATE >='01-jan-2024' then AVG_rolling_week
                     else NULL
                end
       END AVG_rolling_week,

       case when FLIGHT_DATE >='01-jan-2024' then AVG_rolling_PREV_WEEK else NULL end AVG_rolling_PREV_WEEK,
       case when FLIGHT_DATE >='01-jan-2025' then AVG_rolling_week_PREV_YEAR else NULL end AVG_rolling_week_PREV_YEAR,
       NULL as AVG_rolling_week_2020,
       NULL as AVG_rolling_week_2019,

       CASE WHEN AVG_rolling_week_PREV_YEAR <> 0 and flight_DATE < trunc(sysdate) AND FLIGHT_DATE >='01-jan-2025'
           THEN avg_rolling_week/AVG_rolling_week_PREV_YEAR -1
           ELSE NULL
       END  DIF_WEEK_PREV_YEAR_PERC,
       NULL as  DIF_ROLLING_WEEK_2019_perc,

       case when FLIGHT_DATE >='01-jan-2024' then Y2D_TFC_YEAR else NULL end Y2D_TFC_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then Y2D_TFC_PREV_YEAR else NULL end Y2D_TFC_PREV_YEAR,
       NULL as Y2D_TFC_2019,
       CASE WHEN flight_DATE >= trunc(sysdate)
           THEN NULL
      	   ELSE (case when FLIGHT_DATE >='01-jan-2024' then Y2D_AVG_TFC_YEAR else NULL END)
       END Y2D_AVG_TFC_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then Y2D_AVG_TFC_PREV_YEAR else NULL end Y2D_AVG_TFC_PREV_YEAR,
       NULL as Y2D_AVG_TFC_2019,

       CASE WHEN Y2D_AVG_TFC_PREV_YEAR <> 0 AND FLIGHT_DATE >='01-jan-2025' THEN
        Y2D_AVG_TFC_YEAR/Y2D_AVG_TFC_PREV_YEAR - 1
        ELSE NULL
       END Y2D_DIFF_PREV_YEAR_PERC,
       NULL as Y2D_DIFF_2019_PERC,

       trunc(sysdate) -1 as LAST_DATA_DAY

      FROM DATA_COUNTRY_3
      where flight_DATE >=to_date('01-01-2024','dd-mm-yyyy')
      AND country_name in ('ICELAND', 'Iceland')
      order by country_name, flight_date
      "

## st_dai ----
st_dai_day_base_query <- paste0("
WITH

COUNTRY_ICAO2LETTER  as (
 select distinct
       ec_icao_country_code  ICAO2LETTER,
       CASE
             WHEN ec_icao_country_code = 'GE' then 'LE'
             WHEN ec_icao_country_code = 'ET' then 'ED'
             ELSE ec_icao_country_code
        END  COUNTRY_code    
  from SWH_FCT.dim_icao_country a
  WHERE Valid_to > trunc(sysdate) - 1
  AND  (  (SUBSTR(ec_icao_country_code,1,1) IN ('E','L')
       OR SUBSTR(ec_icao_country_code,1,2) IN ('GC','GM','GE','UD','UG','UK','BI'))  )
  AND  ec_icao_country_code not in ('LV', 'LX', 'EU','LN')
  ORDER BY COUNTRY_code
 ) ,

LIST_COUNTRY as (
select  COUNTRY_code
FROM COUNTRY_ICAO2LETTER
group by  COUNTRY_code),

 CTRY_DAY AS (
SELECT a.COUNTRY_code,
        t.day_date,
        t.month,
        t.week,
        t.week_nb_year,
        t.day_type,
        t.day_of_week_nb AS day_of_week,
        t.year
FROM LIST_COUNTRY a, prudev.pru_time_references t
WHERE
   t.day_date >= ", query_from, "
  	AND t.DAY_date < trunc(sysdate)
       ),


DATA_DEP AS (
(SELECT
        B.COUNTRY_code ,
        TRUNC(A.flt_a_asp_prof_time_entry) flight_DATE,
        COUNT(a.flt_uid) DAY_TFC
FROM prudev.v_aiu_flt a,
     COUNTRY_ICAO2LETTER b
WHERE  SUBSTR(A.flt_dep_ad,1,2) =  b.ICAO2LETTER
    AND A.flt_lobt >= ", query_from, " -2
    AND A.flt_lobt <  trunc(sysdate) + 2
    AND A.flt_a_asp_prof_time_entry >= ", query_from, "
    AND A.flt_a_asp_prof_time_entry <  trunc(sysdate)
    AND A.flt_state IN ('TE','TA','AA')
GROUP BY  B.COUNTRY_code  ,
        TRUNC(A.flt_a_asp_prof_time_entry)
)
),

DATA_ARR AS (
SELECT
        C.COUNTRY_code ,
        TRUNC(A.flt_a_asp_prof_time_entry) flight_DATE,
        COUNT(a.flt_uid) DAY_TFC
FROM prudev.v_aiu_flt a,
     COUNTRY_ICAO2LETTER c
WHERE
     SUBSTR(A.flt_ctfm_ades,1,2) = C.ICAO2LETTER
    AND A.flt_lobt >= ", query_from, " -2
    AND A.flt_lobt <  trunc(sysdate) + 2
    AND A.flt_a_asp_prof_time_entry >= ", query_from, "
    AND A.flt_a_asp_prof_time_entry <  trunc(sysdate)
    AND A.flt_state IN ('TE','TA','AA')
GROUP BY  C.COUNTRY_code  ,
        TRUNC(A.flt_a_asp_prof_time_entry)
),

DATA_DOMESTIC as
(SELECT
        B.COUNTRY_code ,
        TRUNC(A.flt_a_asp_prof_time_entry) FLIGHT_DATE,
        COUNT(a.flt_uid) DAY_TFC
FROM prudev.v_aiu_flt a,
     COUNTRY_ICAO2LETTER b,
     COUNTRY_ICAO2LETTER c
WHERE  SUBSTR(A.flt_dep_ad,1,2) =  b.ICAO2LETTER   AND
       SUBSTR(A.flt_ctfm_ades,1,2) = C.ICAO2LETTER
    AND  B.COUNTRY_code =C.COUNTRY_code
    AND A.flt_lobt >= ", query_from, " -2
    AND A.flt_lobt <  trunc(sysdate) + 2
    AND A.flt_a_asp_prof_time_entry >= ", query_from, "
    AND A.flt_a_asp_prof_time_entry <  trunc(sysdate)
    AND A.flt_state IN ('TE','TA','AA')
GROUP BY  B.COUNTRY_code  ,
        TRUNC(A.flt_a_asp_prof_time_entry)
)

SELECT
         a.YEAR,
         a.MONTH,
          a.WEEK,
          a.WEEK_NB_YEAR,
          a.day_type,
          a.day_of_week,
          a.day_date as flight_date,
          a.COUNTRY_code,
          coalesce(b.DAY_TFC,0) as DEP,
          coalesce( c.DAY_TFC,0) as ARR ,
          coalesce( d.DAY_TFC,0) as DOM ,
         coalesce(b.DAY_TFC,0)  + coalesce( c.DAY_TFC,0) - coalesce( d.DAY_TFC,0) as DAY_TFC
FROM CTRY_DAY A
LEFT JOIN DATA_DEP b on a.COUNTRY_code = B.COUNTRY_code and a.day_date = b.FLIGHT_date
LEFT JOIN DATA_ARR c on a.COUNTRY_code = c.COUNTRY_code and a.day_date = c.FLIGHT_date
LEFT JOIN DATA_DOMESTIC d on a.COUNTRY_code = d.COUNTRY_code and a.day_date = d.FLIGHT_date
"
)

## st_delay ====
st_delay_day_query <- "
WITH

 DATA_DAY
    AS
 (SELECT     agg_asp_entry_date as flight_date,
             agg_asp_id COUNTRY_CODE,
             agg_asp_ty as TYPE,
             CASE WHEN agg_asp_id = 'LQ' then 'Bosnia and Herzegovina'
                  WHEN agg_asp_id = 'LY' then 'Serbia/Montenegro'
                ELSE agg_asp_name
             END COUNTRY_NAME,
             SUM (coalesce(a.agg_asp_a_traffic_asp,0)) as DAY_TFC,
             SUM(coalesce(a.agg_asp_delay_tvs,0)) as DAY_DLY,
             (SUM (coalesce(a.agg_asp_delay_tvs,0)) - SUM (coalesce(a.agg_asp_delay_airport_tvs,0)))  AS DAY_ERT_DLY,
             SUM (coalesce(a.agg_asp_delay_airport_tvs,0))  AS DAY_ARP_DLY
       FROM prudev.v_aiu_agg_asp a
       WHERE
             agg_asp_entry_date >= '24-DEC-2018' AND a.AGG_ASP_ENTRY_DATE < trunc(sysdate)
             and agg_asp_ty = 'COUNTRY_AUA'  AND A.agg_asp_unit_ty <> 'REGION'
             AND (SUBSTR(a.agg_asp_id,1,1) IN ('E','L')
         OR SUBSTR(a.agg_asp_id,1,2) IN ('GC','GM','GE','UD','UG','UK','YY','BI'))
    GROUP BY
             agg_asp_entry_date,
             agg_asp_id,
             agg_asp_ty,
             agg_asp_name
--    ORDER BY agg_asp_name,agg_asp_entry_date
  ),

  LIST_COUNTRY as
  (select distinct country_name from DATA_DAY
  ),


 CTRY_DAY AS (
SELECT a.COUNTRY_NAME,
        t.year,
        t.month,
        t.week,
        t.week_nb_year,
        t.day_type,
        t.day_of_week_nb AS day_of_week,
        t.day_date
FROM LIST_COUNTRY a, prudev.pru_time_references t
WHERE
   t.day_date >= to_date('24-12-2018','DD-MM-YYYY')
   AND t.day_date <= to_date('31-12-'|| extract(year from (trunc(sysdate)-1)),'dd-mm-yyyy')
       ),


DATA_COUNTRY as
(SELECT
          a.country_name,
         a.YEAR,
         a.MONTH,
          a.WEEK,
          a.WEEK_NB_YEAR,
          a.day_type,
          a.day_of_week,
          a.day_date as flight_date,
          coalesce(b.DAY_TFC,0) as DAY_TFC,
          coalesce(b.DAY_DLY,0) as DAY_DLY,
          coalesce(b.DAY_ERT_DLY,0) as DAY_ERT_DLY,
          coalesce(b.DAY_ARP_DLY,0) as DAY_ARP_DLY
FROM CTRY_DAY A
LEFT JOIN DATA_DAY b on a.COUNTRY_NAME = B.COUNTRY_NAME and a.day_date = b.flight_date

),

DATA_COUNTRY_Y2D as
(select
        flight_date ,
        country_name,
       SUM (DAY_TFC) OVER (PARTITION BY country_name ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) Y2D_TFC_YEAR,
       SUM (DAY_TFC) OVER (PARTITION BY country_name ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW)/
                Count (DAY_TFC) OVER (PARTITION BY country_name ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) Y2D_AVG_TFC_YEAR,

       SUM (DAY_DLY) OVER (PARTITION BY country_name ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) Y2D_DLY_YEAR,
       SUM (DAY_DLY) OVER (PARTITION BY country_name ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW)/
                Count (DAY_DLY) OVER (PARTITION BY country_name ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) Y2D_AVG_DLY_YEAR,

       SUM (DAY_ERT_DLY) OVER (PARTITION BY country_name ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) Y2D_ERT_DLY_YEAR,
       SUM (DAY_ERT_DLY) OVER (PARTITION BY country_name ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW)/
                Count (DAY_ERT_DLY) OVER (PARTITION BY country_name ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) Y2D_AVG_ERT_DLY_YEAR,

       SUM (DAY_ARP_DLY) OVER (PARTITION BY country_name ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) Y2D_ARP_DLY_YEAR,
       SUM (DAY_ARP_DLY) OVER (PARTITION BY country_name ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW)/
                Count (DAY_ARP_DLY) OVER (PARTITION BY country_name ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(flight_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) Y2D_AVG_ARP_DLY_YEAR

FROM DATA_COUNTRY
),

DATA_COUNTRY_2 as
(select
       a.country_name,
       YEAR,
       MONTH,
       WEEK,
       WEEK_NB_YEAR,
       DAY_TYPE,
       day_of_week,
       a.flight_DATE,

       DAY_TFC,
       DAY_DLY,
       DAY_ERT_DLY,
       DAY_ARP_DLY,

       LAG (a.flight_DATE, 7) OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE) flight_DATE_PREV_WEEK,
       LAG (DAY_TFC, 7) OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE)  DAY_TFC_PREV_WEEK,
       LAG (DAY_DLY, 7) OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE)  DAY_DLY_PREV_WEEK,
       LAG (DAY_ERT_DLY, 7) OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE)  DAY_ERT_DLY_PREV_WEEK,
       LAG (DAY_ARP_DLY, 7) OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE)  DAY_ARP_DLY_PREV_WEEK,

       LAG (a.flight_DATE, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE) flight_DATE_PREV_YEAR,
       LAG (DAY_TFC, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE)  DAY_TFC_PREV_YEAR,
       LAG (DAY_DLY, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE)  DAY_DLY_PREV_YEAR,
       LAG (DAY_ERT_DLY, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE)  DAY_ERT_DLY_PREV_YEAR,
       LAG (DAY_ARP_DLY, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE)  DAY_ARP_DLY_PREV_YEAR,

       LAG (a.flight_DATE,  greatest((extract (year from a.flight_DATE)-2019) *364+ floor((extract (year from a.flight_DATE)-2019)/4)*7,0))
                OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE) flight_DATE_2019,
       LAG (DAY_TFC,  greatest((extract (year from a.flight_DATE)-2019) *364+ floor((extract (year from a.flight_DATE)-2019)/4)*7,0))
                OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE)  DAY_TFC_2019,
       LAG (DAY_DLY,  greatest((extract (year from a.flight_DATE)-2019) *364+ floor((extract (year from a.flight_DATE)-2019)/4)*7,0))
                OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE)  DAY_DLY_2019,
       LAG (DAY_ERT_DLY,  greatest((extract (year from a.flight_DATE)-2019) *364+ floor((extract (year from a.flight_DATE)-2019)/4)*7,0))
                OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE)  DAY_ERT_DLY_2019,
       LAG (DAY_ARP_DLY,  greatest((extract (year from a.flight_DATE)-2019) *364+ floor((extract (year from a.flight_DATE)-2019)/4)*7,0))
                OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE)  DAY_ARP_DLY_2019,

       AVG (DAY_TFC)  OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS AVG_TFC_ROLLING_WEEK,
       AVG (DAY_DLY)  OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS AVG_DLY_ROLLING_WEEK,
       AVG (DAY_ERT_DLY)  OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS AVG_ERT_DLY_ROLLING_WEEK,
       AVG (DAY_ARP_DLY)  OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS AVG_ARP_DLY_ROLLING_WEEK,

       AVG (DAY_TFC)  OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) AS AVG_TFC_ROLLING_PREV_WEEK,
       AVG (DAY_DLY)  OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) AS AVG_DLY_ROLLING_PREV_WEEK,
       AVG (DAY_ERT_DLY)  OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) AS AVG_ERT_DLY_ROLLING_PREV_WEEK,
       AVG (DAY_ARP_DLY)  OVER (PARTITION BY a.country_name ORDER BY a.flight_DATE ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) AS AVG_ARP_DLY_ROLLING_PREV_WEEK,

       b.Y2D_TFC_YEAR,
       b.Y2D_DLY_YEAR,
       b.Y2D_ERT_DLY_YEAR,
       b.Y2D_ARP_DLY_YEAR,

       c.Y2D_TFC_YEAR as Y2D_TFC_PREV_YEAR,
       c.Y2D_DLY_YEAR as Y2D_DLY_PREV_YEAR,
       c.Y2D_ERT_DLY_YEAR as Y2D_ERT_DLY_PREV_YEAR,
       c.Y2D_ARP_DLY_YEAR as Y2D_ARP_DLY_PREV_YEAR,

       d.Y2D_TFC_YEAR as Y2D_TFC_2019,
       d.Y2D_DLY_YEAR as Y2D_DLY_2019,
       d.Y2D_ERT_DLY_YEAR as Y2D_ERT_DLY_2019,
       d.Y2D_ARP_DLY_YEAR as Y2D_ARP_DLY_2019,

       b.Y2D_AVG_TFC_YEAR,
       b.Y2D_AVG_DLY_YEAR,
       b.Y2D_AVG_ERT_DLY_YEAR,
       b.Y2D_AVG_ARP_DLY_YEAR,

       c.Y2D_AVG_TFC_YEAR as Y2D_AVG_TFC_PREV_YEAR,
       c.Y2D_AVG_DLY_YEAR as Y2D_AVG_DLY_PREV_YEAR,
       c.Y2D_AVG_ERT_DLY_YEAR as Y2D_AVG_ERT_DLY_PREV_YEAR,
       c.Y2D_AVG_ARP_DLY_YEAR as Y2D_AVG_ARP_DLY_PREV_YEAR,

       d.Y2D_AVG_TFC_YEAR as Y2D_AVG_TFC_2019,
       d.Y2D_AVG_DLY_YEAR as Y2D_AVG_DLY_2019,
       d.Y2D_AVG_ERT_DLY_YEAR as Y2D_AVG_ERT_DLY_2019,
       d.Y2D_AVG_ARP_DLY_YEAR as Y2D_AVG_ARP_DLY_2019

      FROM DATA_COUNTRY a
      left join DATA_COUNTRY_Y2D b on a.flight_DATE = b.flight_DATE and a.country_name = b.country_name
      left join DATA_COUNTRY_Y2D c on add_months(a.flight_DATE,-12) = c.flight_DATE and a.country_name = c.country_name
      left join DATA_COUNTRY_Y2D d on add_months(a.flight_DATE,-12*(extract (year from a.flight_DATE)-2019)) = d.flight_DATE and a.country_name = d.country_name

)  ,

  DATA_COUNTRY_3  as
  (
      select
       country_name,
        YEAR,
       MONTH,
       WEEK,
       WEEK_NB_YEAR,
       DAY_TYPE,
       day_of_week,

       flight_DATE,
       flight_DATE_PREV_WEEK,
       flight_DATE_PREV_YEAR,
       flight_DATE_2019,

       DAY_TFC,
       DAY_DLY,
       DAY_ERT_DLY,
       DAY_ARP_DLY,

       DAY_TFC_PREV_WEEK,
       DAY_DLY_PREV_WEEK,
       DAY_ERT_DLY_PREV_WEEK,
       DAY_ARP_DLY_PREV_WEEK,

       DAY_TFC_PREV_YEAR,
       DAY_DLY_PREV_YEAR,
       DAY_ERT_DLY_PREV_YEAR,
       DAY_ARP_DLY_PREV_YEAR,

       DAY_TFC_2019,
       DAY_DLY_2019,
       DAY_ERT_DLY_2019,
       DAY_ARP_DLY_2019,

      CASE WHEN flight_DATE >= trunc(sysdate)
           THEN NULL
           ELSE avg_TFC_rolling_week
       END avg_TFC_rolling_week,
      CASE WHEN flight_DATE >= trunc(sysdate)
           THEN NULL
           ELSE avg_DLY_rolling_week
       END avg_DLY_rolling_week,
      CASE WHEN flight_DATE >= trunc(sysdate)
           THEN NULL
           ELSE avg_ERT_DLY_rolling_week
       END avg_ERT_DLY_rolling_week,
      CASE WHEN flight_DATE >= trunc(sysdate)
           THEN NULL
           ELSE avg_ARP_DLY_rolling_week
       END avg_ARP_DLY_rolling_week,

       AVG_TFC_rolling_PREV_WEEK,
       AVG_DLY_rolling_PREV_WEEK,
       AVG_ERT_DLY_rolling_PREV_WEEK,
       AVG_ARP_DLY_rolling_PREV_WEEK,

       LAG (avg_TFC_rolling_week,364) OVER (PARTITION BY country_name ORDER BY flight_DATE) as AVG_TFC_rolling_week_PREV_YEAR,
       LAG (avg_DLY_rolling_week,364) OVER (PARTITION BY country_name ORDER BY flight_DATE) as AVG_DLY_rolling_week_PREV_YEAR,
       LAG (avg_ERT_DLY_rolling_week,364) OVER (PARTITION BY country_name ORDER BY flight_DATE) as AVG_ERT_DLY_rolling_week_PREV_YEAR,
       LAG (avg_ARP_DLY_rolling_week,364) OVER (PARTITION BY country_name ORDER BY flight_DATE) as AVG_ARP_DLY_rolling_week_PREV_YEAR,

       LAG (avg_TFC_rolling_week, greatest((extract (year from flight_DATE)-2019) *364+ floor((extract (year from flight_DATE)-2019)/4)*7,0))
            OVER (PARTITION BY country_name ORDER BY flight_DATE) as AVG_TFC_rolling_week_2019,
       LAG (avg_DLY_rolling_week, greatest((extract (year from flight_DATE)-2019) *364+ floor((extract (year from flight_DATE)-2019)/4)*7,0))
            OVER (PARTITION BY country_name ORDER BY flight_DATE) as AVG_DLY_rolling_week_2019,
       LAG (avg_ERT_DLY_rolling_week, greatest((extract (year from flight_DATE)-2019) *364+ floor((extract (year from flight_DATE)-2019)/4)*7,0))
            OVER (PARTITION BY country_name ORDER BY flight_DATE) as AVG_ERT_DLY_rolling_week_2019,
       LAG (avg_ARP_DLY_rolling_week, greatest((extract (year from flight_DATE)-2019) *364+ floor((extract (year from flight_DATE)-2019)/4)*7,0))
            OVER (PARTITION BY country_name ORDER BY flight_DATE) as AVG_ARP_DLY_rolling_week_2019,

       Y2D_TFC_YEAR,
       Y2D_DLY_YEAR,
       Y2D_ERT_DLY_YEAR,
       Y2D_ARP_DLY_YEAR,

       Y2D_TFC_PREV_YEAR,
       Y2D_DLY_PREV_YEAR,
       Y2D_ERT_DLY_PREV_YEAR,
       Y2D_ARP_DLY_PREV_YEAR,

       Y2D_TFC_2019,
       Y2D_DLY_2019,
       Y2D_ERT_DLY_2019,
       Y2D_ARP_DLY_2019,

       CASE WHEN flight_DATE >= trunc(sysdate)
           THEN NULL
      	   ELSE Y2D_AVG_TFC_YEAR
       END Y2D_AVG_TFC_YEAR,
       CASE WHEN flight_DATE >= trunc(sysdate)
           THEN NULL
      	   ELSE Y2D_AVG_DLY_YEAR
       END Y2D_AVG_DLY_YEAR,
       CASE WHEN flight_DATE >= trunc(sysdate)
           THEN NULL
      	   ELSE Y2D_AVG_ERT_DLY_YEAR
       END Y2D_AVG_ERT_DLY_YEAR,
       CASE WHEN flight_DATE >= trunc(sysdate)
           THEN NULL
      	   ELSE Y2D_AVG_ARP_DLY_YEAR
       END Y2D_AVG_ARP_DLY_YEAR,

       Y2D_AVG_TFC_PREV_YEAR,
       Y2D_AVG_DLY_PREV_YEAR,
       Y2D_AVG_ERT_DLY_PREV_YEAR,
       Y2D_AVG_ARP_DLY_PREV_YEAR,

       Y2D_AVG_TFC_2019,
       Y2D_AVG_DLY_2019,
       Y2D_AVG_ERT_DLY_2019,
       Y2D_AVG_ARP_DLY_2019

      FROM DATA_COUNTRY_2
  )
  select
--  from prev table
       country_name,
       YEAR,
       MONTH,
       WEEK,
       WEEK_NB_YEAR,
       DAY_TYPE,
       day_of_week,

       flight_DATE,
       flight_DATE_PREV_WEEK,
       flight_DATE_PREV_YEAR,
       flight_DATE_2019,

       DAY_TFC,
       DAY_DLY,
       DAY_ERT_DLY,
       DAY_ARP_DLY,

       DAY_TFC_PREV_WEEK,
       DAY_DLY_PREV_WEEK,
       DAY_ERT_DLY_PREV_WEEK,
       DAY_ARP_DLY_PREV_WEEK,

       DAY_TFC_PREV_YEAR,
       DAY_DLY_PREV_YEAR,
       DAY_ERT_DLY_PREV_YEAR,
       DAY_ARP_DLY_PREV_YEAR,

       DAY_TFC_2019,
       DAY_DLY_2019,
       DAY_ERT_DLY_2019,
       DAY_ARP_DLY_2019,

       avg_TFC_rolling_week,
       avg_DLY_rolling_week,
       avg_ERT_DLY_rolling_week,
       avg_ARP_DLY_rolling_week,

       AVG_TFC_rolling_PREV_WEEK,
       AVG_DLY_rolling_PREV_WEEK,
       AVG_ERT_DLY_rolling_PREV_WEEK,
       AVG_ARP_DLY_rolling_PREV_WEEK,

       AVG_TFC_rolling_week_PREV_YEAR,
       AVG_DLY_rolling_week_PREV_YEAR,
       AVG_ERT_DLY_rolling_week_PREV_YEAR,
       AVG_ARP_DLY_rolling_week_PREV_YEAR,

       AVG_TFC_rolling_week_2019,
       AVG_DLY_rolling_week_2019,
       AVG_ERT_DLY_rolling_week_2019,
       AVG_ARP_DLY_rolling_week_2019,

       Y2D_TFC_YEAR,
       Y2D_DLY_YEAR,
       Y2D_ERT_DLY_YEAR,
       Y2D_ARP_DLY_YEAR,

       Y2D_TFC_PREV_YEAR,
       Y2D_DLY_PREV_YEAR,
       Y2D_ERT_DLY_PREV_YEAR,
       Y2D_ARP_DLY_PREV_YEAR,

       Y2D_TFC_2019,
       Y2D_DLY_2019,
       Y2D_ERT_DLY_2019,
       Y2D_ARP_DLY_2019,

       Y2D_AVG_TFC_YEAR,
       Y2D_AVG_DLY_YEAR,
       Y2D_AVG_ERT_DLY_YEAR,
       Y2D_AVG_ARP_DLY_YEAR,

       Y2D_AVG_TFC_PREV_YEAR,
       Y2D_AVG_DLY_PREV_YEAR,
       Y2D_AVG_ERT_DLY_PREV_YEAR,
       Y2D_AVG_ARP_DLY_PREV_YEAR,

       Y2D_AVG_TFC_2019,
       Y2D_AVG_DLY_2019,
       Y2D_AVG_ERT_DLY_2019,
       Y2D_AVG_ARP_DLY_2019,

--  new calcs
      DAY_DLY - DAY_DLY_PREV_WEEK  as DAY_DLY_DIFF_PREV_WEEK,
      DAY_DLY - DAY_DLY_PREV_YEAR  as DAY_DLY_DIFF_PREV_YEAR,
      DAY_DLY - DAY_DLY_2019  as DAY_DLY_DIFF_2019,

      CASE WHEN DAY_DLY_PREV_WEEK  <>0  then
            DAY_DLY/DAY_DLY_PREV_WEEK -1
            ELSE NULL
      END as DAY_DLY_PREV_WEEK_perc,
      CASE WHEN DAY_DLY_PREV_YEAR <>0
           THEN DAY_DLY/DAY_DLY_PREV_YEAR -1
       	   ELSE NULL
      END  DAY_DLY_DIF_PREV_YEAR_PERC,
      CASE WHEN DAY_DLY_2019 <>0
           THEN DAY_DLY/DAY_DLY_2019 -1
       	   ELSE NULL
      END  DAY_DLY_DIF_2019_PERC,

      CASE WHEN AVG_DLY_rolling_week_PREV_YEAR <> 0 and flight_DATE < trunc(sysdate)
           THEN avg_DLY_rolling_week/AVG_DLY_rolling_week_PREV_YEAR -1
           ELSE NULL
       END  DIF_DLY_ROLLING_WEEK_PREV_YEAR_PERC,
      CASE WHEN AVG_DLY_rolling_week_2019 <> 0 and flight_DATE < trunc(sysdate)
           THEN avg_DLY_rolling_week/AVG_DLY_rolling_week_2019 -1
           ELSE NULL
       END  DIF_DLY_ROLLING_WEEK_2019_perc,

       CASE WHEN Y2D_AVG_DLY_PREV_YEAR <> 0 THEN
        Y2D_AVG_DLY_YEAR/Y2D_AVG_DLY_PREV_YEAR - 1
        ELSE NULL
       END Y2D_DLY_DIF_PREV_YEAR_PERC,
       CASE WHEN Y2D_AVG_DLY_2019 <> 0 THEN
           Y2D_AVG_DLY_YEAR/Y2D_AVG_DLY_2019 - 1
           ELSE NULL
       END Y2D_DLY_DIF_2019_PERC,
       trunc(sysdate) -1 as LAST_DATA_DAY

      FROM DATA_COUNTRY_3
      where flight_DATE >=to_date('01-01-2024','dd-mm-yyyy')
                  AND country_name not in ('ICELAND', 'Iceland')

UNION ALL

select
       country_name,
       YEAR,
       MONTH,
       WEEK,
       WEEK_NB_YEAR,
       DAY_TYPE,
       day_of_week,

       flight_DATE,
       flight_DATE_PREV_WEEK,
       flight_DATE_PREV_YEAR,
       flight_DATE_2019,

       case when FLIGHT_DATE >='01-jan-2024' then DAY_TFC else NULL end DAY_TFC,
       case when FLIGHT_DATE >='01-jan-2024' then DAY_DLY else NULL end DAY_DLY,
       case when FLIGHT_DATE >='01-jan-2024' then DAY_ERT_DLY else NULL end DAY_ERT_DLY,
       case when FLIGHT_DATE >='01-jan-2024' then DAY_ARP_DLY else NULL end DAY_ARP_DLY,

       case when FLIGHT_DATE >='01-jan-2024' then DAY_TFC_PREV_WEEK else NULL end DAY_TFC_PREV_WEEK,
       case when FLIGHT_DATE >='01-jan-2024' then DAY_DLY_PREV_WEEK else NULL end DAY_DLY_PREV_WEEK,
       case when FLIGHT_DATE >='01-jan-2024' then DAY_ERT_DLY_PREV_WEEK else NULL end DAY_ERT_DLY_PREV_WEEK,
       case when FLIGHT_DATE >='01-jan-2024' then DAY_ARP_DLY_PREV_WEEK else NULL end DAY_ARP_DLY_PREV_WEEK,

       case when FLIGHT_DATE >='01-jan-2025' then DAY_TFC_PREV_YEAR else NULL end DAY_TFC_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then DAY_DLY_PREV_YEAR else NULL end DAY_DLY_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then DAY_ERT_DLY_PREV_YEAR else NULL end DAY_ERT_DLY_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then DAY_ARP_DLY_PREV_YEAR else NULL end DAY_ARP_DLY_PREV_YEAR,

       NULL as DAY_TFC_2019,
       NULL as DAY_DLY_2019,
       NULL as DAY_ERT_DLY_2019,
       NULL as DAY_ARP_DLY_2019,

       case when FLIGHT_DATE >='01-jan-2024' then avg_TFC_rolling_week else NULL end avg_TFC_rolling_week,
       case when FLIGHT_DATE >='01-jan-2024' then avg_DLY_rolling_week else NULL end avg_DLY_rolling_week,
       case when FLIGHT_DATE >='01-jan-2024' then avg_ERT_DLY_rolling_week else NULL end avg_ERT_DLY_rolling_week,
       case when FLIGHT_DATE >='01-jan-2024' then avg_ARP_DLY_rolling_week else NULL end avg_ARP_DLY_rolling_week,

       case when FLIGHT_DATE >='01-jan-2024' then AVG_TFC_rolling_PREV_WEEK else NULL end AVG_TFC_rolling_PREV_WEEK,
       case when FLIGHT_DATE >='01-jan-2024' then AVG_DLY_rolling_PREV_WEEK else NULL end AVG_DLY_rolling_PREV_WEEK,
       case when FLIGHT_DATE >='01-jan-2024' then AVG_ERT_DLY_rolling_PREV_WEEK else NULL end AVG_ERT_DLY_rolling_PREV_WEEK,
       case when FLIGHT_DATE >='01-jan-2024' then AVG_ARP_DLY_rolling_PREV_WEEK else NULL end AVG_ARP_DLY_rolling_PREV_WEEK,

       case when FLIGHT_DATE >='01-jan-2025' then AVG_TFC_rolling_week_PREV_YEAR else NULL end AVG_TFC_rolling_week_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then AVG_DLY_rolling_week_PREV_YEAR else NULL end AVG_DLY_rolling_week_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then AVG_ERT_DLY_rolling_week_PREV_YEAR else NULL end AVG_ERT_DLY_rolling_week_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then AVG_ARP_DLY_rolling_week_PREV_YEAR else NULL end AVG_ARP_DLY_rolling_week_PREV_YEAR,

       NULL as AVG_TFC_rolling_week_2019,
       NULL as AVG_DLY_rolling_week_2019,
       NULL as AVG_ERT_DLY_rolling_week_2019,
       NULL as AVG_ARP_DLY_rolling_week_2019,

       case when FLIGHT_DATE >='01-jan-2024' then Y2D_TFC_YEAR else NULL end Y2D_TFC_YEAR,
       case when FLIGHT_DATE >='01-jan-2024' then Y2D_DLY_YEAR else NULL end Y2D_DLY_YEAR,
       case when FLIGHT_DATE >='01-jan-2024' then Y2D_ERT_DLY_YEAR else NULL end Y2D_ERT_DLY_YEAR,
       case when FLIGHT_DATE >='01-jan-2024' then Y2D_ARP_DLY_YEAR else NULL end Y2D_ARP_DLY_YEAR,

       case when FLIGHT_DATE >='01-jan-2025' then Y2D_TFC_PREV_YEAR else NULL end Y2D_TFC_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then Y2D_DLY_PREV_YEAR else NULL end Y2D_DLY_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then Y2D_ERT_DLY_PREV_YEAR else NULL end Y2D_ERT_DLY_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then Y2D_ARP_DLY_PREV_YEAR else NULL end Y2D_ARP_DLY_PREV_YEAR,

       NULL as Y2D_TFC_2019,
       NULL as Y2D_DLY_2019,
       NULL as Y2D_ERT_DLY_2019,
       NULL as Y2D_ARP_DLY_2019,

       case when FLIGHT_DATE >='01-jan-2024' then Y2D_AVG_TFC_YEAR else NULL end Y2D_AVG_TFC_YEAR,
       case when FLIGHT_DATE >='01-jan-2024' then Y2D_AVG_DLY_YEAR else NULL end Y2D_AVG_DLY_YEAR,
       case when FLIGHT_DATE >='01-jan-2024' then Y2D_AVG_ERT_DLY_YEAR else NULL end Y2D_AVG_ERT_DLY_YEAR,
       case when FLIGHT_DATE >='01-jan-2024' then Y2D_AVG_ARP_DLY_YEAR else NULL end Y2D_AVG_ARP_DLY_YEAR,

       case when FLIGHT_DATE >='01-jan-2025' then Y2D_AVG_TFC_PREV_YEAR else NULL end Y2D_AVG_TFC_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then Y2D_AVG_DLY_PREV_YEAR else NULL end Y2D_AVG_DLY_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then Y2D_AVG_ERT_DLY_PREV_YEAR else NULL end Y2D_AVG_ERT_DLY_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then Y2D_AVG_ARP_DLY_PREV_YEAR else NULL end Y2D_AVG_ARP_DLY_PREV_YEAR,

       NULL as Y2D_AVG_TFC_2019,
       NULL as Y2D_AVG_DLY_2019,
       NULL as Y2D_AVG_ERT_DLY_2019,
       NULL as Y2D_AVG_ARP_DLY_2019,

--  new calcs
      case when FLIGHT_DATE >='01-jan-2024' then DAY_DLY - DAY_DLY_PREV_WEEK else NULL end DAY_DLY_DIFF_PREV_WEEK,
      case when FLIGHT_DATE >='01-jan-2025' then DAY_DLY - DAY_DLY_PREV_YEAR else NULL end DAY_DLY_DIFF_PREV_YEAR,
      NULL as DAY_DLY_DIFF_2019,

      CASE WHEN DAY_DLY_PREV_WEEK  <>0 and FLIGHT_DATE >='01-jan-2024'
            THEN DAY_DLY/DAY_DLY_PREV_WEEK -1
            ELSE NULL
      END as DAY_DLY_PREV_WEEK_perc,
      CASE WHEN DAY_DLY_PREV_YEAR <>0  and FLIGHT_DATE >='01-jan-2025'
           THEN DAY_DLY/DAY_DLY_PREV_YEAR -1
       	   ELSE NULL
      END  DAY_DLY_DIF_PREV_YEAR_PERC,
      NULL as DAY_DLY_DIF_2019_PERC,

      CASE WHEN AVG_DLY_rolling_week_PREV_YEAR <> 0 and flight_DATE < trunc(sysdate) and FLIGHT_DATE >='01-jan-2025'
           THEN avg_DLY_rolling_week/AVG_DLY_rolling_week_PREV_YEAR -1
           ELSE NULL
       END  DIF_DLY_ROLLING_WEEK_PREV_YEAR_PERC,
      NULL as DIF_DLY_ROLLING_WEEK_2019_perc,

       CASE WHEN Y2D_AVG_DLY_PREV_YEAR <> 0 and FLIGHT_DATE >='01-jan-2025'
        THEN Y2D_AVG_DLY_YEAR/Y2D_AVG_DLY_PREV_YEAR - 1
        ELSE NULL
       END Y2D_DLY_DIF_PREV_YEAR_PERC,
       NULL as Y2D_DLY_DIF_2019_PERC,
       trunc(sysdate) -1 as LAST_DATA_DAY

      FROM DATA_COUNTRY_3
      where flight_DATE >=to_date('01-01-2024','dd-mm-yyyy')
                  AND country_name in ('ICELAND', 'Iceland')
      order by country_name, flight_date
"

## st_delay_cause ----
st_delay_cause_day_query <- "
WITH

COUNTRY_ICAO2LETTER  as (
select distinct
       ec_icao_country_code  ICAO2LETTER,
       CASE
             WHEN ec_icao_country_code = 'GE' then 'LE'
             WHEN ec_icao_country_code = 'ET' then 'ED'
             ELSE ec_icao_country_code
        END  COUNTRY_code,    
        CASE WHEN ec_icao_country_code = 'GC' then 'Spain Canaries'
             WHEN ec_icao_country_code = 'GE' then 'Spain Continental'
             WHEN ec_icao_country_code = 'LE' then 'Spain Continental'
             WHEN ec_icao_country_code = 'LY' then 'Serbia/Montenegro'
             WHEN ec_icao_country_code = 'LU' then 'Moldova'
             ELSE  ec_icao_country_name
        END  COUNTRY_NAME
  from SWH_FCT.dim_icao_country a
  WHERE Valid_to > trunc(sysdate) - 1
  AND  (  (SUBSTR(ec_icao_country_code,1,1) IN ('E','L')
       OR SUBSTR(ec_icao_country_code,1,2) IN ('GC','GM','GE','UD','UG','UK','BI'))  )
  AND  ec_icao_country_code not in ('LV', 'LX', 'EU','LN')
  ORDER BY COUNTRY_code
),
 
LIST_COUNTRY as (
select  COUNTRY_NAME FROM COUNTRY_ICAO2LETTER
group by  COUNTRY_NAME),

REL_CFMU_TVS_CTRY_CODE
 as (
 select a.pru_tvs_code, a.wef, a.till, b.country_code, b.country_name
  from prudev.v_pru_rel_cfmu_tvs_Country_fir a ,  COUNTRY_ICAO2LETTER  b
 where a.unit_code = b.ICAO2LETTER )
,


CTRY_DAY AS (
SELECT a.COUNTRY_NAME,
        t.year,
        t.month,
        t.week,
        t.week_nb_year,
        t.day_type,
        t.day_of_week_nb AS day_of_week,
        t.day_date
FROM LIST_COUNTRY a, prudev.pru_time_references t
WHERE
    day_date >= TO_DATE ('01-01-2019', 'dd-mm-yyyy')
    AND day_date < trunc(sysdate)
)   ,



DELAY_TVS_day
  AS
     (SELECT a.*,
            a.agg_flt_tv_set_id AS pru_tvs_code
      FROM prudev.v_aiu_agg_flt_flow a
      WHERE a.agg_flt_a_first_entry_date >= '01-jan-2019'),

DELAY_TVS
        AS
(  SELECT
         a.pru_tvs_code,
      a.agg_flt_tv_set_id,
      a.agg_flt_a_first_entry_date AS flight_date,
     SUM (NVL(a.agg_flt_total_delay, 0))
        tdm,
     SUM (NVL((CASE WHEN a.agg_flt_mp_regu_loc_ty = 'En route' THEN  a.agg_flt_total_delay END),0))
          tdm_ert,
     SUM (NVL((CASE WHEN a.agg_flt_mp_regu_loc_ty = 'Airport'  THEN a.agg_flt_total_delay END),0))
        tdm_arp,
     SUM (NVL((CASE WHEN a.agg_flt_mp_regu_loc_ty = 'na' THEN a.agg_flt_total_delay END),0))
          tdm_na,
     SUM (NVL(agg_flt_delayed_traffic, 0))
        tdf,
     SUM (NVL((CASE WHEN a.agg_flt_mp_regu_loc_ty = 'En route' THEN  a.agg_flt_delayed_traffic END),0))
          tdf_ert,
     SUM (NVL((CASE WHEN a.agg_flt_mp_regu_loc_ty = 'Airport'  THEN a.agg_flt_delayed_traffic END),0))
         tdf_arp,
     SUM (NVL((CASE WHEN a.agg_flt_mp_regu_loc_ty = 'na' THEN a.agg_flt_delayed_traffic END),0))
          tdf_na,
     SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('C', 'S') AND a.agg_flt_mp_regu_loc_ty = 'En route' THEN a.agg_flt_total_delay END),0))
          tdm_ert_cs,
     SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('G') AND a.agg_flt_mp_regu_loc_ty = 'En route' THEN a.agg_flt_total_delay END),0))
          tdm_ert_g,
     SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('C', 'S', 'G') AND a.agg_flt_mp_regu_loc_ty = 'En route' THEN a.agg_flt_total_delay END),0))
          tdm_ert_csg,
      SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('I', 'T') AND a.agg_flt_mp_regu_loc_ty = 'En route' THEN a.agg_flt_total_delay END),0))
          tdm_ert_it,
     SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('W', 'D') AND a.agg_flt_mp_regu_loc_ty = 'En route' THEN a.agg_flt_total_delay END),0))
          tdm_ert_wd,
     SUM (NVL((CASE WHEN agg_flt_regu_reas NOT IN ('C','S','G','I','T','W','D') AND a.agg_flt_mp_regu_loc_ty = 'En route' THEN a.agg_flt_total_delay END),0))
          tdm_ert_no_csgitwd,
--     SUM (NVL(a.agg_flt_regulated_traffic, 0))
--          trf,
--     SUM (NVL((CASE WHEN a.agg_flt_mp_regu_loc_ty = 'Airport'  THEN NVL (a.agg_flt_regulated_traffic, 0) END),0))
--          trf_arp,
--     SUM (NVL((CASE WHEN a.agg_flt_mp_regu_loc_ty = 'En route'THEN  NVL (a.agg_flt_regulated_traffic, 0) END),0))
--          trf_ert,
     SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('C', 'S') AND a.agg_flt_mp_regu_loc_ty = 'Airport' THEN   a.agg_flt_total_delay END),0))
          tdm_arp_cs,
     SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('G') AND a.agg_flt_mp_regu_loc_ty = 'Airport' THEN   a.agg_flt_total_delay END),0))
          tdm_arp_g,
     SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('C', 'S', 'G') AND a.agg_flt_mp_regu_loc_ty = 'Airport' THEN   a.agg_flt_total_delay END),0))
          tdm_arp_csg,
     SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('I', 'T')  AND a.agg_flt_mp_regu_loc_ty = 'Airport' THEN   a.agg_flt_total_delay END),0))
          tdm_arp_it,
     SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('W', 'D') AND a.agg_flt_mp_regu_loc_ty = 'Airport' THEN   a.agg_flt_total_delay END),0))
          tdm_arp_wd,SUM (NVL((CASE WHEN agg_flt_regu_reas NOT IN ('C','S','G','I','T','W','D') AND a.agg_flt_mp_regu_loc_ty = 'Airport' THEN   a.agg_flt_total_delay END),0))
          tdm_arp_no_csgitwd
    FROM DELAY_TVS_day a
    WHERE a.agg_flt_a_first_entry_date >= '01-jan-2019'
    GROUP BY  a.agg_flt_a_first_entry_date,
              a.agg_flt_tv_set_id,
              a.pru_tvs_code)
,

   DATA_DELAY_COUNTRY as
   (
      SELECT
             r.country_code,
             r.country_name,
             flight_date,
            sum(TDF) as TDF,
            sum(TDF_ARP) as TDF_ARP,
            sum(TDF_ERT) as TDF_ERT,
            sum(TDF_NA) as TDF_NA,
            sum(TDM) as TDM,
            sum(TDM_ARP) as TDM_ARP,
            sum(TDM_ARP_CSG) as TDM_ARP_CSG,
            sum(TDM_ARP_CS) as TDM_ARP_CS,
            sum(TDM_ARP_G) as TDM_ARP_G,
            sum(TDM_ARP_IT) as TDM_ARP_IT,
            sum(TDM_ARP_WD) as TDM_ARP_WD,
            sum(TDM_ARP_NO_CSGITWD) as TDM_ARP_NO_CSGITWD,
            sum(TDM_ERT) as TDM_ERT,
            sum(TDM_ERT_CSG) as TDM_ERT_CSG,
            sum(TDM_ERT_CS) as TDM_ERT_CS,
            sum(TDM_ERT_G) as TDM_ERT_G,
            sum(TDM_ERT_IT) as TDM_ERT_IT,
            sum(TDM_ERT_WD) as TDM_ERT_WD,
            sum(TDM_ERT_NO_CSGITWD) as TDM_ERT_NO_CSGITWD
        FROM delay_tvs a, REL_CFMU_TVS_CTRY_CODE  r
       WHERE     a.flight_date BETWEEN r.wef AND r.till
             AND a.pru_tvs_code = r.pru_tvs_code

    GROUP BY a.flight_date,
             r.country_code,
             r.country_name
 )  ,

ALL_DAY_DATA as (
select
    a.country_name,
    a.YEAR,
    a.MONTH,
    a.WEEK,
    a.WEEK_NB_YEAR,
    a.day_type,
    a.day_of_week,
    a.day_date as flight_date,
  -- a.country_code,
    coalesce(TDF,0) as TDF,
    coalesce(TDF_ARP,0) as TDF_ARP,
    coalesce(TDF_ERT,0) as TDF_ERT,
    coalesce(TDF_NA,0) as TDF_NA,
    coalesce(TDM,0) as TDM,
    coalesce(TDM_ARP,0) as TDM_ARP,
    coalesce(TDM_ARP_CSG,0) as TDM_ARP_CSG,
    coalesce(TDM_ARP_CS,0) as TDM_ARP_CS,
    coalesce(TDM_ARP_G,0) as TDM_ARP_G,
    coalesce(TDM_ARP_IT,0) as TDM_ARP_IT,
    coalesce(TDM_ARP_WD,0) as TDM_ARP_WD,
    coalesce(TDM_ARP_NO_CSGITWD,0) as TDM_ARP_NO_CSGITWD,
    coalesce(TDM_ERT,0) as TDM_ERT,
    coalesce(TDM_ERT_CSG,0) as TDM_ERT_CSG,
    coalesce(TDM_ERT_CS,0) as TDM_ERT_CS,
    coalesce(TDM_ERT_G,0) as TDM_ERT_G,
    coalesce(TDM_ERT_IT,0) as TDM_ERT_IT,
    coalesce(TDM_ERT_WD,0) as TDM_ERT_WD,
    coalesce(TDM_ERT_NO_CSGITWD,0) as TDM_ERT_NO_CSGITWD

    FROM CTRY_DAY A
    LEFT JOIN DATA_DELAY_COUNTRY b on a.COUNTRY_NAME = B.COUNTRY_NAME and a.day_date = b.flight_date
    ),

DAY_DATA_CALC AS (
select
       a.country_name,
       YEAR,
       MONTH,
       WEEK,
       WEEK_NB_YEAR,
       DAY_TYPE,
       day_of_week,
       flight_date,

       TDF,
       TDF_ARP,
       TDF_ERT,
       TDF_NA,
       TDM,
       TDM_ARP,
       TDM_ARP_CSG,
       TDM_ARP_CS,
       TDM_ARP_G,
       TDM_ARP_IT,
       TDM_ARP_WD,
       TDM_ARP_NO_CSGITWD,
       TDM_ERT,
       TDM_ERT_CSG,
       TDM_ERT_CS,
       TDM_ERT_G,
       TDM_ERT_IT,
       TDM_ERT_WD,
       TDM_ERT_NO_CSGITWD,

       LAG (a.flight_date, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_date) flight_date_PREV_YEAR,
       LAG (TDM, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_date)  TDM_PREV_YEAR,
       LAG (TDM_ARP, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_date)  TDM_ARP_PREV_YEAR,
       LAG (TDM_ARP_CSG, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_date)  TDM_ARP_CSG_PREV_YEAR,
       LAG (TDM_ARP_CS, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_date)  TDM_ARP_CS_PREV_YEAR,
       LAG (TDM_ARP_G, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_date)  TDM_ARP_G_PREV_YEAR,
       LAG (TDM_ARP_IT, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_date)  TDM_ARP_IT_PREV_YEAR,
       LAG (TDM_ARP_WD, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_date)  TDM_ARP_WD_PREV_YEAR,
       LAG (TDM_ARP_NO_CSGITWD, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_date)  TDM_ARP_NO_CSGITWD_PREV_YEAR,
       LAG (TDM_ERT, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_date)  TDM_ERT_PREV_YEAR,
       LAG (TDM_ERT_CSG, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_date)  TDM_ERT_CSG_PREV_YEAR,
       LAG (TDM_ERT_CS, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_date)  TDM_ERT_CS_PREV_YEAR,
       LAG (TDM_ERT_G, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_date)  TDM_ERT_G_PREV_YEAR,
       LAG (TDM_ERT_IT, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_date)  TDM_ERT_IT_PREV_YEAR,
       LAG (TDM_ERT_WD, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_date)  TDM_ERT_WD_PREV_YEAR,
       LAG (TDM_ERT_NO_CSGITWD, 364) OVER (PARTITION BY a.country_name ORDER BY a.flight_date)  TDM_ERT_NO_CSGITWD_PREV_YEAR

      FROM ALL_DAY_DATA a
 )

 select
       country_name,
       YEAR,
       MONTH,
       WEEK,
       WEEK_NB_YEAR,
       DAY_TYPE,
       day_of_week,
       flight_date,

       TDF,
       TDF_ARP,
       TDF_ERT,
       TDF_NA,
       TDM,
       TDM_ARP,
       TDM_ARP_CSG,
       TDM_ARP_CS,
       TDM_ARP_G,
       TDM_ARP_IT,
       TDM_ARP_WD,
       TDM_ARP_NO_CSGITWD,
       TDM_ERT,
       TDM_ERT_CSG,
       TDM_ERT_CS,
       TDM_ERT_G,
       TDM_ERT_IT,
       TDM_ERT_WD,
       TDM_ERT_NO_CSGITWD,

       flight_date_PREV_YEAR,
       TDM_PREV_YEAR,

       TDM_ARP_PREV_YEAR,
       TDM_ARP_CSG_PREV_YEAR,
       TDM_ARP_CS_PREV_YEAR,
       TDM_ARP_G_PREV_YEAR,
       TDM_ARP_IT_PREV_YEAR,
       TDM_ARP_WD_PREV_YEAR,
       TDM_ARP_NO_CSGITWD_PREV_YEAR,
       TDM_ERT_PREV_YEAR,
       TDM_ERT_CSG_PREV_YEAR,
       TDM_ERT_CS_PREV_YEAR,
       TDM_ERT_G_PREV_YEAR,
       TDM_ERT_IT_PREV_YEAR,
       TDM_ERT_WD_PREV_YEAR,
       TDM_ERT_NO_CSGITWD_PREV_YEAR

 from DAY_DATA_CALC
 where country_name not in ('ICELAND', 'Iceland')

 union all

 select
       country_name,
       YEAR,
       MONTH,
       WEEK,
       WEEK_NB_YEAR,
       DAY_TYPE,
       day_of_week,
       flight_date,

       case when FLIGHT_DATE >='01-jan-2024' then TDF else NULL end TDF,
       case when FLIGHT_DATE >='01-jan-2024' then TDF_ARP else NULL end TDF_ARP,
       case when FLIGHT_DATE >='01-jan-2024' then TDF_ERT else NULL end TDF_ERT,
       case when FLIGHT_DATE >='01-jan-2024' then TDF_NA else NULL end TDF_NA,

       case when FLIGHT_DATE >='01-jan-2024' then TDM else NULL end TDM,

       case when FLIGHT_DATE >='01-jan-2024' then TDM_ARP else NULL end TDM_ARP,
       case when FLIGHT_DATE >='01-jan-2024' then TDM_ARP_CSG else NULL end TDM_ARP_CSG,
       case when FLIGHT_DATE >='01-jan-2024' then TDM_ARP_CS else NULL end TDM_ARP_CS,
       case when FLIGHT_DATE >='01-jan-2024' then TDM_ARP_G else NULL end TDM_ARP_G,
       case when FLIGHT_DATE >='01-jan-2024' then TDM_ARP_IT else NULL end TDM_ARP_IT,
       case when FLIGHT_DATE >='01-jan-2024' then TDM_ARP_WD else NULL end TDM_ARP_WD,
       case when FLIGHT_DATE >='01-jan-2024' then TDM_ARP_NO_CSGITWD else NULL end TDM_ARP_NO_CSGITWD,

       case when FLIGHT_DATE >='01-jan-2024' then TDM_ERT else NULL end TDM_ERT,
       case when FLIGHT_DATE >='01-jan-2024' then TDM_ERT_CSG else NULL end TDM_ERT_CSG,
       case when FLIGHT_DATE >='01-jan-2024' then TDM_ERT_CS else NULL end TDM_ERT_CS,
       case when FLIGHT_DATE >='01-jan-2024' then TDM_ERT_G else NULL end TDM_ERT_G,
       case when FLIGHT_DATE >='01-jan-2024' then TDM_ERT_IT else NULL end TDM_ERT_IT,
       case when FLIGHT_DATE >='01-jan-2024' then TDM_ERT_WD else NULL end TDM_ERT_WD,
       case when FLIGHT_DATE >='01-jan-2024' then TDM_ERT_NO_CSGITWD else NULL end TDM_ERT_NO_CSGITWD,


       flight_date_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then TDM_PREV_YEAR else NULL end TDM_PREV_YEAR,

       case when FLIGHT_DATE >='01-jan-2025' then TDM_ARP_PREV_YEAR else NULL end TDM_ARP_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then TDM_ARP_CSG_PREV_YEAR else NULL end TDM_ARP_CSG_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then TDM_ARP_CS_PREV_YEAR else NULL end TDM_ARP_CS_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then TDM_ARP_G_PREV_YEAR else NULL end TDM_ARP_G_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then TDM_ARP_IT_PREV_YEAR else NULL end TDM_ARP_IT_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then TDM_ARP_WD_PREV_YEAR else NULL end TDM_ARP_WD_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then TDM_ARP_NO_CSGITWD_PREV_YEAR else NULL end TDM_ARP_NO_CSGITWD_PREV_YEAR,

       case when FLIGHT_DATE >='01-jan-2025' then TDM_ERT_PREV_YEAR else NULL end TDM_ERT_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then TDM_ERT_CSG_PREV_YEAR else NULL end TDM_ERT_CSG_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then TDM_ERT_CS_PREV_YEAR else NULL end TDM_ERT_CS_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then TDM_ERT_G_PREV_YEAR else NULL end TDM_ERT_G_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then TDM_ERT_IT_PREV_YEAR else NULL end TDM_ERT_IT_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then TDM_ERT_WD_PREV_YEAR else NULL end TDM_ERT_WD_PREV_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then TDM_ERT_NO_CSGITWD_PREV_YEAR else NULL end TDM_ERT_NO_CSGITWD_PREV_YEAR

 from DAY_DATA_CALC
 where country_name in ('ICELAND', 'Iceland')
 order by country_name, flight_date

"
## st_ao ----
st_ao_day_base_query <- paste0("
with 

DIM_AO
 as ( SELECT distinct
 		ao_id,
 		ao_code, 
 		wef,
 		til
 from  ldw_acc.AO_GROUPS_ASSOCIATION
 ) ,
 
REL_AP_CTRY as (
select cfmu_ap_code ,
       CASE WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'GC' then 'IC'
            ELSE country_code 
       END iso_ct_code,
       region,
       pru_dashboard_ap_name  as ad_name
 from  prudev.v_covid_rel_airport_area 

),  


AIRP_FLIGHT as (
SELECT flt_uid,
       TRUNC (flt_a_asp_prof_time_entry) AS ENTRY_DATE,
       c1.iso_ct_code dep_ctry ,
       c2.iso_ct_code arr_ctry,
        CASE WHEN (TRUNC(A.flt_a_asp_prof_time_entry) >= d.wef AND TRUNC(A.flt_a_asp_prof_time_entry) <= d.til)
        		THEN nvl(d.ao_id, 1777) 
        		ELSE 99999
        END ao_id, 
        nvl(d.ao_code,'ZZZ') ao_code
       
  FROM prudev.v_aiu_flt a
     left outer join REL_AP_CTRY c1 ON  (a.flt_dep_ad = c1.cfmu_ap_code)
     left outer join REL_AP_CTRY c2 ON  (a.flt_ctfm_ades  = c2.cfmu_ap_code)
     left outer join DIM_AO d  on ( (a.ao_icao_id = d.ao_code ) )
  WHERE A.flt_lobt >= ", query_from, " - 2 
                        AND A.flt_lobt < TRUNC (SYSDATE) + 2
                        AND A.flt_a_asp_prof_time_entry >=  ", query_from, " 
                        AND A.flt_a_asp_prof_time_entry < TRUNC (SYSDATE)-0 
 
       AND A.flt_state IN ('TE', 'TA', 'AA')
     )
     
   
, CTRY_PAIR_FLIGHT as (
SELECT entry_date,
      CASE WHEN   dep_ctry <= arr_ctry
           THEN dep_ctry 
           ELSE   arr_ctry 
       END          
        ctry1, 
      CASE WHEN  dep_ctry <= arr_ctry
           THEN arr_ctry  
           ELSE   dep_ctry 
       END          
        ctry2 , 
        ao_id,
        ao_code,
        flt_uid
  FROM AIRP_FLIGHT 
 
  )
  
 , CTRY_PAIR_ARP_1 as (
 SELECT
        entry_date, 
        ctry1,
        ctry2, 
        ao_id,
        ao_code,
        flt_uid 
 FROM 
 CTRY_PAIR_FLIGHT 
 
 
 )  
 
 , CTRY_PAIR_ARP_2 as (
SELECT entry_date,
       ctry2 as ctry1,
       ctry1 as ctry2,
        ao_id,
        ao_code,
        flt_uid 
FROM CTRY_PAIR_ARP_1  
  WHERE ctry1 <> ctry2
 )
 
, CTRY_PAIR_ARP as (
 SELECT  ctry1 as iso_ct_code, entry_date,
 		ao_id,
        ao_code,
        flt_uid 
 FROM CTRY_PAIR_ARP_1
 UNION ALL
 SELECT  ctry1 as iso_ct_code, entry_date,
 		ao_id,
        ao_code,
        flt_uid 
 FROM  CTRY_PAIR_ARP_2
 ) 


select entry_date,
		iso_ct_code,  
 		ao_id,
        ao_code,
		count(flt_uid) as flight
 from CTRY_PAIR_ARP a
where ao_id != 99999
 group by entry_date,
 		iso_ct_code,  
 		ao_id,
        ao_code
ORDER BY iso_ct_code, flight desc
"
)

## st_ap ----
st_ap_day_base_query <- paste0("
with 

 DIM_APT as
(select * from prudev.pru_airport
), 
 
REL_AP_CTRY as (
select cfmu_ap_code ,
       CASE WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'GC' then 'IC'
            ELSE country_code 
       END iso_ct_code
 from  prudev.v_covid_rel_airport_area 

),  

DATA_FLIGHT_DEP as (
SELECT nvl(A.adep_day_all_trf,0) AS mvt,
     		    COALESCE(b.icao_code, 'ZZZZ') arp_icao_code,
				    COALESCE(b.id, 1618) as arp_pru_id, -- 1618 code for unknown
                'DEP' as airport_flow,
                 A.adep_DAY_FLT_DATE  AS FLIGHT_DATE
 FROM  prudev.v_aiu_agg_dep_day A 
     left outer  join DIM_APT b ON  ( A.adep_day_adep =  b.icao_code)

 where A.adep_DAY_FLT_DATE >=  ", query_from, "
        AND A.adep_DAY_FLT_DATE < TRUNC (SYSDATE)-0 
    
)

,DATA_FLIGHT_ARR as (
SELECT nvl(A.ades_day_all_trf,0) AS mvt,
     		    COALESCE(b.icao_code, 'ZZZZ') arp_icao_code,
				COALESCE(b.id, 1618) as arp_pru_id, -- 1618 code for unknown
                'ARR' as airport_flow,
                 A.ades_DAY_FLT_DATE  AS FLIGHT_DATE
 FROM  prudev.v_aiu_agg_arr_day A 
     left outer  join DIM_APT b ON  ( A.ades_day_ades_ctfm =  b.icao_code)
 
 where  A.ades_DAY_FLT_DATE >=  ", query_from, "
         AND A.ades_DAY_FLT_DATE < TRUNC (SYSDATE)-0 
     
)

SELECT FLIGHT_DATE,
        mvt, 
        a.arp_icao_code, 
        a.arp_pru_id,
      a.airport_flow,
        c.iso_ct_code
  FROM DATA_FLIGHT_DEP a   JOIN REL_AP_CTRY C on (a.arp_icao_code = c.cfmu_ap_code)                
UNION ALL -- union is all as we count all dep and arrival
 SELECT FLIGHT_DATE,
       mvt, 
        a.arp_icao_code, 
        a.arp_pru_id,
        a.airport_flow,
        c.iso_ct_code
  FROM DATA_FLIGHT_ARR a   JOIN REL_AP_CTRY C on (a.arp_icao_code = c.cfmu_ap_code) 
  ORDER BY iso_ct_code, mvt desc

"
)

## st_st ----
st_st_day_base_query <- paste0("
with 

 DIM_APT as
(select * from prudev.pru_airport
), 
 
REL_AP_CTRY as (
select cfmu_ap_code ,
       CASE WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'GC' then 'IC'
            WHEN country_code = 'SPAIN' then 'ES'
            ELSE country_code 
       END iso_ct_code
 from  prudev.v_covid_rel_airport_area 

), 

AIRP_FLIGHT as (
SELECT count(flt_uid) as mvt,
       TRUNC (flt_a_asp_prof_time_entry) AS entry_date,
       c1.iso_ct_code dep_ctry,
       c2.iso_ct_code arr_ctry
       
  FROM v_aiu_flt a, REL_AP_CTRY c1, REL_AP_CTRY c2
 WHERE A.flt_lobt >= ", query_from, " -2
                        AND A.flt_lobt < TRUNC (SYSDATE)+2
                        AND A.flt_a_asp_prof_time_entry >=  ", query_from, "
                        AND A.flt_a_asp_prof_time_entry < TRUNC (SYSDATE)
 
       AND A.flt_state IN ('TE', 'TA', 'AA')
       AND flt_dep_ad = c1.cfmu_ap_code and  flt_ctfm_ades = c2.cfmu_ap_code
       GROUP BY c1.iso_ct_code, c2.iso_ct_code, TRUNC (flt_a_asp_prof_time_entry)
     ),
     
    
CTRY_PAIR_FLIGHT as
(
SELECT entry_date,
      CASE WHEN   dep_ctry <= arr_ctry
           THEN dep_ctry 
           ELSE   arr_ctry 
       END          
        iso_ct_code1, 
      CASE WHEN  dep_ctry <= arr_ctry
           THEN arr_ctry  
           ELSE   dep_ctry 
       END          
        iso_ct_code2 , 
        mvt
  FROM AIRP_FLIGHT 
 
  ),
  
 CTRY_PAIR_ARP_1 as 
 (SELECT entry_date,
        iso_ct_code1,
        iso_ct_code2, 
        sum(mvt) as TOT_MVT 
 FROM 
 CTRY_PAIR_FLIGHT 
 group by entry_date, iso_ct_code1, iso_ct_code2
 
 ),  
 
 CTRY_PAIR_ARP_2 as 
(
SELECT entry_date,
      iso_ct_code2 as iso_ct_code1,
       iso_ct_code1 as iso_ct_code2,
       TOT_MVT
FROM CTRY_PAIR_ARP_1  
  WHERE iso_ct_code1 <> iso_ct_code2
 )

 SELECT entry_date, iso_ct_code1, iso_ct_code2, TOT_MVT 
 FROM CTRY_PAIR_ARP_1
 UNION ALL
 SELECT entry_date, iso_ct_code1, iso_ct_code2, TOT_MVT
 FROM  CTRY_PAIR_ARP_2
ORDER BY entry_date, iso_ct_code1, tot_mvt desc
 
 "
)

# AIRPORT ----
## ap_day -----
ap_traffic_delay_day_base_query <- paste0 (
" WITH 

 DIM_AIRPORT AS (
 	SELECT 
 	 code as arp_code,
 	 id AS arp_id
 FROM prudev.PRU_AIRPORT 
),

  arp_SYN_ARR
  AS
  (SELECT 
    SUM (nvl(f.ades_day_all_trf,0)) AS arr,
    f.ades_day_ades_ctfm AS arp_code,
    trunc(f.ades_DAY_FLT_DATE) AS flight_date
    FROM prudev.v_aiu_agg_arr_day f
    WHERE trunc(f.ades_DAY_FLT_DATE) >= ", query_from, "
    GROUP BY
    f.ades_day_ades_ctfm,
    trunc(f.ades_DAY_FLT_DATE)
  ),

  
  arp_SYN_DEP
  AS
  (SELECT 
    SUM (nvl(f.adep_day_all_trf,0)) AS dep,
    f.adep_day_adep AS arp_code,
    trunc(f.adep_DAY_FLT_DATE) AS flight_date
    FROM prudev.v_aiu_agg_dep_day f
    WHERE trunc(f.adep_DAY_FLT_DATE) >= ", query_from, "
    GROUP BY
    f.adep_day_adep,
    trunc(f.adep_DAY_FLT_DATE)
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
    tdm_arp_dep_na
    
    FROM prudev.v_aiu_agg_flt_flow a
    WHERE
    a.agg_flt_a_first_entry_date >= ", query_from, "
    and a.AGG_FLT_MP_REGU_LOC_TY = 'Airport'
    GROUP BY
    a.agg_flt_a_first_entry_date,
    a.ref_loc_id
  ),
  
  ALL_DAY_DATA as (
    SELECT
    coalesce(coalesce(a.arp_code,c.arp_code), b.arp_code) arp_code,
    coalesce(coalesce(a.flight_date,c.flight_date), b.entry_date) flight_date,
    coalesce(a.arr, 0) arr,
    coalesce(c.dep, 0) dep, 
    coalesce(TDM_ARP,0) as TDM_ARP,
    coalesce(TDF_ARP,0) as TDF_ARP,
    coalesce(TRF_ARP,0) as TRF_ARP,
    coalesce(TDM_15_ARP,0) as TDM_15_ARP,
    coalesce(TDF_15_ARP,0) as TDF_15_ARP,
    coalesce(TRF_15_ARP,0) as TRF_15_ARP,
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
    coalesce(TDM_ARP_ARR,0) as TDM_ARP_ARR,
    coalesce(TDF_ARP_ARR,0) as TDF_ARP_ARR,
    coalesce(TRF_ARP_ARR,0) as TRF_ARP_ARR,
    coalesce(TDM_15_ARP_ARR,0) as TDM_15_ARP_ARR,
    coalesce(TDF_15_ARP_ARR,0) as TDF_15_ARP_ARR,
    coalesce(TRF_15_ARP_ARR,0) as TRF_15_ARP_ARR,
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
    coalesce(TDM_ARP_DEP,0) as TDM_ARP_DEP,
    coalesce(TDF_ARP_DEP,0) as TDF_ARP_DEP,
    coalesce(TRF_ARP_DEP,0) as TRF_ARP_DEP,
    coalesce(TDM_15_ARP_DEP,0) as TDM_15_ARP_DEP,
    coalesce(TDF_15_ARP_DEP,0) as TDF_15_ARP_DEP,
    coalesce(TRF_15_ARP_DEP,0) as TRF_15_ARP_DEP,
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
    coalesce(TDM_ARP_DEP_NA,0) as TDM_ARP_DEP_NA    
    FROM arp_SYN_ARR A
    FULL OUTER JOIN arp_SYN_dep c on a.arp_code  = c.arp_code  and a.flight_date = c.flight_date
    FULL OUTER JOIN ARP_DELAY b on a.arp_code  = B.arp_code  and a.flight_date = b.entry_date
    
  )
  
  SELECT
  a.arp_code,
  b.arp_id,
  flight_date,
  sum(arr) as arr,
  sum(dep) as dep,
  sum(dep) + sum(arr) dep_arr,
  sum(TDM_ARP) as TDM_ARP,
  sum(TDF_ARP) as TDF_ARP,
  sum(TRF_ARP) as TRF_ARP,
  sum(TDM_15_ARP) as TDM_15_ARP,
  sum(TDF_15_ARP) as TDF_15_ARP,
  sum(TRF_15_ARP) as TRF_15_ARP,
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
  sum(TDM_ARP_ARR) as TDM_ARP_ARR,
  sum(TDF_ARP_ARR) as TDF_ARP_ARR,
  sum(TRF_ARP_ARR) as TRF_ARP_ARR,
  sum(TDM_15_ARP_ARR) as TDM_15_ARP_ARR,
  sum(TDF_15_ARP_ARR) as TDF_15_ARP_ARR,
  sum(TRF_15_ARP_ARR) as TRF_15_ARP_ARR,
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
  sum(TDM_ARP_DEP) as TDM_ARP_DEP,
  sum(TDF_ARP_DEP) as TDF_ARP_DEP,
  sum(TRF_ARP_DEP) as TRF_ARP_DEP,
  sum(TDM_15_ARP_DEP) as TDM_15_ARP_DEP,
  sum(TDF_15_ARP_DEP) as TDF_15_ARP_DEP,
  sum(TRF_15_ARP_DEP) as TRF_15_ARP_DEP,
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
  sum(TDM_ARP_DEP_NA) as TDM_ARP_DEP_NA
  FROM ALL_DAY_DATA a
  LEFT JOIN dim_airport b ON a.arp_code = b.arp_code
  GROUP BY flight_date, a.arp_code, b.arp_id
  order by arp_code,  flight_date
  "
)

## ap_ao ----
ap_ao_day_base_query <- paste0("
WITH

DIM_AO
 as ( SELECT distinct
 		ao_id,
 		ao_code, 
 		wef,
 		til
 from  ldw_acc.AO_GROUPS_ASSOCIATION
 ) ,   

DIM_APT as
(select * from prudev.PRU_AIRPORT
--WHERE code = 'EBBR'
)
                               

, DATA_DAY AS (
SELECT  
        b.id as dep_arp_pru_id,
        c.id as arr_arp_pru_id,
        CASE WHEN (TRUNC(A.flt_a_asp_prof_time_entry) >= d.wef AND TRUNC(A.flt_a_asp_prof_time_entry) <= d.til)
        		THEN nvl(d.ao_id, 1777 ) 
        		ELSE 99999
        END ao_id, 
        nvl(d.ao_code,'ZZZ') ao_code,
        TRUNC(A.flt_a_asp_prof_time_entry) ENTRY_DATE,
        A.flt_uid
FROM prudev.v_aiu_flt A 
     left outer  join DIM_APT b ON  ( A.flt_dep_ad=  b.icao_code)
     left outer join DIM_APT C  ON  (A.flt_ctfm_ades = C.icao_code)
     left outer join DIM_AO d  on ( (a.ao_icao_id = d.ao_code ) )
WHERE  
     A.flt_lobt>= ", query_from, " -2 
     AND A.flt_lobt < TRUNC (SYSDATE) +2
     AND flt_a_asp_prof_time_entry >= ", query_from, "
     AND flt_a_asp_prof_time_entry < TRUNC (SYSDATE)

     AND A.flt_state IN ('TE', 'TA', 'AA')
),


DATA_DEP AS (
SELECT  
		dep_arp_pru_id AS arp_pru_id, 
        a.ENTRY_DATE,
        ao_code,
        ao_id,
        COUNT(a.flt_uid) DEP_ARR
FROM DATA_DAY a   
where ao_id != 99999
GROUP BY  
		dep_arp_pru_id, 
        ENTRY_DATE,
        ao_code,
        ao_id
),        
 
DATA_ARR AS (
SELECT  
		arr_arp_pru_id AS arp_pru_id, 
        ENTRY_DATE,
        ao_code,
        ao_id,
        COUNT(a.flt_uid) DEP_ARR
FROM DATA_DAY a  
where ao_id != 99999
GROUP BY  arr_arp_pru_id,
          ENTRY_DATE,
        ao_code,
        ao_id
        )

SELECT  
          coalesce(a.entry_date, b.entry_date) as entry_date,  
          coalesce(a.arp_pru_id,b.arp_pru_id) as arp_pru_id,
          coalesce(  a.ao_id,b.ao_id) as ao_id,
          coalesce(  a.ao_code, b.ao_code) as ao_code,
          coalesce(a.DEP_ARR,0)  + coalesce( b.DEP_ARR,0)  as DEP_ARR
FROM DATA_DEP a 
full outer join DATA_ARR b  on (a.arp_pru_id = b.arp_pru_id  and a.entry_date = b.entry_date and a.ao_id = b.ao_id and a.ao_code = b.ao_code)
oRDER BY dep_arr desc  
"
)

## ap_st_des ----
ap_st_des_day_base_query <- paste0("
with 

DIM_APT as
(select 
		a.id,
		a.icao_code,
		b.AIU_ISO_COUNTRY_CODE,
		b.AIU_ISO_COUNTRY_NAME 
from prudev.pru_airport a
LEFT JOIN  prudev.pru_country_iso b
ON a.ISO_COUNTRY_CODE = b.ec_ISO_COUNTRY_CODE
),       

ARP_FLIGHT as (
SELECT flt_uid,
       TRUNC (flt_a_asp_prof_time_entry) AS entry_date, 
        COALESCE(b.id, 1618) as dep_arp_pru_id, -- 1618 code for unknown
        COALESCE(c.AIU_ISO_COUNTRY_CODE, '##') as arr_iso_cty_code -- ## code for unknown      
  FROM prudev.v_aiu_flt a
     left outer  join DIM_APT b ON  ( A.flt_dep_ad=  b.icao_code)
     left outer join DIM_APT C  ON  (A.flt_ctfm_ades = C.icao_code)
 WHERE     
     A.flt_lobt>= ", query_from, "  -2  
     AND A.flt_lobt < TRUNC (SYSDATE) +2
     AND a.flt_a_asp_prof_time_entry >= ", query_from, " 
     AND a.flt_a_asp_prof_time_entry < TRUNC (SYSDATE)
     and extract (year from flt_a_asp_prof_time_entry) IN (2019, extract(year from (TRUNC (SYSDATE)-1))-1,extract(year from (TRUNC (SYSDATE)-1)))
       AND A.flt_state IN ('TE', 'TA', 'AA')
     )
     
SELECT ENTRY_DATE,
		dep_arp_pru_id,
		arr_iso_cty_code,
       count(flt_uid) AS dep
 FROM  ARP_FLIGHT A 
GROUP BY ENTRY_DATE, dep_arp_pru_id, arr_iso_cty_code
ORDER BY A.ENTRY_DATE DESC , A.DEP_ARP_PRU_ID,  dep DESC
"
)

## ap_ap_des ----
ap_ap_des_day_base_query <- paste0("
with 

DIM_APT as
(select * from prudev.pru_airport
),       

ARP_FLIGHT as (
SELECT flt_uid,
       TRUNC (flt_a_asp_prof_time_entry) AS ENTRY_DATE, 
        COALESCE(b.id, 1618) as dep_arp_pru_id, -- 1618 code for unknown
        COALESCE(c.id, 1618) as arr_arp_pru_id
  FROM prudev.v_aiu_flt a
     left outer  join DIM_APT b ON  ( A.flt_dep_ad=  b.icao_code)
     left outer join DIM_APT C  ON  (A.flt_ctfm_ades = C.icao_code)
 WHERE      
     A.flt_lobt>= ", query_from, " -2 
     	AND A.flt_lobt < TRUNC (SYSDATE) + 2
     AND a.flt_a_asp_prof_time_entry >= ", query_from, "
     AND a.flt_a_asp_prof_time_entry < TRUNC (SYSDATE)
       AND A.flt_state IN ('TE', 'TA', 'AA')
)

SELECT ENTRY_DATE,
		dep_arp_pru_id,
		arr_arp_pru_id,
       count(flt_uid) AS dep
 FROM  ARP_FLIGHT A 
GROUP BY ENTRY_DATE, dep_arp_pru_id, arr_arp_pru_id
ORDER BY A.ENTRY_DATE DESC , A.DEP_ARP_PRU_ID,  dep DESC
"
)

## ap_ms ----
ap_ms_day_base_query <- paste0("
WITH

dim_market_segment as 
(select SK_FLT_TYPE_RULE_ID AS ms_id,  
 		rule_description as market_segment
 from  SWH_FCT.DIM_FLIGHT_TYPE_RULE
),
 
DIM_APT as
(select * from prudev.pru_airport)       

 
, DATA_SOURCE as (
SELECT  
        b.id as dep_arp_pru_id,
        c.id as arr_arp_pru_id,
        TRUNC(A.flt_a_asp_prof_time_entry) ENTRY_DATE,
        ms_id,
        A.flt_uid
FROM prudev.v_aiu_flt_mark_seg A 
     left outer  join DIM_APT b ON  ( A.flt_dep_ad=  b.icao_code)
     left outer join DIM_APT C  ON  (A.flt_ctfm_ades = C.icao_code)
     left outer join dim_market_segment   d  ON (a.SK_FLT_TYPE_RULE_ID = d.ms_id )
 WHERE  
         A.flt_lobt>= ", query_from, " -2  
         	AND A.flt_lobt < TRUNC (SYSDATE) + 2
			  	AND a.flt_a_asp_prof_time_entry >= ", query_from, "
			  	AND a.flt_a_asp_prof_time_entry < TRUNC (SYSDATE)
    AND A.flt_state IN ('TE','TA','AA')

),


DATA_DEP AS (
(SELECT  
    		dep_arp_pru_id AS arp_pru_id,
        a.ENTRY_DATE,
        ms_id,
        COUNT(a.flt_uid) DEP_ARR
FROM DATA_SOURCE a   
GROUP BY  
        dep_arp_pru_id,
        ENTRY_DATE,
        ms_id
)
),        
 
DATA_ARR AS (
SELECT  
    		arr_arp_pru_id AS arp_pru_id,
        ENTRY_DATE,
        ms_id,
        COUNT(a.flt_uid) DEP_ARR
FROM DATA_SOURCE a  
GROUP BY  arr_arp_pru_id,
          ENTRY_DATE,
        ms_id
)


SELECT  
          coalesce(a.entry_date, b.entry_date) as entry_date,  
          coalesce(a.arp_pru_id, b.arp_pru_id) as arp_pru_id, 
          coalesce(a.ms_id, b.ms_id) as ms_id,
          coalesce(a.DEP_ARR, 0) + coalesce(b.DEP_ARR, 0)  as DEP_ARR
FROM DATA_DEP a full outer join DATA_ARR b  on (a.arp_pru_id = b.arp_pru_id  and a.entry_date = b.entry_date and a.ms_id = b.ms_id)
ORDER BY entry_date, arp_pru_id, ms_id
"
)

# AIRLINE ----
## ao_day ----
ao_traffic_delay_day_base_query <- paste0("
with 
DIM_AO
 as ( SELECT distinct
 		ao_id,
 		ao_code, 
 		wef,
 		til
 from  ldw_acc.AO_GROUPS_ASSOCIATION
 ) ,
 
 
AO_GRP_DAY AS (
SELECT a.ao_id,
        CASE WHEN (TRUNC(t.day_date) >= a.wef AND TRUNC(t.day_date) <= a.til)
        		THEN a.ao_code 
        		ELSE NULL
        END ao_code, 
        t.day_date,
        t.month,
        t.week,
        t.week_nb_year,
        t.day_type,
        t.day_of_week_nb AS day_of_week,
        t.year
FROM DIM_AO a, prudev.pru_time_references t
WHERE  
   t.day_date >= ", query_from, "
    AND t.day_date < trunc(sysdate) 
       ),

DATA_FLIGHT as (
SELECT  
        CASE WHEN (TRUNC(A.flt_a_asp_prof_time_entry) >= b.wef AND TRUNC(A.flt_a_asp_prof_time_entry) <= b.til)
        		THEN nvl(b.ao_id, 1777 ) 
        		ELSE 99999
        END ao_id, 
        nvl(b.ao_code,'ZZZ') ao_code,
        nvl(atfm_delay,0) as TDM, 
        case when nvl(ATFM_delay,0) > 15 then ATFM_DELAY else 0 end as TDM_15,
        case when nvl(ATFM_delay,0) > 0 then 1 else 0 end as TDF,
        case when nvl(ATFM_delay,0) > 15 then 1 else 0 end as TDF_15,
        trunc(A.flt_a_asp_prof_time_entry ) as entry_date,
        extract(month from A.flt_a_asp_prof_time_entry ) as month,
        extract(year from A.flt_a_asp_prof_time_entry ) as year,
        A.flt_uid 
FROM prudev.v_aiu_flt A 
     inner join  DIM_AO b ON   (a.ao_icao_id = b.ao_code)
     
WHERE  A.flt_lobt>= ", query_from, " -2 
   AND A.flt_lobt < TRUNC (SYSDATE) +2
   AND a.flt_a_asp_prof_time_entry >= ", query_from, "
   AND A.flt_a_asp_prof_time_entry <  TRUNC(SYSDATE)
   AND A.flt_state IN ('TE', 'TA', 'AA')
   and extract (year from flt_a_asp_prof_time_entry) <= extract(year from (TRUNC (SYSDATE)-1))
),

DATA_DAY
    AS
 (SELECT     
        ao_id, 
        ao_code,
        entry_date,
        COUNT(flt_uid) DAY_TFC,
        SUM(TDM)as DAY_DLY,
        SUM(TDM_15) as DAY_DLY_15,
        SUM(TDF) as DAY_DELAYED_TFC,
        SUM(TDF_15) as DAY_DELAYED_TFC_15
        
FROM DATA_FLIGHT
WHERE ao_id != 99999
GROUP BY ao_id, 
         ao_code,
         entry_date
  ) 
                       
SELECT a.YEAR,
       a.MONTH,
       a.day_date  AS flight_date,
       a.WEEK,
       a.WEEK_NB_YEAR,
       a.DAY_TYPE,
       a.day_of_week,      
       a.ao_id,
       a.ao_code,
       coalesce(b.DAY_TFC,0) AS DAY_TFC,
       coalesce(b.DAY_DLY,0) AS DAY_DLY,      
       coalesce(b.DAY_DLY_15,0) AS DAY_DLY_15,
       coalesce(b.DAY_DELAYED_TFC,0) AS DAY_DELAYED_TFC,
       coalesce(b.DAY_DELAYED_TFC_15,0) AS DAY_DELAYED_TFC_15       
       FROM ao_grp_day  A
       LEFT JOIN DATA_DAY B
           ON a.ao_id = b.ao_id AND a.ao_code = b.ao_code AND b.entry_date = a.day_date

"
)

## ao_st_des ----
ao_st_des_day_base_query <- paste0("
with 

DIM_AO
 as ( SELECT distinct
 		ao_id,
 		ao_code, 
 		wef,
 		til
 from  ldw_acc.AO_GROUPS_ASSOCIATION
 ) , 

DIM_APT as
(select 
		a.id,
		a.icao_code,
		b.AIU_ISO_COUNTRY_CODE,
		b.AIU_ISO_COUNTRY_NAME 
from prudev.pru_airport a
LEFT JOIN  prudev.pru_country_iso b
ON a.ISO_COUNTRY_CODE = b.ec_ISO_COUNTRY_CODE
)


, DATA_DAY AS (
SELECT  
        COALESCE(c.AIU_ISO_COUNTRY_CODE, '##') as arr_iso_cty_code, -- ## code for unknown      
        CASE WHEN (TRUNC(A.flt_a_asp_prof_time_entry) >= d.wef AND TRUNC(A.flt_a_asp_prof_time_entry) <= d.til)
        		THEN nvl(d.ao_id, 1777 ) 
        		ELSE 99999
        END ao_id, 
        nvl(d.ao_code,'ZZZ') ao_code,
        TRUNC(A.flt_a_asp_prof_time_entry) ENTRY_DATE,
        A.flt_uid
FROM prudev.v_aiu_flt A 
--     left outer  join DIM_APT b ON  ( A.flt_dep_ad=  b.icao_code)
     left outer join DIM_APT C  ON  (A.flt_ctfm_ades = C.icao_code)
     left outer join DIM_AO d  on ( (a.ao_icao_id = d.ao_code ) )
WHERE  
     A.flt_lobt>= ", query_from, " -2 
     AND A.flt_lobt < TRUNC (SYSDATE) +2
     AND flt_a_asp_prof_time_entry >= ", query_from, "
     AND flt_a_asp_prof_time_entry < TRUNC (SYSDATE)

     AND A.flt_state IN ('TE', 'TA', 'AA')
)

 SELECT 
       a.entry_date,
      a.ao_id,
      a.ao_code,
      a.arr_iso_cty_code,
     count(flt_uid) as flight
  FROM DATA_DAY a  
  where ao_id != 99999
GROUP BY  
       a.entry_date,
      a.ao_id,
      a.ao_code,
      a.arr_iso_cty_code
ORDER BY a.entry_date, a.ao_id, flight desc     
"
)


## ao_ap_dep ----
ao_ap_dep_day_base_query <- paste0("
with 

DIM_AO
 as ( SELECT distinct
 		ao_id,
 		ao_code, 
 		wef,
 		til
 from  ldw_acc.AO_GROUPS_ASSOCIATION
 ) , 

DIM_APT as
(select 
		a.id,
		a.icao_code,
		b.AIU_ISO_COUNTRY_CODE,
		b.AIU_ISO_COUNTRY_NAME 
from prudev.pru_airport a
LEFT JOIN  prudev.pru_country_iso b
ON a.ISO_COUNTRY_CODE = b.ec_ISO_COUNTRY_CODE
)


, DATA_DAY AS (
SELECT  
         COALESCE(b.id, 1618) as dep_arp_pru_id, -- 1618 code for unknown
         CASE WHEN (TRUNC(A.flt_a_asp_prof_time_entry) >= d.wef AND TRUNC(A.flt_a_asp_prof_time_entry) <= d.til)
        		THEN nvl(d.ao_id, 1777 ) 
        		ELSE 99999
        END ao_id, 
        nvl(d.ao_code,'ZZZ') ao_code,
        TRUNC(A.flt_a_asp_prof_time_entry) ENTRY_DATE,
        A.flt_uid
FROM prudev.v_aiu_flt A 
     left outer  join DIM_APT b ON  ( A.flt_dep_ad=  b.icao_code)
--     left outer join DIM_APT C  ON  (A.flt_ctfm_ades = C.icao_code)
     left outer join DIM_AO d  on ( (a.ao_icao_id = d.ao_code ) )
WHERE  
     A.flt_lobt>= ", query_from, " -2 
     AND A.flt_lobt < TRUNC (SYSDATE) +2
     AND flt_a_asp_prof_time_entry >= ", query_from, "
     AND flt_a_asp_prof_time_entry < TRUNC (SYSDATE)

     AND A.flt_state IN ('TE', 'TA', 'AA')
)

 SELECT 
       a.entry_date,
      a.ao_id,
      a.ao_code,
      a.dep_arp_pru_id,
     count(flt_uid) as flight
  FROM DATA_DAY a  
  where ao_id != 99999
GROUP BY  
       a.entry_date,
      a.ao_id,
      a.ao_code,
      a.dep_arp_pru_id
ORDER BY a.entry_date, a.ao_id, flight desc     
"
)


## ao_ap_pair ----
ao_ap_pair_day_base_query <- paste0("
with 
DIM_AO
 as ( SELECT distinct
 		ao_id,
 		ao_code, 
 		wef,
 		til
 from  ldw_acc.AO_GROUPS_ASSOCIATION
 ) , 

DIM_APT as
(select 
		a.id,
		a.icao_code,
		b.AIU_ISO_COUNTRY_CODE,
		b.AIU_ISO_COUNTRY_NAME 
from prudev.pru_airport a
LEFT JOIN  prudev.pru_country_iso b
ON a.ISO_COUNTRY_CODE = b.ec_ISO_COUNTRY_CODE
)


, DATA_DAY AS (
SELECT  
-- 1618 code for unknown

        case when  COALESCE(b.id, 1618) <= COALESCE(c.id, 1618)
        	 THEN  COALESCE(c.id, 1618)
        	 else COALESCE(b.id, 1618)
        END arp_pru_id_1,
        
        case when  COALESCE(b.id, 1618) <= COALESCE(c.id, 1618)
        	 THEN  COALESCE(b.id, 1618)
        	 else COALESCE(c.id, 1618)
        END arp_pru_id_2,
        
        CASE WHEN (TRUNC(A.flt_a_asp_prof_time_entry) >= d.wef AND TRUNC(A.flt_a_asp_prof_time_entry) <= d.til)
        		THEN nvl(d.ao_id, 1777 ) 
        		ELSE 99999
        END ao_id, 
        
        nvl(d.ao_code,'ZZZ') ao_code,
        TRUNC(A.flt_a_asp_prof_time_entry) ENTRY_DATE,
        A.flt_uid
FROM prudev.v_aiu_flt A 
     left outer  join DIM_APT b ON  ( A.flt_dep_ad=  b.icao_code)
     left outer join DIM_APT C  ON  (A.flt_ctfm_ades = C.icao_code)
     left outer join DIM_AO d  on ( (a.ao_icao_id = d.ao_code ) )
WHERE  
     A.flt_lobt>= ", query_from, " -2 
     AND A.flt_lobt < TRUNC (SYSDATE) +2
     AND flt_a_asp_prof_time_entry >= ", query_from, "
     AND flt_a_asp_prof_time_entry < TRUNC (SYSDATE)

     AND A.flt_state IN ('TE', 'TA', 'AA')
)

 SELECT 
       a.entry_date,
      a.ao_id,
      a.ao_code,
      a.arp_pru_id_1,
      a.arp_pru_id_2,
     count(flt_uid) as flight
  FROM DATA_DAY a  
  where ao_id != 99999
GROUP BY  
       a.entry_date,
      a.ao_id,
      a.ao_code,
      a.arp_pru_id_1,
      a.arp_pru_id_2
ORDER BY a.entry_date, a.ao_id, flight desc     
"
)


## ao_ap_arr_delay ----
ao_ap_arr_delay_day_base_query <- paste0("
WITH

DIM_AO
 as ( SELECT distinct
 		ao_id,
 		ao_code, 
 		wef,
 		til
 from  ldw_acc.AO_GROUPS_ASSOCIATION
 ) , 
                              

 DIM_APT as
(select * from prudev.PRU_AIRPORT
),
   
 
 Apt_arr_reg AS
        (SELECT DISTINCT
                Agg_flt_lobt     Flts_lobt
              , Agg_flt_mp_regu_id
              , Ref_loc_id       Reg_arpt
           FROM Prudev.V_aiu_agg_flt_flow
          WHERE Mp_regu_loc_cat = 'Arrival'
            AND Agg_flt_mp_regu_loc_ty = 'Airport'
            and agg_flt_lobt >= ", query_from, "
 )
 
   
, DATA_DAY AS (
SELECT  
        Flt_ftfm_ades,
        Flt_ctfm_ades,
        Flt_most_penal_regu_id,
        Atfm_delay,
        CASE WHEN (TRUNC(A.Arvt_3) >= d.wef AND TRUNC(A.Arvt_3) <= d.til)
        		THEN nvl(d.ao_id, 1777 ) 
        		ELSE 99999
        END ao_id, 
        nvl(d.ao_code,'ZZZ') ao_code,
        TRUNC(A.Arvt_3) arr_date,
        TRUNC(A.Flt_lobt) flt_lobt,
        A.flt_uid
FROM prudev.v_aiu_flt A 
     left outer join DIM_AO d  on ( (a.ao_icao_id = d.ao_code ) )
WHERE  
            TRUNC(A.Arvt_3) >= ", query_from, "
            AND TRUNC(A.Arvt_3) < trunc(sysdate)
            AND Flt_lobt >= ", query_from, " -2
            AND Flt_lobt < trunc(sysdate) +2

     AND A.flt_state IN ('TE', 'TA', 'AA')
),

            
            
    Apt_arr_delay_ao AS
        (
        SELECT DISTINCT
              arr_date
              , ao_id
              , ao_code
              , Flt_ctfm_ades
              , COUNT(*)
                    OVER(
                        PARTITION BY arr_date
                                   , ao_code
                                   , Flt_ctfm_ades
                    )                 Flts
              , SUM(NVL2(Agg_flt_mp_regu_id, decode(atfm_delay,NULL,0,0,0, 1),0))
                    OVER(
                        PARTITION BY arr_date
                                   , ao_code
                                   , Flt_ctfm_ades
                    )                 AS Delayed_flts
              , SUM(NVL2(Agg_flt_mp_regu_id, Atfm_delay, 0))
                    OVER(
                        PARTITION BY arr_date
                                   , ao_code
                                   , Flt_ctfm_ades
                    )                 AS Delay_amnt
           FROM DATA_DAY  A
                LEFT JOIN Apt_arr_reg B
                    ON (Flt_lobt = B.Flts_lobt
                    AND A.Flt_most_penal_regu_id =
                        B.Agg_flt_mp_regu_id
                    AND A.Flt_ftfm_ades = B.Reg_arpt)
            where ao_id != 99999

 )

        SELECT DISTINCT 
        				arr_date
                       , Ao_code
                       , ao_id
					             , c.id as arr_arp_pru_id
                       , SUM(Flts) Flts
                       , SUM(Delayed_flts) Delayed_flts
                       , SUM(Delay_amnt) Delay_amnt
           FROM Apt_arr_delay_ao a
     left outer join DIM_APT C  ON  (A.flt_ctfm_ades = C.icao_code)
     GROUP BY arr_date, ao_code, ao_id, c.id    
order BY arr_date DESC, flts DESC, ao_code 

"
)