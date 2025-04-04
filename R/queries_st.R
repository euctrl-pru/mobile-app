# daio ----
query_state_daio_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
WITH

 DATA_DAY
    AS
 (SELECT     agg_asp_entry_date as flight_date,
             agg_asp_id COUNTRY_CODE,
             agg_asp_ty as TYPE,
             CASE WHEN agg_asp_id = 'LW' THEN 'North Macedonia'
                WHEN agg_asp_id = 'LT' THEN 'Türkiye'
                  WHEN agg_asp_id = 'LQ' then 'Bosnia and Herzegovina'
                  WHEN agg_asp_id = 'LY' then 'Serbia/Montenegro'
                  WHEN agg_asp_id = 'LE' then 'Spain Continental'
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
             agg_asp_entry_date >= '24-DEC-2018' AND a.AGG_ASP_ENTRY_DATE < ", mydate, "
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
   AND t.day_date <= to_date('31-12-'|| extract(year from (", mydate, "-1)),'dd-mm-yyyy')
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

       CASE WHEN flight_DATE >= ", mydate, " THEN NULL
           ELSE DAY_TFC - DAY_TFC_PREV_WEEK
       END DAY_TFC_DIFF_PREV_WEEK,
       CASE WHEN flight_DATE >= ", mydate, " THEN NULL
           ELSE DAY_TFC - DAY_TFC_PREV_YEAR
       END DAY_TFC_DIFF_PREV_YEAR,
       CASE WHEN flight_DATE >= ", mydate, " THEN NULL
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

       CASE WHEN flight_DATE >= ", mydate, "
           THEN NULL
           ELSE AVG_rolling_week
       END AVG_rolling_week,
       AVG_rolling_PREV_WEEK,
       AVG_rolling_week_PREV_YEAR,
       AVG_rolling_week_2020,
       AVG_rolling_week_2019,

      CASE WHEN AVG_rolling_week_PREV_YEAR <> 0 and flight_DATE < ", mydate, "
           THEN avg_rolling_week/AVG_rolling_week_PREV_YEAR -1
           ELSE NULL
       END  DIF_WEEK_PREV_YEAR_PERC,
      CASE WHEN AVG_rolling_week_2019 <> 0 and flight_DATE < ", mydate, "
           THEN avg_rolling_week/AVG_rolling_week_2019 -1
           ELSE NULL
       END  DIF_ROLLING_WEEK_2019_perc,

       Y2D_TFC_YEAR,
       Y2D_TFC_PREV_YEAR,
       Y2D_TFC_2019,
       CASE WHEN flight_DATE >= ", mydate, "
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
        ", mydate, " -1 as LAST_DATA_DAY

      FROM DATA_COUNTRY_3
      where flight_DATE >=to_date('01-01-'|| extract(year from (", mydate, "-1)),'dd-mm-yyyy')
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

       CASE WHEN flight_DATE >= ", mydate, " THEN NULL
           ELSE case when FLIGHT_DATE >='01-jan-2024' then DAY_TFC - DAY_TFC_PREV_WEEK
                     else NULL
                end
       END DAY_TFC_DIFF_PREV_WEEK,
       CASE WHEN flight_DATE >= ", mydate, " THEN NULL
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

       CASE WHEN flight_DATE >= ", mydate, " THEN NULL
           ELSE case when FLIGHT_DATE >='01-jan-2024' then AVG_rolling_week
                     else NULL
                end
       END AVG_rolling_week,

       case when FLIGHT_DATE >='01-jan-2024' then AVG_rolling_PREV_WEEK else NULL end AVG_rolling_PREV_WEEK,
       case when FLIGHT_DATE >='01-jan-2025' then AVG_rolling_week_PREV_YEAR else NULL end AVG_rolling_week_PREV_YEAR,
       NULL as AVG_rolling_week_2020,
       NULL as AVG_rolling_week_2019,

       CASE WHEN AVG_rolling_week_PREV_YEAR <> 0 and flight_DATE < ", mydate, " AND FLIGHT_DATE >='01-jan-2025'
           THEN avg_rolling_week/AVG_rolling_week_PREV_YEAR -1
           ELSE NULL
       END  DIF_WEEK_PREV_YEAR_PERC,
       NULL as  DIF_ROLLING_WEEK_2019_perc,

       case when FLIGHT_DATE >='01-jan-2024' then Y2D_TFC_YEAR else NULL end Y2D_TFC_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then Y2D_TFC_PREV_YEAR else NULL end Y2D_TFC_PREV_YEAR,
       NULL as Y2D_TFC_2019,
       CASE WHEN flight_DATE >= ", mydate, "
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

       ", mydate, " -1 as LAST_DATA_DAY

      FROM DATA_COUNTRY_3
      where flight_DATE >=to_date('01-01-'|| extract(year from (", mydate, "-1)),'dd-mm-yyyy')
      AND country_name in ('ICELAND', 'Iceland')
      order by country_name, flight_date
      "
)
}

# dai ----
query_state_dai_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
WITH

COUNTRY_ICAO2LETTER  as (
 SELECT COUNTRY_ID  ICAO2LETTER,
        CASE
             --WHEN COUNTRY_ID = 'GC' then 'LE'
             WHEN COUNTRY_ID = 'GE' then 'LE'
             WHEN COUNTRY_ID = 'ET' then 'ED'
             ELSE COUNTRY_ID
        END  COUNTRY_CODE,
         CASE WHEN COUNTRY_ID = 'GC' then 'SPAIN CANARIES'
             WHEN COUNTRY_ID = 'GE' then 'SPAIN CONTINENTAL'
             WHEN COUNTRY_ID = 'LE' then 'SPAIN CONTINENTAL'
             WHEN COUNTRY_ID = 'ET' then 'GERMANY'
             WHEN COUNTRY_ID = 'LT' then 'TÜRKIYE'
             WHEN COUNTRY_ID = 'LQ' then 'BOSNIA AND HERZEGOVINA'
             WHEN COUNTRY_ID = 'LY' then 'SERBIA/MONTENEGRO'
             ELSE  UPPER(COUNTRY_NAME)
        END  COUNTRY_NAME
 FROM  ARU_SYN.stat_coda_country
 WHERE
  (  (SUBSTR(COUNTRY_ID,1,1) IN ('E','L')
       OR SUBSTR(COUNTRY_ID,1,2) IN ('GC','GM','GE','UD','UG','UK','BI'))  )
  AND  COUNTRY_ID not in ('LV', 'LX', 'EU','LN')
 ) ,

LIST_COUNTRY as (
select  COUNTRY_NAME
FROM COUNTRY_ICAO2LETTER
group by  COUNTRY_NAME),

 CTRY_DAY AS (
SELECT a.COUNTRY_NAME,
        t.day_date,
        t.month,
        t.week,
        t.week_nb_year,
        t.day_type,
        t.day_of_week_nb AS day_of_week,
        t.year
FROM LIST_COUNTRY a, prudev.pru_time_references t
WHERE
   t.day_date >= to_date('24-12-2018','DD-MM-YYYY')
   AND t.day_date <= to_date('31-12-'|| extract(year from (", mydate, "-1)),'dd-mm-yyyy')
       ),


DATA_DEP AS (
(SELECT
        B.COUNTRY_NAME ,
        TRUNC(A.flt_a_asp_prof_time_entry) flight_DATE,
        COUNT(a.flt_uid) DAY_TFC
FROM prudev.v_aiu_flt a,
     COUNTRY_ICAO2LETTER b
WHERE  SUBSTR(A.flt_dep_ad,1,2) =  b.ICAO2LETTER
--    AND A.flt_lobt >= to_date('24-12-2018','DD-MM-YYYY') -2
--    AND A.flt_lobt <  ", mydate, " + 2
    AND A.flt_a_asp_prof_time_entry >= to_date('24-12-2018','DD-MM-YYYY')
    AND A.flt_a_asp_prof_time_entry <  ", mydate, "
    AND A.flt_state IN ('TE','TA','AA')
GROUP BY  B.COUNTRY_NAME  ,
        TRUNC(A.flt_a_asp_prof_time_entry)
)
),

DATA_ARR AS (
SELECT
        C.COUNTRY_NAME ,
        TRUNC(A.flt_a_asp_prof_time_entry) flight_DATE,
        COUNT(a.flt_uid) DAY_TFC
FROM prudev.v_aiu_flt a,
     COUNTRY_ICAO2LETTER c
WHERE
     SUBSTR(A.flt_ctfm_ades,1,2) = C.ICAO2LETTER
--    AND A.flt_lobt >= to_date('24-12-2018','DD-MM-YYYY') -2
--    AND A.flt_lobt <  ", mydate, " + 2
    AND A.flt_a_asp_prof_time_entry >= to_date('24-12-2018','DD-MM-YYYY')
    AND A.flt_a_asp_prof_time_entry <  ", mydate, "
    AND A.flt_state IN ('TE','TA','AA')
GROUP BY  C.COUNTRY_NAME  ,
        TRUNC(A.flt_a_asp_prof_time_entry)
),

DATA_DOMESTIC as
(SELECT
        B.COUNTRY_NAME ,
        TRUNC(A.flt_a_asp_prof_time_entry) FLIGHT_DATE,
        COUNT(a.flt_uid) DAY_TFC
FROM prudev.v_aiu_flt a,
     COUNTRY_ICAO2LETTER b,
     COUNTRY_ICAO2LETTER c
WHERE  SUBSTR(A.flt_dep_ad,1,2) =  b.ICAO2LETTER   AND
       SUBSTR(A.flt_ctfm_ades,1,2) = C.ICAO2LETTER
    AND  B.COUNTRY_NAME =C.COUNTRY_NAME
--    AND A.flt_lobt >= to_date('24-12-2018','DD-MM-YYYY') -2
--    AND A.flt_lobt <  ", mydate, " + 2
    AND A.flt_a_asp_prof_time_entry >= to_date('24-12-2018','DD-MM-YYYY')
    AND A.flt_a_asp_prof_time_entry <  ", mydate, "
    AND A.flt_state IN ('TE','TA','AA')
GROUP BY  B.COUNTRY_NAME  ,
        TRUNC(A.flt_a_asp_prof_time_entry)
),

DATA_COUNTRY as
(SELECT
         a.YEAR,
         a.MONTH,
          a.WEEK,
          a.WEEK_NB_YEAR,
          a.day_type,
          a.day_of_week,
          a.day_date as flight_date,
          a.country_name,
          coalesce(b.DAY_TFC,0) as DEP,
          coalesce( c.DAY_TFC,0) as ARR ,
          coalesce( d.DAY_TFC,0) as DOM ,
         coalesce(b.DAY_TFC,0)  + coalesce( c.DAY_TFC,0) - coalesce( d.DAY_TFC,0) as DAY_TFC
FROM CTRY_DAY A
LEFT JOIN DATA_DEP b on a.COUNTRY_NAME = B.COUNTRY_NAME and a.day_date = b.FLIGHT_date
LEFT JOIN DATA_ARR c on a.country_name = c.country_name and a.day_date = c.FLIGHT_date
LEFT JOIN DATA_DOMESTIC d on a.country_name = d.country_name and a.day_date = d.FLIGHT_date
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

       CASE WHEN flight_DATE >= ", mydate, " THEN NULL
           ELSE DAY_TFC - DAY_TFC_PREV_WEEK
       END DAY_TFC_DIFF_PREV_WEEK,
       CASE WHEN flight_DATE >= ", mydate, " THEN NULL
           ELSE DAY_TFC - DAY_TFC_PREV_YEAR
       END DAY_TFC_DIFF_PREV_YEAR,
       CASE WHEN flight_DATE >= ", mydate, " THEN NULL
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

       CASE WHEN flight_DATE >= ", mydate, "
           THEN NULL
           ELSE AVG_rolling_week
       END AVG_rolling_week,
       AVG_rolling_PREV_WEEK,
       AVG_rolling_week_PREV_YEAR,
       AVG_rolling_week_2020,
       AVG_rolling_week_2019,

      CASE WHEN AVG_rolling_week_PREV_YEAR <> 0 and flight_DATE < ", mydate, "
           THEN avg_rolling_week/AVG_rolling_week_PREV_YEAR -1
           ELSE NULL
       END  DIF_WEEK_PREV_YEAR_PERC,
      CASE WHEN AVG_rolling_week_2019 <> 0 and flight_DATE < ", mydate, "
           THEN avg_rolling_week/AVG_rolling_week_2019 -1
           ELSE NULL
       END  DIF_ROLLING_WEEK_2019_perc,

       Y2D_TFC_YEAR,
       Y2D_TFC_PREV_YEAR,
       Y2D_TFC_2019,
       CASE WHEN flight_DATE >= ", mydate, "
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
        ", mydate, " -1 as LAST_DATA_DAY

      FROM DATA_COUNTRY_3
      where flight_DATE >=to_date('01-01-'|| extract(year from (", mydate, "-1)),'dd-mm-yyyy')
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

       CASE WHEN flight_DATE >= ", mydate, " THEN NULL
           ELSE case when FLIGHT_DATE >='01-jan-2024' then DAY_TFC - DAY_TFC_PREV_WEEK
                     else NULL
                end
       END DAY_TFC_DIFF_PREV_WEEK,
       CASE WHEN flight_DATE >= ", mydate, " THEN NULL
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

       CASE WHEN flight_DATE >= ", mydate, " THEN NULL
           ELSE case when FLIGHT_DATE >='01-jan-2024' then AVG_rolling_week
                     else NULL
                end
       END AVG_rolling_week,

       case when FLIGHT_DATE >='01-jan-2024' then AVG_rolling_PREV_WEEK else NULL end AVG_rolling_PREV_WEEK,
       case when FLIGHT_DATE >='01-jan-2025' then AVG_rolling_week_PREV_YEAR else NULL end AVG_rolling_week_PREV_YEAR,
       NULL as AVG_rolling_week_2020,
       NULL as AVG_rolling_week_2019,

       CASE WHEN AVG_rolling_week_PREV_YEAR <> 0 and flight_DATE < ", mydate, " AND FLIGHT_DATE >='01-jan-2025'
           THEN avg_rolling_week/AVG_rolling_week_PREV_YEAR -1
           ELSE NULL
       END  DIF_WEEK_PREV_YEAR_PERC,
       NULL as  DIF_ROLLING_WEEK_2019_perc,

       case when FLIGHT_DATE >='01-jan-2024' then Y2D_TFC_YEAR else NULL end Y2D_TFC_YEAR,
       case when FLIGHT_DATE >='01-jan-2025' then Y2D_TFC_PREV_YEAR else NULL end Y2D_TFC_PREV_YEAR,
       NULL as Y2D_TFC_2019,
       CASE WHEN flight_DATE >= ", mydate, "
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
        ", mydate, " -1 as LAST_DATA_DAY

      FROM DATA_COUNTRY_3
      where flight_DATE >=to_date('01-01-'|| extract(year from (", mydate, "-1)),'dd-mm-yyyy')
            AND country_name in ('ICELAND', 'Iceland')
      order by country_name, flight_date
"
)
}

# state delay ----
query_state_delay_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
WITH

 DATA_DAY
    AS
 (SELECT     agg_asp_entry_date as flight_date,
             agg_asp_id COUNTRY_CODE,
             agg_asp_ty as TYPE,
             CASE WHEN agg_asp_id = 'LW' THEN 'North Macedonia'
                WHEN agg_asp_id = 'LT' THEN 'Türkiye'
                  WHEN agg_asp_id = 'LQ' then 'Bosnia and Herzegovina'
                  WHEN agg_asp_id = 'LY' then 'Serbia/Montenegro'
                ELSE agg_asp_name
             END COUNTRY_NAME,
             SUM (coalesce(a.agg_asp_a_traffic_asp,0)) as DAY_TFC,
             SUM(coalesce(a.agg_asp_delay_tvs,0)) as DAY_DLY,
             (SUM (coalesce(a.agg_asp_delay_tvs,0)) - SUM (coalesce(a.agg_asp_delay_airport_tvs,0)))  AS DAY_ERT_DLY,
             SUM (coalesce(a.agg_asp_delay_airport_tvs,0))  AS DAY_ARP_DLY
       FROM prudev.v_aiu_agg_asp a
       WHERE
             agg_asp_entry_date >= '24-DEC-2018' AND a.AGG_ASP_ENTRY_DATE < ", mydate, "
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
   AND t.day_date <= to_date('31-12-'|| extract(year from (", mydate, "-1)),'dd-mm-yyyy')
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

      CASE WHEN flight_DATE >= ", mydate, "
           THEN NULL
           ELSE avg_TFC_rolling_week
       END avg_TFC_rolling_week,
      CASE WHEN flight_DATE >= ", mydate, "
           THEN NULL
           ELSE avg_DLY_rolling_week
       END avg_DLY_rolling_week,
      CASE WHEN flight_DATE >= ", mydate, "
           THEN NULL
           ELSE avg_ERT_DLY_rolling_week
       END avg_ERT_DLY_rolling_week,
      CASE WHEN flight_DATE >= ", mydate, "
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

       CASE WHEN flight_DATE >= ", mydate, "
           THEN NULL
      	   ELSE Y2D_AVG_TFC_YEAR
       END Y2D_AVG_TFC_YEAR,
       CASE WHEN flight_DATE >= ", mydate, "
           THEN NULL
      	   ELSE Y2D_AVG_DLY_YEAR
       END Y2D_AVG_DLY_YEAR,
       CASE WHEN flight_DATE >= ", mydate, "
           THEN NULL
      	   ELSE Y2D_AVG_ERT_DLY_YEAR
       END Y2D_AVG_ERT_DLY_YEAR,
       CASE WHEN flight_DATE >= ", mydate, "
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

      CASE WHEN AVG_DLY_rolling_week_PREV_YEAR <> 0 and flight_DATE < ", mydate, "
           THEN avg_DLY_rolling_week/AVG_DLY_rolling_week_PREV_YEAR -1
           ELSE NULL
       END  DIF_DLY_ROLLING_WEEK_PREV_YEAR_PERC,
      CASE WHEN AVG_DLY_rolling_week_2019 <> 0 and flight_DATE < ", mydate, "
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
       ", mydate, " -1 as LAST_DATA_DAY

      FROM DATA_COUNTRY_3
      where flight_DATE >=to_date('01-01-'|| extract(year from (", mydate, "-1)),'dd-mm-yyyy')
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

      CASE WHEN AVG_DLY_rolling_week_PREV_YEAR <> 0 and flight_DATE < ", mydate, " and FLIGHT_DATE >='01-jan-2025'
           THEN avg_DLY_rolling_week/AVG_DLY_rolling_week_PREV_YEAR -1
           ELSE NULL
       END  DIF_DLY_ROLLING_WEEK_PREV_YEAR_PERC,
      NULL as DIF_DLY_ROLLING_WEEK_2019_perc,

       CASE WHEN Y2D_AVG_DLY_PREV_YEAR <> 0 and FLIGHT_DATE >='01-jan-2025'
        THEN Y2D_AVG_DLY_YEAR/Y2D_AVG_DLY_PREV_YEAR - 1
        ELSE NULL
       END Y2D_DLY_DIF_PREV_YEAR_PERC,
       NULL as Y2D_DLY_DIF_2019_PERC,
       ", mydate, " -1 as LAST_DATA_DAY

      FROM DATA_COUNTRY_3
      where flight_DATE >=to_date('01-01-'|| extract(year from (", mydate, "-1)),'dd-mm-yyyy')
                  AND country_name in ('ICELAND', 'Iceland')
      order by country_name, flight_date
"
)
}
# state delay cause ----
query_state_delay_cause_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
WITH

  COUNTRY_ICAO2LETTER as
 (SELECT COUNTRY_ID  ICAO2LETTER,
        CASE
             --WHEN COUNTRY_ID = 'GC' then 'LE'
             WHEN COUNTRY_ID = 'GE' then 'LE'
             WHEN COUNTRY_ID = 'ET' then 'ED'
             ELSE COUNTRY_ID
        END  COUNTRY_CODE,
        CASE WHEN COUNTRY_ID = 'GC' then 'Spain Canaries'
             WHEN COUNTRY_ID = 'GE' then 'Spain Continental'
             WHEN COUNTRY_ID = 'LE' then 'Spain Continental'
             WHEN COUNTRY_ID = 'ET' then 'Germany'
             WHEN COUNTRY_ID = 'LT' then 'Türkiye'
             WHEN COUNTRY_ID = 'LY' then 'Serbia/Montenegro'
             WHEN COUNTRY_ID = 'LQ' then 'Bosnia and Herzegovina'
             ELSE  COUNTRY_NAME
        END  COUNTRY_NAME
 FROM  ARU_SYN.stat_coda_country
 WHERE
  (  (SUBSTR(COUNTRY_ID,1,1) IN ('E','L')
       OR SUBSTR(COUNTRY_ID,1,2) IN ('GC','GM','GE','UD','UG','UK','BI'))  )
  AND  COUNTRY_ID not in ('LV', 'LX', 'EU','LN')
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
    AND day_date < ", mydate, "
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
 where flight_date >= TO_DATE ('24-12-' || to_char(extract(year from (", mydate, "-1))-1 ), 'dd-mm-yyyy')
             AND country_name not in ('ICELAND', 'Iceland')

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
 where flight_date >= TO_DATE ('24-12-' || to_char(extract(year from (", mydate, "-1))-1 ), 'dd-mm-yyyy')
            AND country_name in ('ICELAND', 'Iceland')
 order by country_name, flight_date
"
)
}

# state ao ----
## day ----
query_st_ao_data_day_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
paste0("
with

DIM_AO
 as (
       select  ao_code, ao_name, ao_grp_code, ao_grp_name, ao_nm_group_code, ao_nm_group_name  from  prudev.v_covid_dim_ao

        ) ,

REL_AP_CTRY as (
select cfmu_ap_code ,
       CASE WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'GC' then 'Spain Canaries'
            WHEN iso_ct_name = 'SPAIN' then 'Spain Continental'
            ELSE iso_ct_name
       END iso_ct_name,
       region,
       pru_dashboard_ap_name  as ad_name
 from  v_covid_rel_airport_area

),

CTRY_LIST as (select distinct
                   iso_ct_name
                    from REL_AP_CTRY where region = 'ECAC'
                or iso_ct_name in ('ISRAEL', 'MOROCCO')
 )

, AIRP_FLIGHT as (
SELECT flt_uid,
       TRUNC (flt_a_asp_prof_time_entry) AS entry_day,
       c1.iso_ct_name dep_ctry ,
       c2.iso_ct_name arr_ctry,
       A.ao_icao_id

  FROM v_aiu_flt a, REL_AP_CTRY c1, REL_AP_CTRY c2
 WHERE       (
                      (     A.flt_lobt >= ", mydate, " -1 -1
                        AND A.flt_lobt < ", mydate, "-0
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -1
                        AND A.flt_a_asp_prof_time_entry < ", mydate, " -0
                        )
                        or
                      (     A.flt_lobt >= ", mydate, " -1 -1 -7
                        AND A.flt_lobt < ", mydate, " -0 -7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -1 -7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, " -0 - 7
                        )
                        or
                        (     A.flt_lobt >= ", mydate, " -364 -1 -1
                        AND A.flt_lobt < ", mydate, " -364
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -364-1
                        AND A.flt_a_asp_prof_time_entry < ", mydate, " -364
                        )
                        or
                        (     A.flt_lobt >= ", mydate, " - ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 -1 -1
                        AND A.flt_lobt < ", mydate, "- ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " - ((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7 -1
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "- ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7



                        )
                     )
       AND A.flt_state IN ('TE', 'TA', 'AA')
       AND  flt_dep_ad IS NOT NULL
       AND    flt_ctfm_ades IS NOT NULL
       AND flt_dep_ad = c1.cfmu_ap_code and    flt_ctfm_ades  = c2.cfmu_ap_code
      AND A.ao_icao_id <> 'ZZZ'
     --  GROUP BY c1.iso_ct_name,c2.iso_ct_name,A.ao_icao_id, TRUNC (flt_a_asp_prof_time_entry)
     )


, CTRY_PAIR_FLIGHT as (
SELECT entry_day,
      CASE WHEN   dep_ctry <= arr_ctry
           THEN dep_ctry
           ELSE   arr_ctry
       END
        ctry1,
      CASE WHEN  dep_ctry <= arr_ctry
           THEN arr_ctry
           ELSE   dep_ctry
       END
        ctry2 , flt_uid, ao_icao_id
  FROM AIRP_FLIGHT

  )

 , CTRY_PAIR_ARP_1 as (
 SELECT
        entry_day,
        ctry1,
        ctry2,
        flt_uid ,ao_icao_id
 FROM
 CTRY_PAIR_FLIGHT


 )

 , CTRY_PAIR_ARP_2 as (
SELECT entry_day,
       ctry2 as ctry1,
       ctry1 as ctry2,
       flt_uid,
       ao_icao_id
FROM CTRY_PAIR_ARP_1
  WHERE ctry1 <> ctry2
 )

, CTRY_PAIR_ARP as (
 SELECT  ctry1 as country_id, entry_day,ao_icao_id, flt_uid
 FROM CTRY_PAIR_ARP_1
 UNION ALL
 SELECT  ctry1 as country_id, entry_day,ao_icao_id, flt_uid
 FROM  CTRY_PAIR_ARP_2
 )

, CTRY_PAIR_SELECTION as (
SELECT  country_id , entry_day, flt_uid,ao_icao_id
  FROM  CTRY_PAIR_ARP
  WHERE country_id in (select iso_ct_name from ctry_list)
)
, AO_COUNTRY_FLIGHT as
(
SELECT entry_day,
       flt_uid,
        a.ao_icao_id,
        nvl(b.ao_name,'UNKNOWN'),
        nvl(b.ao_grp_code,'---')ao_grp_code, nvl(b.ao_grp_name,'UNKNOWN') ao_grp_name,
        a.country_id as ctry_name,
         case when entry_day >= ", mydate, " -1 and entry_day < ", mydate, "  then 'CURRENT_DAY'
      when entry_day >= ", mydate, " -364-1 and entry_day < ", mydate, " -364  then 'DAY_PREV_YEAR'
      when entry_day >= ", mydate, " -1-7 and entry_day < ", mydate, " -7  then 'DAY_PREV_WEEK'
      when entry_day >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7-1
              and entry_day < ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
              then 'DAY_2019'
         else '-'
    end  flag_day
  FROM  CTRY_PAIR_SELECTION a  left join DIM_AO b  on (a.ao_icao_id = b.ao_code )
  ),

AO_COUNTRY_FLIGHT_GRP as
(
select ctry_name,  ao_grp_code,   ao_grp_name , flag_day,
 max(entry_day) as to_date,
 count(flt_uid) as flight_without_overflight
 from AO_COUNTRY_FLIGHT a
 where ao_grp_name <> 'UNKNOWN' AND ctry_name <> '#UNKNOWN#'
 group by ao_grp_code, ao_grp_name, ctry_name, flag_day
),

AO_RANK_DAY as
(SELECT  ao_grp_code, ctry_name, flag_day,
        ROW_NUMBER() OVER (PARTITION BY  ctry_name, flag_day
                ORDER BY flight_without_overflight DESC, ao_grp_name) as R_RANK,
        RANK() OVER (PARTITION BY  ctry_name, flag_day
                ORDER BY flight_without_overflight DESC) as RANK

FROM AO_COUNTRY_FLIGHT_GRP
WHERE flag_day <> '-' AND flag_day = 'CURRENT_DAY'
),

AO_RANK_WEEK_PREV as
(SELECT  ao_grp_code, ctry_name, flag_day,
        RANK() OVER (PARTITION BY  ctry_name, flag_day
                ORDER BY flight_without_overflight DESC) as RANK_PREV_WEEK
FROM AO_COUNTRY_FLIGHT_GRP
WHERE flag_day <> '-' AND flag_day = 'DAY_PREV_WEEK'
)

select  a.ctry_name as country_name, a.flag_day, a.ao_grp_code, ao_grp_name ,
       flight_without_overflight, r_rank, rank, RANK_PREV_WEEK,
       a.to_date

 from AO_COUNTRY_FLIGHT_GRP a
 left join AO_RANK_DAY b on a.ao_grp_code =  b.ao_grp_code  AND a.ctry_name = b.ctry_name
 left join AO_RANK_WEEK_PREV c on a.ao_grp_code =  c.ao_grp_code  AND a.ctry_name = c.ctry_name
 where r_rank <= '10'
order by country_name,  a.flag_day, r_rank
"
)
}

## week ----
query_st_ao_data_week_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0(
"
with

DIM_AO
 as (
       select  ao_code, ao_name, ao_grp_code, ao_grp_name, ao_nm_group_code, ao_nm_group_name  from  prudev.v_covid_dim_ao

        ) ,

REL_AP_CTRY as (
select cfmu_ap_code ,
       CASE WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'GC' then 'Spain Canaries'
            WHEN country_name = 'SPAIN' then 'Spain Continental'
            ELSE country_name
       END iso_ct_name,
       region,
       pru_dashboard_ap_name  as ad_name
 from  prudev.v_covid_rel_airport_area

),

CTRY_LIST as (select distinct
                   iso_ct_name
                    from REL_AP_CTRY where region = 'ECAC'
                or iso_ct_name in ('ISRAEL', 'MOROCCO')
 )

, AIRP_FLIGHT as (
/* Formatted on 06-07-2020 15:56:37 (QP5 v5.318) */
SELECT flt_uid,
       TRUNC (flt_a_asp_prof_time_entry) AS entry_day,
       c1.iso_ct_name dep_ctry ,
       c2.iso_ct_name arr_ctry,
       A.ao_icao_id

  FROM v_aiu_flt a, REL_AP_CTRY c1, REL_AP_CTRY c2
 WHERE       (
                      (     A.flt_lobt >= ", mydate, " -7 -1
                        AND A.flt_lobt < ", mydate, "-0
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0
                        )
                        or
                      (     A.flt_lobt >= ", mydate, " -7 -1 -7
                        AND A.flt_lobt < ", mydate, "-0 -7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -7 -7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0 - 7
                        )
                        or
                        (     A.flt_lobt >= ", mydate, " -364-7 -1
                        AND A.flt_lobt < ", mydate, "-364
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -364-7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-364
                        )
                        or
                        (     A.flt_lobt >= ", mydate, " - ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 -7 -1
                        AND A.flt_lobt < ", mydate, "- ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " - ((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7 -7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "- ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7



                        )
                     )
       AND A.flt_state IN ('TE', 'TA', 'AA')
       AND  flt_dep_ad IS NOT NULL
       AND    flt_ctfm_ades IS NOT NULL
       AND flt_dep_ad = c1.cfmu_ap_code and    flt_ctfm_ades  = c2.cfmu_ap_code
       AND A.ao_icao_id <> 'ZZZ'
     --  GROUP BY c1.iso_ct_name,c2.iso_ct_name,A.ao_icao_id, TRUNC (flt_a_asp_prof_time_entry)
     )


, CTRY_PAIR_FLIGHT as (
SELECT entry_day,
      CASE WHEN   dep_ctry <= arr_ctry
           THEN dep_ctry
           ELSE   arr_ctry
       END
        ctry1,
      CASE WHEN  dep_ctry <= arr_ctry
           THEN arr_ctry
           ELSE   dep_ctry
       END
        ctry2 , flt_uid, ao_icao_id
  FROM AIRP_FLIGHT

  )

 , CTRY_PAIR_ARP_1 as (
 SELECT
        entry_day,
        ctry1,
        ctry2,
        flt_uid ,ao_icao_id
 FROM
 CTRY_PAIR_FLIGHT


 )

 , CTRY_PAIR_ARP_2 as (
SELECT entry_day,
       ctry2 as ctry1,
       ctry1 as ctry2,
       flt_uid,
       ao_icao_id
FROM CTRY_PAIR_ARP_1
  WHERE ctry1 <> ctry2
 )

, CTRY_PAIR_ARP as (
 SELECT  ctry1 as country_id, entry_day,ao_icao_id, flt_uid
 FROM CTRY_PAIR_ARP_1
 UNION ALL
 SELECT  ctry1 as country_id, entry_day,ao_icao_id, flt_uid
 FROM  CTRY_PAIR_ARP_2
 )

, CTRY_PAIR_SELECTION as (
SELECT  country_id , entry_day, flt_uid,ao_icao_id
  FROM  CTRY_PAIR_ARP
  WHERE country_id in (select iso_ct_name from ctry_list)
)
, AO_COUNTRY_FLIGHT as
(
SELECT entry_day,
       flt_uid,
        a.ao_icao_id,
        nvl(b.ao_name,'UNKNOWN'),
        nvl(b.ao_grp_code,'---')ao_grp_code, nvl(b.ao_grp_name,'UNKNOWN') ao_grp_name,
        a.country_id as ctry_name,
         case when entry_day >= ", mydate, " -7 and entry_day < ", mydate, "  then 'CURRENT_ROLLING_WEEK'
      when entry_day >= ", mydate, " -364-7 and entry_day < ", mydate, " -364  then 'ROLLING_WEEK_PREV_YEAR'
      when entry_day >= ", mydate, " -7-7 and entry_day < ", mydate, " -7  then 'PREV_ROLLING_WEEK'
      when entry_day >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7-7
              and entry_day < ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
              then 'ROLLING_WEEK_2019'
         else '-'
    end  flag_rolling_week
  FROM  CTRY_PAIR_SELECTION a  left join DIM_AO b  on (a.ao_icao_id = b.ao_code )
  ),

AO_COUNTRY_FLIGHT_GRP as
(
select ctry_name,  ao_grp_code,   ao_grp_name , flag_rolling_week,
 count(flt_uid) as flight_without_overflight
 from AO_COUNTRY_FLIGHT a
 where ao_grp_name <> 'UNKNOWN' AND ctry_name <> '#UNKNOWN#'
 group by ao_grp_code, ao_grp_name, ctry_name, flag_rolling_week
),

AO_RANK_WEEK as
(SELECT  ao_grp_code, ctry_name, flag_rolling_week,
        ROW_NUMBER() OVER (PARTITION BY  ctry_name, flag_rolling_week
                ORDER BY flight_without_overflight DESC, ao_grp_name) R_RANK,
        RANK() OVER (PARTITION BY  ctry_name, flag_rolling_week
                ORDER BY flight_without_overflight DESC) RANK

FROM AO_COUNTRY_FLIGHT_GRP
WHERE flag_rolling_week <> '-' AND flag_rolling_week = 'CURRENT_ROLLING_WEEK'
),

AO_RANK_WEEK_PREV as
(SELECT  ao_grp_code, ctry_name, flag_rolling_week,
        RANK() OVER (PARTITION BY  ctry_name, flag_rolling_week
                ORDER BY flight_without_overflight DESC) RANK_PREV_WEEK
FROM AO_COUNTRY_FLIGHT_GRP
WHERE flag_rolling_week <> '-' AND flag_rolling_week = 'PREV_ROLLING_WEEK'
),

DATA_FILTERED as (
select  a.ctry_name as country_name,
        a.flag_rolling_week,
        a.ao_grp_code,
        ao_grp_name,
        flight_without_overflight,
        r_rank,
        rank,
        RANK_PREV_WEEK,
        to_date(  TO_CHAR (", mydate, "-7, 'dd-mm-yyyy'),'dd-mm-yyyy') as from_date,
        to_date(  TO_CHAR (", mydate, "-1, 'dd-mm-yyyy'),'dd-mm-yyyy') as to_date

 from AO_COUNTRY_FLIGHT_GRP a
 left join AO_RANK_WEEK b on a.ao_grp_code =  b.ao_grp_code  AND a.ctry_name = b.ctry_name
 left join AO_RANK_WEEK_PREV c on a.ao_grp_code =  c.ao_grp_code  AND a.ctry_name = c.ctry_name
 where r_rank <= '10'
)

select  country_name,
        flag_rolling_week,
        case when country_name in ('ICELAND', 'Iceland')
                  and (to_date < '01-jan-2024'
                       or
                       (to_date < '01-jan-2025' and flag_rolling_week in ('ROLLING_WEEK_PREV_YEAR'))
                       or
                       (flag_rolling_week in ('ROLLING_WEEK_2019'))
                       )
            then NULL else ao_grp_code
        end ao_grp_code,
        case when country_name in ('ICELAND', 'Iceland')
                  and (to_date < '01-jan-2024'
                       or
                       (to_date < '01-jan-2025' and flag_rolling_week in ('ROLLING_WEEK_PREV_YEAR'))
                       or
                       (flag_rolling_week in ('ROLLING_WEEK_2019'))
                       )
            then NULL else ao_grp_name
        end ao_grp_name,
        case when country_name in ('ICELAND', 'Iceland')
                  and (to_date < '01-jan-2024'
                       or
                       (to_date < '01-jan-2025' and flag_rolling_week in ('ROLLING_WEEK_PREV_YEAR'))
                       or
                       (flag_rolling_week in ('ROLLING_WEEK_2019'))
                       )
            then NULL else flight_without_overflight
        end flight_without_overflight,
        r_rank,
        rank,
        RANK_PREV_WEEK,
        from_date,
        to_date
from DATA_FILTERED
order by country_name,  flag_rolling_week, r_rank
"
)
}

## year to date ----
query_st_ao_data_y2d_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0(
"
with

DIM_AO
as (
       select  ao_code, ao_name, ao_grp_code, ao_grp_name, ao_nm_group_code, ao_nm_group_name  from  prudev.v_covid_dim_ao

        ) ,

REL_AP_CTRY as (
select cfmu_ap_code ,
       CASE WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'GC' then 'Spain Canaries'
            WHEN country_name = 'SPAIN' then 'Spain Continental'
            ELSE country_name
       END iso_ct_name,
       region,
       pru_dashboard_ap_name  as ad_name
 from  prudev.v_covid_rel_airport_area

),

CTRY_LIST as (select distinct
                   iso_ct_name
                    from REL_AP_CTRY where region = 'ECAC'
                or iso_ct_name in ('ISRAEL', 'MOROCCO')
 )

, AIRP_FLIGHT as (

/* Formatted on 06-07-2020 15:56:37 (QP5 v5.318) */
SELECT  flt_uid,
     TRUNC (flt_a_asp_prof_time_entry) AS entry_day,
       c1.iso_ct_name dep_ctry ,c2.iso_ct_name arr_ctry,
       A.ao_icao_id
FROM prudev.v_aiu_flt a, REL_AP_CTRY c1, REL_AP_CTRY c2
WHERE
    A.flt_lobt>= TO_DATE ('01-01-2019', 'dd-mm-yyyy') -1
        AND A.flt_lobt < ", mydate, "
        AND flt_a_asp_prof_time_entry >= TO_DATE ('01-01-2019', 'dd-mm-yyyy')
        AND TO_NUMBER (TO_CHAR (TRUNC (flt_a_asp_prof_time_entry), 'mmdd')) <=   TO_NUMBER (TO_CHAR (", mydate, "-1, 'mmdd'))
        and extract (year from flt_a_asp_prof_time_entry) <= extract(year from (", mydate, "-1))

        AND A.flt_state IN ('TE', 'TA', 'AA')
        AND  flt_dep_ad IS NOT NULL
        AND  flt_ctfm_ades IS NOT NULL
        AND flt_dep_ad = c1.cfmu_ap_code and    flt_ctfm_ades = c2.cfmu_ap_code
        AND A.ao_icao_id <> 'ZZZ'

),


CTRY_PAIR_FLIGHT as
(
SELECT entry_day,
      CASE WHEN   dep_ctry <= arr_ctry
           THEN dep_ctry
           ELSE   arr_ctry
       END
        ctry1,
      CASE WHEN  dep_ctry <= arr_ctry
           THEN arr_ctry
           ELSE   dep_ctry
       END
        ctry2 , flt_uid, ao_icao_id
  FROM AIRP_FLIGHT

  ),

 CTRY_PAIR_ARP_1 as
 (SELECT
        entry_day,
        ctry1,
        ctry2,
        flt_uid ,ao_icao_id
FROM
 CTRY_PAIR_FLIGHT

 ),

 CTRY_PAIR_ARP_2 as
(
SELECT entry_day,
       ctry2 as ctry1,
       ctry1 as ctry2,
       flt_uid,
       ao_icao_id
FROM CTRY_PAIR_ARP_1
  WHERE ctry1 <> ctry2
),

 CTRY_PAIR_ARP as (
SELECT  ctry1 as country_id, entry_day,ao_icao_id, flt_uid
 FROM CTRY_PAIR_ARP_1
UNION ALL
SELECT  ctry1 as country_id, entry_day,ao_icao_id, flt_uid
FROM  CTRY_PAIR_ARP_2
),

 CTRY_PAIR_SELECTION as
(SELECT  country_id , entry_day, flt_uid,ao_icao_id
  FROM  CTRY_PAIR_ARP
  WHERE country_id in (select iso_ct_name from ctry_list))
,
AO_COUNTRY_FLIGHT as
(
SELECT
      extract(year from entry_day) as year,
      entry_day,
       flt_uid,
        a.ao_icao_id,
        nvl(b.ao_name,'UNKNOWN'),
        nvl(b.ao_grp_code,'---')ao_grp_code, nvl(b.ao_grp_name,'UNKNOWN') ao_grp_name,
        a.country_id as ctry_name
  FROM  CTRY_PAIR_SELECTION a  left join DIM_AO b  on (a.ao_icao_id = b.ao_code )

  ),

REF_DATES as
(
SELECT
  year,
  min(day_date) as from_date,
  max(day_date) as to_date,
  max(day_date) - min(day_date) + 1 as no_days
FROM pru_time_references
where day_date >= TO_DATE ('01-01-2019', 'dd-mm-yyyy')
        AND TO_NUMBER (TO_CHAR (TRUNC (day_date), 'mmdd')) <=   TO_NUMBER (TO_CHAR (", mydate, "-1, 'mmdd'))
        AND year <= extract(year from (", mydate, "-1))
group by year
),

AO_COUNTRY_FLIGHT_GROUP as
(
SELECT
    a.year, ctry_name,  ao_grp_code,   ao_grp_name ,
    count(flt_uid) as flight_without_overflight
FROM AO_COUNTRY_FLIGHT a
WHERE ao_grp_name <> 'UNKNOWN'
GROUP BY a.year, ao_grp_code, ao_grp_name, ctry_name
),

AO_RANK_PREV as
(SELECT  ao_grp_code, ctry_name,
        RANK() OVER (PARTITION BY  ctry_name, year
                ORDER BY flight_without_overflight DESC) RANK_PREV_YEAR
FROM AO_COUNTRY_FLIGHT_GROUP
WHERE year = extract (year from (", mydate, "-1)) - 1
),

AO_RANK as
(SELECT  ao_grp_code, ctry_name,
        ROW_NUMBER() OVER (PARTITION BY  ctry_name, year
                ORDER BY flight_without_overflight DESC, ao_grp_name) R_RANK,
        RANK() OVER (PARTITION BY  ctry_name, year
                ORDER BY flight_without_overflight DESC) RANK
FROM AO_COUNTRY_FLIGHT_GROUP
WHERE year = extract (year from (", mydate, "-1))
),

DATA_FILTERED as (
SELECT a.ctry_name as country_name,
       a.year,
       a.ao_grp_code,
       ao_grp_name,
       flight_without_overflight,
       flight_without_overflight / d.no_days as avg_flt,
       r_rank,
       rank as rank_current,
       RANK_PREV_YEAR,
       d.from_date,
       d.to_date,
       max(d.to_date) over () as max_to_date
FROM AO_COUNTRY_FLIGHT_GROUP a
left join AO_RANK_PREV b on a.ao_grp_code =  b.ao_grp_code AND a.ctry_name =  b.ctry_name
left join AO_RANK c on a.ao_grp_code =  c.ao_grp_code AND a.ctry_name =  c.ctry_name
left join REF_DATES d on a.year = d.year
WHERE r_rank <=10
AND a.ctry_name <> ANY('#UNKNOWN#')
)

SELECT country_name,
       year,
        case when country_name in ('ICELAND', 'Iceland')
                  and (max_to_date < '01-jan-2024'
                       or
                       (max_to_date < '01-jan-2025' and to_date < '01-jan-2024')
                       or
                       (to_date < '01-jan-2024')
                       )
            then NULL else ao_grp_code
        end ao_grp_code,
        case when country_name in ('ICELAND', 'Iceland')
                  and (max_to_date < '01-jan-2024'
                       or
                       (max_to_date < '01-jan-2025' and to_date < '01-jan-2024')
                       or
                       (to_date < '01-jan-2024')
                       )
            then NULL else ao_grp_name
        end ao_grp_name,
        case when country_name in ('ICELAND', 'Iceland')
                  and (max_to_date < '01-jan-2024'
                       or
                       (max_to_date < '01-jan-2025' and to_date < '01-jan-2024')
                       or
                       (to_date < '01-jan-2024')
                       )
            then NULL else flight_without_overflight
        end flight_without_overflight,
        case when country_name in ('ICELAND', 'Iceland')
                  and (max_to_date < '01-jan-2024'
                       or
                       (max_to_date < '01-jan-2025' and to_date < '01-jan-2024')
                       or
                       (to_date < '01-jan-2024')
                       )
            then NULL else avg_flt
        end avg_flt,
       r_rank,
       rank_current,
       RANK_PREV_YEAR,
       from_date,
       to_date
FROM DATA_FILTERED
ORDER by country_name, year DESC, r_rank
"
  )
}

# state apt ----
## day ----
query_st_apt_data_day_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
with

REL_AP_CTRY as (
select cfmu_ap_code ,
       CASE WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'GC' then 'Spain Canaries'
            WHEN country_name = 'SPAIN' then 'Spain Continental'
            ELSE country_name
       END iso_ct_name,
       region,
       pru_dashboard_ap_name  as ad_name
 from  prudev.v_covid_rel_airport_area

),

CTRY_LIST as (select distinct
                   iso_ct_name
                    from REL_AP_CTRY where region = 'ECAC'
                or iso_ct_name in ('ISRAEL', 'MOROCCO')
 )

, DATA_FLIGHT_DEP as (
SELECT nvl(A.adep_day_all_trf,0) AS mvt,
                A.adep_day_adep  ad, 'DEP' as airport_flow,
                 A.adep_DAY_FLT_DATE  AS FLIGHT_DATE
 FROM  prudev.v_aiu_agg_dep_day A
  where
                   (
                      (
                         A.adep_DAY_FLT_DATE >=  ", mydate, " -1
                        AND A.adep_DAY_FLT_DATE < ", mydate, "-0
                        )
                        or
                      (
                         A.adep_DAY_FLT_DATE >=  ", mydate, " -1 -7
                        AND A.adep_DAY_FLT_DATE < ", mydate, "-0 -7
                        )
                        or
                      (
                         A.adep_DAY_FLT_DATE >=  ", mydate, " -1 -364
                        AND A.adep_DAY_FLT_DATE < ", mydate, "-0 -364
                        )

                        or
                      (
                        A.adep_DAY_FLT_DATE >=  ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 -1
                        AND A.adep_DAY_FLT_DATE < ", mydate, "-((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        )
                   )
)

,DATA_FLIGHT_ARR as (
SELECT nvl(A.ades_day_all_trf,0) AS mvt,
                A.ades_day_ades_ctfm  ad, 'ARR' as airport_flow,
                 A.ades_DAY_FLT_DATE  AS FLIGHT_DATE
 FROM  prudev.v_aiu_agg_arr_day A
  where
                   (
                      (
                         A.ades_DAY_FLT_DATE >=  ", mydate, " -1
                        AND A.ades_DAY_FLT_DATE < ", mydate, "-0
                        )
                        or
                      (
                         A.ades_DAY_FLT_DATE >=  ", mydate, " -1 -7
                        AND A.ades_DAY_FLT_DATE < ", mydate, "-0 -7
                        )
                        or
                      (
                         A.ades_DAY_FLT_DATE >=  ", mydate, " -1 -364
                        AND A.ades_DAY_FLT_DATE < ", mydate, "-0 -364
                        )
                        or
                        (
                        A.ades_DAY_FLT_DATE >=  ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 -1
                        AND A.ades_DAY_FLT_DATE < ", mydate, "-((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        )
                   )
)

, COUNTRY_FLIGHT as (
SELECT FLIGHT_DATE,
        mvt,
        a.ad,
      a.airport_flow,
        c.ad_name,
        c.iso_ct_name
  FROM DATA_FLIGHT_DEP a   JOIN REL_AP_CTRY C on (a.ad = c.cfmu_ap_code)
UNION ALL -- union is all as we count all dep and arrival
 SELECT FLIGHT_DATE,
       mvt,
        a.ad,
        a.airport_flow,
        c.ad_name,
        c.iso_ct_name
  FROM DATA_FLIGHT_ARR a   JOIN REL_AP_CTRY C on (a.ad = c.cfmu_ap_code)

 )


, COUNTRY_FLIGHT_ECAC as  (
select
        mvt,
        ad,
        ad_name,
        iso_ct_name ,
        airport_flow,
         case when FLIGHT_DATE >= ", mydate, " -1 and FLIGHT_DATE < ", mydate, "  then 'CURRENT_DAY'
              when FLIGHT_DATE >= ", mydate, " -1-364 and FLIGHT_DATE < ", mydate, "-364  then 'DAY_PREV_YEAR'
              when FLIGHT_DATE >= ", mydate, " -1-7 and FLIGHT_DATE < ", mydate, "-7  then 'DAY_PREV_WEEK'
              when FLIGHT_DATE >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 -1
                    and FLIGHT_DATE < ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                    then 'DAY_2019'
                    else '-'
          end  flag_day
 from COUNTRY_FLIGHT where iso_ct_name in (select iso_ct_name from CTRY_LIST)
 )


, COUNTRY_FLIGHT_GROUP as  (
select
      flag_day, iso_ct_name, ad, ad_name,
      sum(mvt) DEP_ARR

 from COUNTRY_FLIGHT_ECAC a
 where iso_ct_name <> '#UNKNOWN#'
group by ad, ad_name , iso_ct_name, flag_day

)

, APT_RANK_DAY as(
SELECT  flag_day, iso_ct_name, ad,
        ROW_NUMBER() OVER (PARTITION BY  iso_ct_name, flag_day
                ORDER BY dep_arr DESC, ad_name) R_RANK,
        RANK() OVER (PARTITION BY  iso_ct_name, flag_day
                ORDER BY dep_arr DESC) RANK
FROM COUNTRY_FLIGHT_GROUP
WHERE flag_day <> '-' AND flag_day = 'CURRENT_DAY'
),

APT_RANK_PREV_WEEK as
(SELECT  flag_day, iso_ct_name, ad,
        ROW_NUMBER() OVER (PARTITION BY  iso_ct_name, flag_day
                ORDER BY dep_arr DESC) RANK_PREV_WEEK
FROM COUNTRY_FLIGHT_GROUP
WHERE flag_day <> '-' AND flag_day = 'DAY_PREV_WEEK'
),

DATA_FILTERED as (
select  a.iso_ct_name as country_name,
        a.flag_day,
        a.ad as airport_code,
        ad_name as airport_name,
        dep_arr,
        r_rank,
        rank,
        RANK_PREV_WEEK,
        ", mydate, "-1 as to_date

 from COUNTRY_FLIGHT_GROUP a
 left join APT_RANK_DAY b on a.iso_ct_name = b.iso_ct_name  AND a.ad = b.ad
 left join APT_RANK_PREV_WEEK c on a.iso_ct_name = c.iso_ct_name  AND a.ad = c.ad
 where r_rank <= '10'
)

select  country_name,
        flag_day,
        case when country_name in ('ICELAND', 'Iceland')
          and (to_date < '01-jan-2024'
               or
               (to_date < '01-jan-2025' and flag_day in ('DAY_PREV_YEAR'))
               or
               (flag_day in ('DAY_2019'))
               )
            then NULL else airport_code
        end airport_code,
        case when country_name in ('ICELAND', 'Iceland')
          and (to_date < '01-jan-2024'
               or
               (to_date < '01-jan-2025' and flag_day in ('DAY_PREV_YEAR'))
               or
               (flag_day in ('DAY_2019'))
               )
            then NULL else airport_name
        end airport_name,
        case when country_name in ('ICELAND', 'Iceland')
          and (to_date < '01-jan-2024'
               or
               (to_date < '01-jan-2025' and flag_day in ('DAY_PREV_YEAR'))
               or
               (flag_day in ('DAY_2019'))
               )
            then NULL else dep_arr
        end dep_arr,
        r_rank,
        rank,
        RANK_PREV_WEEK,
        to_date

 from DATA_FILTERED
 order by country_name,  flag_day, r_rank
"
)
}

## week ----
query_st_apt_data_week_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
with

REL_AP_CTRY as (
select cfmu_ap_code ,
       CASE WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'GC' then 'Spain Canaries'
            WHEN country_name = 'SPAIN' then 'Spain Continental'
            ELSE country_name
       END iso_ct_name,
       region,
       pru_dashboard_ap_name  as ad_name
 from  prudev.v_covid_rel_airport_area

),

CTRY_LIST as (select distinct
                   iso_ct_name
                    from REL_AP_CTRY where region = 'ECAC'
                or iso_ct_name in ('ISRAEL', 'MOROCCO')
 )

, DATA_FLIGHT_DEP as (
SELECT nvl(A.adep_day_all_trf,0) AS mvt,
                A.adep_day_adep  ad, 'DEP' as airport_flow,
                 A.adep_DAY_FLT_DATE  AS FLIGHT_DATE
 FROM  prudev.v_aiu_agg_dep_day A
  where
                   (
                      (
                         A.adep_DAY_FLT_DATE >=  ", mydate, " - 7
                        AND A.adep_DAY_FLT_DATE < ", mydate, "-0
                        )
                        or
                      (
                         A.adep_DAY_FLT_DATE >=  ", mydate, " -7 -7
                        AND A.adep_DAY_FLT_DATE < ", mydate, "-0 -7
                        )
                        or
                      (
                         A.adep_DAY_FLT_DATE >=  ", mydate, " -7 -364
                        AND A.adep_DAY_FLT_DATE < ", mydate, "-0 -364
                        )

                        or
                      (
                        A.adep_DAY_FLT_DATE >=  ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 -7
                        AND A.adep_DAY_FLT_DATE < ", mydate, "-((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        )
                   )
),

DATA_FLIGHT_ARR as (
SELECT nvl(A.ades_day_all_trf,0) AS mvt,
                A.ades_day_ades_ctfm  ad, 'ARR' as airport_flow,
                 A.ades_DAY_FLT_DATE  AS FLIGHT_DATE
 FROM prudev.v_aiu_agg_arr_day A
  where
                   (
                      (
                         A.ades_DAY_FLT_DATE >=  ", mydate, " -7
                        AND A.ades_DAY_FLT_DATE < ", mydate, "-0
                        )
                        or
                      (
                         A.ades_DAY_FLT_DATE >=  ", mydate, " -7 -7
                        AND A.ades_DAY_FLT_DATE < ", mydate, "-0 -7
                        )
                        or
                      (
                         A.ades_DAY_FLT_DATE >=  ", mydate, " -7 -364
                        AND A.ades_DAY_FLT_DATE < ", mydate, "-0 -364
                        )
                        or
                        (
                        A.ades_DAY_FLT_DATE >=  ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 -7
                        AND A.ades_DAY_FLT_DATE < ", mydate, "-((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        )
                   )
),

COUNTRY_FLIGHT as
(
SELECT FLIGHT_DATE,
        mvt,
        a.ad,
      a.airport_flow,
        c.ad_name,
        c.iso_ct_name
  FROM DATA_FLIGHT_DEP a   JOIN REL_AP_CTRY C on (a.ad = c.cfmu_ap_code)
UNION ALL -- union is all as we count all dep and arrival
 SELECT FLIGHT_DATE,
       mvt,
        a.ad,
        a.airport_flow,
        c.ad_name,
        c.iso_ct_name
  FROM DATA_FLIGHT_ARR a   JOIN REL_AP_CTRY C on (a.ad = c.cfmu_ap_code)

 ),


COUNTRY_FLIGHT_ECAC as

 (select
        mvt,
        ad,
        ad_name,
        iso_ct_name ,
        airport_flow,
         case when FLIGHT_DATE >= ", mydate, " -7 and FLIGHT_DATE < ", mydate, "  then 'CURRENT_ROLLING_WEEK'
              when FLIGHT_DATE >= ", mydate, " -7-364 and FLIGHT_DATE < ", mydate, "-364  then 'ROLLING_WEEK_PREV_YEAR'
              when FLIGHT_DATE >= ", mydate, " -7-7 and FLIGHT_DATE < ", mydate, "-7  then 'PREV_ROLLING_WEEK'
              when FLIGHT_DATE >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 -7
                    and FLIGHT_DATE < ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                    then 'ROLLING_WEEK_2019'
                    else '-'
          end  flag_rolling_week
 from COUNTRY_FLIGHT where iso_ct_name in (select iso_ct_name from CTRY_LIST)),


 COUNTRY_FLIGHT_GROUP as
(
select
      flag_rolling_week, iso_ct_name, ad, ad_name,
      sum(mvt) DEP_ARR

 from COUNTRY_FLIGHT_ECAC a
 where iso_ct_name <> '#UNKNOWN#'
group by ad, ad_name , iso_ct_name, flag_rolling_week

),

APT_RANK_DAY as
(SELECT  flag_rolling_week, iso_ct_name, ad,
        ROW_NUMBER() OVER (PARTITION BY  iso_ct_name, flag_rolling_week
                ORDER BY dep_arr DESC, ad_name) R_RANK,
        RANK() OVER (PARTITION BY  iso_ct_name, flag_rolling_week
                ORDER BY dep_arr DESC) RANK
FROM COUNTRY_FLIGHT_GROUP
WHERE flag_rolling_week <> '-' AND flag_rolling_week = 'CURRENT_ROLLING_WEEK'
),

APT_RANK_PREV_WEEK as
(SELECT  flag_rolling_week, iso_ct_name, ad,
        ROW_NUMBER() OVER (PARTITION BY  iso_ct_name, flag_rolling_week
                ORDER BY dep_arr DESC) RANK_PREV_WEEK
FROM COUNTRY_FLIGHT_GROUP
WHERE flag_rolling_week <> '-' AND flag_rolling_week = 'PREV_ROLLING_WEEK'
),

DATA_FILTERED as (
select  a.iso_ct_name as country_name, a.flag_rolling_week, a.ad as airport_code, ad_name as airport_name,
       dep_arr, r_rank, rank, RANK_PREV_WEEK,
        ", mydate, "-7 as from_date,
        ", mydate, "-1 as to_date

 from COUNTRY_FLIGHT_GROUP a
 left join APT_RANK_DAY b on a.iso_ct_name = b.iso_ct_name  AND a.ad = b.ad
 left join APT_RANK_PREV_WEEK c on a.iso_ct_name = c.iso_ct_name  AND a.ad = c.ad
 where r_rank <= '10'
)

select
        country_name,
        flag_rolling_week,
        case when country_name in ('ICELAND', 'Iceland')
                  and (to_date < '01-jan-2024'
                       or
                       (to_date < '01-jan-2025' and flag_rolling_week in ('ROLLING_WEEK_PREV_YEAR'))
                       or
                       (flag_rolling_week in ('ROLLING_WEEK_2019'))
                       )
            then NULL else airport_code
        end airport_code,
        case when country_name in ('ICELAND', 'Iceland')
                  and (to_date < '01-jan-2024'
                       or
                       (to_date < '01-jan-2025' and flag_rolling_week in ('ROLLING_WEEK_PREV_YEAR'))
                       or
                       (flag_rolling_week in ('ROLLING_WEEK_2019'))
                       )
            then NULL else airport_name
        end airport_name,
        case when country_name in ('ICELAND', 'Iceland')
                  and (to_date < '01-jan-2024'
                       or
                       (to_date < '01-jan-2025' and flag_rolling_week in ('ROLLING_WEEK_PREV_YEAR'))
                       or
                       (flag_rolling_week in ('ROLLING_WEEK_2019'))
                       )
            then NULL else dep_arr
        end dep_arr,
        r_rank,
        rank,
        RANK_PREV_WEEK,
        from_date,
        to_date

 from DATA_FILTERED
order by country_name,  flag_rolling_week, r_rank
"
)
}

## year to date ----
query_st_apt_data_y2d_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
with

REL_AP_CTRY as (
select cfmu_ap_code ,
       CASE WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'GC' then 'Spain Canaries'
            WHEN country_name = 'SPAIN' then 'Spain Continental'
            ELSE country_name
       END iso_ct_name,
       region,
       pru_dashboard_ap_name  as ad_name
 from  prudev.v_covid_rel_airport_area
where region = 'ECAC'
                or iso_ct_name in ('ISRAEL', 'MOROCCO')
),


REF_DATES as
(
SELECT
  year,
  min(day_date) as from_date,
  max(day_date) as to_date,
  max(day_date) - min(day_date) + 1 as no_days
FROM prudev.pru_time_references
where day_date >= TO_DATE ('01-01-2019', 'dd-mm-yyyy')
        AND TO_NUMBER (TO_CHAR (TRUNC (day_date), 'mmdd')) <=   TO_NUMBER (TO_CHAR (", mydate, "-1, 'mmdd'))
        AND year <= extract(year from (", mydate, "-1))
group by year
),

DATA_FLIGHT_DEP as (
SELECT nvl(A.adep_day_all_trf,0) AS mvt,
                A.adep_day_adep  ad, 'DEP' as airport_flow,
                 A.adep_DAY_FLT_DATE  AS FLIGHT_DATE,
                 extract (year from A.adep_DAY_FLT_DATE) as year,
                  c.ad_name,
        c.iso_ct_name
 FROM prudev.v_aiu_agg_dep_day A  JOIN REL_AP_CTRY C on (A.adep_day_adep = c.cfmu_ap_code)
  where
        A.adep_DAY_FLT_DATE >= TO_DATE ('01-01-2019', 'dd-mm-yyyy') AND A.adep_DAY_FLT_DATE < ", mydate, "
        AND TO_NUMBER (TO_CHAR (TRUNC (adep_DAY_FLT_DATE), 'mmdd')) <=   TO_NUMBER (TO_CHAR (", mydate, "-1, 'mmdd'))
),

DATA_FLIGHT_DEP_YTD
as ( select year, ad,  ad_name, iso_ct_name,sum(mvt) as mvt, airport_flow from data_flight_dep group by  year, ad, ad_name, iso_ct_name, airport_flow) ,



DATA_FLIGHT_ARR as (
SELECT nvl(A.ades_day_all_trf,0) AS mvt,
                A.ades_day_ades_ctfm  ad, 'ARR' as airport_flow,
                 A.ades_DAY_FLT_DATE  AS FLIGHT_DATE
                 , extract (year from A.ades_DAY_FLT_DATE) as year,
                  c.ad_name,
        c.iso_ct_name
 FROM  prudev.v_aiu_agg_arr_day  A  JOIN REL_AP_CTRY C on (  A.ades_day_ades_ctfm  = c.cfmu_ap_code)
 WHERE
        A.ades_DAY_FLT_DATE >= TO_DATE ('01-01-2019', 'dd-mm-yyyy') AND A.ades_DAY_FLT_DATE < ", mydate, "
        AND TO_NUMBER (TO_CHAR (TRUNC (ades_DAY_FLT_DATE), 'mmdd')) <=   TO_NUMBER (TO_CHAR (", mydate, "-1, 'mmdd'))
),

DATA_FLIGHT_ARR_YTD
as ( select year,ad, ad_name, iso_ct_name, sum(mvt) as mvt, airport_flow from data_flight_arr group by year, ad, ad_name, iso_ct_name, airport_flow),

AIRPORT_LIST as
(
SELECT year,
        a.ad,
        a.ad_name,
        a.iso_ct_name
  FROM DATA_FLIGHT_DEP_YTD a
UNION ALL -- union is all as we count all dep and arrival
 SELECT  year,
        a.ad,
        a.ad_name,
        a.iso_ct_name
  FROM DATA_FLIGHT_ARR_YTD a
),

COUNTRY_FLIGHT as
(
SELECT DISTINCT a.ad, a.ad_name, a.year, a.iso_ct_name,
       b.mvt as ARR, c.mvt as DEP,
       (COALESCE(b.mvt, 0) + COALESCE(c.mvt, 0)) as dep_arr
from AIRPORT_LIST a
left join DATA_FLIGHT_ARR_YTD b on a.ad = b.ad AND a.year = b.year
left join DATA_FLIGHT_DEP_YTD c on a.ad = c.ad AND a.year = c.year
),

APT_RANK_PY as
(
SELECT  ad, year, iso_ct_name,
        RANK() OVER (PARTITION BY  iso_ct_name, year
                ORDER BY dep_arr DESC) RANK_PREV_YEAR
FROM COUNTRY_FLIGHT
WHERE year = extract (year from (", mydate, "-1)) -1
),

APT_RANK as
(
SELECT  ad, year, iso_ct_name,
        ROW_NUMBER() OVER (PARTITION BY  iso_ct_name, year
                ORDER BY dep_arr DESC, ad) R_RANK,
        RANK() OVER (PARTITION BY  iso_ct_name, year
                ORDER BY dep_arr DESC) RANK_CURRENT
FROM COUNTRY_FLIGHT
WHERE year = extract (year from (", mydate, "-1))
),

DATA_FILTERED as (
SELECT
        a.iso_ct_name as country_name,
        a.year,
        a.ad as airport_code, a.ad_name as airport_name,
        dep_arr
       , dep_arr/d.no_days as avg_dep_arr
       , r_rank, RANK_CURRENT, RANK_PREV_YEAR,
       d.from_date,
       d.to_date,
       max(d.to_date) over () as max_to_date
FROM COUNTRY_FLIGHT a
left join APT_RANK_PY b on a.ad = b.ad
left join APT_RANK c on a.ad = c.ad
left join REF_DATES d on a.year=d.year
where r_rank <=10
)

SELECT
        country_name,
        year,
        case when country_name in ('ICELAND', 'Iceland')
                  and (max_to_date < '01-jan-2024'
                       or
                       (max_to_date < '01-jan-2025' and to_date < '01-jan-2024')
                       or
                       (to_date < '01-jan-2024')
                       )
            then NULL else airport_code
        end airport_code,
        case when country_name in ('ICELAND', 'Iceland')
                  and (max_to_date < '01-jan-2024'
                       or
                       (max_to_date < '01-jan-2025' and to_date < '01-jan-2024')
                       or
                       (to_date < '01-jan-2024')
                       )
            then NULL else airport_name
        end airport_name,
        case when country_name in ('ICELAND', 'Iceland')
                  and (max_to_date < '01-jan-2024'
                       or
                       (max_to_date < '01-jan-2025' and to_date < '01-jan-2024')
                       or
                       (to_date < '01-jan-2024')
                       )
            then NULL else dep_arr
        end dep_arr,
        case when country_name in ('ICELAND', 'Iceland')
                  and (max_to_date < '01-jan-2024'
                       or
                       (max_to_date < '01-jan-2025' and to_date < '01-jan-2024')
                       or
                       (to_date < '01-jan-2024')
                       )
            then NULL else avg_dep_arr
        end avg_dep_arr,
        r_rank, RANK_CURRENT, RANK_PREV_YEAR,
        from_date,
        to_date
FROM DATA_FILTERED a
order by country_name, year DESC, r_rank
"
  )
}

# state pair ----
## day ----
query_st_st_data_day_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
with

REL_AP_CTRY as (
select cfmu_ap_code ,
       CASE WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'GC' then 'IC'
            WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'GE' then 'ES'
            WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'LE' then 'ES'
            ELSE country_code
       END country_code,
       region,
       pru_dashboard_ap_name  as ad_name
 from  prudev.v_covid_rel_airport_area
 where iso_ct_code <> '##'

),

CTRY_LIST as (
select distinct
            case when country_code = 'ES' then 'Spain Continental'
                else country_name
            end iso_ct_name,
            country_code as iso_ct_code
from prudev.v_covid_rel_airport_area where region = 'ECAC'
                              or country_name in ('ISRAEL', 'MOROCCO')
UNION ALL
SELECT
        'Spain Canaries' AS iso_ct_name,
         'IC' AS iso_ct_code
FROM dual
 ),

CTRY_LIST_ALL as (
select
            case when aiu_iso_country_code = 'ES' then 'Spain Continental'
                 else aiu_iso_country_name
            end ec_iso_ct_name,
            aiu_iso_country_code as ec_iso_ct_code
from prudev.pru_country_iso
group by aiu_iso_country_name,  aiu_iso_country_code

UNION ALL

SELECT
        'Spain Canaries' AS ec_iso_ct_name,
        'IC' AS ec_iso_ct_code
FROM dual
),


AIRP_FLIGHT as (
/* Formatted on 06-07-2020 15:56:37 (QP5 v5.318) */
SELECT count(flt_uid) as mvt,
       TRUNC (flt_a_asp_prof_time_entry) AS entry_day,
       c1.country_code dep_ctry ,c2.country_code arr_ctry

  FROM v_aiu_flt a, REL_AP_CTRY c1, REL_AP_CTRY c2
 WHERE       (
                      (     A.flt_lobt >= ", mydate, " -1 -1
                        AND A.flt_lobt < ", mydate, "-0
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -1
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0
                        )
                        or
                      (     A.flt_lobt >= ", mydate, " -1-1-7
                        AND A.flt_lobt < ", mydate, "-0-7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -1 -7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0 -7
                        )
                        or
                      (     A.flt_lobt >= ", mydate, " -1-1-364
                        AND A.flt_lobt < ", mydate, "-0-364
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -1-364
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0 -364
                        )
                        or
                        (     A.flt_lobt >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 -1-1
                        AND A.flt_lobt < ", mydate, "-((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 -1
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7



                        )
                     )
       AND A.flt_state IN ('TE', 'TA', 'AA')
       AND  flt_dep_ad IS NOT NULL
       AND    flt_ctfm_ades
 IS NOT NULL
       AND flt_dep_ad = c1.cfmu_ap_code and    flt_ctfm_ades
 = c2.cfmu_ap_code
       GROUP BY c1.country_code, c2.country_code, TRUNC (flt_a_asp_prof_time_entry)
     ),


CTRY_PAIR_FLIGHT as
(
SELECT entry_day,
      CASE WHEN   dep_ctry <= arr_ctry
           THEN dep_ctry
           ELSE   arr_ctry
       END
        ctry1,
      CASE WHEN  dep_ctry <= arr_ctry
           THEN arr_ctry
           ELSE   dep_ctry
       END
        ctry2 , mvt,
         case when entry_day >= ", mydate, " -1 and entry_day < ", mydate, "  then 'CURRENT_DAY'
              when entry_day >= ", mydate, " -1-364 and entry_day < ", mydate, "-364  then 'DAY_PREV_YEAR'
              when entry_day >= ", mydate, " -1-7 and entry_day < ", mydate, "-7  then 'DAY_PREV_WEEK'
              when entry_day >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 -1
                    and entry_day < ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                    then 'DAY_2019'
                    else '-'
          end  flag_day
  FROM AIRP_FLIGHT

  ),

 CTRY_PAIR_ARP_1 as
 (SELECT
        flag_day,
        ctry1,
        ctry2,
        sum(mvt) as TOT_MVT
 FROM
 CTRY_PAIR_FLIGHT
 group by flag_day, ctry1, ctry2

 ),

 CTRY_PAIR_ARP_2 as
(
SELECT flag_day,
       ctry2 as ctry1,
       ctry1 as ctry2,
       TOT_MVT
FROM CTRY_PAIR_ARP_1
  WHERE ctry1 <> ctry2
 ),

 CTRY_PAIR_ARP as (
 SELECT ctry1, ctry2, ctry1 as country_id, flag_day, TOT_MVT
 FROM CTRY_PAIR_ARP_1
 UNION ALL
 SELECT ctry1, ctry2, ctry1 as country_id, flag_day, TOT_MVT
 FROM  CTRY_PAIR_ARP_2
 ),

 CTRY_PAIR_SELECTION as
 (SELECT ctry1, ctry2, country_id , flag_day,
      TOT_MVT,

      ctry1  ||  ' <-> ' ||  ctry2 country_pair
 FROM  CTRY_PAIR_ARP
  WHERE country_id in (select iso_ct_code from ctry_list) AND ctry2 <> '#UNKNOWN#'


 ),

CTRY_PAIR_RANK as
(
 select
            country_id,
            country_pair,
            flag_day,
            RANK() OVER (PARTITION BY  country_id, flag_day
                ORDER BY TOT_MVT DESC) RANK,
            ROW_NUMBER() OVER (PARTITION BY  country_id, flag_day
                ORDER BY TOT_MVT DESC, ctry2) R_RANK
from CTRY_PAIR_SELECTION
where flag_day = 'CURRENT_DAY'
),

CTRY_PAIR_RANK_PREV as
(
 select
            country_id,
            country_pair,
            flag_day,
            ROW_NUMBER() OVER (PARTITION BY  country_id, flag_day
                ORDER BY TOT_MVT DESC) RANK_PREV_WEEK
from CTRY_PAIR_SELECTION
where flag_day = 'DAY_PREV_WEEK'
),

DATA_FILTERED as (
 select
            d.ec_iso_ct_name as country_name,
            a.flag_day,
            ctry2 as from_to_iso_ct_code,
            e.ec_iso_ct_name as from_to_country_name,
            TOT_MVT,
            r_rank, rank, rank_prev_week,
            ", mydate, "-1 as to_date

from CTRY_PAIR_SELECTION a

 left join CTRY_PAIR_RANK b on a.country_id =  b.country_id AND a.country_pair =  b.country_pair
 left join CTRY_PAIR_RANK_PREV c on a.country_id =  c.country_id AND a.country_pair =  c.country_pair
 left join CTRY_LIST_ALL d on a.country_id = d.ec_iso_ct_code
 left join CTRY_LIST_ALL e on a.ctry2 = e.ec_iso_ct_code
where r_rank <= '10'
)

 select
          country_name,
          flag_day,
          case when country_name in ('ICELAND', 'Iceland')
          and (to_date < '01-jan-2024'
               or
               (to_date < '01-jan-2025' and flag_day in ('DAY_PREV_YEAR'))
               or
               (flag_day in ('DAY_2019'))
               )
            then NULL else from_to_iso_ct_code
        end from_to_iso_ct_code,
        case when country_name in ('ICELAND', 'Iceland')
          and (to_date < '01-jan-2024'
               or
               (to_date < '01-jan-2025' and flag_day in ('DAY_PREV_YEAR'))
               or
               (flag_day in ('DAY_2019'))
               )
            then NULL else from_to_country_name
        end from_to_country_name,
        case when country_name in ('ICELAND', 'Iceland')
          and (to_date < '01-jan-2024'
               or
               (to_date < '01-jan-2025' and flag_day in ('DAY_PREV_YEAR'))
               or
               (flag_day in ('DAY_2019'))
               )
            then NULL else TOT_MVT
        end TOT_MVT,
        r_rank, rank, rank_prev_week,
        to_date

from DATA_FILTERED a
order by country_name,   flag_day, r_rank
"
)
}

## week ----
query_st_st_data_week_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
with

REL_AP_CTRY as (
select cfmu_ap_code ,
       CASE WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'GC' then 'IC'
            WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'GE' then 'ES'
            WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'LE' then 'ES'
            ELSE country_code
       END country_code,
       region,
       pru_dashboard_ap_name  as ad_name
 from  prudev.v_covid_rel_airport_area
 where iso_ct_code <> '##'

),

CTRY_LIST as (
select distinct
            case when country_code = 'ES' then 'Spain Continental'
                else country_name
            end iso_ct_name,
            country_code as iso_ct_code
from prudev.v_covid_rel_airport_area where region = 'ECAC'
                              or country_name in ('ISRAEL', 'MOROCCO')
UNION ALL
SELECT
        'Spain Canaries' AS iso_ct_name,
         'IC' AS iso_ct_code
FROM dual
 ),

CTRY_LIST_ALL as (
select
            case when aiu_iso_country_code = 'ES' then 'Spain Continental'
                 else aiu_iso_country_name
            end ec_iso_ct_name,
            aiu_iso_country_code as ec_iso_ct_code
from prudev.pru_country_iso
group by aiu_iso_country_name,  aiu_iso_country_code

UNION ALL

SELECT
        'Spain Canaries' AS ec_iso_ct_name,
        'IC' AS ec_iso_ct_code
FROM dual
),

AIRP_FLIGHT as (
/* Formatted on 06-07-2020 15:56:37 (QP5 v5.318) */
SELECT count(flt_uid) as mvt,
       TRUNC (flt_a_asp_prof_time_entry) AS entry_day,
       c1.country_code dep_ctry ,c2.country_code arr_ctry

  FROM v_aiu_flt a, REL_AP_CTRY c1, REL_AP_CTRY c2
 WHERE       (
                      (     A.flt_lobt >= ", mydate, " -1 -7
                        AND A.flt_lobt < ", mydate, "-0
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0
                        )
                        or
                      (     A.flt_lobt >= ", mydate, " -1-7-7
                        AND A.flt_lobt < ", mydate, "-0-7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -7 -7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0 -7
                        )
                        or
                      (     A.flt_lobt >= ", mydate, " -1-7-364
                        AND A.flt_lobt < ", mydate, "-0-364
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -7-364
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0 -364
                        )
                        or
                        (     A.flt_lobt >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 -7-1
                        AND A.flt_lobt < ", mydate, "-((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 -7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7



                        )
                     )
       AND A.flt_state IN ('TE', 'TA', 'AA')
       AND  flt_dep_ad IS NOT NULL
       AND    flt_ctfm_ades
 IS NOT NULL
       AND flt_dep_ad = c1.cfmu_ap_code and    flt_ctfm_ades
 = c2.cfmu_ap_code
       GROUP BY c1.country_code, c2.country_code, TRUNC (flt_a_asp_prof_time_entry)
     ),


CTRY_PAIR_FLIGHT as
(
SELECT entry_day,
      CASE WHEN   dep_ctry <= arr_ctry
           THEN dep_ctry
           ELSE   arr_ctry
       END
        ctry1,
      CASE WHEN  dep_ctry <= arr_ctry
           THEN arr_ctry
           ELSE   dep_ctry
       END
        ctry2 , mvt,
         case when entry_day >= ", mydate, " -7 and entry_day < ", mydate, "  then 'CURRENT_ROLLING_WEEK'
            when entry_day >= ", mydate, " -364-7 and entry_day < ", mydate, " -364  then 'ROLLING_WEEK_PREV_YEAR'
            when entry_day >= ", mydate, " -7-7 and entry_day < ", mydate, " -7  then 'PREV_ROLLING_WEEK'
            when entry_day >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7-7
              and entry_day < ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
              then 'ROLLING_WEEK_2019'
         else '-'
    end  flag_rolling_week
  FROM AIRP_FLIGHT

  ),

 CTRY_PAIR_ARP_1 as
 (SELECT
        flag_rolling_week,
        ctry1,
        ctry2,
        sum(mvt) as TOT_MVT
 FROM
 CTRY_PAIR_FLIGHT
 group by flag_rolling_week, ctry1, ctry2

 ),

 CTRY_PAIR_ARP_2 as
(
SELECT flag_rolling_week,
       ctry2 as ctry1,
       ctry1 as ctry2,
       TOT_MVT
FROM CTRY_PAIR_ARP_1
  WHERE ctry1 <> ctry2
 ),

 CTRY_PAIR_ARP as (
 SELECT ctry1, ctry2, ctry1 as country_id, flag_rolling_week, TOT_MVT
 FROM CTRY_PAIR_ARP_1
 UNION ALL
 SELECT ctry1, ctry2, ctry1 as country_id, flag_rolling_week, TOT_MVT
 FROM  CTRY_PAIR_ARP_2
 ),

 CTRY_PAIR_SELECTION as
 (SELECT ctry1, ctry2, country_id , flag_rolling_week,
      TOT_MVT,

      ctry1  ||  ' <-> ' ||  ctry2 country_pair
 FROM  CTRY_PAIR_ARP
  WHERE country_id in (select iso_ct_code from ctry_list) AND ctry2 <> '#UNKNOWN#'


 ),

CTRY_PAIR_RANK as
(
 select
            country_id,
            country_pair,
            flag_rolling_week,
            RANK() OVER (PARTITION BY  country_id, flag_rolling_week
                ORDER BY TOT_MVT DESC) RANK,
            ROW_NUMBER() OVER (PARTITION BY  country_id, flag_rolling_week
                ORDER BY TOT_MVT DESC, ctry2) R_RANK
from CTRY_PAIR_SELECTION
where flag_rolling_week = 'CURRENT_ROLLING_WEEK'
),

CTRY_PAIR_RANK_PREV as
(
 select
            country_id,
            country_pair,
            flag_rolling_week,
            ROW_NUMBER() OVER (PARTITION BY  country_id, flag_rolling_week
                ORDER BY TOT_MVT DESC) RANK_PREV_WEEK
from CTRY_PAIR_SELECTION
where flag_rolling_week = 'PREV_ROLLING_WEEK'
),

DATA_FILTERED as (
 select
            d.ec_iso_ct_name as country_name,
            a.flag_rolling_week,
            ctry2 as from_to_iso_ct_code,
            e.ec_iso_ct_name as from_to_country_name,
            TOT_MVT,
            r_rank, rank, rank_prev_week,
            ", mydate, "-7 as from_date,
            ", mydate, "-1 as to_date

from CTRY_PAIR_SELECTION a
 left join CTRY_PAIR_RANK b on a.country_id =  b.country_id AND a.country_pair =  b.country_pair
 left join CTRY_PAIR_RANK_PREV c on a.country_id =  c.country_id AND a.country_pair =  c.country_pair
 left join CTRY_LIST_ALL d on a.country_id = d.ec_iso_ct_code
 left join CTRY_LIST_ALL e on a.ctry2 = e.ec_iso_ct_code
where r_rank <= '10'
)

 select
        country_name,
        flag_rolling_week,
        case when country_name in ('ICELAND', 'Iceland')
                  and (to_date < '01-jan-2024'
                       or
                       (to_date < '01-jan-2025' and flag_rolling_week in ('ROLLING_WEEK_PREV_YEAR'))
                       or
                       (flag_rolling_week in ('ROLLING_WEEK_2019'))
                       )
            then NULL else from_to_iso_ct_code
        end from_to_iso_ct_code,
        case when country_name in ('ICELAND', 'Iceland')
                  and (to_date < '01-jan-2024'
                       or
                       (to_date < '01-jan-2025' and flag_rolling_week in ('ROLLING_WEEK_PREV_YEAR'))
                       or
                       (flag_rolling_week in ('ROLLING_WEEK_2019'))
                       )
            then NULL else from_to_country_name
        end from_to_country_name,
        case when country_name in ('ICELAND', 'Iceland')
                  and (to_date < '01-jan-2024'
                       or
                       (to_date < '01-jan-2025' and flag_rolling_week in ('ROLLING_WEEK_PREV_YEAR'))
                       or
                       (flag_rolling_week in ('ROLLING_WEEK_2019'))
                       )
            then NULL else TOT_MVT
        end TOT_MVT,
        r_rank, rank, rank_prev_week,
        from_date,
        to_date

from DATA_FILTERED
order by country_name,   flag_rolling_week, r_rank
"
  )
}

## year to date ----
query_st_st_data_y2d_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
with

REL_AP_CTRY as (
select cfmu_ap_code ,
       CASE WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'GC' then 'IC'
            WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'GE' then 'ES'
            WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'LE' then 'ES'
            ELSE country_code
       END country_code,
       region,
       pru_dashboard_ap_name  as ad_name
 from  prudev.v_covid_rel_airport_area
 where iso_ct_code <> '##'

),

CTRY_LIST as (
select distinct
            case when country_code = 'ES' then 'Spain Continental'
                else country_name
            end iso_ct_name,
            country_code as iso_ct_code
from prudev.v_covid_rel_airport_area where region = 'ECAC'
                              or country_name in ('ISRAEL', 'MOROCCO')
UNION ALL
SELECT
        'Spain Canaries' AS iso_ct_name,
         'IC' AS iso_ct_code
FROM dual
 ),

CTRY_LIST_ALL as (
select
            case when aiu_iso_country_code = 'ES' then 'Spain Continental'
                 else aiu_iso_country_name
            end ec_iso_ct_name,
            aiu_iso_country_code as ec_iso_ct_code
from prudev.pru_country_iso
group by aiu_iso_country_name,  aiu_iso_country_code

UNION ALL

SELECT
        'Spain Canaries' AS ec_iso_ct_name,
        'IC' AS ec_iso_ct_code
FROM dual
),


REF_DATES as
(
SELECT
  year,
  min(day_date) as from_date,
  max(day_date) as to_date,
  max(day_date) - min(day_date) + 1 as no_days
FROM prudev.pru_time_references
where day_date >= TO_DATE ('01-01-2019', 'dd-mm-yyyy')
        AND TO_NUMBER (TO_CHAR (TRUNC (day_date), 'mmdd')) <=   TO_NUMBER (TO_CHAR (", mydate, "-1, 'mmdd'))
        AND year <= extract(year from (", mydate, "-1))
group by year
),


AIRP_FLIGHT as (
/* Formatted on 06-07-2020 15:56:37 (QP5 v5.318) */
SELECT count(flt_uid) as mvt,
       TRUNC (flt_a_asp_prof_time_entry) AS entry_day,
       c1.country_code dep_ctry ,c2.country_code arr_ctry

  FROM v_aiu_flt a, REL_AP_CTRY c1, REL_AP_CTRY c2
 WHERE
      flt_dep_ad = c1.cfmu_ap_code and a.flt_ctfm_ades  = c2.cfmu_ap_code
       AND A.flt_lobt>= TO_DATE ('01-01-2019', 'dd-mm-yyyy') -2
       AND A.flt_lobt < ", mydate, " +2
       AND flt_a_asp_prof_time_entry >= TO_DATE ('01-01-2019', 'dd-mm-yyyy')
       AND TO_NUMBER (TO_CHAR (TRUNC (flt_a_asp_prof_time_entry), 'mmdd')) <=   TO_NUMBER (TO_CHAR (", mydate, "-1, 'mmdd'))
       AND extract (year from flt_a_asp_prof_time_entry) <= extract(year from (", mydate, "-1))

       AND A.flt_state IN ('TE', 'TA', 'AA')
       AND  flt_dep_ad IS NOT NULL
       AND   flt_ctfm_ades IS NOT NULL
       AND flt_dep_ad = c1.cfmu_ap_code and    flt_ctfm_ades = c2.cfmu_ap_code
 GROUP BY c1.country_code, c2.country_code, TRUNC (flt_a_asp_prof_time_entry)
     ),


CTRY_PAIR_FLIGHT as
(
SELECT entry_day,
      CASE WHEN   dep_ctry <= arr_ctry
           THEN dep_ctry
           ELSE   arr_ctry
       END
        ctry1,
      CASE WHEN  dep_ctry <= arr_ctry
           THEN arr_ctry
           ELSE   dep_ctry
       END
        ctry2 , mvt
  FROM AIRP_FLIGHT

  ),

 CTRY_PAIR_ARP_1 as
 (SELECT
        extract(year from entry_day) as year,
        ctry1,
        ctry2,
        sum(mvt) as TOT_MVT
 FROM
 CTRY_PAIR_FLIGHT
 group by  extract(year from entry_day), ctry1, ctry2

 ),

 CTRY_PAIR_ARP_2 as
(
SELECT year,
       ctry2 as ctry1,
       ctry1 as ctry2,
       TOT_MVT
FROM CTRY_PAIR_ARP_1
  WHERE ctry1 <> ctry2
 ),

 CTRY_PAIR_ARP as (
 SELECT ctry1, ctry2, ctry1 as country_id, year, TOT_MVT
 FROM CTRY_PAIR_ARP_1
 UNION ALL
 SELECT ctry1, ctry2, ctry1 as country_id, year, TOT_MVT
 FROM  CTRY_PAIR_ARP_2
 ),

 CTRY_PAIR_SELECTION as
 (SELECT ctry1, ctry2, country_id , year,
    ctry1  ||  ' <-> ' ||  ctry2 country_pair,
    TOT_MVT
 FROM  CTRY_PAIR_ARP
  WHERE country_id in (select iso_ct_code from ctry_list) AND ctry2 <> '#UNKNOWN#'
 ),

CTRY_PAIR_RANK as
(
 select
            country_pair,
            year,
            RANK() OVER (PARTITION BY  country_id, year
                ORDER BY TOT_MVT DESC) RANK_CURRENT,
            ROW_NUMBER() OVER (PARTITION BY  country_id, year
                ORDER BY TOT_MVT DESC, ctry2) R_RANK
from CTRY_PAIR_SELECTION
WHERE year = extract (year from (", mydate, "-1))
),

CTRY_PAIR_RANK_PREV as
(
 select
            country_pair,
            year,
            ROW_NUMBER() OVER (PARTITION BY  country_id, year
                ORDER BY TOT_MVT DESC) RANK_PREV_YEAR
from CTRY_PAIR_SELECTION
WHERE year = extract (year from (", mydate, "-1)) - 1
),

DATA_FILTERED as (
 select
            d.ec_iso_ct_name as country_name,
            a.year,
            ctry2 as from_to_iso_ct_code,
            e.ec_iso_ct_name as from_to_country_name,
            TOT_MVT,
            TOT_MVT/f.no_days  as AVG_MVT,
            r_rank, rank_current, rank_prev_year,
            f.from_date, f.to_date,
            max(f.to_date) over () as max_to_date

from CTRY_PAIR_SELECTION a

 left join CTRY_PAIR_RANK b on a.country_pair =  b.country_pair
 left join CTRY_PAIR_RANK_PREV c on a.country_pair =  c.country_pair
 left join CTRY_LIST_ALL d on a.country_id = d.ec_iso_ct_code
 left join CTRY_LIST_ALL e on a.ctry2 = e.ec_iso_ct_code
 left join REF_DATES f on a.year=f.year
where r_rank <= '10'
)

 select
        country_name,
        year,
        case when country_name in ('ICELAND', 'Iceland')
                  and (max_to_date < '01-jan-2024'
                       or
                       (max_to_date < '01-jan-2025' and to_date < '01-jan-2024')
                       or
                       (to_date < '01-jan-2024')
                       )
            then NULL else from_to_iso_ct_code
        end from_to_iso_ct_code,
        case when country_name in ('ICELAND', 'Iceland')
                  and (max_to_date < '01-jan-2024'
                       or
                       (max_to_date < '01-jan-2025' and to_date < '01-jan-2024')
                       or
                       (to_date < '01-jan-2024')
                       )
            then NULL else from_to_country_name
        end from_to_country_name,
        case when country_name in ('ICELAND', 'Iceland')
                  and (max_to_date < '01-jan-2024'
                       or
                       (max_to_date < '01-jan-2025' and to_date < '01-jan-2024')
                       or
                       (to_date < '01-jan-2024')
                       )
            then NULL else TOT_MVT
        end TOT_MVT,
        case when country_name in ('ICELAND', 'Iceland')
                  and (max_to_date < '01-jan-2024'
                       or
                       (max_to_date < '01-jan-2025' and to_date < '01-jan-2024')
                       or
                       (to_date < '01-jan-2024')
                       )
            then NULL else AVG_MVT
        end AVG_MVT,
        r_rank, rank_current, rank_prev_year,
        from_date,
        to_date

from DATA_FILTERED
order by country_name, year desc, r_rank
"
  )
}

