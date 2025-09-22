#nw traffic ----
query_nw_traffic_data <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0(
"
WITH NM_AREA_DATA
    AS
        (SELECT
                a_first_entry_time_date FLIGHT_DATE ,
                SUM(nvl(a.all_traffic,0)) DAY_TFC
           FROM  v_aiu_agg_global_daily_counts a
           WHERE
                 a.a_first_entry_time_date  >= to_date('24-12-2018','dd-mm-yyyy')
            AND a.a_first_entry_time_date  < ", mydate, "
           GROUP BY  a.a_first_entry_time_date
        ),

DAY_ZONE as
(
SELECT
                t.year,
                t.month,
                t.week,
                t.week_nb_year,
                t.day_type,
                t.day_of_week_nb,
                t.day_date
           FROM  pru_time_references t
           WHERE day_date  >=to_date('24-12-2018','dd-mm-yyyy')
--           and  day_date  <  ", mydate, "
             and day_date <= to_date('31-12-'|| extract(year from (", mydate, "-1)),'dd-mm-yyyy')
),

NM_AREA_DATA_DAY as
(SELECT
                t.year,
                t.month,
                t.week,
                t.week_nb_year,
                t.day_type,
                t.day_of_week_nb day_of_week,
                t.day_date,
                coalesce(a.flight_date,t.day_date) as flight_date,
                coalesce(a.DAY_TFC,0) as DAY_tfc ,
                'ZONE_AUA' as UNIT_KIND,
               'NM_AREA' as UNIT_CODE,
               '-Total Network Manager Area'  AS unit_name
           FROM  DAY_ZONE t
           left join  NM_AREA_DATA a on  a.flight_date = t.day_date
   ),

NM_AREA_Y2D as
(select day_date ,
       SUM (DAY_TFC) OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(day_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) Y2D_TFC_YEAR,
       SUM (DAY_TFC) OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(day_date, 'DDD'))-1) PRECEDING AND CURRENT ROW)/Count (DAY_TFC) OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(day_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) Y2D_AVG_TFC_YEAR

FROM NM_AREA_DATA_DAY
),

NM_AREA_CALC as
(select YEAR,
       MONTH,
       a.day_date as FLIGHT_DATE,
       UNIT_KIND,
       unit_code,
       UNIT_NAME,
       DAY_TFC,
       WEEK,
       WEEK_NB_YEAR,
       DAY_TYPE,
       day_of_week,
       LAG (DAY_TFC, greatest((extract (year from flight_date)-2019) *364+ floor((extract (year from flight_date)-2019)/4)*7,0)  ) OVER (PARTITION BY unit_code ORDER BY flight_date)  DAY_TFC_2019,
       LAG (flight_date,greatest((extract (year from flight_date)-2019) *364+ floor((extract (year from flight_date)-2019)/4)*7,0)  ) OVER (PARTITION BY unit_code ORDER BY flight_date)FLIGHT_DATE_2019,
       LAG (DAY_TFC, 7) OVER (PARTITION BY unit_code ORDER BY flight_date)  DAY_TFC_PREV_WEEK,
       LAG (flight_date,7) OVER (PARTITION BY unit_code ORDER BY flight_date)FLIGHT_DATE_PREV_WEEK,
       LAG (DAY_TFC, greatest((extract (year from flight_date)-2020) *364+ floor((extract (year from flight_date)-2020)/4)*7,0)  ) OVER (PARTITION BY unit_code ORDER BY flight_date)  DAY_TFC_2020,
       LAG (flight_date,greatest((extract (year from flight_date)-2020) *364+ floor((extract (year from flight_date)-2020)/4)*7,0)  ) OVER (PARTITION BY unit_code ORDER BY flight_date)FLIGHT_DATE_2020,

       LAG (DAY_TFC,  364) OVER (PARTITION BY unit_code ORDER BY flight_date)  DAY_TFC_PREV_YEAR,
       LAG (flight_date, 364) OVER (PARTITION BY unit_code ORDER BY flight_date)FLIGHT_DATE_PREV_YEAR,
       b.Y2D_TFC_YEAR,
       c.Y2D_TFC_YEAR as Y2D_TFC_PREV_YEAR,
       d.Y2D_TFC_YEAR as Y2D_TFC_2019,
       b.Y2D_AVG_TFC_YEAR,
       c.Y2D_AVG_TFC_YEAR as Y2D_AVG_TFC_PREV_YEAR,
       d.Y2D_AVG_TFC_YEAR as Y2D_AVG_TFC_2019,
--       e.Y2D_TFC_YEAR as Y2D_TFC_2020,
       AVG (DAY_TFC)  OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS AVG_rolling_week,
       SUM (DAY_TFC)  OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS TOTAL_rolling_week,
       AVG (DAY_TFC)  OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) AS AVG_rolling_PREV_WEEK
      FROM NM_AREA_DATA_DAY a
      left join NM_AREA_Y2D b on a.day_date = b.day_date
      left join NM_AREA_Y2D c on add_months(a.day_date,-12) = c.day_date
      left join NM_AREA_Y2D d on add_months(a.day_date,-12*(extract (year from a.day_date)-2019)) = d.day_date
--      left join NM_AREA_Y2D e on add_months(a.day_date,-12*(extract (year from a.day_date)-2020)) = e.day_date
where a.day_date >='24-dec-2018'
)  ,

 NM_AREA_CALC_PREV  as
  (
      select YEAR,
       MONTH,
       FLIGHT_DATE,
       UNIT_KIND,
       unit_code,
       UNIT_NAME,
       DAY_TFC,
       WEEK,
       WEEK_NB_YEAR,
       DAY_TYPE,
       day_of_week,
       TOTAL_rolling_week,
       DAY_TFC_PREV_WEEK,
       FLIGHT_DATE_PREV_WEEK,
       DAY_TFC_PREV_YEAR,
       FLIGHT_DATE_PREV_YEAR,
       DAY_TFC_2019,
       FLIGHT_DATE_2019,
       DAY_TFC_2020,
       FLIGHT_DATE_2020,
       Y2D_TFC_YEAR,
       Y2D_TFC_PREV_YEAR,
       Y2D_TFC_2019,
       Y2D_AVG_TFC_YEAR,
       Y2D_AVG_TFC_PREV_YEAR,
       Y2D_AVG_TFC_2019,
       AVG_rolling_week, AVG_rolling_PREV_WEEK,

       LAG (AVG_rolling_week,364) OVER (PARTITION BY unit_code ORDER BY flight_date)AVG_rolling_week_PREV_YEAR,
       LAG (AVG_rolling_week,greatest((extract (year from flight_date)-2020) *364+ floor((extract (year from flight_date)-2020)/4)*7,0)  ) OVER (PARTITION BY unit_code ORDER BY flight_date)AVG_rolling_week_2020,
       LAG (AVG_rolling_week,greatest((extract (year from flight_date)-2019) *364+ floor((extract (year from flight_date)-2019)/4)*7,0)  ) OVER (PARTITION BY unit_code ORDER BY flight_date)AVG_rolling_week_2019

--       AVG (DAY_TFC_PREV_YEAR)  OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS AVG_rolling_week_PREV_YEAR,
--       AVG (DAY_TFC_2019)  OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS AVG_rolling_week_2019
--       ,
--       AVG (DAY_TFC_2020)  OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS AVG_rolling_week_2020
      FROM NM_AREA_CALC
  )
  select
       unit_code,
       YEAR,
       MONTH,
       WEEK,
       WEEK_NB_YEAR,
       DAY_TYPE,
       day_of_week,

       FLIGHT_DATE,
       FLIGHT_DATE_PREV_YEAR,
       FLIGHT_DATE_2020,
       FLIGHT_DATE_2019,

       DAY_TFC,
       DAY_TFC_PREV_YEAR,
       DAY_TFC_2020,
       DAY_TFC_2019,

       DAY_TFC - DAY_TFC_PREV_WEEK as DAY_TFC_DIFF_PREV_WEEK,
       DAY_TFC - DAY_TFC_PREV_YEAR  as DAY_TFC_DIFF_PREV_YEAR,
       DAY_TFC - DAY_TFC_2019  as DAY_TFC_DIFF_2019,

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

       TOTAL_rolling_week,
       CASE WHEN flight_date >= ", mydate, "
           THEN NULL
           ELSE AVG_rolling_week
       END AVG_rolling_week,
       AVG_rolling_PREV_WEEK,
       AVG_rolling_week_PREV_YEAR,
       AVG_rolling_week_2020,
       AVG_rolling_week_2019,

       CASE WHEN AVG_rolling_PREV_WEEK  <>0  then
            AVG_rolling_week/AVG_rolling_PREV_WEEK -1
            ELSE NULL
       END as DIF_PREV_week_PERC,
       CASE WHEN AVG_rolling_week_PREV_YEAR  <>0  then
            AVG_rolling_week/AVG_rolling_week_PREV_YEAR -1
            ELSE NULL
       END as DIF_WEEK_PREV_YEAR_perc,
       CASE WHEN AVG_rolling_week_2019  <>0  then
            AVG_rolling_week/AVG_rolling_week_2019 -1
            ELSE NULL
       END as DIF_rolling_week_2019_PERC,

       Y2D_TFC_YEAR,
       Y2D_TFC_PREV_YEAR,
       Y2D_TFC_2019,
       CASE WHEN flight_date >= ", mydate, "
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

      ", mydate, "-1 as LAST_DATA_DAY
      FROM NM_AREA_CALC_PREV

"
)
}

# nw delay ----
query_nw_delay_data <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0(
"WITH NM_AREA_DATA
    AS
        (SELECT
                a_first_entry_time_date FLIGHT_DATE ,
                SUM(nvl(a.total_delay_in_minutes,0)) DAY_DLY,
                SUM(nvl(a.airport_delay_in_minutes,0)) DAY_DLY_APT

           FROM  prudev.v_aiu_agg_global_daily_counts a
           WHERE
                 a.a_first_entry_time_date  >= to_date('24-12-2018','dd-mm-yyyy')
            AND a.a_first_entry_time_date  < ", mydate, "
           GROUP BY  a.a_first_entry_time_date
        ),

DAY_ZONE as
(
SELECT
                t.year,
                t.month,
                t.week,
                t.week_nb_year,
                t.day_type,
                t.day_of_week_nb,
                t.day_date
           FROM  prudev.pru_time_references t
          WHERE day_date  >= to_date('24-12-2018','dd-mm-yyyy')
            and day_date <= to_date('31-12-'|| extract(year from (", mydate, "-1)),'dd-mm-yyyy')
),

NM_AREA_DATA_DAY as
(SELECT
                t.year,
                t.month,
                t.week,
                t.week_nb_year,
                t.day_type,
                t.day_of_week_nb day_of_week,
                t.day_date,
                coalesce(a.flight_date,t.day_date) as flight_date,
                coalesce(a.DAY_DLY,0) as day_dly ,
                coalesce(a.DAY_DLY_APT,0) as day_dly_apt ,
                coalesce(a.DAY_DLY,0) - coalesce(a.DAY_DLY_APT,0) AS day_dly_ert,
                'ZONE_AUA' as UNIT_KIND,
               'NM_AREA' as UNIT_CODE,
               '-Total Network Manager Area'  AS unit_name
           FROM  DAY_ZONE t
           left join  NM_AREA_DATA a on  a.flight_date = t.day_date
   ),

NM_AREA_Y2D as
(select day_date ,
       SUM (DAY_DLY) OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(day_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) y2d_dly_year,
       SUM (DAY_DLY) OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(day_date, 'DDD'))-1) PRECEDING AND CURRENT ROW)/Count (flight_date) OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(day_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) y2d_avg_dly_year,

       SUM (DAY_DLY_ERT) OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(day_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) y2d_dly_ert_year,
       SUM (DAY_DLY_ERT) OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(day_date, 'DDD'))-1) PRECEDING AND CURRENT ROW)/Count (flight_date) OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(day_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) y2d_avg_dly_ert_year,

       SUM (DAY_DLY_APT) OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(day_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) y2d_dly_apt_year,
       SUM (DAY_DLY_APT) OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(day_date, 'DDD'))-1) PRECEDING AND CURRENT ROW)/Count (flight_date) OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(day_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) y2d_avg_dly_apt_year

FROM NM_AREA_DATA_DAY
),

NM_AREA_CALC as
(SELECT
	   year,
       month,
       a.day_date AS flight_date,
       unit_kind,
       unit_code,
       unit_name,

       week,
       week_nb_year,
       day_type,
       day_of_week,

       day_dly,
       day_dly_ert,
       day_dly_apt,
       LAG (day_dly, greatest((extract (year from flight_date)-2019) *364+ floor((extract (year from flight_date)-2019)/4)*7,0)  ) OVER (PARTITION BY unit_code ORDER BY flight_date) day_dly_2019,
       LAG (day_dly_ert, greatest((extract (year from flight_date)-2019) *364+ floor((extract (year from flight_date)-2019)/4)*7,0)  ) OVER (PARTITION BY unit_code ORDER BY flight_date) day_dly_ert_2019,
       LAG (day_dly_apt, greatest((extract (year from flight_date)-2019) *364+ floor((extract (year from flight_date)-2019)/4)*7,0)  ) OVER (PARTITION BY unit_code ORDER BY flight_date) day_dly_apt_2019,
       LAG (flight_date, greatest((extract (year from flight_date)-2019) *364+ floor((extract (year from flight_date)-2019)/4)*7,0)  ) OVER (PARTITION BY unit_code ORDER BY flight_date) flight_date_2019,

       LAG (day_dly, greatest((extract (year from flight_date)-2020) *364+ floor((extract (year from flight_date)-2019)/4)*7,0)  ) OVER (PARTITION BY unit_code ORDER BY flight_date)  DAY_DLY_2020,
       LAG (flight_date,greatest((extract (year from flight_date)-2019) *364+ floor((extract (year from flight_date)-2019)/4)*7,0)  ) OVER (PARTITION BY unit_code ORDER BY flight_date)FLIGHT_DATE_2020,

       LAG (day_dly, 7) OVER (PARTITION BY unit_code ORDER BY flight_date)  day_dly_prev_week,
       LAG (day_dly_ert, 7) OVER (PARTITION BY unit_code ORDER BY flight_date)  day_dly_ert_prev_week,
       LAG (day_dly_apt, 7) OVER (PARTITION BY unit_code ORDER BY flight_date)  day_dly_apt_prev_week,
       LAG (flight_date,7) OVER (PARTITION BY unit_code ORDER BY flight_date)   flight_date_prev_week,

       LAG (DAY_DLY,  364) OVER (PARTITION BY unit_code ORDER BY flight_date)  day_dly_prev_year,
       LAG (DAY_DLY_ERT,  364) OVER (PARTITION BY unit_code ORDER BY flight_date)  DAY_DLY_ERT_PREV_YEAR,
       LAG (DAY_DLY_APT,  364) OVER (PARTITION BY unit_code ORDER BY flight_date)  DAY_DLY_APT_PREV_YEAR,
       LAG (flight_date, 364) OVER (PARTITION BY unit_code ORDER BY flight_date)FLIGHT_DATE_PREV_YEAR,

       b.Y2D_dly_YEAR,
       c.Y2D_dly_YEAR as Y2D_dly_PREV_YEAR,
       d.Y2D_dly_YEAR as Y2D_dly_2019,

       b.Y2D_dly_ert_YEAR,
       c.Y2D_dly_ert_YEAR as Y2D_dly_ert_PREV_YEAR,
       d.Y2D_dly_ert_YEAR as Y2D_dly_ert_2019,

       b.Y2D_dly_apt_YEAR,
       c.Y2D_dly_apt_YEAR as Y2D_dly_apt_PREV_YEAR,
       d.Y2D_dly_apt_YEAR as Y2D_dly_apt_2019,

       b.Y2D_AVG_dly_YEAR,
       c.Y2D_AVG_dly_YEAR as Y2D_AVG_dly_PREV_YEAR,
       d.Y2D_AVG_dly_YEAR as Y2D_AVG_dly_2019,

       b.Y2D_AVG_dly_ert_YEAR,
       c.Y2D_AVG_dly_ert_YEAR as Y2D_AVG_dly_ert_PREV_YEAR,
       d.Y2D_AVG_dly_ert_YEAR as Y2D_AVG_dly_ert_2019,

       b.Y2D_AVG_dly_apt_YEAR,
       c.Y2D_AVG_dly_apt_YEAR as Y2D_AVG_dly_apt_PREV_YEAR,
       d.Y2D_AVG_dly_apt_YEAR as Y2D_AVG_dly_apt_2019,

       AVG (DAY_DLY)  OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS AVG_rolling_week,
       SUM (DAY_DLY)  OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS TOTAL_rolling_week,
       AVG (DAY_DLY)  OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) AS AVG_rolling_PREV_WEEK,

       AVG (DAY_DLY_ert)  OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS AVG_ert_rolling_week,
       AVG (DAY_DLY_ert)  OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) AS AVG_ert_rolling_PREV_WEEK,

       AVG (DAY_DLY_apt)  OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS AVG_apt_rolling_week,
       AVG (DAY_DLY_apt)  OVER (PARTITION BY unit_code ORDER BY flight_date ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) AS AVG_apt_rolling_PREV_WEEK

FROM NM_AREA_DATA_DAY a
      left join NM_AREA_Y2D b on a.day_date = b.day_date
      left join NM_AREA_Y2D c on add_months(a.day_date,-12) = c.day_date
      left join NM_AREA_Y2D d on add_months(a.day_date,-12*(extract (year from a.day_date)-2019)) = d.day_date
--      left join NM_AREA_Y2D e on add_months(a.day_date,-12*(extract (year from a.day_date)-2020)) = e.day_date
where a.day_date >='24-dec-2018'
)  ,

 NM_AREA_CALC_PREV  as
  (
      SELECT
       unit_kind,
       unit_code,
       unit_name,

       year,
       month,
       flight_date,
       flight_date_prev_week,
       flight_date_prev_year,
       flight_date_2019,
       flight_date_2020,
       week,
       week_nb_year,
       day_type,
       day_of_week,

       --total
       day_dly,
       day_dly_prev_week,
       day_dly_prev_year,
       day_dly_2019,
       day_dly_2020,

       total_rolling_week,
       avg_rolling_week,
       avg_rolling_prev_week,

       y2d_dly_year,
       y2d_dly_prev_year,
       y2d_dly_2019,

       y2d_avg_dly_year,
       y2d_avg_dly_prev_year,
       y2d_avg_dly_2019,

       LAG (avg_rolling_week, 364) OVER (PARTITION BY unit_code ORDER BY flight_date) avg_rolling_week_prev_year,
       LAG (avg_rolling_week, greatest((extract (year from flight_date)-2020) *364+ floor((extract (year from flight_date)-2020)/4)*7,0)  ) OVER (PARTITION BY unit_code ORDER BY flight_date) avg_rolling_week_2020,
       LAG (avg_rolling_week, greatest((extract (year from flight_date)-2019) *364+ floor((extract (year from flight_date)-2019)/4)*7,0)  ) OVER (PARTITION BY unit_code ORDER BY flight_date) avg_rolling_week_2019,

       --en-route
       day_dly_ert,
       day_dly_ert_prev_week,
       day_dly_ert_prev_year,
       day_dly_ert_2019,

       avg_ert_rolling_week,
       avg_ert_rolling_prev_week,

       y2d_dly_ert_year,
       y2d_dly_ert_prev_year,
       y2d_dly_ert_2019,

       y2d_avg_dly_ert_year,
       y2d_avg_dly_ert_prev_year,
       y2d_avg_dly_ert_2019,

       LAG (avg_ert_rolling_week, 364) OVER (PARTITION BY unit_code ORDER BY flight_date) avg_ert_rolling_week_prev_year,
       LAG (avg_ert_rolling_week, greatest((extract (year from flight_date)-2019) *364+ floor((extract (year from flight_date)-2019)/4)*7,0)  ) OVER (PARTITION BY unit_code ORDER BY flight_date) avg_ert_rolling_week_2019,

       --airport
       day_dly_apt,
       day_dly_apt_prev_week,
       day_dly_apt_prev_year,
       day_dly_apt_2019,

       avg_apt_rolling_week,
       avg_apt_rolling_prev_week,

       y2d_dly_apt_year,
       y2d_dly_apt_prev_year,
       y2d_dly_apt_2019,

       y2d_avg_dly_apt_year,
       y2d_avg_dly_apt_prev_year,
       y2d_avg_dly_apt_2019,

       LAG (avg_apt_rolling_week, 364) OVER (PARTITION BY unit_code ORDER BY flight_date) avg_apt_rolling_week_prev_year,
       LAG (avg_apt_rolling_week, greatest((extract (year from flight_date)-2019) *364+ floor((extract (year from flight_date)-2019)/4)*7,0)  ) OVER (PARTITION BY unit_code ORDER BY flight_date) avg_apt_rolling_week_2019

      FROM NM_AREA_CALC
  )
  select
       unit_code,
       YEAR,
       MONTH,
       WEEK,
       WEEK_NB_YEAR,
       DAY_TYPE,
       day_of_week,

       flight_date,
       flight_date_prev_year,
       flight_date_2020,
       flight_date_2019,

       day_dly,
       day_dly_prev_year,
       day_dly_2020,
       day_dly_2019,

       day_dly - day_dly_prev_week AS day_dly_diff_prev_week,
       day_dly - day_dly_prev_year  AS day_dly_diff_prev_year,
       day_dly - day_dly_2019  AS day_dly_diff_2019,

       CASE WHEN day_dly_prev_week  <>0  THEN
            day_dly/day_dly_prev_week -1
            ELSE NULL
       END as day_dly_prev_week_perc,
       CASE WHEN day_dly_prev_year <>0
           THEN day_dly/day_dly_prev_year -1
       	   ELSE NULL
       END  day_diff_prev_year_perc,
       CASE WHEN day_dly_2019 <>0
           THEN day_dly/day_dly_2019 -1
       	   ELSE NULL
       END  day_dly_diff_2019_perc,

       total_rolling_week,
       CASE WHEN flight_date >= ", mydate, "
           THEN NULL
           ELSE avg_rolling_week
       END avg_rolling_week,
       avg_rolling_prev_week,
       avg_rolling_week_prev_year,
       avg_rolling_week_2020,
       avg_rolling_week_2019,

       CASE WHEN avg_rolling_prev_week  <>0  THEN
            avg_rolling_week/avg_rolling_prev_week -1
            ELSE NULL
       END dif_prev_week_perc,
       CASE WHEN avg_rolling_week_prev_year  <>0  THEN
            avg_rolling_week/avg_rolling_week_prev_year -1
            ELSE NULL
       END dif_week_prev_year_perc,
       CASE WHEN avg_rolling_week_2019  <>0  THEN
            AVG_rolling_week/AVG_rolling_week_2019 -1
            ELSE NULL
       END dif_rolling_week_2019_perc,

       y2d_dly_year,
       y2d_dly_prev_year,
       y2d_dly_2019,
       CASE WHEN flight_date >= ", mydate, "
           THEN NULL
      	   ELSE y2d_avg_dly_year
       END y2d_avg_dly_year,
       y2d_avg_dly_prev_year,
       y2d_avg_dly_2019,

       CASE WHEN y2d_avg_dly_prev_year <> 0 THEN
        y2d_avg_dly_year/y2d_avg_dly_prev_year - 1
        ELSE NULL
       END y2d_diff_prev_year_perc,
       CASE WHEN y2d_avg_dly_2019 <> 0 THEN
           y2d_avg_dly_year/y2d_avg_dly_2019 - 1
           ELSE NULL
       END y2d_diff_2019_perc,

      ", mydate, "-1 as last_data_day,

      --en-route
       day_dly_ert,
       day_dly_ert_prev_year,
       day_dly_ert_2019,

       CASE WHEN flight_date >= ", mydate, "
           THEN NULL
           ELSE avg_ert_rolling_week
       END avg_ert_rolling_week,
       avg_ert_rolling_prev_week,
       avg_ert_rolling_week_prev_year,
       avg_ert_rolling_week_2019,

       y2d_dly_ert_year,
       y2d_dly_ert_prev_year,
       y2d_dly_ert_2019,
       CASE WHEN flight_date >= ", mydate, "
           THEN NULL
      	   ELSE y2d_avg_dly_ert_year
       END y2d_avg_dly_ert_year,
       y2d_avg_dly_ert_prev_year,
       y2d_avg_dly_ert_2019,

       --airport
       day_dly_apt,
       day_dly_apt_prev_year,
       day_dly_apt_2019,

       CASE WHEN flight_date >= ", mydate, "
           THEN NULL
           ELSE avg_apt_rolling_week
       END avg_apt_rolling_week,
       avg_apt_rolling_prev_week,
       avg_apt_rolling_week_prev_year,
       avg_apt_rolling_week_2019,

       y2d_dly_apt_year,
       y2d_dly_apt_prev_year,
       y2d_dly_apt_2019,
       CASE WHEN flight_date >= ", mydate, "
           THEN NULL
      	   ELSE y2d_avg_dly_apt_year
       END y2d_avg_dly_apt_year,
       y2d_avg_dly_apt_prev_year,
       y2d_avg_dly_apt_2019

      FROM NM_AREA_CALC_PREV
"
)
}

# nw ao traffic ----
## day ----
query_nw_ao_day_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
WITH


AO_LIST AS (
SELECT ao_code,ao_name,ao_grp_code,ao_grp_name
FROM prudev.v_covid_dim_ao
 ),

AO_NM_GROUP  AS (
SELECT ao_grp_code, ao_grp_name
FROM ao_list
GROUP BY ao_grp_code,ao_grp_name
),

AO_GRP_DAY AS (
SELECT a.ao_grp_code,
        a.ao_grp_name,
        t.day_date,
        t.month,
        t.week,
        t.week_nb_year,
        t.day_type,
        t.day_of_week_nb AS day_of_week,
        t.year
FROM ao_nm_group a, pru_time_references t
WHERE
  (t.day_date >= ", mydate, "-1  AND t.day_date < ", mydate, ")
  or
 (t.day_date >= ", mydate, " -1 - greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0)
    AND t.day_date <  ", mydate, " - greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0)
 )
  or
 (t.day_date >= ", mydate, " - 1 - 364    AND t.day_date <  ", mydate, " - 364)
  or
 (t.day_date >= ", mydate, " - 1 - 14    AND t.day_date <  ", mydate, " - 14)
  or
 (t.day_date >= ", mydate, " - 1 - 7    AND t.day_date <  ", mydate, " - 7)

       ),


DATA_SOURCE AS (
SELECT
        b.ao_grp_code,
        b.ao_grp_name,
        TRUNC(A.flt_a_asp_prof_time_entry) ENTRY_DATE,
        COUNT(a.flt_uid) FLIGHT
FROM v_aiu_flt a,
     AO_LIST b
WHERE b.ao_code = ao_icao_id  AND ao_icao_id <> 'ZZZ'
AND ao_icao_id is not NULL
  AND
 (  (  A.flt_lobt >=  ", mydate, " - 1 -2
    AND A.flt_lobt <  ", mydate, " + 2
  )
 OR
 (  A.flt_lobt >=  ", mydate, "-1 - 2 -  greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0)
    AND A.flt_lobt <   ", mydate, " +2 - greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0)

 )
OR
 (  A.flt_lobt >=  ", mydate, " -364 -1 -2
    AND A.flt_lobt <   ", mydate, " - 364 + 2

  )
OR
 (  A.flt_lobt >=  ", mydate, " -14 -1 -2
    AND A.flt_lobt <   ", mydate, " - 14 + 2

  )
OR
 (  A.flt_lobt >=  ", mydate, " -7 -1 -2
    AND A.flt_lobt <   ", mydate, " - 7 + 2

  ) )
    AND A.flt_state IN ('TE','TA','AA')
GROUP BY b.ao_grp_code,
         b.ao_grp_name,
        TRUNC(A.flt_a_asp_prof_time_entry)
),


DATA_GRP_AO as (
SELECT a.YEAR,
       a.MONTH,
       a.day_date    AS ENTRY_DATE,
       a.WEEK,
       a.WEEK_NB_YEAR,
       a.day_of_week,
       a.ao_grp_code,
       a.ao_grp_name,
       coalesce(b.FLIGHT,0) AS FLIGHT
       FROM ao_grp_day  A
       LEFT JOIN DATA_SOURCE B
           ON a.ao_grp_code = b.ao_grp_code AND b.entry_date = a.day_date
),

DATA_GRP_AO_2 as
(select YEAR,
       MONTH,
       ENTRY_DATE,
       ao_grp_code,
       ao_grp_name,
       FLIGHT,
       WEEK,
       WEEK_NB_YEAR,
       day_of_week,
       sum(FLIGHT) over (PARTITION BY ao_grp_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(364, 'day')  PRECEDING and  NUMTODSINTERVAL( 364, 'day') PRECEDING ) FLIGHT_PREV_YEAR,
       min(ENTRY_DATE) over (PARTITION BY ao_grp_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(364, 'day')  PRECEDING and  NUMTODSINTERVAL( 364, 'day') PRECEDING ) ENTRY_DATE_PREV_YEAR,
       sum(FLIGHT) over (PARTITION BY ao_grp_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day')  PRECEDING
            and  NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day') PRECEDING ) FLIGHT_2019,
       min(ENTRY_DATE) over (PARTITION BY ao_grp_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day')  PRECEDING
            and  NUMTODSINTERVAL(greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0),'day') PRECEDING ) ENTRY_DATE_2019,
       sum(FLIGHT) over (PARTITION BY ao_grp_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(14, 'day')  PRECEDING and  NUMTODSINTERVAL(14, 'day') PRECEDING ) FLIGHT_14DAY,
       min(ENTRY_DATE) over (PARTITION BY ao_grp_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(14, 'day')  PRECEDING and  NUMTODSINTERVAL(14, 'day') PRECEDING ) ENTRY_DATE_14DAY,
       sum(FLIGHT) over (PARTITION BY ao_grp_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(7, 'day')  PRECEDING and  NUMTODSINTERVAL(7, 'day') PRECEDING ) FLIGHT_7DAY,
       min(ENTRY_DATE) over (PARTITION BY ao_grp_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(7, 'day')  PRECEDING and  NUMTODSINTERVAL(7, 'day') PRECEDING ) ENTRY_DATE_7DAY

      FROM DATA_GRP_AO)  ,

  DATA_GRP_AO_3  as
  (
      select YEAR,
       MONTH,
       ENTRY_DATE,
       ao_grp_code,
       ao_grp_name,
       FLIGHT,
       WEEK,
       WEEK_NB_YEAR,
       day_of_week,
       FLIGHT_PREV_YEAR,
       ENTRY_DATE_PREV_YEAR,
       FLIGHT_2019,
       ENTRY_DATE_2019,
       FLIGHT_14DAY,
       ENTRY_DATE_14DAY,
       FLIGHT_7DAY,
       ENTRY_DATE_7DAY,
       FLIGHT - FLIGHT_PREV_YEAR  as FLIGHT_PREV_YEAR_DIFF,
      FLIGHT - FLIGHT_2019  as FLIGHT_2019_DIFF,
      FLIGHT - FLIGHT_14DAY  as FLIGHT_14DAY_DIFF,
      FLIGHT - FLIGHT_7DAY  as FLIGHT_7DAY_DIFF,
      CASE WHEN FLIGHT_PREV_YEAR <>0
           THEN FLIGHT/FLIGHT_PREV_YEAR -1
      ELSE NULL
      END  FLIGHT_PREV_YEAR_DIFF_PERC,
      CASE WHEN FLIGHT_2019 <>0
           THEN FLIGHT/FLIGHT_2019 -1
      ELSE NULL
      END  FLIGHT_2019_DIFF_PERC,
      CASE WHEN FLIGHT_14DAY <>0
           THEN FLIGHT/FLIGHT_14DAY -1
      ELSE NULL
      END  FLIGHT_14DAY_DIFF_PERC,
      CASE WHEN FLIGHT_7DAY <>0
           THEN FLIGHT/FLIGHT_7DAY -1
      ELSE NULL
      END  FLIGHT_7DAY_DIFF_PERC
        FROM DATA_GRP_AO_2
  ),

  DATA_GRP_AO_4 as (
  select YEAR,
       MONTH,
       WEEK,
       WEEK_NB_YEAR,
       day_of_week,
       ao_grp_code  ,
       ao_grp_name,
       ao_grp_code  as ao_nm_group_code,
       ao_grp_name as ao_nm_group_name,
       ENTRY_DATE,
       ENTRY_DATE_PREV_YEAR,
       ENTRY_DATE_2019,
       ENTRY_DATE_14DAY,
       ENTRY_DATE_7DAY,
       FLIGHT,
       FLIGHT_PREV_YEAR,
       FLIGHT_2019,
       FLIGHT_14DAY,
       FLIGHT_7DAY,
       CASE WHEN entry_date = ", mydate, "-1 then 'YES' ELSE '-' END as FILTER_LAST_DAY,


      FLIGHT - FLIGHT_PREV_YEAR  as FLIGHT_DIFF,
      FLIGHT - FLIGHT_2019  as FLIGHT_2019_DIFF,
       FLIGHT - FLIGHT_14DAY  as FLIGHT_14DAY_DIFF,
       FLIGHT - FLIGHT_7DAY  as FLIGHT_7DAY_DIFF,
      CASE WHEN FLIGHT_PREV_YEAR <>0
           THEN FLIGHT/FLIGHT_PREV_YEAR -1
      ELSE NULL
      END  FLIGHT_DIFF_PERC,
      CASE WHEN FLIGHT_2019 <>0
           THEN FLIGHT/FLIGHT_2019 -1
      ELSE NULL
      END  FLIGHT_DIFF_2019_PERC,
      CASE WHEN FLIGHT_14DAY <>0
           THEN FLIGHT/FLIGHT_14DAY -1
      ELSE NULL
      END  FLIGHT_DIFF_14DAY_PERC,
      CASE WHEN FLIGHT_7DAY <>0
           THEN FLIGHT/FLIGHT_7DAY -1
      ELSE NULL
      END  FLIGHT_DIFF_7DAY_PERC

       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (FLIGHT) DESC)  r_rank_by_day
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (FLIGHT_PREV_YEAR) desc)  r_rank_by_day_PREV_YY
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (FLIGHT_2019) desc)  r_rank_by_day_2019
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (FLIGHT_14DAY) desc)  r_rank_by_day_14DAY
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (FLIGHT_7DAY) desc)  r_rank_by_day_7DAY
       ,RANK() OVER (PARTITION BY entry_date ORDER BY FLIGHT DESC)  rank_by_day
       ,RANK() OVER (PARTITION BY entry_date ORDER BY (FLIGHT_PREV_YEAR) desc)  rank_by_day_PREV_YY
       ,RANK() OVER (PARTITION BY entry_date ORDER BY (FLIGHT_2019) desc)   rank_by_day_2019
       ,RANK() OVER (PARTITION BY entry_date ORDER BY (FLIGHT_14DAY) desc)  rank_by_day_14DAY
       ,RANK() OVER (PARTITION BY entry_date ORDER BY (FLIGHT_7DAY) desc)  rank_by_day_7DAY
       ,DENSE_RANK() OVER (PARTITION BY entry_date ORDER BY FLIGHT DESC)  d_rank_by_day
       ,DENSE_RANK() OVER (PARTITION BY entry_date ORDER BY (FLIGHT_PREV_YEAR) desc)  d_rank_by_day_PREV_YY
       ,DENSE_RANK() OVER (PARTITION BY entry_date ORDER BY (FLIGHT_2019) desc)   d_rank_by_day_2019
       ,DENSE_RANK() OVER (PARTITION BY entry_date ORDER BY (FLIGHT_14DAY) desc)  d_rank_by_day_14DAY
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (FLIGHT - FLIGHT_2019) asc)  r_rank_by_day_diff_2019_asc
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (FLIGHT - FLIGHT_14DAY) asc)  r_rank_by_day_diff_14DAY_asc
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (FLIGHT - FLIGHT_7DAY) asc)  r_rank_by_day_diff_7DAY_asc
      FROM DATA_GRP_AO_3
      WHERE entry_date = ", mydate, "-1
  )

  select * from DATA_GRP_AO_4
  WHERE r_rank_by_day <= 100
"
  )
}

## week ----
query_nw_ao_week_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
  WITH

AO_LIST AS (
SELECT ao_code,ao_name,ao_grp_code,ao_grp_name --,ao_group2_code, ao_group2_name,ao_group2_covid_list
FROM prudev.v_covid_dim_ao
where ao_code <> 'ZZZ'
--WHERE ( AO_nm_list = 'Y' and ao_grp_code <> 'TCX' )
--or (ao_code in ('BCS', 'QTR', 'BHL', 'TAY', 'FDX', 'UAE'))
 ),-- OR  AO_GROUP2_COVId_LIST = 'Y'

AO_NM_GROUP  AS (
SELECT ao_grp_code, ao_grp_name
FROM ao_list
GROUP BY ao_grp_code,ao_grp_name
),

LIST_DAY as
(
SELECT  t.day_date
FROM  pru_time_references t
WHERE (t.day_date >= ", mydate, " - 7 - 14    AND t.day_date <  ", mydate, " )
or    (t.day_date >= ", mydate, " - 7 - 364    AND t.day_date <  ", mydate, " - 364)

UNION
SELECT
        t.day_date - greatest((extract (year from t.day_date)-2019) *364+ floor((extract (year from t.day_date)-2019)/4)*7,0)
FROM  pru_time_references t
WHERE
  (t.day_date >= ", mydate, " - 7  AND t.day_date <  ", mydate, " )
),

DIM_DAY as
(
select t.day_date,
        t.month,
        t.week,
        t.week_nb_year,
        t.day_type,
        t.day_of_week_nb,
        t.year
from pru_time_references t inner join list_day a on (t.day_date = a.day_date)
),

AO_GRP_DAY AS (
SELECT a.ao_grp_code,
        a.ao_grp_name,
        t.day_date,
        t.month,
        t.week,
        t.week_nb_year,
        t.day_type,
        t.day_of_week_nb AS day_of_week,
        t.year
FROM ao_nm_group a, DIM_DAY t
 ),

DATA_SOURCE AS (
SELECT
        b.ao_grp_code,
        b.ao_grp_name,
        TRUNC(A.flt_a_asp_prof_time_entry) ENTRY_DATE,
        COUNT(a.flt_uid) FLIGHT
FROM v_aiu_flt a,
     AO_LIST b
WHERE b.ao_code = ao_icao_id  and ao_icao_id is not NULL
  AND
 (
         (A.flt_lobt >=  ", mydate, " - 14-7 -2  AND A.flt_lobt <  ", mydate, " + 2)
       OR
         (A.flt_lobt >=  ", mydate, " -364 -7 -2 AND A.flt_lobt <   ", mydate, " - 364 + 2 )
       OR
         (A.flt_lobt >=  ", mydate, "-1 -((extract (year from ", mydate, "-1)-2019)*364+ floor((extract (year from ", mydate, "-1)-2019)/4)*7)   -7 -2
            AND A.flt_lobt <   ", mydate, "-1 - ((extract (year from ", mydate, "-1)-2019)*364+ floor((extract (year from ", mydate, "-1)-2019)/4)*7) + 2
          )
       OR
         (A.flt_lobt >= to_date ( '23-12-2018','dd-mm-yyyy')-1 and a.flt_lobt  <   to_date ( '08-01-2019','dd-mm-yyyy'))
       OR
         ( A.flt_lobt >= to_date ( '23-12-2019','dd-mm-yyyy')-1 and a.flt_lobt  <   to_date ( '08-01-2020','dd-mm-yyyy') )
    )

    AND A.flt_state IN ('TE','TA','AA')
GROUP BY b.ao_grp_code,
         b.ao_grp_name,
        TRUNC(A.flt_a_asp_prof_time_entry)
),

DATA_GRP_AO as (
SELECT a.YEAR,
       a.MONTH,
       a.day_date    AS ENTRY_DATE,
       a.WEEK,
       a.WEEK_NB_YEAR,
       a.day_of_week,
       a.ao_grp_code,
       a.ao_grp_name,
       coalesce(b.FLIGHT,0) AS FLIGHT
       FROM ao_grp_day  A
       LEFT JOIN DATA_SOURCE B
           ON a.ao_grp_code = b.ao_grp_code  AND  a.ao_grp_name = b.ao_grp_name AND b.entry_date = a.day_date
),

DATA_GRP_AO_2 as
(
select YEAR,
       MONTH,
       ENTRY_DATE,
       ao_grp_code,
       ao_grp_name,
       FLIGHT,
       WEEK,
       WEEK_NB_YEAR,
       day_of_week,
       sum(FLIGHT) over (PARTITION BY ao_grp_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(364, 'day')  PRECEDING and  NUMTODSINTERVAL( 364, 'day') PRECEDING ) FLIGHT_PREV_YEAR,
       min(ENTRY_DATE) over (PARTITION BY ao_grp_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(364, 'day')  PRECEDING and  NUMTODSINTERVAL( 364, 'day') PRECEDING ) ENTRY_DATE_PREV_YEAR,
       sum(FLIGHT) over (PARTITION BY ao_grp_code ORDER BY ENTRY_DATE range  between  NUMTODSINTERVAL(greatest((extract (year from entry_date)-2019) *364+ floor((extract (year from entry_date)-2019)/4)*7,0),'day')  PRECEDING
                                                                                and   NUMTODSINTERVAL(greatest((extract (year from entry_date)-2019) *364+ floor((extract (year from entry_date)-2019)/4)*7,0),'day') PRECEDING ) FLIGHT_2019,
       min(ENTRY_DATE) over (PARTITION BY ao_grp_code ORDER BY ENTRY_DATE range  between  NUMTODSINTERVAL(greatest((extract (year from entry_date)-2019) *364+ floor((extract (year from entry_date)-2019)/4)*7,0),'day')  PRECEDING
                                                                                    and   NUMTODSINTERVAL(greatest((extract (year from entry_date)-2019) *364+ floor((extract (year from entry_date)-2019)/4)*7,0),'day') PRECEDING ) ENTRY_DATE_2019,
       sum(FLIGHT) over (PARTITION BY ao_grp_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(14, 'day')  PRECEDING and  NUMTODSINTERVAL(14, 'day') PRECEDING ) FLIGHT_14DAY,
       min(ENTRY_DATE) over (PARTITION BY ao_grp_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(14, 'day')  PRECEDING and  NUMTODSINTERVAL(14, 'day') PRECEDING ) ENTRY_DATE_14DAY,
       sum(FLIGHT) over (PARTITION BY ao_grp_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(7, 'day')  PRECEDING and  NUMTODSINTERVAL(7, 'day') PRECEDING ) FLIGHT_7DAY,
       min(ENTRY_DATE) over (PARTITION BY ao_grp_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(7, 'day')  PRECEDING and  NUMTODSINTERVAL(7, 'day') PRECEDING ) ENTRY_DATE_7DAY
      FROM DATA_GRP_AO
     ),

        DATA_GRP_AO_3  as
  (
      select
      ao_grp_code,
      ao_grp_name,
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
      sum(flight_2019) as FLIGHT_2019,
      avg(flight_2019) as daily_FLIGHT_2019,
      min(ENTRY_DATE_2019) as min_ENTRY_DATE_2019,
      max (ENTRY_DATE_2019) as max_ENTRY_DATE_2019,
      to_char(min(entry_date_2019), 'dd-mm-yyyy') || ' -> ' || to_char(max(entry_date_2019),'DD-MM-YYYY') as entry_date_2019,
      sum(FLIGHT_14DAY) as FLIGHT_14DAY,
      avg(FLIGHT_14DAY) as DAILY_FLIGHT_14DAY,
      min(ENTRY_DATE_14DAY) as MIN_ENTRY_DATE_14DAY,
      max (ENTRY_DATE_14DAY) as max_ENTRY_DATE_14DAY,
       to_char(min(entry_date_14DAY), 'dd-mm-yyyy') || ' -> ' || to_char(max(entry_date_14DAY),'DD-MM-YYYY') as entry_date_14DAY,
      sum(FLIGHT_7DAY) as FLIGHT_7DAY,
      avg(FLIGHT_7DAY) as DAILY_FLIGHT_7DAY,
      min(ENTRY_DATE_7DAY) as MIN_ENTRY_DATE_7DAY,
      max (ENTRY_DATE_7DAY) as max_ENTRY_DATE_7DAY,
      to_char(min(entry_date_7DAY), 'dd-mm-yyyy') || ' -> ' || to_char(max(entry_date_7DAY),'DD-MM-YYYY') as entry_date_7DAY
      FROM DATA_GRP_AO_2
      where entry_date >= ", mydate, " -7 and entry_date <", mydate, "
      group by
      ao_grp_code,
      ao_grp_name
  ),

        DATA_GRP_AO_4  as
  (
    select
       ao_grp_code,
       ao_grp_name,
       ENTRY_DATE,
       ENTRY_DATE_PREV_YEAR,
       ENTRY_DATE_2019,
       ENTRY_DATE_14DAY,
       ENTRY_DATE_7DAY,
       MIN_ENTRY_DATE,
       MIN_ENTRY_DATE_PREV_YEAR,
       MIN_ENTRY_DATE_2019,
       MIN_ENTRY_DATE_14DAY,
       MIN_ENTRY_DATE_7DAY,
       MAX_ENTRY_DATE,
       MAX_ENTRY_DATE_PREV_YEAR,
       MAX_ENTRY_DATE_2019,
       MAX_ENTRY_DATE_14DAY,
       MAX_ENTRY_DATE_7DAY,
       FLIGHT,
       FLIGHT_PREV_YEAR,
       FLIGHT_2019,
       FLIGHT_14DAY,
       FLIGHT_7DAY,
       DAILY_FLIGHT,
       DAILY_FLIGHT_PREV_YEAR,
       DAILY_FLIGHT_2019,
       DAILY_FLIGHT_14DAY,
       DAILY_FLIGHT_7DAY,
       FLIGHT - FLIGHT_PREV_YEAR  as FLIGHT_PREV_YEAR_DIFF,
       FLIGHT - FLIGHT_2019  as FLIGHT_2019_DIFF,
       FLIGHT - FLIGHT_14DAY  as FLIGHT_14DAY_DIFF,
       FLIGHT - FLIGHT_7DAY  as FLIGHT_7DAY_DIFF,
       DAILY_FLIGHT - DAILY_FLIGHT_PREV_YEAR  as DAILY_FLIGHT_DIFF,
       DAILY_FLIGHT - DAILY_FLIGHT_2019  as DAILY_FLIGHT_2019_DIFF,
       DAILY_FLIGHT - DAILY_FLIGHT_14DAY  as DAILY_FLIGHT_14DAY_DIFF,
       DAILY_FLIGHT - DAILY_FLIGHT_7DAY  as DAILY_FLIGHT_7DAY_DIFF,

      CASE WHEN FLIGHT_PREV_YEAR <>0
           THEN FLIGHT/FLIGHT_PREV_YEAR -1
      ELSE NULL
      END  FLIGHT_DIFF_PREV_YEAR_PERC,
      CASE WHEN FLIGHT_2019 <>0
           THEN FLIGHT/FLIGHT_2019 -1
      ELSE NULL
      END  FLIGHT_DIFF_2019_PERC,
      CASE WHEN FLIGHT_14DAY <>0
           THEN FLIGHT/FLIGHT_14DAY -1
      ELSE NULL
      END  FLIGHT_DIFF_14DAY_PERC,
      CASE WHEN FLIGHT_7DAY <>0
           THEN FLIGHT/FLIGHT_7DAY -1
      ELSE NULL
      END  FLIGHT_DIFF_7DAY_PERC


       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (FLIGHT) DESC)  r_rank_by_day
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (FLIGHT_PREV_YEAR) desc)  r_rank_by_day_PREV_YY
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (FLIGHT_2019) desc)  r_rank_by_day_2019
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (FLIGHT_14DAY) desc)  r_rank_by_day_14DAY
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (FLIGHT_7DAY) desc)  r_rank_by_day_7DAY
       ,RANK() OVER (PARTITION BY entry_date ORDER BY FLIGHT DESC)  rank_by_day
       ,RANK() OVER (PARTITION BY entry_date ORDER BY (FLIGHT_PREV_YEAR) desc)  rank_by_day_PREV_YY
       ,RANK() OVER (PARTITION BY entry_date ORDER BY (FLIGHT_2019) desc)   rank_by_day_2019
       ,RANK() OVER (PARTITION BY entry_date ORDER BY (FLIGHT_14DAY) desc)  rank_by_day_14DAY
       ,RANK() OVER (PARTITION BY entry_date ORDER BY (FLIGHT_7DAY) desc)  rank_by_day_7DAY
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (FLIGHT - FLIGHT_2019) asc)  r_rank_by_day_diff_2019_asc
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (FLIGHT - FLIGHT_14DAY) asc)  r_rank_by_day_diff_14DAY_asc
        ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (FLIGHT - FLIGHT_7DAY) asc)  r_rank_by_day_diff_7DAY_asc
        ,ao_grp_code as ao_nm_group_code,
       ao_grp_name as ao_nm_group_name

      FROM DATA_GRP_AO_3
      -- where (flight <> 0 or flight_prev_year <> 0 or flight_2019 <>  0 or flight_14day <> 0 or flight_7day <>0)
  )

  select * from DATA_GRP_AO_4
  WHERE r_rank_by_day <= 100

"
  )
}

## y2d ----
query_nw_ao_y2d_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
  WITH

DIM_AO
 as (
SELECT ao_code,ao_name,ao_grp_code,ao_grp_name , ao_grp_level,ao_nm_list
FROM prudev.v_covid_dim_ao
) ,

AO_LIST as (
SELECT ao_code,ao_name,ao_grp_code,ao_grp_name ,ao_grp_level
FROM dim_ao
 ),


DATA_FLIGHT as (

     SELECT
        b.ao_grp_code,
        b.ao_grp_name ,
        b.ao_grp_level,
        trunc(A.flt_a_asp_prof_time_entry ) as entry_day,
        extract(year from A.flt_a_asp_prof_time_entry ) as year,
        A.flt_uid
FROM v_aiu_flt A
     inner join  AO_LIST b ON   (a.ao_icao_id = b.ao_code)

WHERE
    A.flt_lobt>= TO_DATE ('01-01-2019', 'dd-mm-yyyy') -1
    AND A.flt_lobt < ", mydate, "
     AND a.flt_a_asp_prof_time_entry >= TO_DATE ('01-01-2019', 'dd-mm-yyyy')
     AND TO_NUMBER (TO_CHAR (TRUNC (flt_a_asp_prof_time_entry), 'mmdd')) <=   TO_NUMBER (TO_CHAR (", mydate, "-1, 'mmdd'))
     and extract(year from A.flt_a_asp_prof_time_entry ) <= extract (year from (", mydate, "-1))
     AND A.flt_state IN ('TE', 'TA', 'AA')


)

, DATA_FLIGHT_GRP as (
 SELECT
     a.year,
     a.ao_grp_code, a.ao_grp_name, a.ao_grp_level,
    CASE when a.year = extract(year from (", mydate, "-1)) then
         '1_Y2D_CURRENT_YEAR'
         when a.year = extract(year from (", mydate, "-1))-1 then
         '2_Y2D_PREV_YEAR'
         ELSE 'Y2D_' || year
    END Y2D_YEAR,
--    to_date(  '01-01-' || year ,'dd-mm-yyyy') as from_date,
--    to_date(  TO_CHAR (", mydate, "-1, 'dd-mm-') || year ,'dd-mm-yyyy') as to_date,
     count(flt_uid) as TOT_FLT ,
      min(entry_Day) as from_date_with_data,
      max(entry_day) as to_date_with_data
  FROM DATA_FLIGHT a
 where a.ao_grp_name <> 'Unidentified'
GROUP BY
        a.year,
       a.ao_grp_code, a.ao_grp_name,a.ao_grp_level
)

, DATA_FLIGHT_GRP2 AS(
SELECT
    a.*,
    MIN(from_date_with_data) OVER (PARTITION BY year) as from_date,
    MAX(to_date_with_data) OVER (PARTITION BY year) as to_date
FROM DATA_FLIGHT_GRP a

)

, AO_RANK as
(
SELECT
  ao_grp_code, year, ao_grp_name,
        ROW_NUMBER() OVER (PARTITION BY year
                ORDER BY TOT_FLT DESC, ao_grp_name) as R_RANK,
        RANK() OVER (PARTITION BY year
                ORDER BY TOT_FLT DESC, ao_grp_name) as RANK
FROM DATA_FLIGHT_GRP
where year = extract(year from ", mydate, "-1 )
),

AO_RANK_PREV as
(
SELECT
  ao_grp_code, year, ao_grp_name,
        RANK() OVER (PARTITION BY year
                ORDER BY TOT_FLT DESC, ao_grp_name) as RANK_PY
FROM DATA_FLIGHT_GRP
where year = extract(year from ", mydate, "-1 ) -1
)

 SELECT
     a.year,
     a.ao_grp_code, a.ao_grp_name, a.ao_grp_level,
    a.Y2D_YEAR, a.from_date_with_data, a.to_date_with_data,
    a.from_date, a.to_date,
     a.TOT_FLT/(a.to_date-a.from_date+1) flt,
     a.TOT_FLT,
     b.rank, b.R_RANK,
     c.rank_py

  FROM DATA_FLIGHT_GRP2 a
left join AO_RANK b on a.ao_grp_code = b.ao_grp_code
left join AO_RANK_PREV c on a.ao_grp_code = c.ao_grp_code

where rank <= 100
order by year desc, R_RANK

"
  )
}

# nw apt traffic ----
## day ----
query_nw_apt_day_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
  WITH

LIST_AIRPORT as
(
SELECT
       a.code as airport_code,
       a.code as db_airport_code ,  -- was used for grouping istanbul and ataturk airport
       a.dashboard_name as airport_name
FROM prudev.pru_airport a
 ),

LIST_DAY  as (
select  t.* from
 pru_time_references t
WHERE
 (t.day_date >= ", mydate, "-1  AND t.day_date < ", mydate, ")
  or
 (t.day_date >= ", mydate, " -1 - greatest(((((extract (year from (", mydate, "-1))-2019) *364)+ floor((extract (year from (", mydate, "-1))-2019)/4)*7)),0)
    AND t.day_date <  ", mydate, " - greatest(((((extract (year from (", mydate, "-1))-2019) *364)+ floor((extract (year from (", mydate, "-1))-2019)/4)*7)),0)
  )
  or
 (t.day_date >= ", mydate, " - 1 - 364    AND t.day_date <  ", mydate, " - 364)
  or
 (t.day_date >= ", mydate, " - 1 - 14    AND t.day_date <  ", mydate, " - 14)
  or
 (t.day_date >= ", mydate, " - 1 - 7    AND t.day_date <  ", mydate, " - 7)
),

AIRPORT_DAY AS (
SELECT a.airport_code,
       a.db_airport_code,
        a.airport_name,
        t.day_date,
        t.month,
        t.week,
        t.week_nb_year,
        t.day_type,
        t.day_of_week_nb AS day_of_week,
        t.year
FROM LIST_AIRPORT a , list_day t

)  ,

ttf_dep as(
SELECT adep_day_adep, sum(coalesce(adep_day_all_trf,0)) AS adep_day_all_trf,f.adep_DAY_FLT_DATE
              FROM  v_aiu_agg_dep_day f
              where f.adep_DAY_FLT_DATE in (select day_date from list_day)
              group by  adep_day_adep ,f.adep_DAY_FLT_DATE
   ),


 ttf_arr as (
                  SELECT ades_day_ades_ctfm, sum(coalesce(ades_day_all_trf,0)) AS ades_day_all_trf,ades_DAY_FLT_DATE
              FROM  v_aiu_agg_arr_day
              where ades_DAY_FLT_DATE in (select day_date from list_day)
               group by ades_day_ades_ctfm,ades_DAY_FLT_DATE
),

 ARP_SYN_DEP_ARR
 as (
select c.day_date as entry_date ,c.airport_code, c.year, c.airport_name,c.WEEK_NB_YEAR,c.DAY_TYPE,c.month,c.WEEK, c.day_of_week,
   coalesce(SUM(coalesce(a.ades_day_all_trf,0)),0) AS ttf_arr,
       coalesce(SUM(coalesce(d.adep_day_all_trf,0)),0) AS TTF_DEP,
        coalesce(SUM(coalesce(a.ades_day_all_trf,0)),0) +  coalesce(SUM(coalesce(d.adep_day_all_trf,0)),0) AS DEP_ARR
from airport_day c
     left join ttf_dep d  on ( c.day_date= d.adep_DAY_FLT_DATE  and c.db_airport_code=d.adep_day_adep  )
     left join ttf_arr a on (c.day_date=a.ades_DAY_FLT_DATE  and c.db_airport_code=a.ades_day_ades_ctfm )
group by c.day_date ,c.airport_code, c.year, c.airport_name,c.WEEK_NB_YEAR,c.DAY_TYPE,c.month,c.WEEK, c.day_of_week
) ,

DATA_ARP_2 as
(select YEAR,
       MONTH,
       ENTRY_DATE,
       airport_code,
       airport_name,
       dep_arr,
       WEEK,
       WEEK_NB_YEAR,
       day_of_week,
       sum(dep_arr) over (PARTITION BY airport_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(364, 'day')  PRECEDING and  NUMTODSINTERVAL( 364, 'day') PRECEDING ) dep_arr_PREV_YEAR,
       min(ENTRY_DATE) over (PARTITION BY airport_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(364, 'day')  PRECEDING and  NUMTODSINTERVAL( 364, 'day') PRECEDING ) ENTRY_DATE_PREV_YEAR,
       sum(dep_arr) over (PARTITION BY airport_code ORDER BY ENTRY_DATE range   between NUMTODSINTERVAL(greatest(((((extract (year from entry_date)-2019) *364)+ floor((extract (year from entry_date)-2019)/4)*7)),0)  ,'day')  PRECEDING
                                                                                     and  NUMTODSINTERVAL(greatest(((((extract (year from entry_date)-2019) *364)+ floor((extract (year from entry_date)-2019)/4)*7)),0)  ,'day') PRECEDING )   dep_arr_2019,
       min(ENTRY_DATE) over (PARTITION BY airport_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(greatest(((((extract (year from entry_date)-2019) *364)+ floor((extract (year from entry_date)-2019)/4)*7)),0)  ,'day')   PRECEDING
                                                                                     and  NUMTODSINTERVAL(greatest(((((extract (year from entry_date)-2019) *364)+ floor((extract (year from entry_date)-2019)/4)*7)),0)  ,'day')  PRECEDING )  ENTRY_DATE_2019,
       sum(dep_arr) over (PARTITION BY airport_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(14, 'day')  PRECEDING and  NUMTODSINTERVAL(14, 'day') PRECEDING ) dep_arr_14DAY,
       min(ENTRY_DATE) over (PARTITION BY airport_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(14, 'day')  PRECEDING and  NUMTODSINTERVAL(14, 'day') PRECEDING ) ENTRY_DATE_14DAY,
       sum(dep_arr) over (PARTITION BY airport_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(7, 'day')  PRECEDING and  NUMTODSINTERVAL(7, 'day') PRECEDING ) dep_arr_7DAY,
       min(ENTRY_DATE) over (PARTITION BY airport_code ORDER BY ENTRY_DATE range  between NUMTODSINTERVAL(7, 'day')  PRECEDING and  NUMTODSINTERVAL(7, 'day') PRECEDING ) ENTRY_DATE_7DAY

      FROM ARP_SYN_DEP_ARR)  ,

  DATA_ARP_3 as
   (select YEAR,
       MONTH,
       WEEK,
       WEEK_NB_YEAR,
       day_of_week,
       airport_code,
       airport_name,
       ENTRY_DATE,
       ENTRY_DATE_PREV_YEAR,
       ENTRY_DATE_2019,
       ENTRY_DATE_14DAY,
       ENTRY_DATE_7DAY,
       DEP_ARR,
       DEP_ARR_PREV_YEAR,
       DEP_ARR_2019,
       DEP_ARR_14DAY,
       DEP_ARR_7DAY,
      DEP_ARR - DEP_ARR_PREV_YEAR  as DEP_ARR_PREV_YEAR_DIFF,
      DEP_ARR - DEP_ARR_2019  as DEP_ARR_2019_DIFF,
      DEP_ARR - DEP_ARR_14DAY  as DEP_ARR_14DAY_DIFF,
      DEP_ARR - DEP_ARR_7DAY  as DEP_ARR_7DAY_DIFF,
      CASE WHEN DEP_ARR_PREV_YEAR <>0
           THEN DEP_ARR/DEP_ARR_PREV_YEAR -1
      ELSE NULL
      END  DEP_ARR_PREV_YEAR_DIFF_PERC,
      CASE WHEN DEP_ARR_2019 <>0
           THEN DEP_ARR/DEP_ARR_2019 -1
      ELSE NULL
      END  DEP_ARR_2019_DIFF_PERC,
      CASE WHEN DEP_ARR_14DAY <>0
           THEN DEP_ARR/DEP_ARR_14DAY -1
      ELSE NULL
      END  DEP_ARR_14DAY_DIFF_PERC,
      CASE WHEN DEP_ARR_7DAY <>0
           THEN DEP_ARR/DEP_ARR_7DAY -1
      ELSE NULL
      END  DEP_ARR_7DAY_DIFF_PERC

      FROM DATA_ARP_2
      where ENTRY_DATE= ", mydate, " -1 --'01-mar-2020'
      ),

  DATA_ARP_4 as
  (
   SELECT
        airport_code,
        airport_name,
        ENTRY_DATE,
        ENTRY_DATE_2019,
        ENTRY_DATE_PREV_YEAR,
        ENTRY_DATE_14DAY,
         ENTRY_DATE_7DAY,
       DEP_ARR,
       DEP_ARR_2019,
       DEP_ARR_PREV_YEAR,
       DEP_ARR_14DAY,
       DEP_ARR_7DAY,
       DEP_ARR_2019_DIFF,
       DEP_ARR_PREV_YEAR_DIFF,
       DEP_ARR_14DAY_DIFF,
       DEP_ARR_7DAY_DIFF,
       DEP_ARR_2019_DIFF_PERC,
       DEP_ARR_PREV_YEAR_DIFF_PERC,
       DEP_ARR_14DAY_DIFF_PERC,
       DEP_ARR_7DAY_DIFF_PERC

       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (DEP_ARR) DESC)  r_rank_by_day
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (DEP_ARR_PREV_YEAR) desc)  r_rank_by_day_PREV_YY
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (DEP_ARR_2019) desc)  r_rank_by_day_2019
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (DEP_ARR_14DAY) desc)  r_rank_by_day_14DAY
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (DEP_ARR_7DAY) desc)  r_rank_by_day_7DAY
       ,RANK() OVER (PARTITION BY entry_date ORDER BY DEP_ARR DESC)  rank_by_day
       ,RANK() OVER (PARTITION BY entry_date ORDER BY (DEP_ARR_PREV_YEAR) desc)  rank_by_day_PREV_YY
       ,RANK() OVER (PARTITION BY entry_date ORDER BY (DEP_ARR_2019) desc)   rank_by_day_2019
       ,RANK() OVER (PARTITION BY entry_date ORDER BY (DEP_ARR_14DAY) desc)  rank_by_day_14DAY
       ,RANK() OVER (PARTITION BY entry_date ORDER BY (DEP_ARR_7DAY) desc)  rank_by_day_7DAY
       ,DENSE_RANK() OVER (PARTITION BY entry_date ORDER BY DEP_ARR DESC)  d_rank_by_day
       ,DENSE_RANK() OVER (PARTITION BY entry_date ORDER BY (DEP_ARR_PREV_YEAR) desc)  d_rank_by_day_PREV_YY
       ,DENSE_RANK() OVER (PARTITION BY entry_date ORDER BY (DEP_ARR_2019) desc)   d_rank_by_day_2019
       ,DENSE_RANK() OVER (PARTITION BY entry_date ORDER BY (DEP_ARR_14DAY) desc)  d_rank_by_day_14DAY
       ,DENSE_RANK() OVER (PARTITION BY entry_date ORDER BY (DEP_ARR_7DAY) desc)  d_rank_by_day_7DAY
    --   ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (DEP_ARR - DEP_ARR_PREV_YEAR) asc)  r_rank_by_day_diff_prev_YY_asc
    --   ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (DEP_ARR - DEP_ARR_2019) asc)  r_rank_by_day_2019_diff_asc
    --   ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (DEP_ARR - DEP_ARR_14DAY) asc)  r_rank_by_day_14DAY_diff_asc
    --   ,DENSE_RANK() OVER (PARTITION BY entry_date ORDER BY (DEP_ARR - DEP_ARR_PREV_YEAR) asc)  d_rank_by_day_prev_YY_dif_asc
    --   ,DENSE_RANK() OVER (PARTITION BY entry_date ORDER BY (DEP_ARR - DEP_ARR_2019) asc)  d_rank_by_day_2019_diff_asc
    --   ,DENSE_RANK() OVER (PARTITION BY entry_date ORDER BY (DEP_ARR - DEP_ARR_14DAY) asc)  d_rank_by_day_14DAY_diff_asc
    --    ,CASE WHEN entry_date = ", mydate, "-1 then 'YES' ELSE '-' END as FILTER_LAST_DAY
   FROM DATA_ARP_3
--  WHERE DEP_ARR > 0 or DEP_ARR_PREV_YEAR > 0 or DEP_ARR_14DAY >0 or DEP_ARR_2019>0 or DEP_ARR_7DAY>0
)

  select * from DATA_ARP_4
  where r_rank_by_day <= 100

"
  )
  }

## week ----
query_nw_apt_week_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
  WITH

LIST_AIRPORT as
(
SELECT
       a.code as airport_code,
       a.code as db_airport_code ,  -- was used for grouping istanbul and ataturk airport
       a.dashboard_name as airport_name
FROM prudev.pru_airport a
 ),

-- not used anymore -use for linking istanbul airports
LIST_AIRPORT2 as
(select case when  to_number (TO_CHAR ( ", mydate, "-1, 'mmdd')) <415  and a.code = 'LTFM'
            then  'LTFM+LTBA'
            else a.code
       end as airport_code,
       a.code as db_airport_code ,
        case when to_number (TO_CHAR ( ", mydate, "-1, 'mmdd')) <415 and a.code = 'LTFM'
            then  'Istanbul + Ataturk'
            else a.dashboard_name
       end airport_name,
       case when to_number (TO_CHAR ( ", mydate, "-1, 'mmdd')) <415 and a.code = 'LTFM'
            then  'Istanbul + Ataturk'
            else a.dashboard_name
       end
        as dashboard_name
 from prudev.pru_airport a
where a.code <> 'LTBA'
UNION
select case when  to_number (TO_CHAR ( ", mydate, "-1, 'mmdd')) <415
            then  'LTFM+LTBA'
            else a.code
       end as airport_code,
       a.code as db_airport_code ,
      case when to_number (TO_CHAR ( ", mydate, "-1, 'mmdd')) <415
            then  'Istanbul + Ataturk'
            else a.dashboard_name
       end airport_name,
       case when to_number (TO_CHAR ( ", mydate, "-1, 'mmdd')) <415
            then  'Istanbul + Ataturk'
            else dashboard_name
       end
 from prudev.pru_airport  a
  where a.code  = 'LTBA'

 ),

LIST_DAY as
(
SELECT  t.day_date
FROM  pru_time_references t
WHERE (t.day_date >= ", mydate, " - 7 - 14    AND t.day_date <  ", mydate, " )
or    (t.day_date >= ", mydate, " - 7 - 364    AND t.day_date <  ", mydate, " - 364)

UNION
SELECT
        t.day_date - greatest((extract (year from t.day_date)-2019) *364+ floor((extract (year from t.day_date)-2019)/4)*7,0)
FROM  pru_time_references t
WHERE
  (t.day_date >= ", mydate, " - 7  AND t.day_date <  ", mydate, " )
),

DIM_DAY as
(
select t.day_date,
        t.month,
        t.week,
        t.week_nb_year,
        t.day_type,
        t.day_of_week_nb,
        t.year
from pru_time_references t inner join list_day a on (t.day_date = a.day_date)
),

AIRPORT_DAY AS (
SELECT a.airport_code,
       a.db_airport_code,
        a.airport_name,
        t.day_date,
        t.month,
        t.week,
        t.week_nb_year,
        t.day_type,
        t.day_of_week_nb AS day_of_week,
        t.year
FROM LIST_AIRPORT a , dim_day t

)  ,

ttf_dep as(
SELECT adep_day_adep, sum(coalesce(adep_day_all_trf,0)) AS adep_day_all_trf,f.adep_DAY_FLT_DATE
              FROM  v_aiu_agg_dep_day f
              where f.adep_DAY_FLT_DATE in (select day_date from list_day)
              group by  adep_day_adep ,f.adep_DAY_FLT_DATE
   ),


 ttf_arr as (
                  SELECT ades_day_ades_ctfm, sum(coalesce(ades_day_all_trf,0)) AS ades_day_all_trf,ades_DAY_FLT_DATE
              FROM  v_aiu_agg_arr_day
              where ades_DAY_FLT_DATE in (select day_date from list_day)
               group by ades_day_ades_ctfm,ades_DAY_FLT_DATE
),


 ARP_SYN_DEP_ARR
 as (
select c.day_date as entry_date ,c.airport_code as apt_code, c.year, c.airport_name as apt_name,c.month,
       coalesce(SUM(coalesce(a.ades_day_all_trf,0)),0) AS ttf_arr,
       coalesce(SUM(coalesce(d.adep_day_all_trf,0)),0) AS TTF_DEP,
        coalesce(SUM(coalesce(a.ades_day_all_trf,0)),0) +  coalesce(SUM(coalesce(d.adep_day_all_trf,0)),0) AS TTF_DEP_ARR
from airport_day c
     left join ttf_dep d  on ( c.day_date= d.adep_DAY_FLT_DATE  and c.db_airport_code=d.adep_day_adep  )
     left join ttf_arr a on (c.day_date=a.ades_DAY_FLT_DATE  and c.db_airport_code=a.ades_day_ades_ctfm )
group by c.day_date ,c.airport_code, c.year, c.airport_name, c.WEEK_NB_YEAR,c.DAY_TYPE,c.month,c.WEEK, c.day_of_week
) ,

DATA_APT_2 as
(
select YEAR,
       MONTH,
       entry_date,
       apt_code,
       apt_name,
       ttf_dep_arr,
       sum(ttf_dep_arr) over (PARTITION BY apt_code ORDER BY entry_date range  between NUMTODSINTERVAL(364, 'day')  PRECEDING and  NUMTODSINTERVAL( 364, 'day') PRECEDING ) ttf_dep_arr_PREV_YEAR,
       min(entry_date) over (PARTITION BY apt_code ORDER BY entry_date range  between NUMTODSINTERVAL(364, 'day')  PRECEDING and  NUMTODSINTERVAL( 364, 'day') PRECEDING ) entry_date_PREV_YEAR,
       sum(ttf_dep_arr) over (PARTITION BY apt_code ORDER BY entry_date range  between NUMTODSINTERVAL(greatest((extract (year from entry_date)-2019) *364+ floor((extract (year from entry_date)-2019)/4)*7,0),'day')  PRECEDING
                                                                                   and NUMTODSINTERVAL(greatest((extract (year from entry_date)-2019) *364+ floor((extract (year from entry_date)-2019)/4)*7,0),'day') PRECEDING ) ttf_dep_arr_2019,
       min(entry_date) over (PARTITION BY apt_code ORDER BY entry_date range  between NUMTODSINTERVAL(greatest((extract (year from entry_date)-2019) *364+ floor((extract (year from entry_date)-2019)/4)*7,0),'day')  PRECEDING
                                                                                   and NUMTODSINTERVAL(greatest((extract (year from entry_date)-2019) *364+ floor((extract (year from entry_date)-2019)/4)*7,0),'day') PRECEDING ) entry_date_2019,
       sum(ttf_dep_arr) over (PARTITION BY apt_code ORDER BY entry_date range  between NUMTODSINTERVAL(14, 'day')  PRECEDING and  NUMTODSINTERVAL(14, 'day') PRECEDING ) ttf_dep_arr_14DAY,
       min(entry_date) over (PARTITION BY apt_code ORDER BY entry_date range  between NUMTODSINTERVAL(14, 'day')  PRECEDING and  NUMTODSINTERVAL(14, 'day') PRECEDING ) entry_date_14DAY,
       sum(ttf_dep_arr) over (PARTITION BY apt_code ORDER BY entry_date range  between NUMTODSINTERVAL(7, 'day')  PRECEDING and  NUMTODSINTERVAL(7, 'day') PRECEDING ) ttf_dep_arr_7DAY,
       min(entry_date) over (PARTITION BY apt_code ORDER BY entry_date range  between NUMTODSINTERVAL(7, 'day')  PRECEDING and  NUMTODSINTERVAL(7, 'day') PRECEDING ) entry_date_7DAY
      FROM ARP_SYN_DEP_ARR
     ),

DATA_APT_3  as
  (
      select
      apt_code,
      apt_name,
      to_char(min(entry_date), 'dd-mm-yyyy') || ' -> ' || to_char(max(entry_date),'DD-MM-YYYY') as entry_date,
      min(entry_date) as min_entry_date,
      max(entry_date) as max_entry_date,
      sum(ttf_dep_arr) as ttf_dep_arr,
      avg(ttf_dep_arr) as daily_ttf_dep_arr,
      sum(ttf_dep_arr_prev_year) as ttf_dep_arr_prev_year,
      avg(ttf_dep_arr_prev_year) as daily_ttf_dep_arr_prev_year,
      min(entry_date_PREV_YEAR) as min_entry_date_PREV_YEAR,
      max (entry_date_PREV_YEAR) as max_entry_date_PREV_YEAR,
      to_char(min(entry_date_prev_year), 'dd-mm-yyyy') || ' -> ' || to_char(max(entry_date_prev_year),'DD-MM-YYYY')as entry_date_prev_year,
      sum(ttf_dep_arr_2019) as ttf_dep_arr_2019,
      avg(ttf_dep_arr_2019) as daily_ttf_dep_arr_2019,
      min(entry_date_2019) as min_entry_date_2019,
      max (entry_date_2019) as max_entry_date_2019,
      to_char(min(entry_date_2019), 'dd-mm-yyyy') || ' -> ' || to_char(max(entry_date_2019),'DD-MM-YYYY') as entry_date_2019,
      sum(ttf_dep_arr_14DAY) as ttf_dep_arr_14DAY,
      avg(ttf_dep_arr_14DAY) as DAILY_ttf_dep_arr_14DAY,
      min(entry_date_14DAY) as MIN_entry_date_14DAY,
      max (entry_date_14DAY) as max_entry_date_14DAY,
       to_char(min(entry_date_14DAY), 'dd-mm-yyyy') || ' -> ' || to_char(max(entry_date_14DAY),'DD-MM-YYYY') as entry_date_14DAY,
      sum(ttf_dep_arr_7DAY) as ttf_dep_arr_7DAY,
      avg(ttf_dep_arr_7DAY) as DAILY_ttf_dep_arr_7DAY,
      min(entry_date_7DAY) as MIN_entry_date_7DAY,
      max (entry_date_7DAY) as max_entry_date_7DAY,
      to_char(min(entry_date_7DAY), 'dd-mm-yyyy') || ' -> ' || to_char(max(entry_date_7DAY),'DD-MM-YYYY') as entry_date_7DAY
      FROM DATA_APT_2
      where entry_date >= ", mydate, " -7 and entry_date <", mydate, "
      group by
      apt_code,
      apt_name
  ),

DATA_APT_4  as
  (
    select
       apt_code,
       apt_name,
       entry_date,
       entry_date_PREV_YEAR,
       entry_date_2019,
       entry_date_14DAY,
       entry_date_7DAY,
       MIN_entry_date,
       MIN_entry_date_PREV_YEAR,
       MIN_entry_date_2019,
       MIN_entry_date_14DAY,
       MIN_entry_date_7DAY,
       MAX_entry_date,
       MAX_entry_date_PREV_YEAR,
       MAX_entry_date_2019,
       MAX_entry_date_14DAY,
       MAX_entry_date_7DAY,
       ttf_dep_arr,
       ttf_dep_arr_PREV_YEAR,
       ttf_dep_arr_2019,
       ttf_dep_arr_14DAY,
       ttf_dep_arr_7DAY,
       DAILY_ttf_dep_arr,
       DAILY_ttf_dep_arr_PREV_YEAR,
       DAILY_ttf_dep_arr_2019,
       DAILY_ttf_dep_arr_14DAY,
       DAILY_ttf_dep_arr_7DAY,
       ttf_dep_arr - ttf_dep_arr_PREV_YEAR  as ttf_dep_arr_PREV_YEAR_DIFF,
       ttf_dep_arr - ttf_dep_arr_2019  as ttf_dep_arr_2019_DIFF,
       ttf_dep_arr - ttf_dep_arr_14DAY  as ttf_dep_arr_14DAY_DIFF,
       ttf_dep_arr - ttf_dep_arr_7DAY  as ttf_dep_arr_7DAY_DIFF,
       DAILY_ttf_dep_arr - DAILY_ttf_dep_arr_PREV_YEAR  as DAILY_ttf_dep_arr_DIFF,
       DAILY_ttf_dep_arr - DAILY_ttf_dep_arr_2019  as DAILY_ttf_dep_arr_2019_DIFF,
       DAILY_ttf_dep_arr - DAILY_ttf_dep_arr_14DAY  as DAILY_ttf_dep_arr_14DAY_DIFF,
       DAILY_ttf_dep_arr - DAILY_ttf_dep_arr_7DAY  as DAILY_ttf_dep_arr_7DAY_DIFF,

      CASE WHEN ttf_dep_arr_PREV_YEAR <>0
           THEN ttf_dep_arr/ttf_dep_arr_PREV_YEAR -1
      ELSE NULL
      END  ttf_dep_arr_DIFF_PREV_YY_PERC,
      CASE WHEN ttf_dep_arr_2019 <>0
           THEN ttf_dep_arr/ttf_dep_arr_2019 -1
      ELSE NULL
      END  ttf_dep_arr_DIFF_2019_PERC,
      CASE WHEN ttf_dep_arr_14DAY <>0
           THEN ttf_dep_arr/ttf_dep_arr_14DAY -1
      ELSE NULL
      END  ttf_dep_arr_DIFF_14DAY_PERC,
      CASE WHEN ttf_dep_arr_7DAY <>0
           THEN ttf_dep_arr/ttf_dep_arr_7DAY -1
      ELSE NULL
      END  ttf_dep_arr_DIFF_7DAY_PERC


       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (ttf_dep_arr) DESC)  r_rank_by_day
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (ttf_dep_arr_PREV_YEAR) desc)  r_rank_by_day_PREV_YY
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (ttf_dep_arr_2019) desc)  r_rank_by_day_2019
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (ttf_dep_arr_14DAY) desc)  r_rank_by_day_14DAY
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (ttf_dep_arr_7DAY) desc)  r_rank_by_day_7DAY
       ,RANK() OVER (PARTITION BY entry_date ORDER BY ttf_dep_arr DESC)  rank_by_day
       ,RANK() OVER (PARTITION BY entry_date ORDER BY (ttf_dep_arr_PREV_YEAR) desc)  rank_by_day_PREV_YY
       ,RANK() OVER (PARTITION BY entry_date ORDER BY (ttf_dep_arr_2019) desc)   rank_by_day_2019
       ,RANK() OVER (PARTITION BY entry_date ORDER BY (ttf_dep_arr_14DAY) desc)  rank_by_day_14DAY
       ,RANK() OVER (PARTITION BY entry_date ORDER BY (ttf_dep_arr_7DAY) desc)  rank_by_day_7DAY
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (ttf_dep_arr - ttf_dep_arr_2019) asc)  r_rank_by_day_diff_2019_asc
       ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (ttf_dep_arr - ttf_dep_arr_14DAY) asc)  r_rank_by_day_diff_14DAY_asc
        ,ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY (ttf_dep_arr - ttf_dep_arr_7DAY) asc)  r_rank_by_day_diff_7DAY_asc

      FROM DATA_APT_3
)
  SELECT * from DATA_APT_4
  WHERE r_rank_by_day <=100
"
  )
}

## y2d ----
query_nw_apt_y2d_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
  with

LIST_AIRPORT as
(select a.code as arp_code, a.id as arp_id,
    a.dashboard_name as arp_name
 from prudev.pru_airport a
),



 AIRPORT_DAY
as
(select a.arp_id,
       a.arp_code,
       a.arp_name,
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
      t.day_date >= '01-jan-2019'
      AND TO_NUMBER (TO_CHAR (t.day_date, 'mmdd')) <=   TO_NUMBER (TO_CHAR (", mydate, "-1, 'mmdd'))
      AND t.year <= extract (year from (", mydate, "-1))

),

ARP_SYN_ARR
    AS
(SELECT SUM (nvl(f.ades_day_all_trf,0)) AS MVT_arr,
                 u.arp_code,
                 u.day_date  AS FLIGHT_DATE,
                 u.arp_id, u.arp_name,
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
       u.arp_code, u.day_date,u.arp_id, u.arp_name, u.day_date,
       u.year,
       u.month,
       u.WEEK_NB_YEAR,
       u.DAY_TYPE,
       u.day_name,
       u.WEEK,
       u.day_of_week  ),

ARP_SYN_DEP
    AS
        (  SELECT SUM (nvl(adep_day_all_trf,0)) AS MVT_DEP,
                   u.ARP_CODE,
                  u.day_date  AS FLIGHT_DATE,
                   u.arp_id, u.arp_name,
                     u.year,
       u.month,
       u.WEEK_NB_YEAR,
       u.DAY_TYPE,
       u.day_name,
       u.WEEK,
       u.day_of_week
              FROM AIRPORT_DAY u left join v_aiu_agg_dep_day f
               on trunc(f.adep_DAY_FLT_DATE) = u.day_date
                  and f.adep_day_adep = u.arp_code
             and f.adep_day_adep = u.arp_code
            GROUP BY   u.arp_code, u.day_date,u.arp_id, u.arp_name, u.year,
       u.month,
       u.WEEK_NB_YEAR,
       u.DAY_TYPE,
       u.day_name,
       u.WEEK,
       u.day_of_week  ),

 ARP_SYN_DEP_ARR
 as
 ( select d.arp_code, d.flight_date,  d.arp_id, d.arp_name, d.MVT_dep, a.MVT_arr, nvl(d.MVT_dep,0)+nvl(a.MVT_arr,0) as MVT_dep_arr,  d.month,
       d.year,
       d.WEEK_NB_YEAR,
       d.DAY_TYPE,
       d.day_name,
       d.WEEK,
       d.day_of_week
 from arp_syn_dep d, arp_syn_arr a
 where d.arp_id = a.arp_id and d.flight_date = a.flight_date
 and    d.flight_date = a.flight_date
          ),


ALL_DAY_DATA_GRP as (
select
   a.year ,
   a.arp_code,
   a.arp_name,
            MIN (a.flight_date)          FROM_DATE,
            MAX (a.flight_date)          TO_DATE,
   SUM (a.mvt_dep_arr) as mvt_dep_arr,
   AVG (a.mvt_dep_arr) as daily_dep_arr

  FROM ARP_SYN_DEP_ARR A
group by    a.year, a.arp_code, a.arp_name
   ),

APT_RANK as
(
SELECT
   arp_code,
        ROW_NUMBER() OVER (PARTITION BY year
                ORDER BY daily_dep_arr DESC, arp_name) as R_RANK,
        RANK() OVER (PARTITION BY year
                ORDER BY daily_dep_arr DESC, arp_name) as RANK
FROM ALL_DAY_DATA_GRP
where year = extract (year from ", mydate, "-1)
),

APT_RANK_PY as
(
SELECT
   arp_code,
        RANK() OVER (PARTITION BY year
                ORDER BY daily_dep_arr DESC, arp_name) as RANK_PY
FROM ALL_DAY_DATA_GRP
where year = extract (year from ", mydate, "-1) - 1
)

select

   a.arp_code,
   a.arp_name,
   extract (year from TO_DATE) as year,
   FROM_DATE, TO_DATE,
   daily_dep_arr,
--   mvt_dep_arr,
   R_RANK,
   RANK, RANK_PY

FROM ALL_DAY_DATA_GRP a
left join APT_RANK b on a.arp_code = b.arp_code
left join APT_RANK_PY c on a.arp_code = c.arp_code
where R_RANK <= '100'
order by TO_DATE desc, R_RANK
"
  )
  }

# nw state dai ----
## day ----
query_nw_st_dai_day_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
with

REL_AP_CTRY as (
select cfmu_ap_code ,
       case when cfmu_ap_code = 'LYPR' THEN 'RSME'
            WHEN country_code = 'RS' or country_code = 'ME' THEN 'RSME'
--            WHEN country_code = 'MC' THEN 'FR'
            ELSE country_code
      END country_code
 from  v_covid_rel_airport_area
),

CTRY_LIST as (
select distinct
      CASE WHEN iso_ct_code = 'RS' or iso_ct_code = 'ME' THEN 'RSME'
--           WHEN iso_ct_code = 'MC' THEN 'FR'
           ELSE iso_ct_code
      END iso_ct_code,
      CASE WHEN iso_ct_code = 'RS' or iso_ct_code = 'ME' THEN 'Serbia/Montenegro'
--           WHEN iso_ct_code = 'MC' THEN 'FRANCE'
           ELSE iso_ct_name
      END iso_ct_name
from v_covid_rel_airport_area where (region = 'ECAC'
                              or iso_ct_name in ('ISRAEL', 'MOROCCO')
                              ) AND iso_ct_name not in ('AZERBAIJAN' , 'GIBRALTAR', 'KOSOVO*', 'ICELAND',
                              'MONACO', '#UNKNOWN#')
),

CTRY_LIST_ALL as (
select
      CASE WHEN aiu_iso_country_code = 'RS' or aiu_iso_country_code = 'ME' THEN 'RSME'
--           WHEN aiu_iso_country_code = 'MC' THEN 'FR'
           ELSE aiu_iso_country_code
      END ec_iso_ct_code,
      CASE WHEN aiu_iso_country_code = 'RS' or aiu_iso_country_code = 'ME' THEN 'Serbia/Montenegro'
--           WHEN aiu_iso_country_code = 'MC' THEN 'France'
           ELSE aiu_iso_country_name
      END ec_iso_ct_name
from pru_country_iso
group by  CASE WHEN aiu_iso_country_code = 'RS' or aiu_iso_country_code = 'ME' THEN 'RSME'
--            WHEN aiu_iso_country_code = 'MC' THEN 'FR'
            ELSE aiu_iso_country_code
          END,
          CASE WHEN aiu_iso_country_code = 'RS' or aiu_iso_country_code = 'ME' THEN 'Serbia/Montenegro'
--            WHEN aiu_iso_country_code = 'MC' THEN 'France'
            ELSE aiu_iso_country_name
          END
)

, AIRP_FLIGHT as (
/* Formatted on 06-07-2020 15:56:37 (QP5 v5.318) */
SELECT count(flt_uid) as mvt,
       TRUNC (flt_a_asp_prof_time_entry) AS entry_day,
       c1.country_code dep_ctry ,c2.country_code arr_ctry

  FROM v_aiu_flt a, REL_AP_CTRY c1, REL_AP_CTRY c2
 WHERE       (
                      (     A.flt_lobt >= ", mydate, " -2 -1
                        AND A.flt_lobt < ", mydate, "+2
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -1
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0
                        )
                        or
                      (     A.flt_lobt >= ", mydate, " -2-1-7
                        AND A.flt_lobt < ", mydate, "+2-7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -1 -7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0 -7
                        )
                        or
                      (     A.flt_lobt >= ", mydate, " -2-1-364
                        AND A.flt_lobt < ", mydate, "+2-364
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -1-364
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0 -364
                        )
                        or
                        (     A.flt_lobt >=  ", mydate, "-1 - 2 -  greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0)
                        AND A.flt_lobt < ", mydate, " +2 - greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0)
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
 )

, CTRY_DAI as (
 SELECT ctry1, country_id , flag_day,
      sum(TOT_MVT) as tot_mvt
 FROM  CTRY_PAIR_ARP
  WHERE country_id in (select iso_ct_code from ctry_list)
group by ctry1, country_id , flag_day

 ),

CTRY_DAI_RANK as
(
 select
            country_id,
            flag_day,
            RANK() OVER (PARTITION BY  flag_day
                ORDER BY TOT_MVT DESC) as RANK,
            ROW_NUMBER() OVER (PARTITION BY  flag_day
                ORDER BY TOT_MVT DESC, ctry1) as R_RANK
from CTRY_DAI
where flag_day = 'CURRENT_DAY'
),

CTRY_DAI_RANK_PREV as
(
 select
            country_id,
            flag_day,
            ROW_NUMBER() OVER (PARTITION BY  flag_day
                ORDER BY TOT_MVT DESC) as RANK_PREV_WEEK
from CTRY_DAI
where flag_day = 'DAY_PREV_WEEK'
)

 select
            d.ec_iso_ct_name as country_name,
            d.ec_iso_ct_code as iso_country_code,
            a.flag_day,
            TOT_MVT,
            r_rank, rank, rank_prev_week,
            ", mydate, "-1 as to_date

from CTRY_DAI a

 left join CTRY_DAI_RANK b on a.country_id =  b.country_id
 left join CTRY_DAI_RANK_PREV c on a.country_id =  c.country_id
 left join CTRY_LIST_ALL d on a.country_id = d.ec_iso_ct_code
where d.ec_iso_ct_code <> '##'
order by flag_day, r_rank
"
)
}

## week ----
query_nw_st_dai_week_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
with

REL_AP_CTRY as (
  select cfmu_ap_code ,
  case when cfmu_ap_code = 'LYPR' THEN 'RSME'
  WHEN country_code = 'RS' or country_code = 'ME' THEN 'RSME'
  --            WHEN country_code = 'MC' THEN 'FR'
  ELSE country_code
  END country_code
  from  v_covid_rel_airport_area
),

CTRY_LIST as (
  select distinct
  CASE WHEN iso_ct_code = 'RS' or iso_ct_code = 'ME' THEN 'RSME'
  --           WHEN iso_ct_code = 'MC' THEN 'FR'
  ELSE iso_ct_code
  END iso_ct_code,
  CASE WHEN iso_ct_code = 'RS' or iso_ct_code = 'ME' THEN 'Serbia/Montenegro'
  --           WHEN iso_ct_code = 'MC' THEN 'FRANCE'
  ELSE iso_ct_name
  END iso_ct_name
  from v_covid_rel_airport_area where (region = 'ECAC'
                                       or iso_ct_name in ('ISRAEL', 'MOROCCO')
  ) AND iso_ct_name not in ('AZERBAIJAN' , 'GIBRALTAR', 'KOSOVO*', 'ICELAND',
                            'MONACO', '#UNKNOWN#')
),

CTRY_LIST_ALL as (
  select
  CASE WHEN aiu_iso_country_code = 'RS' or aiu_iso_country_code = 'ME' THEN 'RSME'
  --           WHEN aiu_iso_country_code = 'MC' THEN 'FR'
  ELSE aiu_iso_country_code
  END ec_iso_ct_code,
  CASE WHEN aiu_iso_country_code = 'RS' or aiu_iso_country_code = 'ME' THEN 'Serbia/Montenegro'
  --           WHEN aiu_iso_country_code = 'MC' THEN 'France'
  ELSE aiu_iso_country_name
  END ec_iso_ct_name
  from pru_country_iso
  group by  CASE WHEN aiu_iso_country_code = 'RS' or aiu_iso_country_code = 'ME' THEN 'RSME'
  --            WHEN aiu_iso_country_code = 'MC' THEN 'FR'
  ELSE aiu_iso_country_code
  END,
  CASE WHEN aiu_iso_country_code = 'RS' or aiu_iso_country_code = 'ME' THEN 'Serbia/Montenegro'
  --            WHEN aiu_iso_country_code = 'MC' THEN 'France'
  ELSE aiu_iso_country_name
  END
)

, AIRP_FLIGHT as (
  /* Formatted on 06-07-2020 15:56:37 (QP5 v5.318) */
    SELECT count(flt_uid) as mvt,
  TRUNC (flt_a_asp_prof_time_entry) AS entry_day,
  c1.country_code dep_ctry ,c2.country_code arr_ctry

  FROM v_aiu_flt a, REL_AP_CTRY c1, REL_AP_CTRY c2
  WHERE       (
    (     A.flt_lobt >= ", mydate, " -2 -7
          AND A.flt_lobt < ", mydate, "+2
          AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -7
          AND A.flt_a_asp_prof_time_entry < ", mydate, "-0
    )
    or
    (     A.flt_lobt >= ", mydate, " -2-14
      AND A.flt_lobt < ", mydate, "+2-7
      AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -14
      AND A.flt_a_asp_prof_time_entry < ", mydate, "-0 -7
    )
    or
    (     A.flt_lobt >= ", mydate, " -2-7-364
      AND A.flt_lobt < ", mydate, "+2-364
      AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -7-364
      AND A.flt_a_asp_prof_time_entry < ", mydate, "-0 -364
    )
    or
    (     A.flt_lobt >=  ", mydate, "-7 - 2 -  greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0)
      AND A.flt_lobt < ", mydate, " +2 - greatest((extract (year from (", mydate, "-1))-2019) *364+ floor((extract (year from (", mydate, "-1))-2019)/4)*7,0)
      AND A.flt_a_asp_prof_time_entry >=  ", mydate, "-7 -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
      AND A.flt_a_asp_prof_time_entry < ", mydate, "-((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7
    )
  )
  AND A.flt_state IN ('TE', 'TA', 'AA')
  AND  flt_dep_ad IS NOT NULL
  AND    flt_ctfm_ades IS NOT NULL
  AND flt_dep_ad = c1.cfmu_ap_code and    flt_ctfm_ades = c2.cfmu_ap_code
  --       AND c1.country_code = 'RSME'
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
  when entry_day >= ", mydate, " -7-364 and entry_day < ", mydate, "-364  then 'ROLLING_WEEK_PREV_YEAR'
  when entry_day >= ", mydate, " -14 and entry_day < ", mydate, "-7  then 'PREV_ROLLING_WEEK'
  when entry_day >= ", mydate, " -7-((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
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
)

, CTRY_DAI as (
  SELECT ctry1, country_id , flag_rolling_week,
  sum(TOT_MVT) as tot_mvt
  FROM  CTRY_PAIR_ARP
  WHERE country_id in (select iso_ct_code from ctry_list)
  group by ctry1, country_id , flag_rolling_week

),

CTRY_DAI_RANK as
(
  select
  country_id,
  flag_rolling_week,
  RANK() OVER (PARTITION BY  flag_rolling_week
               ORDER BY TOT_MVT DESC) as RANK,
  ROW_NUMBER() OVER (PARTITION BY  flag_rolling_week
                     ORDER BY TOT_MVT DESC, ctry1) as R_RANK
  from CTRY_DAI
  where flag_rolling_week = 'CURRENT_ROLLING_WEEK'
),

CTRY_DAI_RANK_PREV as
(
  select
  country_id,
  flag_rolling_week,
  ROW_NUMBER() OVER (PARTITION BY  flag_rolling_week
                     ORDER BY TOT_MVT DESC) as RANK_PREV_WEEK
  from CTRY_DAI
  where flag_rolling_week = 'PREV_ROLLING_WEEK'
)

select
d.ec_iso_ct_name as country_name,
d.ec_iso_ct_code as iso_country_code,
a.flag_rolling_week,
TOT_MVT,
r_rank, rank, rank_prev_week,
", mydate, "-1 as to_date,
", mydate, "-7 as from_date

from CTRY_DAI a

left join CTRY_DAI_RANK b on a.country_id =  b.country_id
left join CTRY_DAI_RANK_PREV c on a.country_id =  c.country_id
left join CTRY_LIST_ALL d on a.country_id = d.ec_iso_ct_code
where d.ec_iso_ct_code <> '##' --and d.ec_iso_ct_code = 'RSME'
order by flag_rolling_week, r_rank
"
  )
}

## y2d ----
query_nw_st_dai_y2d_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
  with

REL_AP_CTRY as (
select cfmu_ap_code ,
       case when cfmu_ap_code = 'LYPR' THEN 'RSME'
            WHEN country_code = 'RS' or country_code = 'ME' THEN 'RSME'
--            WHEN country_code = 'MC' THEN 'FR'
            ELSE country_code
      END country_code
 from  v_covid_rel_airport_area
),

CTRY_LIST as (
select distinct
      CASE WHEN iso_ct_code = 'RS' or iso_ct_code = 'ME' THEN 'RSME'
--           WHEN iso_ct_code = 'MC' THEN 'FR'
           ELSE iso_ct_code
      END iso_ct_code,
      CASE WHEN iso_ct_code = 'RS' or iso_ct_code = 'ME' THEN 'Serbia/Montenegro'
--           WHEN iso_ct_code = 'MC' THEN 'FRANCE'
           ELSE iso_ct_name
      END iso_ct_name
from v_covid_rel_airport_area where (region = 'ECAC'
                              or iso_ct_name in ('ISRAEL', 'MOROCCO')
                              ) AND iso_ct_name not in ('AZERBAIJAN' , 'GIBRALTAR', 'KOSOVO*', 'ICELAND',
                              'MONACO', '#UNKNOWN#')
),

CTRY_LIST_ALL as (
select
      CASE WHEN aiu_iso_country_code = 'RS' or aiu_iso_country_code = 'ME' THEN 'RSME'
--           WHEN aiu_iso_country_code = 'MC' THEN 'FR'
           ELSE aiu_iso_country_code
      END ec_iso_ct_code,
      CASE WHEN aiu_iso_country_code = 'RS' or aiu_iso_country_code = 'ME' THEN 'Serbia/Montenegro'
--           WHEN aiu_iso_country_code = 'MC' THEN 'France'
           ELSE aiu_iso_country_name
      END ec_iso_ct_name
from pru_country_iso
group by  CASE WHEN aiu_iso_country_code = 'RS' or aiu_iso_country_code = 'ME' THEN 'RSME'
--            WHEN aiu_iso_country_code = 'MC' THEN 'FR'
            ELSE aiu_iso_country_code
          END,
          CASE WHEN aiu_iso_country_code = 'RS' or aiu_iso_country_code = 'ME' THEN 'Serbia/Montenegro'
--            WHEN aiu_iso_country_code = 'MC' THEN 'France'
            ELSE aiu_iso_country_name
          END
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
)

, AIRP_FLIGHT as (
/* Formatted on 06-07-2020 15:56:37 (QP5 v5.318) */
SELECT count(flt_uid) as mvt,
       TRUNC (flt_a_asp_prof_time_entry) AS entry_day,
       extract (year from flt_a_asp_prof_time_entry) as year,
       c1.country_code dep_ctry ,c2.country_code arr_ctry
FROM v_aiu_flt a, REL_AP_CTRY c1, REL_AP_CTRY c2
WHERE   A.flt_lobt>= TO_DATE ('01-01-2019', 'dd-mm-yyyy') -2
            AND A.flt_lobt < ", mydate, " + 2
            AND flt_a_asp_prof_time_entry >= TO_DATE ('01-01-2019', 'dd-mm-yyyy')
            AND TO_NUMBER (TO_CHAR (TRUNC (flt_a_asp_prof_time_entry), 'mmdd')) <=   TO_NUMBER (TO_CHAR (", mydate, "-1, 'mmdd'))
            AND A.flt_state IN ('TE', 'TA', 'AA')
            AND flt_dep_ad IS NOT NULL
            AND flt_ctfm_ades IS NOT NULL
            AND flt_dep_ad = c1.cfmu_ap_code
            AND flt_ctfm_ades = c2.cfmu_ap_code
   GROUP BY c1.country_code,
            c2.country_code,
            TRUNC (flt_a_asp_prof_time_entry),
            extract (year from flt_a_asp_prof_time_entry)
     )

, CTRY_PAIR_FLIGHT as (

SELECT year, entry_day,
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

  )

, CTRY_PAIR_ARP_1 as (
SELECT
        year,
        ctry1,
        ctry2,
        sum(mvt) as TOT_MVT
 FROM
 CTRY_PAIR_FLIGHT
 group by year, ctry1, ctry2

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
 )

, CTRY_DAI as (
 SELECT ctry1, country_id , year,
      sum(TOT_MVT) as tot_mvt
 FROM  CTRY_PAIR_ARP
-- where country_id = 'RSME'
WHERE country_id in (select iso_ct_code from ctry_list)
group by ctry1, country_id , year

 ),

CTRY_DAI_RANK as
(
 select
            country_id,
            year,
            RANK() OVER (PARTITION BY  year
                ORDER BY TOT_MVT DESC) as RANK,
            ROW_NUMBER() OVER (PARTITION BY  year
                ORDER BY TOT_MVT DESC, ctry1) as R_RANK
from CTRY_DAI
where year = extract (year from (", mydate, " -1))
),

CTRY_DAI_RANK_PREV as
(
 select
            country_id,
            year,
            ROW_NUMBER() OVER (PARTITION BY  year
                ORDER BY TOT_MVT DESC) as RANK_PREV_YEAR
from CTRY_DAI
where year = extract (year from ", mydate, " -1) -1
)

 select
            d.ec_iso_ct_name as country_name,
            d.ec_iso_ct_code as iso_country_code,
            a.year,
            TOT_MVT,
            TOT_MVT/e.no_days as avg_MVT,
            r_rank, rank, rank_prev_year
            ,e.from_date,
            e.to_date,
            e.no_days

from CTRY_DAI a

 left join CTRY_DAI_RANK b on a.country_id =  b.country_id
 left join CTRY_DAI_RANK_PREV c on a.country_id =  c.country_id
 left join CTRY_LIST_ALL d on a.country_id = d.ec_iso_ct_code
 left join REF_DATES e on a.year=e.year
order by year, r_rank
"
  )
  }

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
'EDDV','ESGG','LFRS','LEIB','LFML','LEBB','LFPB','LIPZ','LIRN','GCRR','LTFM','LTBA','LATI', 'UDYZ', 'LDZA', 'EETN', 'LJLJ', 'LZIB', 'LUKK', 'LWSK','LYPG', 'LQSA','UGTB', 'BIKF', 'BIRK')
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
 --            SUM(coalesce(a.agg_asp_delay_tvs,0)) / SUM (coalesce(a.agg_asp_a_traffic_asp,0)) as DY_CTRY_DLY_PER_FLT,
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
            TO_CHAR (MIN (entry_date), 'dd-mm-yyyy')   || ' to ' || TO_CHAR (MAX (entry_date), 'dd-mm-yyyy') AS period,
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
         TO_CHAR (MIN (entry_date), 'dd-mm-yyyy')   || ' to ' || TO_CHAR (MAX (entry_date), 'dd-mm-yyyy') AS period,
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
