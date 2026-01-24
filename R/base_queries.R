# NETWORK ----
## nw delay cause ----
nw_delay_cause_day_base_query <- paste0("
WITH

NM_AREA_FLT
    AS
        (SELECT 
                a_first_entry_time_date FLIGHT_DATE , 
                SUM(nvl(a.all_traffic,0)) FLT,
                SUM(nvl(a.delayed_traffic,0)) TDF
           FROM  prudev.v_aiu_agg_global_daily_counts a
           WHERE 
                 a.a_first_entry_time_date  >= ", query_from, "
            AND a.a_first_entry_time_date  < TRUNC (SYSDATE)
           GROUP BY  a.a_first_entry_time_date 
        ),

NM_AREA_DLY as
(
SELECT 
        agg_flt_a_first_entry_date FLIGHT_DATE,
        SUM (NVL(agg_flt_total_delay, 0)) TDM,
        SUM (NVL((CASE WHEN agg_flt_mp_regu_loc_ty = 'Airport' THEN  agg_flt_total_delay END),0)) 
          TDM_ARP,
        
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('A') AND agg_flt_mp_regu_loc_ty = 'En route' THEN agg_flt_total_delay END),0))  
          TDM_ERT_A,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('C') AND agg_flt_mp_regu_loc_ty = 'En route' THEN agg_flt_total_delay END),0))  
          TDM_ERT_C,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('D') AND agg_flt_mp_regu_loc_ty = 'En route' THEN agg_flt_total_delay END),0))  
          TDM_ERT_D,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('E') AND agg_flt_mp_regu_loc_ty = 'En route' THEN agg_flt_total_delay END),0))  
          TDM_ERT_E,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('G') AND agg_flt_mp_regu_loc_ty = 'En route' THEN agg_flt_total_delay END),0))  
          TDM_ERT_G,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('I') AND agg_flt_mp_regu_loc_ty = 'En route' THEN agg_flt_total_delay END),0))  
          TDM_ERT_I,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('M') AND agg_flt_mp_regu_loc_ty = 'En route' THEN agg_flt_total_delay END),0))  
          TDM_ERT_M,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('N') AND agg_flt_mp_regu_loc_ty = 'En route' THEN agg_flt_total_delay END),0))  
          TDM_ERT_N,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('O') AND agg_flt_mp_regu_loc_ty = 'En route' THEN agg_flt_total_delay END),0))  
          TDM_ERT_O,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('P') AND agg_flt_mp_regu_loc_ty = 'En route' THEN agg_flt_total_delay END),0))  
          TDM_ERT_P,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('R') AND agg_flt_mp_regu_loc_ty = 'En route' THEN agg_flt_total_delay END),0))  
          TDM_ERT_R,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('S') AND agg_flt_mp_regu_loc_ty = 'En route' THEN agg_flt_total_delay END),0))  
          TDM_ERT_S,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('T') AND agg_flt_mp_regu_loc_ty = 'En route' THEN agg_flt_total_delay END),0))  
          TDM_ERT_T,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('V') AND agg_flt_mp_regu_loc_ty = 'En route' THEN agg_flt_total_delay END),0))  
          TDM_ERT_V,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('W') AND agg_flt_mp_regu_loc_ty = 'En route' THEN agg_flt_total_delay END),0))  
          TDM_ERT_W,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('NA') AND agg_flt_mp_regu_loc_ty = 'En route' THEN agg_flt_total_delay END),0))  
          TDM_ERT_NA,

        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('A') AND agg_flt_mp_regu_loc_ty = 'Airport' THEN agg_flt_total_delay END),0))  
          TDM_ARP_A,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('C') AND agg_flt_mp_regu_loc_ty = 'Airport' THEN agg_flt_total_delay END),0))  
          TDM_ARP_C,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('D') AND agg_flt_mp_regu_loc_ty = 'Airport' THEN agg_flt_total_delay END),0))  
          TDM_ARP_D,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('E') AND agg_flt_mp_regu_loc_ty = 'Airport' THEN agg_flt_total_delay END),0))  
          TDM_ARP_E,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('G') AND agg_flt_mp_regu_loc_ty = 'Airport' THEN agg_flt_total_delay END),0))  
          TDM_ARP_G,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('I') AND agg_flt_mp_regu_loc_ty = 'Airport' THEN agg_flt_total_delay END),0))  
          TDM_ARP_I,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('M') AND agg_flt_mp_regu_loc_ty = 'Airport' THEN agg_flt_total_delay END),0))  
          TDM_ARP_M,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('N') AND agg_flt_mp_regu_loc_ty = 'Airport' THEN agg_flt_total_delay END),0))  
          TDM_ARP_N,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('O') AND agg_flt_mp_regu_loc_ty = 'Airport' THEN agg_flt_total_delay END),0))  
          TDM_ARP_O,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('P') AND agg_flt_mp_regu_loc_ty = 'Airport' THEN agg_flt_total_delay END),0))  
          TDM_ARP_P,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('R') AND agg_flt_mp_regu_loc_ty = 'Airport' THEN agg_flt_total_delay END),0))  
          TDM_ARP_R,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('S') AND agg_flt_mp_regu_loc_ty = 'Airport' THEN agg_flt_total_delay END),0))  
          TDM_ARP_S,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('T') AND agg_flt_mp_regu_loc_ty = 'Airport' THEN agg_flt_total_delay END),0))  
          TDM_ARP_T,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('V') AND agg_flt_mp_regu_loc_ty = 'Airport' THEN agg_flt_total_delay END),0))  
          TDM_ARP_V,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('W') AND agg_flt_mp_regu_loc_ty = 'Airport' THEN agg_flt_total_delay END),0))  
          TDM_ARP_W,
        SUM (NVL((CASE WHEN agg_flt_regu_reas IN ('NA') AND agg_flt_mp_regu_loc_ty = 'Airport' THEN agg_flt_total_delay END),0))  
          TDM_ARP_NA


      FROM prudev.v_aiu_agg_flt_flow
      WHERE agg_flt_a_first_entry_date >= ", query_from, "
        AND agg_flt_a_first_entry_date < TRUNC (SYSDATE)
        AND agg_flt_regu_reas is not null 
        AND agg_flt_mp_regu_loc_ty is not null
      group by agg_flt_a_first_entry_date
)

select 
    'Total Network Manager Area' as AREA,
    cast(extract(year from a.flight_date) as integer) as year,
    a.*,
        TDM,
 		TDM - TDM_ARP AS TDM_ERT,
		TDM_ARP,
        
        TDM_ERT_A,
        TDM_ERT_C,
        TDM_ERT_D,
        TDM_ERT_E,
        TDM_ERT_G,
        TDM_ERT_I,
        TDM_ERT_M,
        TDM_ERT_N,
        TDM_ERT_O,
        TDM_ERT_P,
        TDM_ERT_R,
        TDM_ERT_S,
        TDM_ERT_T,
        TDM_ERT_V,
        TDM_ERT_W,
        TDM_ERT_NA,

        TDM_ARP_A,
        TDM_ARP_C,
        TDM_ARP_D,
        TDM_ARP_E,
        TDM_ARP_G,
        TDM_ARP_I,
        TDM_ARP_M,
        TDM_ARP_N,
        TDM_ARP_O,
        TDM_ARP_P,
        TDM_ARP_R,
        TDM_ARP_S,
        TDM_ARP_T,
        TDM_ARP_V,
        TDM_ARP_W,
        TDM_ARP_NA 


from NM_AREA_FLT a
left join NM_AREA_DLY b on a.FLIGHT_DATE=b.FLIGHT_DATE
order by a.FLIGHT_DATE
"
)


# STATE ----
## st_daio_delay ----
st_daio_delay_day_base_query <- paste0("
SELECT   
      agg_asp_entry_date as flight_date,
      CAST(EXTRACT(YEAR FROM agg_asp_entry_date) AS INTEGER) AS year,
      agg_asp_id COUNTRY_CODE,
      agg_asp_ty as TYPE,
      CASE WHEN agg_asp_id = 'LQ' then 'Bosnia and Herzegovina'
           WHEN agg_asp_id = 'LY' then 'Serbia/Montenegro'
           ELSE agg_asp_name
      END COUNTRY_NAME,
      SUM (coalesce(a.agg_asp_a_traffic_asp,0)) as DAY_TFC,
      SUM(coalesce(a.agg_asp_delay_tvs,0)) as DAY_DLY,
      (SUM (coalesce(a.agg_asp_delay_tvs,0)) - SUM (coalesce(a.agg_asp_delay_airport_tvs,0)))  AS DAY_ERT_DLY,
      SUM (coalesce(a.agg_asp_delay_airport_tvs,0))  AS DAY_ARP_DLY,
      SUM (coalesce(a.agg_asp_delay_airport_tvs,0))  AS DAY_DLYED_TFC
       FROM prudev.v_aiu_agg_asp a
       WHERE
             agg_asp_entry_date >= ", query_from, " AND a.AGG_ASP_ENTRY_DATE < trunc(sysdate)
             and agg_asp_ty = 'COUNTRY_AUA'  AND A.agg_asp_unit_ty <> 'REGION'
             AND (SUBSTR(a.agg_asp_id,1,1) IN ('E','L')
         OR SUBSTR(a.agg_asp_id,1,2) IN ('GC','GM','GE','UD','UG','UK','YY','BI'))
    GROUP BY
             agg_asp_entry_date,
             agg_asp_id,
             agg_asp_ty,
             agg_asp_name

"
)

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

## st_dai  ----
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
),

DATA_SPAIN_SEPARATED AS (
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
),


 CTRY_DAY_SPAIN AS (
SELECT 'LEGC' AS COUNTRY_code,
        t.day_date,
        t.month,
        t.week,
        t.week_nb_year,
        t.day_type,
        t.day_of_week_nb AS day_of_week,
        t.year
FROM prudev.pru_time_references t
WHERE
   t.day_date >= ", query_from, "
  	AND t.DAY_date < trunc(sysdate)
       ),


DATA_DEP_SPAIN AS (
(SELECT
		'LEGC' AS country_code,
        TRUNC(A.flt_a_asp_prof_time_entry) flight_DATE,
        COUNT(a.flt_uid) DAY_TFC
FROM prudev.v_aiu_flt a
WHERE  SUBSTR(A.flt_dep_ad,1,2) IN ('GE', 'GC', 'LE') 
    AND A.flt_lobt >= ", query_from, " -2
    AND A.flt_lobt <  trunc(sysdate) + 2
    AND A.flt_a_asp_prof_time_entry >= ", query_from, "
    AND A.flt_a_asp_prof_time_entry <  trunc(sysdate)
    AND A.flt_state IN ('TE','TA','AA')
GROUP BY  TRUNC(A.flt_a_asp_prof_time_entry)
)
),

DATA_ARR_SPAIN AS (
SELECT
		'LEGC' AS country_code,
        TRUNC(A.flt_a_asp_prof_time_entry) flight_DATE,
        COUNT(a.flt_uid) DAY_TFC
FROM prudev.v_aiu_flt a
WHERE  SUBSTR(A.flt_ctfm_ades,1,2) IN ('GE', 'GC', 'LE') 
    AND A.flt_lobt >= ", query_from, " -2
    AND A.flt_lobt <  trunc(sysdate) + 2
    AND A.flt_a_asp_prof_time_entry >= ", query_from, "
    AND A.flt_a_asp_prof_time_entry <  trunc(sysdate)
    AND A.flt_state IN ('TE','TA','AA')
GROUP BY  TRUNC(A.flt_a_asp_prof_time_entry)
),


DATA_DOMESTIC_SPAIN as
(SELECT
        'LEGC' AS country_code,
        TRUNC(A.flt_a_asp_prof_time_entry) FLIGHT_DATE,
        COUNT(a.flt_uid) DAY_TFC
FROM prudev.v_aiu_flt a
WHERE  SUBSTR(A.flt_dep_ad,1,2) IN ('GE', 'GC', 'LE')  AND
       SUBSTR(A.flt_ctfm_ades,1,2) IN ('GE', 'GC', 'LE') 
    AND A.flt_lobt >= ", query_from, " -2
    AND A.flt_lobt <  trunc(sysdate) + 2
    AND A.flt_a_asp_prof_time_entry >= ", query_from, "
    AND A.flt_a_asp_prof_time_entry <  trunc(sysdate)
    AND A.flt_state IN ('TE','TA','AA')
GROUP BY  TRUNC(A.flt_a_asp_prof_time_entry)
),

DATA_SPAIN_TOGETHER AS (
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
FROM CTRY_DAY_SPAIN A
LEFT JOIN DATA_DEP_SPAIN b on a.day_date = b.FLIGHT_date
LEFT JOIN DATA_ARR_SPAIN c on a.day_date = c.FLIGHT_date
LEFT JOIN DATA_DOMESTIC_SPAIN d on a.day_date = d.FLIGHT_date
)

SELECT * FROM DATA_SPAIN_SEPARATED
UNION ALL
SELECT * FROM DATA_SPAIN_TOGETHER
"
)

## st_delay ----
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
## st_ao  ----
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
 
DIM_AIRPORT AS (
SELECT 
    	BK_AP_ID,
	    EC_AP_CODE,
	    EC_AP_NAME,
	    CFMU_AP_CODE,
	    LATITUDE,
	    LONGITUDE,
      CASE WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'GC' then 'IC'
          ELSE aiu_iso_ct_code
      END iso_ct_code_spain,
      aiu_iso_ct_code,
	    VALID_FROM,
	    VALID_TO
FROM pruread.V_AIU_DIM_AIRPORT
WHERE cfmu_ap_code IS NOT NULL
),

-- Pre-filter flights once
FLIGHTS AS (
  SELECT
    a.flt_uid,
    a.flt_dep_ad,
    a.flt_ctfm_ades,
    a.ao_icao_id,
    TRUNC(a.flt_a_asp_prof_time_entry) AS entry_date
  FROM prudev.v_aiu_flt a
  WHERE a.flt_lobt >= ", query_from, " - 2
    AND a.flt_lobt <  TRUNC(SYSDATE) + 2
    AND a.flt_a_asp_prof_time_entry >= ", query_from, "
    AND a.flt_a_asp_prof_time_entry <  TRUNC(SYSDATE)
    AND a.flt_state IN ('TE','TA','AA')
),
-- One ARR airport per flight by validity
ARR_X AS (
  SELECT
    f.flt_uid,
    da.iso_ct_code_spain AS arr_iso_ct_code,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY da.valid_from DESC NULLS LAST, da.valid_to DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AIRPORT da
    ON da.cfmu_ap_code = f.flt_ctfm_ades
   AND f.entry_date BETWEEN da.valid_from AND da.valid_to
),
-- One DEP airport per flight by validity
DEP_X AS (
  SELECT
    f.flt_uid,
    dd.iso_ct_code_spain AS dep_iso_ct_code,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY dd.valid_from DESC NULLS LAST, dd.valid_to DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AIRPORT dd
    ON dd.cfmu_ap_code = f.flt_dep_ad
   AND f.entry_date BETWEEN dd.valid_from AND dd.valid_to
),
-- One AO record per flight by validity
AO_X AS (
  SELECT
    f.flt_uid,
    d.ao_id,
    d.ao_code,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY d.wef DESC NULLS LAST, d.til DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AO d
    ON d.ao_code = f.ao_icao_id
   AND f.entry_date BETWEEN d.wef AND d.til
),

AIRP_FLIGHT AS(
SELECT
  f.flt_uid,
  f.entry_date,
  NVL(arr.arr_iso_ct_code, '99999') AS arr_iso_ct_code,
  NVL(dep.dep_iso_ct_code, '99999') AS dep_iso_ct_code,
  NVL(ao.ao_id, 99999)              AS ao_id,
  NVL(ao.ao_code, 'ZZZ')            AS ao_code
FROM FLIGHTS f
LEFT JOIN ARR_X arr ON arr.flt_uid = f.flt_uid AND arr.rn = 1
LEFT JOIN DEP_X dep ON dep.flt_uid = f.flt_uid AND dep.rn = 1
LEFT JOIN AO_X  ao  ON ao.flt_uid  = f.flt_uid  AND ao.rn  = 1
 )  
 
, CTRY_PAIR_FLIGHT as (
SELECT entry_date,
      CASE WHEN   dep_iso_ct_code <= arr_iso_ct_code
           THEN dep_iso_ct_code 
           ELSE   arr_iso_ct_code 
       END          
        iso_ct_code1, 
      CASE WHEN  dep_iso_ct_code <= arr_iso_ct_code
           THEN arr_iso_ct_code  
           ELSE   dep_iso_ct_code 
       END          
        iso_ct_code2, 
        ao_id,
        ao_code,
        flt_uid
  FROM AIRP_FLIGHT 
  WHERE dep_iso_ct_code != '99999' AND arr_iso_ct_code != '99999'
 
  )
  
 , CTRY_PAIR_ARP_1 as (
 SELECT
        entry_date, 
        iso_ct_code1,
        iso_ct_code2, 
        ao_id,
        ao_code,
        flt_uid 
 FROM 
 CTRY_PAIR_FLIGHT 
 
 
 )  
 
 , CTRY_PAIR_ARP_2 as (
SELECT entry_date,
       iso_ct_code2 as iso_ct_code1,
       iso_ct_code1 as iso_ct_code2,
        ao_id,
        ao_code,
        flt_uid 
FROM CTRY_PAIR_ARP_1  
  WHERE iso_ct_code1 <> iso_ct_code2
 )
 
, CTRY_PAIR_ARP as (
 SELECT  iso_ct_code1 as iso_ct_code,
 		entry_date,
 		ao_id,
        ao_code,
        flt_uid 
 FROM CTRY_PAIR_ARP_1
 UNION ALL
 SELECT  iso_ct_code1 as iso_ct_code, entry_date,
 		ao_id,
        ao_code,
        flt_uid 
 FROM  CTRY_PAIR_ARP_2
 ) 

select 
    extract(year from entry_date) as year,
    entry_date,
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
ORDER BY iso_ct_code, entry_date desc, flight DESC


")

## st_ap ----
st_ap_day_base_query <- paste0("
WITH

DIM_AIRPORT AS (
SELECT
    	BK_AP_ID,
	    EC_AP_CODE,
	    EC_AP_NAME,
	    CFMU_AP_CODE,
	    LATITUDE,
	    LONGITUDE,
      CASE WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'GC' then 'IC'
          ELSE aiu_iso_ct_code
      END iso_ct_code_spain,
      aiu_iso_ct_code,
	    VALID_FROM,
	    VALID_TO
FROM pruread.v_aiu_dim_airport
WHERE cfmu_ap_code IS NOT NULL
),

-- Pre-aggregate departures by date & ADEP code
DEP_AGG AS (
  SELECT
    TRUNC(A.adep_DAY_FLT_DATE)      AS flight_date,
    A.adep_day_adep                 AS adep_code,
    SUM(NVL(A.adep_day_all_trf,0))  AS dep
  FROM prudev.v_aiu_agg_dep_day A
  WHERE A.adep_DAY_FLT_DATE >= ", query_from, "
    AND A.adep_DAY_FLT_DATE <  TRUNC(SYSDATE)
  GROUP BY TRUNC(A.adep_DAY_FLT_DATE), A.adep_day_adep
),

-- Pick exactly one DIM row per (date, adep_code)
DEP_X AS (
  SELECT
    d.flight_date,
    d.adep_code,
    d.dep,
    x.iso_ct_code_spain,
    NVL(x.bk_ap_id, 99999) AS dep_bk_ap_id,
    ROW_NUMBER() OVER (
      PARTITION BY d.flight_date, d.adep_code
      ORDER BY x.valid_from DESC NULLS LAST, x.valid_to DESC NULLS LAST
    ) AS rn
  FROM DEP_AGG d
  LEFT JOIN DIM_AIRPORT x
    ON x.cfmu_ap_code = d.adep_code
   AND d.flight_date BETWEEN x.valid_from AND x.valid_to
),

DATA_FLIGHT_DEP AS (
  SELECT
    flight_date,
    iso_ct_code_spain AS dep_iso_ct_code,
    dep_bk_ap_id,
    adep_code,
    dep
  FROM DEP_X
  WHERE rn = 1
),

-- Pre-aggregate arrivals by date & ADES code
ARR_AGG AS (
  SELECT
    TRUNC(A.ades_DAY_FLT_DATE)      AS flight_date,
    A.ades_day_ades_ctfm            AS ades_code,
    SUM(NVL(A.ades_day_all_trf,0))  AS arr
  FROM prudev.v_aiu_agg_arr_day A
  WHERE A.ades_DAY_FLT_DATE >= ", query_from, "
    AND A.ades_DAY_FLT_DATE <  TRUNC(SYSDATE)
  GROUP BY TRUNC(A.ades_DAY_FLT_DATE), A.ades_day_ades_ctfm
),

-- Pick exactly one DIM row per (date, ades_code)
ARR_X AS (
  SELECT
    a.flight_date,
    a.ades_code,
    a.arr,
    x.iso_ct_code_spain,
    NVL(x.bk_ap_id, 99999) AS arr_bk_ap_id,
    ROW_NUMBER() OVER (
      PARTITION BY a.flight_date, a.ades_code
      ORDER BY x.valid_from DESC NULLS LAST, x.valid_to DESC NULLS LAST
    ) AS rn
  FROM ARR_AGG a
  LEFT JOIN DIM_AIRPORT x
    ON x.cfmu_ap_code = a.ades_code
   AND a.flight_date BETWEEN x.valid_from AND x.valid_to
),

DATA_FLIGHT_ARR AS (
  SELECT
    flight_date,
    iso_ct_code_spain AS arr_iso_ct_code,
    arr_bk_ap_id,
    ades_code,
    arr
  FROM ARR_X
  WHERE rn = 1
)

SELECT
  CAST(EXTRACT(YEAR FROM COALESCE(a.flight_date, b.flight_date)) AS INTEGER) AS year,
  COALESCE(a.flight_date, b.flight_date)                                     AS flight_date,
  COALESCE(a.dep_iso_ct_code, b.arr_iso_ct_code)                             AS iso_ct_code,
  COALESCE(a.dep_bk_ap_id,  b.arr_bk_ap_id)                                  AS bk_ap_id,
  COALESCE(a.adep_code,     b.ades_code)                                     AS ap_code,
  COALESCE(a.dep, 0)                                                         AS dep,
  COALESCE(b.arr, 0)                                                         AS arr,
  COALESCE(a.dep, 0) + COALESCE(b.arr, 0)                                    AS dep_arr
FROM DATA_FLIGHT_DEP a
FULL OUTER JOIN DATA_FLIGHT_ARR b
  ON a.flight_date = b.flight_date
 AND a.dep_bk_ap_id = b.arr_bk_ap_id
 WHERE (a.dep_bk_ap_id IS NULL OR a.dep_bk_ap_id <> 99999)
   OR (b.arr_bk_ap_id IS NULL OR b.arr_bk_ap_id <> 99999)
ORDER BY 2, 4

")

## st_st ----
st_st_day_base_query <- paste0("
WITH
-- Airport dim (has validity ranges)
DIM_AIRPORT AS (
  SELECT
    	BK_AP_ID,
	    EC_AP_CODE,
	    EC_AP_NAME,
	    CFMU_AP_CODE,
	    LATITUDE,
	    LONGITUDE,
      CASE WHEN SUBSTR(cfmu_ap_code, 1, 2) = 'GC' then 'IC'
          ELSE aiu_iso_ct_code
      END iso_ct_code_spain,
      aiu_iso_ct_code,
	    VALID_FROM,
	    VALID_TO
	FROM pruread.v_aiu_dim_airport
  WHERE cfmu_ap_code IS NOT NULL
),

-- Pre-filter flights and compute entry date once
FLIGHTS AS (
  SELECT
    a.flt_uid,
    a.flt_dep_ad,
    a.flt_ctfm_ades,
    TRUNC(a.flt_a_asp_prof_time_entry) AS entry_date
  FROM prudev.v_aiu_flt a
  WHERE a.flt_lobt >= ", query_from, " - 2
    AND a.flt_lobt <  TRUNC(SYSDATE) + 2
    AND a.flt_a_asp_prof_time_entry >= ", query_from, "
    AND a.flt_a_asp_prof_time_entry <  TRUNC(SYSDATE)
    AND a.flt_state IN ('TE','TA','AA')
),

-- Pick exactly one DEP dim row per flight/date
DEP_X AS (
  SELECT
    f.flt_uid,
    da.iso_ct_code_spain AS dep_iso_ct_code,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY da.valid_from DESC NULLS LAST, da.valid_to DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AIRPORT da
    ON da.cfmu_ap_code = f.flt_dep_ad
   AND f.entry_date BETWEEN da.valid_from AND da.valid_to
),

-- Pick exactly one ARR dim row per flight/date
ARR_X AS (
  SELECT
    f.flt_uid,
    aa.iso_ct_code_spain AS arr_iso_ct_code,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY aa.valid_from DESC NULLS LAST, aa.valid_to DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AIRPORT aa
    ON aa.cfmu_ap_code = f.flt_ctfm_ades
   AND f.entry_date BETWEEN aa.valid_from AND aa.valid_to
),

-- One row per flight with resolved country codes (default to '99999' if no match)
AIRP_FLIGHT AS (
  SELECT
    f.entry_date,
    NVL(dep.dep_iso_ct_code, '99999') AS dep_iso_ct_code,
    NVL(arr.arr_iso_ct_code, '99999') AS arr_iso_ct_code
  FROM FLIGHTS f
  LEFT JOIN DEP_X dep ON dep.flt_uid = f.flt_uid AND dep.rn = 1
  LEFT JOIN ARR_X arr ON arr.flt_uid = f.flt_uid AND arr.rn = 1
),

-- Order country codes to build undirected pairs; keep only fully-mapped
CTRY_PAIR_FLIGHT AS (
  SELECT
    entry_date,
    CASE WHEN dep_iso_ct_code <= arr_iso_ct_code THEN dep_iso_ct_code ELSE arr_iso_ct_code END AS iso_ct_code1,
    CASE WHEN dep_iso_ct_code <= arr_iso_ct_code THEN arr_iso_ct_code ELSE dep_iso_ct_code END AS iso_ct_code2
  FROM AIRP_FLIGHT
  WHERE dep_iso_ct_code <> '99999'
    AND arr_iso_ct_code <> '99999'
),

-- Aggregate movements per day & ordered pair
CTRY_PAIR_ARP_1 AS (
  SELECT
    entry_date,
    iso_ct_code1 AS iso_ct_code,
    iso_ct_code2,
    COUNT(*) AS flight
  FROM CTRY_PAIR_FLIGHT
  GROUP BY entry_date, iso_ct_code1, iso_ct_code2
),

-- Mirror for the opposite direction (exclude self-pairs)
CTRY_PAIR_ARP_2 AS (
  SELECT
    entry_date,
    iso_ct_code2 AS iso_ct_code,
    iso_ct_code  AS iso_ct_code2,
    flight
  FROM CTRY_PAIR_ARP_1
  WHERE iso_ct_code <> iso_ct_code2
)

-- Final output
SELECT
  CAST(EXTRACT(YEAR FROM entry_date) AS INTEGER) AS year,
  entry_date,
  iso_ct_code,
  iso_ct_code2,
  flight
FROM CTRY_PAIR_ARP_1
UNION ALL
SELECT
  CAST(EXTRACT(YEAR FROM entry_date) AS INTEGER) AS year,
  entry_date,
  iso_ct_code,
  iso_ct_code2,
  flight
FROM CTRY_PAIR_ARP_2
ORDER BY entry_date, iso_ct_code, flight DESC

")

# AIRPORT ----
## ap_day -----
ap_traffic_delay_day_base_query <- paste0 (
"
WITH
DIM_AIRPORT AS (
  SELECT 
      BK_AP_ID,
	    EC_AP_CODE,
	    EC_AP_NAME,
	    CFMU_AP_CODE,
	    LATITUDE,
	    LONGITUDE,
      aiu_iso_ct_code,
	    VALID_FROM,
	    VALID_TO
  FROM pruread.v_aiu_dim_airport
  WHERE cfmu_ap_code IS NOT NULL
),

arp_SYN_ARR AS (
  SELECT
    SUM(NVL(f.ades_day_all_trf, 0)) AS arr,
    f.ades_day_ades_ctfm           AS arp_code,
    TRUNC(f.ades_day_flt_date)     AS flight_date
  FROM prudev.v_aiu_agg_arr_day f
  WHERE TRUNC(f.ades_day_flt_date) >= ", query_from, "
  GROUP BY f.ades_day_ades_ctfm, TRUNC(f.ades_day_flt_date)
),

arp_SYN_DEP AS (
  SELECT
    SUM(NVL(f.adep_day_all_trf, 0)) AS dep,
    f.adep_day_adep                AS arp_code,
    TRUNC(f.adep_day_flt_date)     AS flight_date
  FROM prudev.v_aiu_agg_dep_day f
  WHERE TRUNC(f.adep_day_flt_date) >= ", query_from, "
  GROUP BY f.adep_day_adep, TRUNC(f.adep_day_flt_date)
),

/* Make sure EVERY field referenced later exists here */
arp_DELAY AS (
  SELECT
    a.ref_loc_id                 AS arp_code,
    a.agg_flt_a_first_entry_date AS entry_date,

    /* totals */
    SUM(NVL(a.agg_flt_total_delay, 0))       AS tdm_arp,
    SUM(NVL(a.agg_flt_delayed_traffic, 0))   AS tdf_arp,
    SUM(NVL(a.agg_flt_regulated_traffic, 0)) AS trf_arp,

    /* 15+ buckets overall */
    SUM(NVL(CASE WHEN a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60')
                 THEN a.agg_flt_total_delay END, 0))               AS tdm_15_arp,
    SUM(NVL(CASE WHEN a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60')
                 THEN a.agg_flt_delayed_traffic END, 0))           AS tdf_15_arp,
    SUM(NVL(CASE WHEN a.agg_flt_delay_interval IN (']15,30]', ']30,60]', '> 60')
                 THEN NVL(a.agg_flt_regulated_traffic, 0) END, 0)) AS trf_15_arp,

    /* reasons overall */
    SUM(NVL(CASE WHEN agg_flt_regu_reas IN ('A')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_a,
    SUM(NVL(CASE WHEN agg_flt_regu_reas IN ('C')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_c,
    SUM(NVL(CASE WHEN agg_flt_regu_reas IN ('D')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_d,
    SUM(NVL(CASE WHEN agg_flt_regu_reas IN ('E')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_e,
    SUM(NVL(CASE WHEN agg_flt_regu_reas IN ('G')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_g,
    SUM(NVL(CASE WHEN agg_flt_regu_reas IN ('I')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_i,
    SUM(NVL(CASE WHEN agg_flt_regu_reas IN ('M')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_m,
    SUM(NVL(CASE WHEN agg_flt_regu_reas IN ('N')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_n,
    SUM(NVL(CASE WHEN agg_flt_regu_reas IN ('O')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_o,
    SUM(NVL(CASE WHEN agg_flt_regu_reas IN ('P')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_p,
    SUM(NVL(CASE WHEN agg_flt_regu_reas IN ('R')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_r,
    SUM(NVL(CASE WHEN agg_flt_regu_reas IN ('S')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_s,
    SUM(NVL(CASE WHEN agg_flt_regu_reas IN ('T')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_t,
    SUM(NVL(CASE WHEN agg_flt_regu_reas IN ('V')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_v,
    SUM(NVL(CASE WHEN agg_flt_regu_reas IN ('W')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_w,
    SUM(NVL(CASE WHEN agg_flt_regu_reas IN ('NA') THEN a.agg_flt_total_delay END,0)) AS tdm_arp_na,

    /* arrivals category (and 15+ buckets) */
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' THEN a.agg_flt_total_delay END,0))                   AS tdm_arp_arr,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' THEN a.agg_flt_delayed_traffic END,0))               AS tdf_arp_arr,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' THEN NVL(a.agg_flt_regulated_traffic,0) END,0))      AS trf_arp_arr,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' AND a.agg_flt_delay_interval IN (']15,30]',']30,60]','> 60')
                 THEN a.agg_flt_total_delay END,0))                                                    AS tdm_15_arp_arr,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' AND a.agg_flt_delay_interval IN (']15,30]',']30,60]','> 60')
                 THEN a.agg_flt_delayed_traffic END,0))                                                AS tdf_15_arp_arr,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' AND a.agg_flt_delay_interval IN (']15,30]',']30,60]','> 60')
                 THEN NVL(a.agg_flt_regulated_traffic,0) END,0))                                       AS trf_15_arp_arr,

    /* arrivals by reason */
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' AND agg_flt_regu_reas IN ('A')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_arr_a,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' AND agg_flt_regu_reas IN ('C')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_arr_c,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' AND agg_flt_regu_reas IN ('D')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_arr_d,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' AND agg_flt_regu_reas IN ('E')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_arr_e,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' AND agg_flt_regu_reas IN ('G')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_arr_g,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' AND agg_flt_regu_reas IN ('I')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_arr_i,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' AND agg_flt_regu_reas IN ('M')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_arr_m,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' AND agg_flt_regu_reas IN ('N')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_arr_n,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' AND agg_flt_regu_reas IN ('O')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_arr_o,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' AND agg_flt_regu_reas IN ('P')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_arr_p,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' AND agg_flt_regu_reas IN ('R')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_arr_r,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' AND agg_flt_regu_reas IN ('S')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_arr_s,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' AND agg_flt_regu_reas IN ('T')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_arr_t,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' AND agg_flt_regu_reas IN ('V')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_arr_v,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' AND agg_flt_regu_reas IN ('W')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_arr_w,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Arrival' AND agg_flt_regu_reas IN ('NA') THEN a.agg_flt_total_delay END,0)) AS tdm_arp_arr_na,

    /* departures category (and 15+ buckets) */
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' THEN a.agg_flt_total_delay END,0))                   AS tdm_arp_dep,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' THEN a.agg_flt_delayed_traffic END,0))               AS tdf_arp_dep,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' THEN NVL(a.agg_flt_regulated_traffic,0) END,0))      AS trf_arp_dep,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' AND a.agg_flt_delay_interval IN (']15,30]',']30,60]','> 60')
                 THEN a.agg_flt_total_delay END,0))                                                      AS tdm_15_arp_dep,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' AND a.agg_flt_delay_interval IN (']15,30]',']30,60]','> 60')
                 THEN a.agg_flt_delayed_traffic END,0))                                                  AS tdf_15_arp_dep,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' AND a.agg_flt_delay_interval IN (']15,30]',']30,60]','> 60')
                 THEN NVL(a.agg_flt_regulated_traffic,0) END,0))                                         AS trf_15_arp_dep,

    /* departures by reason — COMPLETE SET */
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' AND agg_flt_regu_reas IN ('A')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_dep_a,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' AND agg_flt_regu_reas IN ('C')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_dep_c,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' AND agg_flt_regu_reas IN ('D')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_dep_d,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' AND agg_flt_regu_reas IN ('E')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_dep_e,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' AND agg_flt_regu_reas IN ('G')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_dep_g,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' AND agg_flt_regu_reas IN ('I')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_dep_i,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' AND agg_flt_regu_reas IN ('M')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_dep_m,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' AND agg_flt_regu_reas IN ('N')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_dep_n,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' AND agg_flt_regu_reas IN ('O')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_dep_o,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' AND agg_flt_regu_reas IN ('P')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_dep_p,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' AND agg_flt_regu_reas IN ('R')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_dep_r,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' AND agg_flt_regu_reas IN ('S')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_dep_s,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' AND agg_flt_regu_reas IN ('T')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_dep_t,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' AND agg_flt_regu_reas IN ('V')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_dep_v,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' AND agg_flt_regu_reas IN ('W')  THEN a.agg_flt_total_delay END,0)) AS tdm_arp_dep_w,
    SUM(NVL(CASE WHEN a.mp_regu_loc_cat='Departure' AND agg_flt_regu_reas IN ('NA') THEN a.agg_flt_total_delay END,0)) AS tdm_arp_dep_na

  FROM prudev.v_aiu_agg_flt_flow a
  WHERE a.agg_flt_a_first_entry_date >= ", query_from, "
    AND a.agg_flt_mp_regu_loc_ty = 'Airport'
  GROUP BY a.agg_flt_a_first_entry_date, a.ref_loc_id
),

ALL_DAY_DATA AS (
  SELECT
    COALESCE(COALESCE(a.arp_code, c.arp_code), b.arp_code)        AS arp_code,
    COALESCE(COALESCE(a.flight_date, c.flight_date), b.entry_date) AS flight_date,
    COALESCE(a.arr, 0)  AS arr,
    COALESCE(c.dep, 0)  AS dep,

    COALESCE(b.tdm_arp, 0)        AS tdm_arp,
    COALESCE(b.tdf_arp, 0)        AS tdf_arp,
    COALESCE(b.trf_arp, 0)        AS trf_arp,
    COALESCE(b.tdm_15_arp, 0)     AS tdm_15_arp,
    COALESCE(b.tdf_15_arp, 0)     AS tdf_15_arp,
    COALESCE(b.trf_15_arp, 0)     AS trf_15_arp,

    COALESCE(b.tdm_arp_a, 0)      AS tdm_arp_a,
    COALESCE(b.tdm_arp_c, 0)      AS tdm_arp_c,
    COALESCE(b.tdm_arp_d, 0)      AS tdm_arp_d,
    COALESCE(b.tdm_arp_e, 0)      AS tdm_arp_e,
    COALESCE(b.tdm_arp_g, 0)      AS tdm_arp_g,
    COALESCE(b.tdm_arp_i, 0)      AS tdm_arp_i,
    COALESCE(b.tdm_arp_m, 0)      AS tdm_arp_m,
    COALESCE(b.tdm_arp_n, 0)      AS tdm_arp_n,
    COALESCE(b.tdm_arp_o, 0)      AS tdm_arp_o,
    COALESCE(b.tdm_arp_p, 0)      AS tdm_arp_p,
    COALESCE(b.tdm_arp_r, 0)      AS tdm_arp_r,
    COALESCE(b.tdm_arp_s, 0)      AS tdm_arp_s,
    COALESCE(b.tdm_arp_t, 0)      AS tdm_arp_t,
    COALESCE(b.tdm_arp_v, 0)      AS tdm_arp_v,
    COALESCE(b.tdm_arp_w, 0)      AS tdm_arp_w,
    COALESCE(b.tdm_arp_na, 0)     AS tdm_arp_na,

    COALESCE(b.tdm_arp_arr, 0)    AS tdm_arp_arr,
    COALESCE(b.tdf_arp_arr, 0)    AS tdf_arp_arr,
    COALESCE(b.trf_arp_arr, 0)    AS trf_arp_arr,
    COALESCE(b.tdm_15_arp_arr, 0) AS tdm_15_arp_arr,
    COALESCE(b.tdf_15_arp_arr, 0) AS tdf_15_arp_arr,
    COALESCE(b.trf_15_arp_arr, 0) AS trf_15_arp_arr,

    COALESCE(b.tdm_arp_arr_a, 0)  AS tdm_arp_arr_a,
    COALESCE(b.tdm_arp_arr_c, 0)  AS tdm_arp_arr_c,
    COALESCE(b.tdm_arp_arr_d, 0)  AS tdm_arp_arr_d,
    COALESCE(b.tdm_arp_arr_e, 0)  AS tdm_arp_arr_e,
    COALESCE(b.tdm_arp_arr_g, 0)  AS tdm_arp_arr_g,
    COALESCE(b.tdm_arp_arr_i, 0)  AS tdm_arp_arr_i,
    COALESCE(b.tdm_arp_arr_m, 0)  AS tdm_arp_arr_m,
    COALESCE(b.tdm_arp_arr_n, 0)  AS tdm_arp_arr_n,
    COALESCE(b.tdm_arp_arr_o, 0)  AS tdm_arp_arr_o,
    COALESCE(b.tdm_arp_arr_p, 0)  AS tdm_arp_arr_p,
    COALESCE(b.tdm_arp_arr_r, 0)  AS tdm_arp_arr_r,
    COALESCE(b.tdm_arp_arr_s, 0)  AS tdm_arp_arr_s,
    COALESCE(b.tdm_arp_arr_t, 0)  AS tdm_arp_arr_t,
    COALESCE(b.tdm_arp_arr_v, 0)  AS tdm_arp_arr_v,
    COALESCE(b.tdm_arp_arr_w, 0)  AS tdm_arp_arr_w,
    COALESCE(b.tdm_arp_arr_na, 0) AS tdm_arp_arr_na,

    COALESCE(b.tdm_arp_dep, 0)    AS tdm_arp_dep,
    COALESCE(b.tdf_arp_dep, 0)    AS tdf_arp_dep,
    COALESCE(b.trf_arp_dep, 0)    AS trf_arp_dep,
    COALESCE(b.tdm_15_arp_dep, 0) AS tdm_15_arp_dep,
    COALESCE(b.tdf_15_arp_dep, 0) AS tdf_15_arp_dep,
    COALESCE(b.trf_15_arp_dep, 0) AS trf_15_arp_dep,

    COALESCE(b.tdm_arp_dep_a, 0)  AS tdm_arp_dep_a,
    COALESCE(b.tdm_arp_dep_c, 0)  AS tdm_arp_dep_c,
    COALESCE(b.tdm_arp_dep_d, 0)  AS tdm_arp_dep_d,
    COALESCE(b.tdm_arp_dep_e, 0)  AS tdm_arp_dep_e,
    COALESCE(b.tdm_arp_dep_g, 0)  AS tdm_arp_dep_g,
    COALESCE(b.tdm_arp_dep_i, 0)  AS tdm_arp_dep_i,
    COALESCE(b.tdm_arp_dep_m, 0)  AS tdm_arp_dep_m,
    COALESCE(b.tdm_arp_dep_n, 0)  AS tdm_arp_dep_n,
    COALESCE(b.tdm_arp_dep_o, 0)  AS tdm_arp_dep_o,
    COALESCE(b.tdm_arp_dep_p, 0)  AS tdm_arp_dep_p,
    COALESCE(b.tdm_arp_dep_r, 0)  AS tdm_arp_dep_r,
    COALESCE(b.tdm_arp_dep_s, 0)  AS tdm_arp_dep_s,
    COALESCE(b.tdm_arp_dep_t, 0)  AS tdm_arp_dep_t,
    COALESCE(b.tdm_arp_dep_v, 0)  AS tdm_arp_dep_v,
    COALESCE(b.tdm_arp_dep_w, 0)  AS tdm_arp_dep_w,
    COALESCE(b.tdm_arp_dep_na, 0) AS tdm_arp_dep_na
  FROM arp_SYN_ARR A
  FULL OUTER JOIN arp_SYN_DEP C
    ON A.arp_code    = C.arp_code
   AND A.flight_date = C.flight_date
  FULL OUTER JOIN arp_DELAY B
    ON COALESCE(A.arp_code, C.arp_code)       = B.arp_code
   AND COALESCE(A.flight_date, C.flight_date) = B.entry_date
),

/* Map (arp_code, flight_date) to one airport row using date validity */
AP_BK_MAP AS (
  SELECT
    d.flight_date,
    d.arp_code,
    r.bk_ap_id,
    r.ec_ap_code,
    ROW_NUMBER() OVER (
      PARTITION BY d.flight_date, d.arp_code
      ORDER BY r.valid_from DESC NULLS LAST, r.valid_to DESC NULLS LAST
    ) AS rn
  FROM (SELECT DISTINCT flight_date, arp_code FROM ALL_DAY_DATA) d
  LEFT JOIN DIM_AIRPORT r
    ON r.cfmu_ap_code = d.arp_code
   AND d.flight_date BETWEEN r.valid_from AND r.valid_to
),

DATA_AP_ID AS (
  SELECT
    COALESCE(m.bk_ap_id, 99999)             AS bk_ap_id,
    COALESCE(m.ec_ap_code, '99999')         AS ap_code,
    CAST(EXTRACT(YEAR FROM a.flight_date) AS INTEGER) AS year,
    a.flight_date,
    SUM(a.arr)              AS arr,
    SUM(a.dep)              AS dep,
    SUM(a.dep) + SUM(a.arr) AS dep_arr,

    SUM(a.tdm_arp)          AS tdm_arp,
    SUM(a.tdf_arp)          AS tdf_arp,
    SUM(a.trf_arp)          AS trf_arp,
    SUM(a.tdm_15_arp)       AS tdm_15_arp,
    SUM(a.tdf_15_arp)       AS tdf_15_arp,
    SUM(a.trf_15_arp)       AS trf_15_arp,

    SUM(a.tdm_arp_a)        AS tdm_arp_a,
    SUM(a.tdm_arp_c)        AS tdm_arp_c,
    SUM(a.tdm_arp_d)        AS tdm_arp_d,
    SUM(a.tdm_arp_e)        AS tdm_arp_e,
    SUM(a.tdm_arp_g)        AS tdm_arp_g,
    SUM(a.tdm_arp_i)        AS tdm_arp_i,
    SUM(a.tdm_arp_m)        AS tdm_arp_m,
    SUM(a.tdm_arp_n)        AS tdm_arp_n,
    SUM(a.tdm_arp_o)        AS tdm_arp_o,
    SUM(a.tdm_arp_p)        AS tdm_arp_p,
    SUM(a.tdm_arp_r)        AS tdm_arp_r,
    SUM(a.tdm_arp_s)        AS tdm_arp_s,
    SUM(a.tdm_arp_t)        AS tdm_arp_t,
    SUM(a.tdm_arp_v)        AS tdm_arp_v,
    SUM(a.tdm_arp_w)        AS tdm_arp_w,
    SUM(a.tdm_arp_na)       AS tdm_arp_na,

    SUM(a.tdm_arp_arr)      AS tdm_arp_arr,
    SUM(a.tdf_arp_arr)      AS tdf_arp_arr,
    SUM(a.trf_arp_arr)      AS trf_arp_arr,
    SUM(a.tdm_15_arp_arr)   AS tdm_15_arp_arr,
    SUM(a.tdf_15_arp_arr)   AS tdf_15_arp_arr,
    SUM(a.trf_15_arp_arr)   AS trf_15_arp_arr,

    SUM(a.tdm_arp_arr_a)    AS tdm_arp_arr_a,
    SUM(a.tdm_arp_arr_c)    AS tdm_arp_arr_c,
    SUM(a.tdm_arp_arr_d)    AS tdm_arp_arr_d,
    SUM(a.tdm_arp_arr_e)    AS tdm_arp_arr_e,
    SUM(a.tdm_arp_arr_g)    AS tdm_arp_arr_g,
    SUM(a.tdm_arp_arr_i)    AS tdm_arp_arr_i,
    SUM(a.tdm_arp_arr_m)    AS tdm_arp_arr_m,
    SUM(a.tdm_arp_arr_n)    AS tdm_arp_arr_n,
    SUM(a.tdm_arp_arr_o)    AS tdm_arp_arr_o,
    SUM(a.tdm_arp_arr_p)    AS tdm_arp_arr_p,
    SUM(a.tdm_arp_arr_r)    AS tdm_arp_arr_r,
    SUM(a.tdm_arp_arr_s)    AS tdm_arp_arr_s,
    SUM(a.tdm_arp_arr_t)    AS tdm_arp_arr_t,
    SUM(a.tdm_arp_arr_v)    AS tdm_arp_arr_v,
    SUM(a.tdm_arp_arr_w)    AS tdm_arp_arr_w,
    SUM(a.tdm_arp_arr_na)   AS tdm_arp_arr_na,

    SUM(a.tdm_arp_dep)      AS tdm_arp_dep,
    SUM(a.tdf_arp_dep)      AS tdf_arp_dep,
    SUM(a.trf_arp_dep)      AS trf_arp_dep,
    SUM(a.tdm_15_arp_dep)   AS tdm_15_arp_dep,
    SUM(a.tdf_15_arp_dep)   AS tdf_15_arp_dep,
    SUM(a.trf_15_arp_dep)   AS trf_15_arp_dep,

    SUM(a.tdm_arp_dep_a)    AS tdm_arp_dep_a,
    SUM(a.tdm_arp_dep_c)    AS tdm_arp_dep_c,
    SUM(a.tdm_arp_dep_d)    AS tdm_arp_dep_d,
    SUM(a.tdm_arp_dep_e)    AS tdm_arp_dep_e,
    SUM(a.tdm_arp_dep_g)    AS tdm_arp_dep_g,
    SUM(a.tdm_arp_dep_i)    AS tdm_arp_dep_i,
    SUM(a.tdm_arp_dep_m)    AS tdm_arp_dep_m,
    SUM(a.tdm_arp_dep_n)    AS tdm_arp_dep_n,
    SUM(a.tdm_arp_dep_o)    AS tdm_arp_dep_o,
    SUM(a.tdm_arp_dep_p)    AS tdm_arp_dep_p,
    SUM(a.tdm_arp_dep_r)    AS tdm_arp_dep_r,
    SUM(a.tdm_arp_dep_s)    AS tdm_arp_dep_s,
    SUM(a.tdm_arp_dep_t)    AS tdm_arp_dep_t,
    SUM(a.tdm_arp_dep_v)    AS tdm_arp_dep_v,
    SUM(a.tdm_arp_dep_w)    AS tdm_arp_dep_w,
    SUM(a.tdm_arp_dep_na)   AS tdm_arp_dep_na
  FROM ALL_DAY_DATA a
  LEFT JOIN AP_BK_MAP m
    ON m.arp_code    = a.arp_code
   AND m.flight_date = a.flight_date
   AND m.rn = 1
  GROUP BY a.flight_date, m.ec_ap_code, m.bk_ap_id
)

SELECT *
FROM DATA_AP_ID
WHERE bk_ap_id <> 99999
  AND ap_code <> '99999'
")

## ap_ao ----
ap_ao_day_base_query <- paste0("
WITH
DIM_AO AS (
  SELECT DISTINCT ao_id, ao_code, wef, til
  FROM ldw_acc.AO_GROUPS_ASSOCIATION
),
DIM_AIRPORT AS (
  SELECT 
        BK_AP_ID,
	    EC_AP_CODE,
	    EC_AP_NAME,
	    CFMU_AP_CODE,
	    LATITUDE,
	    LONGITUDE,
      aiu_iso_ct_code,
	    VALID_FROM,
	    VALID_TO
  FROM pruread.v_aiu_dim_airport
  WHERE cfmu_ap_code IS NOT NULL
),

FLIGHTS AS (
  SELECT
    a.flt_uid,
    a.flt_dep_ad,
    a.flt_ctfm_ades,
    a.ao_icao_id,
    TRUNC(a.flt_a_asp_prof_time_entry) AS entry_date
  FROM prudev.v_aiu_flt a
  WHERE a.flt_lobt >= ", query_from, " - 2
    AND a.flt_lobt <  TRUNC(SYSDATE) + 2
    AND a.flt_a_asp_prof_time_entry >= ", query_from, "
    AND a.flt_a_asp_prof_time_entry <  TRUNC(SYSDATE)
    AND a.flt_state IN ('TE','TA','AA')
),
-- DEP airport mapping by date; pick one row per flight
DEP_MAP AS (
  SELECT
    f.flt_uid,
    r.bk_ap_id AS dep_bk_ap_id,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY r.valid_from DESC NULLS LAST, r.valid_to DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AIRPORT r
    ON r.cfmu_ap_code = f.flt_dep_ad
   AND f.entry_date >= r.valid_from
   AND f.entry_date  < r.valid_to       -- half-open as in your query
),
-- ARR airport mapping by date; pick one row per flight
ARR_MAP AS (
  SELECT
    f.flt_uid,
    r.bk_ap_id AS arr_bk_ap_id,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY r.valid_from DESC NULLS LAST, r.valid_to DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AIRPORT r
    ON r.cfmu_ap_code = f.flt_ctfm_ades
   AND f.entry_date >= r.valid_from
   AND f.entry_date  < r.valid_to
),
-- AO mapping by date; pick one row per flight
AO_MAP AS (
  SELECT
    f.flt_uid,
    d.ao_id,
    d.ao_code,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY d.wef DESC NULLS LAST, d.til DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AO d
    ON d.ao_code = f.ao_icao_id
   AND f.entry_date BETWEEN d.wef AND d.til  -- inclusive ends
),
DATA_DAY AS (
  SELECT
    f.flt_uid,
    f.entry_date,
    f.flt_dep_ad,
    f.flt_ctfm_ades,
    NVL(d.dep_bk_ap_id, 99999) AS dep_bk_ap_id,
    NVL(a.arr_bk_ap_id, 99999) AS arr_bk_ap_id,
    NVL(o.ao_id, 99999)        AS ao_id,
    NVL(o.ao_code, 'ZZZ')      AS ao_code
  FROM FLIGHTS f
  LEFT JOIN DEP_MAP d ON d.flt_uid = f.flt_uid AND d.rn = 1
  LEFT JOIN ARR_MAP a ON a.flt_uid = f.flt_uid AND a.rn = 1
  LEFT JOIN AO_MAP  o ON o.flt_uid = f.flt_uid AND o.rn = 1
),
DATA_DEP AS (
  SELECT
    dep_bk_ap_id          AS bk_ap_id,
    flt_dep_ad            AS ap_code,
    entry_date,
    ao_code,
    ao_id,
    COUNT(flt_uid)        AS dep_arr
  FROM DATA_DAY
  WHERE ao_id <> 99999
    AND dep_bk_ap_id <> 99999
    AND arr_bk_ap_id <> 99999
  GROUP BY dep_bk_ap_id, flt_dep_ad, entry_date, ao_code, ao_id
),
DATA_ARR AS (
  SELECT
    arr_bk_ap_id          AS bk_ap_id,
    flt_ctfm_ades         AS ap_code,
    entry_date,
    ao_code,
    ao_id,
    COUNT(flt_uid)        AS dep_arr
  FROM DATA_DAY
  WHERE ao_id <> 99999
    AND dep_bk_ap_id <> 99999
    AND arr_bk_ap_id <> 99999
  GROUP BY arr_bk_ap_id, flt_ctfm_ades, entry_date, ao_code, ao_id
)
SELECT
  CAST(EXTRACT(YEAR FROM COALESCE(a.entry_date, b.entry_date)) AS INTEGER) AS year,
  COALESCE(a.entry_date, b.entry_date)                                     AS entry_date,
  COALESCE(a.bk_ap_id,  b.bk_ap_id)                                        AS bk_ap_id,
  COALESCE(a.ap_code,   b.ap_code)                                         AS ap_code,
  COALESCE(a.ao_id,     b.ao_id)                                           AS ao_id,
  COALESCE(a.ao_code,   b.ao_code)                                         AS ao_code,
  COALESCE(a.dep_arr, 0)                                                   AS dep,
  COALESCE(b.dep_arr, 0)                                                   AS arr,
  COALESCE(a.dep_arr, 0) + COALESCE(b.dep_arr, 0)                          AS dep_arr
FROM DATA_DEP a
FULL OUTER JOIN DATA_ARR b
  ON a.bk_ap_id   = b.bk_ap_id
 AND a.entry_date = b.entry_date
 AND a.ao_id      = b.ao_id
 AND a.ao_code    = b.ao_code
ORDER BY dep_arr DESC
"
)

## ap_st_des ----
ap_st_des_day_base_query <- paste0("
WITH
DIM_AIRPORT AS (
  SELECT 
          BK_AP_ID,
	    EC_AP_CODE,
	    EC_AP_NAME,
	    CFMU_AP_CODE,
	    LATITUDE,
	    LONGITUDE,
      aiu_iso_ct_code,
	    VALID_FROM,
	    VALID_TO
  FROM pruread.v_aiu_dim_airport
  WHERE cfmu_ap_code IS NOT NULL
),

FLIGHTS AS (
  SELECT
    a.flt_uid,
    a.flt_dep_ad,
    a.flt_ctfm_ades,
    TRUNC(a.flt_a_asp_prof_time_entry) AS entry_date
  FROM prudev.v_aiu_flt a
  WHERE a.flt_lobt >= ", query_from, " - 2
    AND a.flt_lobt <  TRUNC(SYSDATE) + 2
    AND a.flt_a_asp_prof_time_entry >= ", query_from, "
    AND a.flt_a_asp_prof_time_entry <  TRUNC(SYSDATE)
    AND a.flt_state IN ('TE','TA','AA')
),

/* One dep DIM row per flight/date */
DEP_X AS (
  SELECT
    f.flt_uid,
    d.bk_ap_id,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY d.valid_from DESC NULLS LAST, d.valid_to DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AIRPORT d
    ON d.cfmu_ap_code = f.flt_dep_ad
   AND f.entry_date BETWEEN d.valid_from AND d.valid_to
),

/* One arr DIM row per flight/date */
ARR_X AS (
  SELECT
    f.flt_uid,
    a.aiu_iso_ct_code,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY a.valid_from DESC NULLS LAST, a.valid_to DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AIRPORT a
    ON a.cfmu_ap_code = f.flt_ctfm_ades
   AND f.entry_date BETWEEN a.valid_from AND a.valid_to
),

/* Single resolved row per flight */
ARP_FLIGHT AS (
  SELECT
    f.flt_uid,
    f.entry_date,
    f.flt_dep_ad                AS adep_code,
    NVL(dx.bk_ap_id, 99999)     AS dep_bk_ap_id,
    NVL(ax.aiu_iso_ct_code,'99999') AS arr_iso_ct_code
  FROM FLIGHTS f
  LEFT JOIN DEP_X dx ON dx.flt_uid = f.flt_uid AND dx.rn = 1
  LEFT JOIN ARR_X ax ON ax.flt_uid = f.flt_uid AND ax.rn = 1
)

SELECT
  CAST(EXTRACT(YEAR FROM entry_date) AS INTEGER) AS year,
  entry_date,
  dep_bk_ap_id AS bk_ap_id,
  adep_code,
  arr_iso_ct_code,
  COUNT(flt_uid) AS dep
FROM ARP_FLIGHT
WHERE dep_bk_ap_id <> 99999
  AND arr_iso_ct_code <> '99999'
GROUP BY entry_date, dep_bk_ap_id, adep_code, arr_iso_ct_code
ORDER BY entry_date DESC, dep_bk_ap_id, dep DESC
"
)

## ap_ap_des ----
ap_ap_des_day_base_query <- paste0("
WITH
DIM_AIRPORT AS (
  SELECT 
      BK_AP_ID,
	    EC_AP_CODE,
	    EC_AP_NAME,
	    CFMU_AP_CODE,
	    LATITUDE,
	    LONGITUDE,
      aiu_iso_ct_code,
	    VALID_FROM,
	    VALID_TO
  FROM pruread.v_aiu_dim_airport
  WHERE cfmu_ap_code IS NOT NULL
),

FLIGHTS AS (
  SELECT
    a.flt_uid,
    a.flt_dep_ad,
    a.flt_ctfm_ades,
    TRUNC(a.flt_a_asp_prof_time_entry) AS entry_date
  FROM prudev.v_aiu_flt a
  WHERE a.flt_lobt >= ", query_from, " - 2
    AND a.flt_lobt <  TRUNC(SYSDATE) + 2
    AND a.flt_a_asp_prof_time_entry >= ", query_from, "
    AND a.flt_a_asp_prof_time_entry <  TRUNC(SYSDATE)
    AND a.flt_state IN ('TE','TA','AA')
),
-- One dep mapping per flight by date-validity
DEP_X AS (
  SELECT
    f.flt_uid,
    d.bk_ap_id AS dep_bk_ap_id,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY d.valid_from DESC NULLS LAST, d.valid_to DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AIRPORT d
    ON d.cfmu_ap_code = f.flt_dep_ad
   AND f.entry_date BETWEEN d.valid_from AND d.valid_to
),
-- One arr mapping per flight by date-validity
ARR_X AS (
  SELECT
    f.flt_uid,
    a.bk_ap_id AS arr_bk_ap_id,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY a.valid_from DESC NULLS LAST, a.valid_to DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AIRPORT a
    ON a.cfmu_ap_code = f.flt_ctfm_ades
   AND f.entry_date BETWEEN a.valid_from AND a.valid_to
),
-- Single resolved row per flight
ARP_FLIGHT AS (
  SELECT
    f.flt_uid,
    f.entry_date,
    f.flt_dep_ad  AS adep_code,
    f.flt_ctfm_ades AS ades_code,
    NVL(dx.dep_bk_ap_id, 99999) AS dep_bk_ap_id,
    NVL(ax.arr_bk_ap_id, 99999) AS arr_bk_ap_id
  FROM FLIGHTS f
  LEFT JOIN DEP_X dx ON dx.flt_uid = f.flt_uid AND dx.rn = 1
  LEFT JOIN ARR_X ax ON ax.flt_uid = f.flt_uid AND ax.rn = 1
)
SELECT
  CAST(EXTRACT(YEAR FROM entry_date) AS INTEGER) AS year,
  entry_date,
  dep_bk_ap_id AS bk_ap_id,
  arr_bk_ap_id,
  adep_code,
  ades_code,
  COUNT(flt_uid) AS dep
FROM ARP_FLIGHT
WHERE dep_bk_ap_id <> 99999
  AND arr_bk_ap_id <> 99999
GROUP BY entry_date, dep_bk_ap_id, arr_bk_ap_id, adep_code, ades_code
ORDER BY entry_date DESC, dep_bk_ap_id, dep DESC
"
)

## ap_ms----
ap_ms_day_base_query <- paste0("
WITH
DIM_AIRPORT AS (
  SELECT 
      BK_AP_ID,
	    EC_AP_CODE,
	    EC_AP_NAME,
	    CFMU_AP_CODE,
	    LATITUDE,
	    LONGITUDE,
      aiu_iso_ct_code,
	    VALID_FROM,
	    VALID_TO
  FROM pruread.v_aiu_dim_airport
  WHERE cfmu_ap_code IS NOT NULL
),

/* Prefilter flights; keep ms_id and entry_date once */
FLIGHTS AS (
  SELECT
    A.flt_uid,
    A.flt_dep_ad,
    A.flt_ctfm_ades,
    A.SK_FLT_TYPE_RULE_ID AS ms_id,
    TRUNC(A.flt_a_asp_prof_time_entry) AS entry_date
  FROM prudev.v_aiu_flt_mark_seg A
  WHERE A.flt_lobt >= ", query_from, " - 2
    AND A.flt_lobt <  TRUNC(SYSDATE) + 2
    AND A.flt_a_asp_prof_time_entry >= ", query_from, "
    AND A.flt_a_asp_prof_time_entry <  TRUNC(SYSDATE)
    AND A.flt_state IN ('TE','TA','AA')
),

/* One dep airport mapping per flight/date */
DEP_X AS (
  SELECT
    f.flt_uid,
    d.bk_ap_id AS dep_bk_ap_id,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY d.valid_from DESC NULLS LAST, d.valid_to DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AIRPORT d
    ON d.cfmu_ap_code = f.flt_dep_ad
   AND f.entry_date BETWEEN d.valid_from AND d.valid_to
),

/* One arr airport mapping per flight/date */
ARR_X AS (
  SELECT
    f.flt_uid,
    a.bk_ap_id AS arr_bk_ap_id,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY a.valid_from DESC NULLS LAST, a.valid_to DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AIRPORT a
    ON a.cfmu_ap_code = f.flt_ctfm_ades
   AND f.entry_date BETWEEN a.valid_from AND a.valid_to
),

/* Single resolved row per flight */
DATA_SOURCE AS (
  SELECT
    f.entry_date,
    f.ms_id,
    f.flt_dep_ad  AS adep_code,
    f.flt_ctfm_ades AS ades_code,
    NVL(dx.dep_bk_ap_id, 99999) AS dep_bk_ap_id,
    NVL(ax.arr_bk_ap_id, 99999) AS arr_bk_ap_id,
    f.flt_uid
  FROM FLIGHTS f
  LEFT JOIN DEP_X dx ON dx.flt_uid = f.flt_uid AND dx.rn = 1
  LEFT JOIN ARR_X ax ON ax.flt_uid = f.flt_uid AND ax.rn = 1
),

/* Aggregate departures */
DATA_DEP AS (
  SELECT
    dep_bk_ap_id AS bk_ap_id,
    adep_code    AS ap_code,
    entry_date,
    ms_id,
    COUNT(flt_uid) AS dep_arr
  FROM DATA_SOURCE
  WHERE dep_bk_ap_id <> 99999
    AND arr_bk_ap_id <> 99999
  GROUP BY dep_bk_ap_id, adep_code, entry_date, ms_id
),

/* Aggregate arrivals */
DATA_ARR AS (
  SELECT
    arr_bk_ap_id AS bk_ap_id,
    ades_code    AS ap_code,
    entry_date,
    ms_id,
    COUNT(flt_uid) AS dep_arr
  FROM DATA_SOURCE
  WHERE dep_bk_ap_id <> 99999
    AND arr_bk_ap_id <> 99999
  GROUP BY arr_bk_ap_id, ades_code, entry_date, ms_id
)

SELECT
  CAST(EXTRACT(YEAR FROM COALESCE(a.entry_date, b.entry_date)) AS INTEGER) AS year,
  COALESCE(a.entry_date, b.entry_date) AS entry_date,
  COALESCE(a.bk_ap_id,  b.bk_ap_id)    AS bk_ap_id,
  COALESCE(a.ap_code,   b.ap_code)     AS ap_code,
  COALESCE(a.ms_id,     b.ms_id)       AS ms_id,
  COALESCE(a.dep_arr, 0) + COALESCE(b.dep_arr, 0) AS dep_arr
FROM DATA_DEP a
FULL OUTER JOIN DATA_ARR b
  ON a.bk_ap_id   = b.bk_ap_id
 AND a.entry_date = b.entry_date
 AND a.ms_id      = b.ms_id
ORDER BY entry_date, bk_ap_id, ms_id
")

# AIRLINE ----
## ao_day ----
ao_traffic_delay_day_base_query <- paste0("
WITH
DIM_AO AS (
  SELECT DISTINCT ao_id, ao_code, wef, til
  FROM ldw_acc.AO_GROUPS_ASSOCIATION
),
CAL AS (
  SELECT
    t.day_date,
    t.month,
    t.week,
    t.week_nb_year,
    t.day_type,
    t.day_of_week_nb AS day_of_week,
    t.year
  FROM prudev.pru_time_references t
  WHERE t.day_date >= ", query_from, "
    AND t.day_date <  TRUNC(SYSDATE)
),
AO_GRP_DAY AS (
  SELECT
    x.day_date,
    x.month,
    x.week,
    x.week_nb_year,
    x.day_type,
    x.day_of_week,
    x.year,
    x.ao_id,
    x.ao_code
  FROM (
    SELECT
      c.day_date,
      c.month,
      c.week,
      c.week_nb_year,
      c.day_type,
      c.day_of_week,
      c.year,
      d.ao_id,
      d.ao_code,
      ROW_NUMBER() OVER (
        PARTITION BY c.day_date, d.ao_code
        ORDER BY d.wef DESC NULLS LAST, d.til DESC NULLS LAST
      ) AS rn
    FROM CAL c
    JOIN DIM_AO d
      ON c.day_date BETWEEN d.wef AND d.til
  ) x
  WHERE x.rn = 1
),
FLIGHTS AS (
  SELECT
    a.flt_uid,
    TRUNC(a.flt_a_asp_prof_time_entry) AS entry_date,
    NVL(a.atfm_delay, 0)               AS atfm_delay,
    a.ao_icao_id
  FROM prudev.v_aiu_flt a
  WHERE a.flt_lobt >= ", query_from, " - 2
    AND a.flt_lobt <  TRUNC(SYSDATE) + 2
    AND a.flt_a_asp_prof_time_entry >= ", query_from, "
    AND a.flt_a_asp_prof_time_entry <  TRUNC(SYSDATE)
    AND a.flt_state IN ('TE','TA','AA')
    AND EXTRACT(YEAR FROM a.flt_a_asp_prof_time_entry) <= EXTRACT(YEAR FROM (TRUNC(SYSDATE) - 1))
),
AO_MAP AS (
  SELECT
    f.flt_uid,
    d.ao_id,
    d.ao_code,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY d.wef DESC NULLS LAST, d.til DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AO d
    ON d.ao_code = f.ao_icao_id
   AND f.entry_date BETWEEN d.wef AND d.til
),
DATA_FLIGHT AS (
  SELECT
    NVL(m.ao_id,   99999) AS ao_id,
    NVL(m.ao_code, 'ZZZ') AS ao_code,
    f.entry_date,
    f.atfm_delay           AS tdm,
    CASE WHEN f.atfm_delay > 15 THEN f.atfm_delay ELSE 0 END AS tdm_15,
    CASE WHEN f.atfm_delay > 0  THEN 1 ELSE 0 END AS tdf,
    CASE WHEN f.atfm_delay > 15 THEN 1 ELSE 0 END AS tdf_15,
    f.flt_uid
  FROM FLIGHTS f
  LEFT JOIN AO_MAP m
    ON m.flt_uid = f.flt_uid
   AND m.rn = 1
),
DATA_DAY AS (
  SELECT
    ao_id,
    ao_code,
    entry_date,
    COUNT(flt_uid)        AS day_tfc,
    SUM(tdm)              AS day_dly,
    SUM(tdm_15)           AS day_dly_15,
    SUM(tdf)              AS day_delayed_tfc,
    SUM(tdf_15)           AS day_delayed_tfc_15
  FROM DATA_FLIGHT
  WHERE ao_id <> 99999
  GROUP BY ao_id, ao_code, entry_date
)

SELECT
  g.year,
  g.month,
  g.day_date AS flight_date,
  g.week,
  g.week_nb_year,
  g.day_type,
  g.day_of_week,
  g.ao_id,
  g.ao_code,
  COALESCE(d.day_tfc, 0)           AS day_tfc,
  COALESCE(d.day_dly, 0)           AS day_dly,
  COALESCE(d.day_dly_15, 0)        AS day_dly_15,
  COALESCE(d.day_delayed_tfc, 0)   AS day_delayed_tfc,
  COALESCE(d.day_delayed_tfc_15,0) AS day_delayed_tfc_15
FROM AO_GRP_DAY g
LEFT JOIN DATA_DAY d
  ON d.ao_id      = g.ao_id
 AND d.ao_code    = g.ao_code
 AND d.entry_date = g.day_date

"
)

## ao_st_des ----
ao_st_des_day_base_query <- paste0("
/* file: sql/arr_iso_by_ao_no_duplicates.sql */
WITH
DIM_AO AS (
  SELECT DISTINCT ao_id, ao_code, wef, til
  FROM ldw_acc.AO_GROUPS_ASSOCIATION
),

DIM_AIRPORT AS (
  SELECT 
      BK_AP_ID,
	    EC_AP_CODE,
	    EC_AP_NAME,
	    CFMU_AP_CODE,
	    LATITUDE,
	    LONGITUDE,
      aiu_iso_ct_code,
	    VALID_FROM,
	    VALID_TO
  FROM pruread.v_aiu_dim_airport
  WHERE cfmu_ap_code IS NOT NULL
),

/* Prefilter flights & compute entry date once */
FLIGHTS AS (
  SELECT
    a.flt_uid,
    a.flt_ctfm_ades,
    a.ao_icao_id,
    TRUNC(a.flt_a_asp_prof_time_entry) AS entry_date
  FROM prudev.v_aiu_flt a
  WHERE a.flt_lobt >= ", query_from, " - 2
    AND a.flt_lobt <  TRUNC(SYSDATE) + 2
    AND a.flt_a_asp_prof_time_entry >= ", query_from, "
    AND a.flt_a_asp_prof_time_entry <  TRUNC(SYSDATE)
    AND a.flt_state IN ('TE','TA','AA')
),
/* One ARR airport row per flight by date validity */
ARR_X AS (
  SELECT
    f.flt_uid,
    d.aiu_iso_ct_code AS arr_iso_ct_code,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY d.valid_from DESC NULLS LAST, d.valid_to DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AIRPORT d
    ON d.cfmu_ap_code = f.flt_ctfm_ades
   AND f.entry_date BETWEEN d.valid_from AND d.valid_to
),
/* One AO row per flight by date validity */
AO_X AS (
  SELECT
    f.flt_uid,
    a.ao_id,
    a.ao_code,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY a.wef DESC NULLS LAST, a.til DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AO a
    ON a.ao_code = f.ao_icao_id
   AND f.entry_date BETWEEN a.wef AND a.til
),
/* Resolved single row per flight */
DATA_DAY AS (
  SELECT
    f.entry_date,
    NVL(arr.arr_iso_ct_code, '99999') AS arr_iso_ct_code,
    NVL(ao.ao_id, 99999)              AS ao_id,
    NVL(ao.ao_code, 'ZZZ')            AS ao_code,
    f.flt_uid
  FROM FLIGHTS f
  LEFT JOIN ARR_X arr ON arr.flt_uid = f.flt_uid AND arr.rn = 1
  LEFT JOIN AO_X  ao  ON ao.flt_uid  = f.flt_uid  AND ao.rn  = 1
)
SELECT
  CAST(EXTRACT(YEAR FROM a.entry_date) AS INTEGER) AS year,
  a.entry_date,
  a.ao_id,
  a.ao_code,
  a.arr_iso_ct_code,
  COUNT(a.flt_uid) AS flight
FROM DATA_DAY a
WHERE a.ao_id <> 99999
  AND a.arr_iso_ct_code <> '99999'
GROUP BY
  a.entry_date,
  a.ao_id,
  a.ao_code,
  a.arr_iso_ct_code
ORDER BY a.entry_date, a.ao_id, flight DESC
    
"
)

## ao_ap_dep ----
ao_ap_dep_day_base_query <- paste0("
/* file: sql/dep_airport_bk_by_ao_validity_no_dupes.sql */
WITH
DIM_AIRPORT AS (
  SELECT 
      BK_AP_ID,
	    EC_AP_CODE,
	    EC_AP_NAME,
	    CFMU_AP_CODE,
	    LATITUDE,
	    LONGITUDE,
      aiu_iso_ct_code,
	    VALID_FROM,
	    VALID_TO
  FROM pruread.v_aiu_dim_airport
  WHERE cfmu_ap_code IS NOT NULL
),

DIM_AO AS (
  SELECT DISTINCT ao_id, ao_code, wef, til
  FROM ldw_acc.AO_GROUPS_ASSOCIATION
),

/* Prefilter flights; keep entry_date once */
FLIGHTS AS (
  SELECT
    A.flt_uid,
    A.flt_dep_ad,
    A.ao_icao_id,
    TRUNC(A.flt_a_asp_prof_time_entry) AS entry_date
  FROM prudev.v_aiu_flt A
  WHERE A.flt_lobt >= ", query_from, " - 2
    AND A.flt_lobt <  TRUNC(SYSDATE) + 2
    AND A.flt_a_asp_prof_time_entry >= ", query_from, "
    AND A.flt_a_asp_prof_time_entry <  TRUNC(SYSDATE)
    AND A.flt_state IN ('TE','TA','AA')
),

/* One DEP airport mapping per flight by date-validity */
DEP_X AS (
  SELECT
    f.flt_uid,
    d.bk_ap_id,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY d.valid_from DESC NULLS LAST, d.valid_to DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AIRPORT d
    ON d.cfmu_ap_code = f.flt_dep_ad
   AND f.entry_date BETWEEN d.valid_from AND d.valid_to
),

/* One AO mapping per flight by date-validity */
AO_X AS (
  SELECT
    f.flt_uid,
    a.ao_id,
    a.ao_code,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY a.wef DESC NULLS LAST, a.til DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AO a
    ON a.ao_code = f.ao_icao_id
   AND f.entry_date BETWEEN a.wef AND a.til
),
/* Resolved single row per flight */
DATA_DAY AS (
  SELECT
    f.entry_date,
    f.flt_dep_ad                 AS ap_code,
    NVL(dx.bk_ap_id, 99999)      AS bk_ap_id,
    NVL(ax.ao_id, 99999)         AS ao_id,
    NVL(ax.ao_code, 'ZZZ')       AS ao_code,
    f.flt_uid
  FROM FLIGHTS f
  LEFT JOIN DEP_X dx ON dx.flt_uid = f.flt_uid AND dx.rn = 1
  LEFT JOIN AO_X  ax ON ax.flt_uid = f.flt_uid AND ax.rn = 1
)
SELECT
  CAST(EXTRACT(YEAR FROM a.entry_date) AS INTEGER) AS year,
  a.entry_date,
  a.ao_id,
  a.ao_code,
  a.ap_code,
  a.bk_ap_id,
  COUNT(a.flt_uid) AS flight
FROM DATA_DAY a
WHERE a.ao_id   <> 99999
  AND a.bk_ap_id <> 99999
GROUP BY
  a.entry_date,
  a.ao_id,
  a.ao_code,
  a.ap_code,
  a.bk_ap_id
ORDER BY a.entry_date, a.ao_id, flight DESC
"
)

## ao_ap_pair ----
ao_ap_pair_day_base_query <- paste0("
/* file: sql/airport_pairs_dedup.sql */
WITH
DIM_AIRPORT AS (
  SELECT 
      BK_AP_ID,
	    EC_AP_CODE,
	    EC_AP_NAME,
	    CFMU_AP_CODE,
	    LATITUDE,
	    LONGITUDE,
      aiu_iso_ct_code,
	    VALID_FROM,
	    VALID_TO
  FROM pruread.v_aiu_dim_airport
  WHERE cfmu_ap_code IS NOT NULL
),

DIM_AO AS (
  SELECT DISTINCT ao_id, ao_code, wef, til
  FROM ldw_acc.AO_GROUPS_ASSOCIATION
),

/* Prefilter flights and compute entry_date once */
FLIGHTS AS (
  SELECT
    A.flt_uid,
    A.flt_dep_ad,
    A.flt_ctfm_ades,
    A.ao_icao_id,
    TRUNC(A.flt_a_asp_prof_time_entry) AS entry_date
  FROM prudev.v_aiu_flt A
  WHERE A.flt_lobt >= ", query_from, " - 2
    AND A.flt_lobt <  TRUNC(SYSDATE) + 2
    AND A.flt_a_asp_prof_time_entry >= ", query_from, "
    AND A.flt_a_asp_prof_time_entry <  TRUNC(SYSDATE)
    AND A.flt_state IN ('TE','TA','AA')
),
/* One dep airport mapping per flight/date */
DEP_X AS (
  SELECT
    f.flt_uid,
    d.bk_ap_id AS bk_ap_id_dep,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY d.valid_from DESC NULLS LAST, d.valid_to DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AIRPORT d
    ON d.cfmu_ap_code = f.flt_dep_ad
   AND f.entry_date BETWEEN d.valid_from AND d.valid_to
),
/* One arr airport mapping per flight/date */
ARR_X AS (
  SELECT
    f.flt_uid,
    a.bk_ap_id AS bk_ap_id_des,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY a.valid_from DESC NULLS LAST, a.valid_to DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AIRPORT a
    ON a.cfmu_ap_code = f.flt_ctfm_ades
   AND f.entry_date BETWEEN a.valid_from AND a.valid_to
),
/* One AO mapping per flight/date */
AO_X AS (
  SELECT
    f.flt_uid,
    o.ao_id,
    o.ao_code,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY o.wef DESC NULLS LAST, o.til DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AO o
    ON o.ao_code = f.ao_icao_id
   AND f.entry_date BETWEEN o.wef AND o.til
),
/* Single resolved row per flight */
DATA_DAY AS (
  SELECT
    f.entry_date,
    f.flt_dep_ad   AS adep_code,
    f.flt_ctfm_ades AS ades_code,
    NVL(dx.bk_ap_id_dep, 99999) AS bk_ap_id_dep,
    NVL(ax.bk_ap_id_des, 99999) AS bk_ap_id_des,
    NVL(ox.ao_id,  99999)       AS ao_id,
    NVL(ox.ao_code,'ZZZ')       AS ao_code,
    f.flt_uid
  FROM FLIGHTS f
  LEFT JOIN DEP_X dx ON dx.flt_uid = f.flt_uid AND dx.rn = 1
  LEFT JOIN ARR_X ax ON ax.flt_uid = f.flt_uid AND ax.rn = 1
  LEFT JOIN AO_X  ox ON ox.flt_uid = f.flt_uid AND ox.rn = 1
),
/* Canonicalize the airport pair (order by bk_ap_id) and aggregate */
DATA_AIRPORT_PAIR AS (
  SELECT
    a.entry_date,
    a.ao_id,
    a.ao_code,
    CASE WHEN a.bk_ap_id_dep <= a.bk_ap_id_des THEN a.ades_code ELSE a.adep_code END AS ad_code_1,
    CASE WHEN a.bk_ap_id_dep <= a.bk_ap_id_des THEN a.adep_code ELSE a.ades_code END AS ad_code_2,
    CASE WHEN a.bk_ap_id_dep <= a.bk_ap_id_des THEN a.bk_ap_id_des ELSE a.bk_ap_id_dep END AS bk_ap_id_1,
    CASE WHEN a.bk_ap_id_dep <= a.bk_ap_id_des THEN a.bk_ap_id_dep ELSE a.bk_ap_id_des END AS bk_ap_id_2,
    COUNT(a.flt_uid) AS flight
  FROM DATA_DAY a
  WHERE a.ao_id <> 99999
    AND a.bk_ap_id_dep <> 99999
    AND a.bk_ap_id_des <> 99999
  GROUP BY
    a.entry_date,
    a.ao_id,
    a.ao_code,
    CASE WHEN a.bk_ap_id_dep <= a.bk_ap_id_des THEN a.ades_code ELSE a.adep_code END,
    CASE WHEN a.bk_ap_id_dep <= a.bk_ap_id_des THEN a.adep_code ELSE a.ades_code END,
    CASE WHEN a.bk_ap_id_dep <= a.bk_ap_id_des THEN a.bk_ap_id_des ELSE a.bk_ap_id_dep END,
    CASE WHEN a.bk_ap_id_dep <= a.bk_ap_id_des THEN a.bk_ap_id_dep ELSE a.bk_ap_id_des END
)
SELECT
  CAST(EXTRACT(YEAR FROM a.entry_date) AS INTEGER) AS year,
  a.*,
  (a.bk_ap_id_1 || '-' || a.bk_ap_id_2) AS arp_pair_id
FROM DATA_AIRPORT_PAIR a
ORDER BY arp_pair_id, ao_code, entry_date
"
)

## ao_ap_arr_delay ----
ao_ap_arr_delay_day_base_query <- paste0("
/* file: sql/arrival_delay_grouped_by_bk_and_ao_no_dupes.sql */
WITH
DIM_AO AS (
  SELECT DISTINCT ao_id, ao_code, wef, til
  FROM ldw_acc.AO_GROUPS_ASSOCIATION
),

DIM_AIRPORT AS (
  SELECT 
      BK_AP_ID,
	    EC_AP_CODE,
	    EC_AP_NAME,
	    CFMU_AP_CODE,
	    LATITUDE,
	    LONGITUDE,
      aiu_iso_ct_code,
	    VALID_FROM,
	    VALID_TO
  FROM pruread.v_aiu_dim_airport
  WHERE cfmu_ap_code IS NOT NULL
),

Apt_arr_reg AS (
  SELECT DISTINCT
         Agg_flt_lobt    AS flts_lobt,
         Agg_flt_mp_regu_id,
         Ref_loc_id      AS reg_arpt
  FROM Prudev.V_aiu_agg_flt_flow
  WHERE Mp_regu_loc_cat = 'Arrival'
    AND Agg_flt_mp_regu_loc_ty = 'Airport'
    AND agg_flt_lobt >= ", query_from, "
),

/* Prefilter flights & compute dates once */
FLIGHTS AS (
  SELECT
    A.flt_uid,
    A.flt_ftfm_ades,
    A.flt_ctfm_ades,
    A.flt_most_penal_regu_id,
    A.atfm_delay,
    A.ao_icao_id,
    TRUNC(A.Arvt_3)    AS arr_date,
    TRUNC(A.flt_lobt)  AS flt_lobt_trunc
  FROM prudev.v_aiu_flt A
  WHERE TRUNC(A.Arvt_3) >= ", query_from, "
    AND TRUNC(A.Arvt_3) <  TRUNC(SYSDATE)
    AND A.flt_lobt      >= ", query_from, " - 2
    AND A.flt_lobt      <  TRUNC(SYSDATE) + 2
    AND A.flt_state IN ('TE','TA','AA')
),

/* AO validity: choose exactly one AO per flight/date */
AO_X AS (
  SELECT
    f.flt_uid,
    d.ao_id,
    d.ao_code,
    ROW_NUMBER() OVER (
      PARTITION BY f.flt_uid
      ORDER BY d.wef DESC NULLS LAST, d.til DESC NULLS LAST
    ) AS rn
  FROM FLIGHTS f
  LEFT JOIN DIM_AO d
    ON d.ao_code = f.ao_icao_id
   AND f.arr_date BETWEEN d.wef AND d.til  -- inclusive ends
),

/* One resolved row per flight */
DATA_DAY AS (
  SELECT
    f.flt_ftfm_ades,
    f.flt_ctfm_ades,
    f.flt_most_penal_regu_id,
    f.atfm_delay,
    NVL(x.ao_id,   99999) AS ao_id,
    NVL(x.ao_code, 'ZZZ') AS ao_code,
    f.arr_date,
    f.flt_lobt_trunc,
    f.flt_uid
  FROM FLIGHTS f
  LEFT JOIN AO_X x
    ON x.flt_uid = f.flt_uid
   AND x.rn = 1
),

/* Windowed metrics per (arr_date, ao_code, ades) */
Apt_arr_delay_ao AS (
  SELECT DISTINCT
         a.arr_date,
         a.ao_id,
         a.ao_code,
         a.flt_ctfm_ades,
         COUNT(*) OVER (
           PARTITION BY a.arr_date, a.ao_code, a.flt_ctfm_ades
         ) AS flts,
         SUM( NVL2(b.Agg_flt_mp_regu_id, DECODE(a.atfm_delay, NULL, 0, 0, 0, 1), 0) )
           OVER (PARTITION BY a.arr_date, a.ao_code, a.flt_ctfm_ades) AS delayed_flts,
         SUM( NVL2(b.Agg_flt_mp_regu_id, a.atfm_delay, 0) )
           OVER (PARTITION BY a.arr_date, a.ao_code, a.flt_ctfm_ades) AS delay_amnt
  FROM DATA_DAY a
  LEFT JOIN Apt_arr_reg b
    ON a.flt_lobt_trunc = b.flts_lobt
   AND a.flt_most_penal_regu_id = b.Agg_flt_mp_regu_id
   AND a.flt_ftfm_ades = b.reg_arpt
  WHERE a.ao_id <> 99999
),

/* Map BK airport id for (arr_date, ades) with date-validity; pick one */
ADES_BK_MAP AS (
  SELECT
    g.arr_date,
    g.flt_ctfm_ades,
    c.bk_ap_id,
    ROW_NUMBER() OVER (
      PARTITION BY g.arr_date, g.flt_ctfm_ades
      ORDER BY c.valid_from DESC NULLS LAST, c.valid_to DESC NULLS LAST
    ) AS rn
  FROM (SELECT DISTINCT arr_date, flt_ctfm_ades FROM DATA_DAY) g
  LEFT JOIN DIM_AIRPORT c
    ON c.cfmu_ap_code = g.flt_ctfm_ades
   AND g.arr_date BETWEEN c.valid_from AND c.valid_to
),

/* Final grouped output */
DATA_GROUP AS (
  SELECT
    a.arr_date,
    a.ao_code,
    a.ao_id,
    m.bk_ap_id,
    a.flt_ctfm_ades AS ades_code,
    SUM(a.flts)         AS flts,
    SUM(a.delayed_flts) AS delayed_flts,
    SUM(a.delay_amnt)   AS delay_amnt
  FROM Apt_arr_delay_ao a
  LEFT JOIN ADES_BK_MAP m
    ON m.arr_date = a.arr_date
   AND m.flt_ctfm_ades = a.flt_ctfm_ades
   AND m.rn = 1
  GROUP BY a.arr_date, a.ao_code, a.ao_id, m.bk_ap_id, a.flt_ctfm_ades
)

SELECT
  CAST(EXTRACT(YEAR FROM arr_date) AS INTEGER) AS year,
  a.*
FROM DATA_GROUP a
WHERE a.bk_ap_id <> 99999
ORDER BY ao_id, arr_date DESC, flts DESC, ao_code
"
)

# ANSP ----
## sp_day ----
sp_traffic_delay_day_base_query <- paste0("
WITH 
ANSP_DATA AS (
	SELECT
		unit_id,
		entry_date,
		syn_tdm AS tdm,
		syn_tdm_ert AS tdm_ert,
		syn_tdf_ert AS tdf_ert,
		syn_tdm_15_ert AS tdm_15_ert,
		syn_tdf_15_ert AS tdf_15_ert,
		syn_tdm_ert_a AS tdm_ert_a,
		syn_tdm_ert_c AS tdm_ert_c,
		syn_tdm_ert_d AS tdm_ert_d,
		syn_tdm_ert_e AS tdm_ert_e,
		syn_tdm_ert_g AS tdm_ert_g,
		syn_tdm_ert_i AS tdm_ert_i,
		syn_tdm_ert_m AS tdm_ert_m,
		syn_tdm_ert_n AS tdm_ert_n,
		syn_tdm_ert_o AS tdm_ert_o,
		syn_tdm_ert_p AS tdm_ert_p,
		syn_tdm_ert_r AS tdm_ert_r,
		syn_tdm_ert_s AS tdm_ert_s,
		syn_tdm_ert_t AS tdm_ert_t,
		syn_tdm_ert_v AS tdm_ert_v,
		syn_tdm_ert_w AS tdm_ert_w,
		syn_tdm_ert_na AS tdm_ert_na
	FROM
		prudev.V_SYN12_FAC_DC_DD_APP
	WHERE
		entry_date >= to_date('01-01-2019', 'dd-mm-yyyy')-10
			AND unit_type = 'ANSP'
),

-- flights
OP_AUA_DATA as 
(
SELECT 
FLT_MODEL_FLT_UID flight_sk, c.ansp_id,  c.ansp_name,
ASP_PROF_TIME_ENTRY as asp_time_entry
FROM prudev.aiu_Asp_Prof_calc a
inner join prudev.v_aiu_flt b on (a.FLT_MODEL_FLT_UID = b.flt_uid)
inner join PRUDEV.V_PRU_REL_CFMU_AUA_ANSP C ON (a.ASP_PROF_ID = c.aua_code)
where a.FLT_MODEL_TY =3 
 and a.flt_model_lobt >= trunc(sysdate)-8 and a.flt_model_lobt < trunc(sysdate)+1 
   and b.flt_lobt >= trunc(sysdate)-8 and b.flt_lobt < trunc(sysdate)+1
and a.asp_prof_time_entry >= trunc(sysdate)-8  and a.asp_prof_time_entry < trunc(sysdate)+1   and asp_prof_ty = 'AUA'
and b.flt_state IN ('TE', 'TA', 'AA') 
and a.ASP_PROF_ID = c.aua_code and  a.ASP_PROF_TIME_ENTRY >= c.wef and  ASP_PROF_TIME_ENTRY <= c.till
),

 OP_ANSP_DATA as (
SELECT flight_sk, ansp_id, ansp_name,asp_time_entry as first_entry_time,  
     row_number() OVER ( PARTITION BY  ansp_id,flight_sk ORDER BY  asp_time_entry) row_num 
FROM OP_AUA_DATA
 ),
 
OP_ANSP_DATA2  as (
SELECT  ansp_id,  ansp_name
       , trunc(first_entry_time) as ENTRY_DATE
       , count(*) as FLT_DAIO 
FROM  OP_ANSP_DATA
WHERE 
       row_num = 1 
GROUP BY trunc(first_entry_time), ansp_name, ansp_id 
)
,

FLT_ANSP_DATA as (
SELECT ansp_name, ansp_id,
       entry_date,
       flt_daio
FROM op_ansp_data2 
where entry_date >=trunc(sysdate)-7 and entry_date < trunc(sysdate)

UNION
SELECT unit_name as ansp_name, unit_id as ansp_id,
       FLIGHT_DATE as entry_date,
       TTF_FLT as FLT_DAIO
FROM  PRUDEV.V_PRU_FAC_TD_DD  
WHERE  unit_kind = 'ANSP' and
      flight_date >=to_date('01-01-2019', 'dd-mm-yyyy')-10 and 
      flight_date < trunc(sysdate)-7
)  

SELECT
	COALESCE(b.unit_id, c.ansp_id) AS ansp_id,
	cast(extract(year from COALESCE(b.entry_date, c.entry_date)) as integer) AS year,
	COALESCE(b.entry_date, c.entry_date) AS entry_date,
	COALESCE(tdm, 0) AS tdm,
	COALESCE(tdm_ert, 0) AS tdm_ert,
	COALESCE(tdf_ert, 0) AS tdf_ert,
	COALESCE(tdm_15_ert, 0) AS tdm_15_ert,
	COALESCE(tdf_15_ert, 0) AS tdf_15_ert,
	COALESCE(tdm_ert_a, 0) AS tdm_ert_a,
	COALESCE(tdm_ert_c, 0) AS tdm_ert_c,
	COALESCE(tdm_ert_d, 0) AS tdm_ert_d,
	COALESCE(tdm_ert_e, 0) AS tdm_ert_e,
	COALESCE(tdm_ert_g, 0) AS tdm_ert_g,
	COALESCE(tdm_ert_i, 0) AS tdm_ert_i,
	COALESCE(tdm_ert_m, 0) AS tdm_ert_m,
	COALESCE(tdm_ert_n, 0) AS tdm_ert_n,
	COALESCE(tdm_ert_o, 0) AS tdm_ert_o,
	COALESCE(tdm_ert_p, 0) AS tdm_ert_p,
	COALESCE(tdm_ert_r, 0) AS tdm_ert_r,
	COALESCE(tdm_ert_s, 0) AS tdm_ert_s,
	COALESCE(tdm_ert_t, 0) AS tdm_ert_t,
	COALESCE(tdm_ert_v, 0) AS tdm_ert_v,
	COALESCE(tdm_ert_w, 0) AS tdm_ert_w,
	COALESCE(tdm_ert_na, 0) AS tdm_ert_na,
	COALESCE(c.flt_daio, 0) AS flt_daio
FROM ANSP_DATA b
FULL outer JOIN FLT_ANSP_DATA c ON b.entry_DATE = c.entry_date AND b.unit_id = c.ansp_id
ORDER BY ansp_id, entry_date
"
)
