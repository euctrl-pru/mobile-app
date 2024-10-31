# ao state des ----
## day ----
query_ao_st_des_data_day_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 (
  "
  with

LIST_AO AS (
SELECT ao_code,ao_name,ao_grp_code,ao_grp_name, ao_grp_level
FROM prudev.v_covid_dim_ao
where ao_grp_code in ('RYR_GRP',
                      'EZY_GRP',
                      'THY_GRP',
                      'DLH_GRP',
                      'AFR_GRP',
                      'WZZ_GRP',
                      'KLM_GRP',
                      'BAW_GRP',
                      'SAS_GRP',
                      'VLG',
                      'PGT',
                      'EWG_GRP',
                      'SWR_GRP',
                      'NAX_GRP',
                      'IBE_GRP',
                      'ITY',
                      'TUI_GRP',
                      'AEE_GRP',
                      'TAP_GRP',
                      'WIF',
                      'AUA',
                      'LOT',
                      'EXS',
                      'FIN',
                      'BCS_GRP',
                      'SXS',
                      'QTR',
                      'ANE',
                      'UAE',
                      'EIN_GRP',
                      'VOE',
                      'AEA',
                      'RAM',
                      'BEL',
                      'NJE',
                      'UAL',
                      'LOG',
                      'SEH',
                      'DAL',
                      'ASL'
                      )
 ),

DIM_COUNTRY as (
select iso_ct_code, ct_name
from covid_dim_country
),

DIM_AP
 as ( select cfmu_ap_code , pru_dashboard_ap_name  as airport_name,
 case when b.iso_ct_code = 'BA'
  then 'Bosnia and Herzegovina'
  else b.ct_name
 end country_name,
 a.country_code,
 region
 from  v_covid_rel_airport_area a
 left join DIM_COUNTRY b on a.country_code = b.iso_ct_code
 )

, DATA_FLIGHT as (

     SELECT
        b.ao_grp_code,
        b.ao_grp_name ,    b.ao_grp_level,
        coalesce(c.country_name,'Unspecified')  arr_Country_Name ,
        c.country_code as arr_Country_code ,
         coalesce(c.region,'Unspecified')  arr_Region ,
        trunc(A.flt_a_asp_prof_time_entry ) as entry_day,
        extract (year from A.flt_a_asp_prof_time_entry) year,
        A.flt_uid
FROM v_aiu_flt A
     inner join  LIST_AO b ON   (a.ao_icao_id = b.ao_code)
      left join  DIM_AP c on (a.flt_ctfm_ades = c.cfmu_ap_code)
WHERE
    (
    (     A.flt_lobt >= ", mydate, " -1 -1
                        AND A.flt_lobt < ", mydate, "-0
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -1
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0
                        )
                        or
                      (     A.flt_lobt >= ", mydate, " -1 -1 -7
                        AND A.flt_lobt < ", mydate, "-0 -7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -1 -7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0 - 7
                        )
                        or
                        (     A.flt_lobt >= ", mydate, " -364 -1 -1
                        AND A.flt_lobt < ", mydate, "-364
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -364-1
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-364
                        )
                        or
                        (     A.flt_lobt >= ", mydate, " - ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 -1 -1
                        AND A.flt_lobt < ", mydate, "- ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " - ((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7 -1
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "- ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        )

)
    AND A.flt_state IN ('TE','TA','AA')

),

DATA_PERIOD as
 (
 SELECT
      a.entry_day,  a.arr_Country_Name, a.arr_Country_code,
      a.ao_grp_code, a.ao_grp_name,a.ao_grp_level,
         case when entry_day >= ", mydate, " -1 and a.entry_day < ", mydate, "  then 'CURRENT_DAY'
      when a.entry_day >= ", mydate, " -364-1 and a.entry_day < ", mydate, " -364  then 'DAY_PREV_YEAR'
      when a.entry_day >= ", mydate, " -1-7 and a.entry_day < ", mydate, " -7  then 'DAY_PREV_WEEK'
      when a.entry_day >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7-1
              and a.entry_day < ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
              then 'DAY_2019'
         else '-'
    end  flag_day,
    flt_uid
  FROM DATA_FLIGHT a
),

DATA_GRP as
(
 SELECT
      a.flag_day,
      a.arr_Country_Name as iso_ct_name_arr,
      a.arr_Country_code,
      a.ao_grp_code, a.ao_grp_name,a.ao_grp_level,
     count(flt_uid) as flight,
      min(entry_Day) as from_date_with_data,
      max(entry_day) as to_date_with_data

  FROM DATA_PERIOD a
  GROUP BY
       a.flag_day,
       a.arr_Country_Name, a.arr_Country_code,
       a.ao_grp_code, a.ao_grp_name,a.ao_grp_level
),

AO_CTRY_RANK as
(
SELECT
  ao_grp_code, flag_day, iso_ct_name_arr,
        ROW_NUMBER() OVER (PARTITION BY  ao_grp_code, flag_day
                ORDER BY flight DESC, iso_ct_name_arr) as R_RANK,
       RANK() OVER (PARTITION BY  ao_grp_code, flag_day
                ORDER BY flight DESC, iso_ct_name_arr) as RANK

FROM DATA_GRP
where flag_day = 'CURRENT_DAY'
),

AO_CTRY_RANK_PREV as
(
SELECT
  ao_grp_code, flag_day, iso_ct_name_arr,
        RANK() OVER (PARTITION BY  ao_grp_code, flag_day
                ORDER BY flight DESC, iso_ct_name_arr) as RANK_PREV_WEEK
FROM DATA_GRP
where  flag_day = 'DAY_PREV_WEEK'
)


SELECT
      a.ao_grp_name,
      a.ao_grp_code,
      a.flag_day,
      a.iso_ct_name_arr,
      a.arr_Country_code as iso_ct_code_arr,
      flight,
      r_rank,
     rank,
     rank_prev_week,
     to_date(  TO_CHAR (", mydate, "-1, 'dd-mm-yyyy'),'dd-mm-yyyy') as to_date

  FROM DATA_GRP a
left join AO_CTRY_RANK b on a.iso_ct_name_arr = b.iso_ct_name_arr AND a.ao_grp_code = b.ao_grp_code
left join AO_CTRY_RANK_PREV c on a.iso_ct_name_arr = c.iso_ct_name_arr AND a.ao_grp_code = c.ao_grp_code
where r_rank<='10'
order by ao_grp_name, flag_day, r_rank
  "
)
}

## week ----
query_ao_st_des_data_week_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 (
  "
  with

LIST_AO AS (
SELECT ao_code,ao_name,ao_grp_code,ao_grp_name, ao_grp_level
FROM prudev.v_covid_dim_ao
where ao_grp_code in ('RYR_GRP',
                      'EZY_GRP',
                      'THY_GRP',
                      'DLH_GRP',
                      'AFR_GRP',
                      'WZZ_GRP',
                      'KLM_GRP',
                      'BAW_GRP',
                      'SAS_GRP',
                      'VLG',
                      'PGT',
                      'EWG_GRP',
                      'SWR_GRP',
                      'NAX_GRP',
                      'IBE_GRP',
                      'ITY',
                      'TUI_GRP',
                      'AEE_GRP',
                      'TAP_GRP',
                      'WIF',
                      'AUA',
                      'LOT',
                      'EXS',
                      'FIN',
                      'BCS_GRP',
                      'SXS',
                      'QTR',
                      'ANE',
                      'UAE',
                      'EIN_GRP',
                      'VOE',
                      'AEA',
                      'RAM',
                      'BEL',
                      'NJE',
                      'UAL',
                      'LOG',
                      'SEH',
                      'DAL',
                      'ASL'
                      )
 ),

DIM_COUNTRY as (
select iso_ct_code, ct_name
from covid_dim_country
),

DIM_AP
 as ( select cfmu_ap_code , pru_dashboard_ap_name  as airport_name,
 case when b.iso_ct_code = 'BA'
  then 'Bosnia and Herzegovina'
  else b.ct_name
 end country_name,
 a.country_code,
 region
 from  v_covid_rel_airport_area a
 left join DIM_COUNTRY b on a.country_code = b.iso_ct_code
 )

, DATA_FLIGHT as (

     SELECT
        b.ao_grp_code,
        b.ao_grp_name ,    b.ao_grp_level,
        coalesce(c.country_name,'Unspecified')  arr_Country_Name ,
        country_code,
         coalesce(c.region,'Unspecified')  arr_Region ,
        trunc(A.flt_a_asp_prof_time_entry ) as entry_day,
        extract (year from A.flt_a_asp_prof_time_entry) year,
        A.flt_uid
FROM v_aiu_flt A
     inner join  LIST_AO b ON   (a.ao_icao_id = b.ao_code)
      left join  DIM_AP c on (a.flt_ctfm_ades = c.cfmu_ap_code)
WHERE
    (
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
    AND A.flt_state IN ('TE','TA','AA')

),

DATA_PERIOD as
 (
 SELECT
      a.entry_day,  a.arr_Country_Name, a.country_code,
      a.ao_grp_code, a.ao_grp_name,a.ao_grp_level,
         case when entry_day >= ", mydate, " -7 and entry_day < ", mydate, "  then 'CURRENT_ROLLING_WEEK'
      when entry_day >= ", mydate, " -364-7 and entry_day < ", mydate, " -364  then 'ROLLING_WEEK_PREV_YEAR'
      when entry_day >= ", mydate, " -7-7 and entry_day < ", mydate, " -7  then 'PREV_ROLLING_WEEK'
      when entry_day >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7-7
              and entry_day < ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
              then 'ROLLING_WEEK_2019'
         else '-'
    end  flag_rolling_week,
    flt_uid
  FROM DATA_FLIGHT a
),

DATA_GRP as
(
 SELECT
      a.flag_rolling_week,
      a.arr_Country_Name as iso_ct_name_arr,
      a.country_code,
      a.ao_grp_code, a.ao_grp_name,a.ao_grp_level,
     count(flt_uid) as flight,
      min(entry_Day) as from_date_with_data,
      max(entry_day) as to_date_with_data

  FROM DATA_PERIOD a
  GROUP BY
       a.flag_rolling_week,
       a.arr_Country_Name, a.country_code,
       a.ao_grp_code, a.ao_grp_name,a.ao_grp_level
),

AO_CTRY_RANK as
(
SELECT
  ao_grp_code, flag_rolling_week, iso_ct_name_arr,
        ROW_NUMBER() OVER (PARTITION BY  ao_grp_code, flag_rolling_week
                ORDER BY flight DESC, iso_ct_name_arr) as R_RANK,
       RANK() OVER (PARTITION BY  ao_grp_code, flag_rolling_week
                ORDER BY flight DESC, iso_ct_name_arr) as RANK

FROM DATA_GRP
where flag_rolling_week = 'CURRENT_ROLLING_WEEK'
),

AO_CTRY_RANK_PREV as
(
SELECT
  ao_grp_code, flag_rolling_week, iso_ct_name_arr,
        RANK() OVER (PARTITION BY  ao_grp_code, flag_rolling_week
                ORDER BY flight DESC, iso_ct_name_arr) as RANK_PREV_WEEK
FROM DATA_GRP
where  flag_rolling_week = 'PREV_ROLLING_WEEK'
)


SELECT
      a.ao_grp_name,
      a.ao_grp_code,
      a.flag_rolling_week,
      a.iso_ct_name_arr,
      a.Country_code as iso_ct_code_arr,
      flight,
      r_rank,
     rank,
     rank_prev_week,
     to_date(  TO_CHAR (", mydate, "-7, 'dd-mm-yyyy'),'dd-mm-yyyy') as from_date,
     to_date(  TO_CHAR (", mydate, "-1, 'dd-mm-yyyy'),'dd-mm-yyyy') as to_date

  FROM DATA_GRP a
left join AO_CTRY_RANK b on a.iso_ct_name_arr = b.iso_ct_name_arr AND a.ao_grp_code = b.ao_grp_code
left join AO_CTRY_RANK_PREV c on a.iso_ct_name_arr = c.iso_ct_name_arr AND a.ao_grp_code = c.ao_grp_code
where r_rank<='10'
order by ao_grp_name, flag_rolling_week, r_rank
  "
)
}

## y2d ----
query_ao_st_des_data_y2d_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 (
  "
  with

LIST_AO AS (
SELECT ao_code,ao_name,ao_grp_code,ao_grp_name, ao_grp_level
FROM prudev.v_covid_dim_ao
where ao_grp_code in ('RYR_GRP',
                      'EZY_GRP',
                      'THY_GRP',
                      'DLH_GRP',
                      'AFR_GRP',
                      'WZZ_GRP',
                      'KLM_GRP',
                      'BAW_GRP',
                      'SAS_GRP',
                      'VLG',
                      'PGT',
                      'EWG_GRP',
                      'SWR_GRP',
                      'NAX_GRP',
                      'IBE_GRP',
                      'ITY',
                      'TUI_GRP',
                      'AEE_GRP',
                      'TAP_GRP',
                      'WIF',
                      'AUA',
                      'LOT',
                      'EXS',
                      'FIN',
                      'BCS_GRP',
                      'SXS',
                      'QTR',
                      'ANE',
                      'UAE',
                      'EIN_GRP',
                      'VOE',
                      'AEA',
                      'RAM',
                      'BEL',
                      'NJE',
                      'UAL',
                      'LOG',
                      'SEH',
                      'DAL',
                      'ASL'
                      )
 ),

DIM_COUNTRY as (
select iso_ct_code, ct_name
from covid_dim_country
),

DIM_AP
 as ( select cfmu_ap_code , pru_dashboard_ap_name  as airport_name,
 case when b.iso_ct_code = 'BA'
  then 'Bosnia and Herzegovina'
  else b.ct_name
 end country_name,
 a.country_code,
 region
 from  v_covid_rel_airport_area a
 left join DIM_COUNTRY b on a.country_code = b.iso_ct_code
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

, DATA_FLIGHT as (

     SELECT
        b.ao_grp_code,
        b.ao_grp_name ,    b.ao_grp_level,
        coalesce(c.country_name,'Unspecified')  arr_Country_Name ,
         c.country_code,
         coalesce(c.region,'Unspecified')  arr_Region ,
        trunc(A.flt_a_asp_prof_time_entry ) as entry_date,
        extract (year from A.flt_a_asp_prof_time_entry) year,
        A.flt_uid
FROM v_aiu_flt A
     inner join  LIST_AO b ON   (a.ao_icao_id = b.ao_code)
      left join  DIM_AP c on (a.flt_ctfm_ades = c.cfmu_ap_code)
WHERE
    A.flt_lobt>= TO_DATE ('01-01-2019', 'dd-mm-yyyy') -1
        AND A.flt_lobt < ", mydate, "
        AND flt_a_asp_prof_time_entry >= TO_DATE ('01-01-2019', 'dd-mm-yyyy')
        AND TO_NUMBER (TO_CHAR (TRUNC (flt_a_asp_prof_time_entry), 'mmdd')) <=   TO_NUMBER (TO_CHAR (", mydate, "-1, 'mmdd'))
        and extract (year from flt_a_asp_prof_time_entry) <= extract(year from (", mydate, "-1))
        AND A.flt_state IN ('TE','TA','AA')
),

DATA_GRP as
(
 SELECT
       a.year,
      a.arr_Country_Name as iso_ct_name_arr,
      a.country_code,
      a.ao_grp_code, a.ao_grp_name,a.ao_grp_level,
     count(flt_uid) as flight
  FROM DATA_FLIGHT a
  GROUP BY
        a.year,
       a.arr_Country_Name, a.country_code,
       a.ao_grp_code, a.ao_grp_name,a.ao_grp_level
),

AO_CTRY_RANK as
(
SELECT
  ao_grp_code, year, iso_ct_name_arr,
        ROW_NUMBER() OVER (PARTITION BY  ao_grp_code, year
                ORDER BY flight DESC, iso_ct_name_arr) as R_RANK,
       RANK() OVER (PARTITION BY  ao_grp_code, year
                ORDER BY flight DESC, iso_ct_name_arr) as RANK

FROM DATA_GRP
where year = extract (year from (", mydate, "-1))
),

AO_CTRY_RANK_PREV as
(
SELECT
  ao_grp_code, year, iso_ct_name_arr,
        RANK() OVER (PARTITION BY  ao_grp_code, year
                ORDER BY flight DESC, iso_ct_name_arr) as RANK_PREV_YEAR
FROM DATA_GRP
where  year = extract (year from (", mydate, "-1)) - 1
)


SELECT
      a.ao_grp_name,
      a.ao_grp_code,
      a.year,
      a.iso_ct_name_arr,
      a.Country_code as iso_ct_code_arr,
      flight,
      flight/d.no_days as avg_flight,
      r_rank,
     rank,
     rank_prev_year,
     d.from_date,
     d.to_date,
     d.no_days

  FROM DATA_GRP a
left join AO_CTRY_RANK b on a.iso_ct_name_arr = b.iso_ct_name_arr AND a.ao_grp_code = b.ao_grp_code
left join AO_CTRY_RANK_PREV c on a.iso_ct_name_arr = c.iso_ct_name_arr AND a.ao_grp_code = c.ao_grp_code
left join REF_DATES d on a.year=d.year
where r_rank<='10'
order by ao_grp_name, year desc, r_rank
"
)
}

# ao airport dep ----
## day ----
query_ao_apt_dep_data_day_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 (
  "with

LIST_AO AS (
SELECT ao_code,ao_name,ao_grp_code,ao_grp_name, ao_grp_level
FROM prudev.v_covid_dim_ao
where ao_grp_code in ('RYR_GRP',
                      'EZY_GRP',
                      'THY_GRP',
                      'DLH_GRP',
                      'AFR_GRP',
                      'WZZ_GRP',
                      'KLM_GRP',
                      'BAW_GRP',
                      'SAS_GRP',
                      'VLG',
                      'PGT',
                      'EWG_GRP',
                      'SWR_GRP',
                      'NAX_GRP',
                      'IBE_GRP',
                      'ITY',
                      'TUI_GRP',
                      'AEE_GRP',
                      'TAP_GRP',
                      'WIF',
                      'AUA',
                      'LOT',
                      'EXS',
                      'FIN',
                      'BCS_GRP',
                      'SXS',
                      'QTR',
                      'ANE',
                      'UAE',
                      'EIN_GRP',
                      'VOE',
                      'AEA',
                      'RAM',
                      'BEL',
                      'NJE',
                      'UAL',
                      'LOG',
                      'SEH',
                      'DAL',
                      'ASL'
                      )
 ),

DIM_COUNTRY as (
select iso_ct_code, ct_name
from covid_dim_country
),

DIM_AP
 as ( select cfmu_ap_code , pru_dashboard_ap_name  as airport_name,  iso_ct_name as country_name  from  v_covid_rel_airport_area),


DATA_FLIGHT as (

     SELECT
        b.ao_grp_code,
        b.ao_grp_name ,
        a.flt_dep_ad as adep_code,
        c.airport_name as adep_name,
        coalesce(c.country_name,'Unspecified')  country_dep_Name ,
        trunc(A.flt_a_asp_prof_time_entry ) as entry_day,
        extract (year from A.flt_a_asp_prof_time_entry) year,
        A.flt_uid
FROM v_aiu_flt A
     inner join  LIST_AO b ON   (a.ao_icao_id = b.ao_code)
      left join  DIM_AP c on (a.flt_dep_ad = c.cfmu_ap_code)
WHERE
    (
    (     A.flt_lobt >= ", mydate, " -1 -1
                        AND A.flt_lobt < ", mydate, "-0
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -1
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0
                        )
                        or
                      (     A.flt_lobt >= ", mydate, " -1 -1 -7
                        AND A.flt_lobt < ", mydate, "-0 -7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -1 -7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0 - 7
                        )
                        or
                        (     A.flt_lobt >= ", mydate, " -364 -1 -1
                        AND A.flt_lobt < ", mydate, "-364
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -364-1
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-364
                        )
                        or
                        (     A.flt_lobt >= ", mydate, " - ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 -1 -1
                        AND A.flt_lobt < ", mydate, "- ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " - ((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7 -1
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "- ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        )

)
    AND A.flt_state IN ('TE','TA','AA')

),

DATA_PERIOD as
 (
 SELECT
      a.entry_day,  a.adep_code, a.adep_name,
      a.ao_grp_code, a.ao_grp_name,
         case when entry_day >= ", mydate, " -1 and a.entry_day < ", mydate, "  then 'CURRENT_DAY'
      when a.entry_day >= ", mydate, " -364-1 and a.entry_day < ", mydate, " -364  then 'DAY_PREV_YEAR'
      when a.entry_day >= ", mydate, " -1-7 and a.entry_day < ", mydate, " -7  then 'DAY_PREV_WEEK'
      when a.entry_day >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7-1
              and a.entry_day < ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
              then 'DAY_2019'
         else '-'
    end  flag_day,
    flt_uid
  FROM DATA_FLIGHT a
),

DATA_GRP as
(
 SELECT
      a.flag_day,
      a.adep_code,  a.adep_name,
      a.ao_grp_code, a.ao_grp_name,
     count(flt_uid) as flight,
      min(entry_Day) as from_date_with_data,
      max(entry_day) as to_date_with_data

  FROM DATA_PERIOD a
  GROUP BY
       a.flag_day,
      a.adep_code,  a.adep_name,
       a.ao_grp_code, a.ao_grp_name
),

AO_CTRY_RANK as
(
SELECT
  ao_grp_code, flag_day, adep_name,
        ROW_NUMBER() OVER (PARTITION BY  ao_grp_code, flag_day
                ORDER BY flight DESC, adep_name) as R_RANK,
       RANK() OVER (PARTITION BY  ao_grp_code, flag_day
                ORDER BY flight DESC, adep_name) as RANK

FROM DATA_GRP
where flag_day = 'CURRENT_DAY'
),

AO_CTRY_RANK_PREV as
(
SELECT
  ao_grp_code, flag_day, adep_name,
        RANK() OVER (PARTITION BY  ao_grp_code, flag_day
                ORDER BY flight DESC, adep_name) as RANK_PREV_WEEK
FROM DATA_GRP
where  flag_day = 'DAY_PREV_WEEK'
)


SELECT
      a.ao_grp_name,
      a.ao_grp_code,
      a.flag_day,
      a.adep_code,  a.adep_name,
      flight,
      r_rank,
     rank,
     rank_prev_week,
     to_date(  TO_CHAR (", mydate, "-1, 'dd-mm-yyyy'),'dd-mm-yyyy') as to_date

  FROM DATA_GRP a
left join AO_CTRY_RANK b on a.adep_name = b.adep_name AND a.ao_grp_code = b.ao_grp_code
left join AO_CTRY_RANK_PREV c on a.adep_name = c.adep_name AND a.ao_grp_code = c.ao_grp_code
where r_rank<='10'
order by ao_grp_name, flag_day, r_rank
  "
)
}

## week ----
query_ao_apt_dep_data_week_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 (
  "with

LIST_AO AS (
SELECT ao_code,ao_name,ao_grp_code,ao_grp_name, ao_grp_level
FROM prudev.v_covid_dim_ao
where ao_grp_code in ('RYR_GRP',
                      'EZY_GRP',
                      'THY_GRP',
                      'DLH_GRP',
                      'AFR_GRP',
                      'WZZ_GRP',
                      'KLM_GRP',
                      'BAW_GRP',
                      'SAS_GRP',
                      'VLG',
                      'PGT',
                      'EWG_GRP',
                      'SWR_GRP',
                      'NAX_GRP',
                      'IBE_GRP',
                      'ITY',
                      'TUI_GRP',
                      'AEE_GRP',
                      'TAP_GRP',
                      'WIF',
                      'AUA',
                      'LOT',
                      'EXS',
                      'FIN',
                      'BCS_GRP',
                      'SXS',
                      'QTR',
                      'ANE',
                      'UAE',
                      'EIN_GRP',
                      'VOE',
                      'AEA',
                      'RAM',
                      'BEL',
                      'NJE',
                      'UAL',
                      'LOG',
                      'SEH',
                      'DAL',
                      'ASL'
                      )
 ),

DIM_COUNTRY as (
select iso_ct_code, ct_name
from covid_dim_country
),

DIM_AP
 as ( select cfmu_ap_code , pru_dashboard_ap_name  as airport_name,  iso_ct_name as country_name  from  v_covid_rel_airport_area),


DATA_FLIGHT as (

     SELECT
        b.ao_grp_code,
        b.ao_grp_name ,
        a.flt_dep_ad as adep_code,
        c.airport_name as adep_name,
        coalesce(c.country_name,'Unspecified')  country_dep_Name ,
        trunc(A.flt_a_asp_prof_time_entry ) as entry_day,
        extract (year from A.flt_a_asp_prof_time_entry) year,
        A.flt_uid
FROM v_aiu_flt A
     inner join  LIST_AO b ON   (a.ao_icao_id = b.ao_code)
      left join  DIM_AP c on (a.flt_dep_ad = c.cfmu_ap_code)
WHERE
    (
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
    AND A.flt_state IN ('TE','TA','AA')

),

DATA_PERIOD as
 (
 SELECT
      a.entry_day,  a.adep_code, a.adep_name,
      a.ao_grp_code, a.ao_grp_name,
      case when entry_day >= ", mydate, " -7 and entry_day < ", mydate, "  then 'CURRENT_ROLLING_WEEK'
        when entry_day >= ", mydate, " -364-7 and entry_day < ", mydate, " -364  then 'ROLLING_WEEK_PREV_YEAR'
        when entry_day >= ", mydate, " -7-7 and entry_day < ", mydate, " -7  then 'PREV_ROLLING_WEEK'
        when entry_day >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7-7
              and entry_day < ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
              then 'ROLLING_WEEK_2019'
        else '-'
      end flag_rolling_week,
    flt_uid
  FROM DATA_FLIGHT a
),

DATA_GRP as
(
 SELECT
      a.flag_rolling_week,
      a.adep_code,  a.adep_name,
      a.ao_grp_code, a.ao_grp_name,
     count(flt_uid) as flight

  FROM DATA_PERIOD a
  GROUP BY
       a.flag_rolling_week,
      a.adep_code,  a.adep_name,
       a.ao_grp_code, a.ao_grp_name
),

AO_CTRY_RANK as
(
SELECT
  ao_grp_code, flag_rolling_week, adep_name,
        ROW_NUMBER() OVER (PARTITION BY  ao_grp_code, flag_rolling_week
                ORDER BY flight DESC, adep_name) as R_RANK,
       RANK() OVER (PARTITION BY  ao_grp_code, flag_rolling_week
                ORDER BY flight DESC, adep_name) as RANK

FROM DATA_GRP
where flag_rolling_week = 'CURRENT_ROLLING_WEEK'
),

AO_CTRY_RANK_PREV as
(
SELECT
  ao_grp_code, flag_rolling_week, adep_name,
        RANK() OVER (PARTITION BY  ao_grp_code, flag_rolling_week
                ORDER BY flight DESC, adep_name) as RANK_PREV_WEEK
FROM DATA_GRP
where  flag_rolling_week = 'PREV_ROLLING_WEEK'
)


SELECT
      a.ao_grp_name,
      a.ao_grp_code,
      a.flag_rolling_week,
      a.adep_code,  a.adep_name,
      flight,
      r_rank,
     rank,
     rank_prev_week,
     to_date(  TO_CHAR (", mydate, "-7, 'dd-mm-yyyy'),'dd-mm-yyyy') as from_date,
     to_date(  TO_CHAR (", mydate, "-1, 'dd-mm-yyyy'),'dd-mm-yyyy') as to_date


  FROM DATA_GRP a
left join AO_CTRY_RANK b on a.adep_name = b.adep_name AND a.ao_grp_code = b.ao_grp_code
left join AO_CTRY_RANK_PREV c on a.adep_name = c.adep_name AND a.ao_grp_code = c.ao_grp_code
where r_rank<='10'
order by ao_grp_name, flag_rolling_week, r_rank
  "
)
}

## y2d ----
query_ao_apt_dep_data_y2d_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 (
  "with

LIST_AO AS (
SELECT ao_code,ao_name,ao_grp_code,ao_grp_name, ao_grp_level
FROM prudev.v_covid_dim_ao
where ao_grp_code in ('RYR_GRP',
                      'EZY_GRP',
                      'THY_GRP',
                      'DLH_GRP',
                      'AFR_GRP',
                      'WZZ_GRP',
                      'KLM_GRP',
                      'BAW_GRP',
                      'SAS_GRP',
                      'VLG',
                      'PGT',
                      'EWG_GRP',
                      'SWR_GRP',
                      'NAX_GRP',
                      'IBE_GRP',
                      'ITY',
                      'TUI_GRP',
                      'AEE_GRP',
                      'TAP_GRP',
                      'WIF',
                      'AUA',
                      'LOT',
                      'EXS',
                      'FIN',
                      'BCS_GRP',
                      'SXS',
                      'QTR',
                      'ANE',
                      'UAE',
                      'EIN_GRP',
                      'VOE',
                      'AEA',
                      'RAM',
                      'BEL',
                      'NJE',
                      'UAL',
                      'LOG',
                      'SEH',
                      'DAL',
                      'ASL'
                      )
 ),

DIM_COUNTRY as (
select iso_ct_code, ct_name
from covid_dim_country
),

DIM_AP
 as ( select cfmu_ap_code , pru_dashboard_ap_name  as airport_name,  iso_ct_name as country_name  from  v_covid_rel_airport_area),


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

DATA_FLIGHT as (

     SELECT
        b.ao_grp_code,
        b.ao_grp_name ,
        a.flt_dep_ad as adep_code,
        c.airport_name as adep_name,
        coalesce(c.country_name,'Unspecified')  country_dep_Name ,
        trunc(A.flt_a_asp_prof_time_entry ) as entry_day,
        extract (year from A.flt_a_asp_prof_time_entry) year,
        A.flt_uid
FROM v_aiu_flt A
     inner join  LIST_AO b ON   (a.ao_icao_id = b.ao_code)
      left join  DIM_AP c on (a.flt_dep_ad = c.cfmu_ap_code)
WHERE
    A.flt_lobt>= TO_DATE ('01-01-2019', 'dd-mm-yyyy') -1
        AND A.flt_lobt < ", mydate, "
        AND flt_a_asp_prof_time_entry >= TO_DATE ('01-01-2019', 'dd-mm-yyyy')
        AND TO_NUMBER (TO_CHAR (TRUNC (flt_a_asp_prof_time_entry), 'mmdd')) <=   TO_NUMBER (TO_CHAR (", mydate, "-1, 'mmdd'))
        AND extract (year from flt_a_asp_prof_time_entry) <= extract(year from (", mydate, "-1))
        AND A.flt_state IN ('TE','TA','AA')

),


DATA_GRP as
(
 SELECT
      a.year,
      a.adep_code,  a.adep_name,
      a.ao_grp_code, a.ao_grp_name,
     count(flt_uid) as flight

  FROM DATA_FLIGHT  a
  GROUP BY
       a.year,
      a.adep_code,  a.adep_name,
       a.ao_grp_code, a.ao_grp_name
),

AO_CTRY_RANK as
(
SELECT
  ao_grp_code, year , adep_name,
        ROW_NUMBER() OVER (PARTITION BY  ao_grp_code, year
                ORDER BY flight DESC, adep_name) as R_RANK,
       RANK() OVER (PARTITION BY  ao_grp_code, year
                ORDER BY flight DESC, adep_name) as RANK

FROM DATA_GRP
where year = extract (year from (", mydate, "-1))
),

AO_CTRY_RANK_PREV as
(
SELECT
  ao_grp_code, year , adep_name,
        RANK() OVER (PARTITION BY  ao_grp_code, year
                ORDER BY flight DESC, adep_name) as RANK_PREV_YEAR
FROM DATA_GRP
where  year = extract (year from (", mydate, "-1)) - 1
)


SELECT
      a.ao_grp_name,
      a.ao_grp_code,
      a.year ,
      a.adep_code,  a.adep_name,
      flight,
      flight/d.no_days as avg_flight,
      r_rank,
     rank,
     rank_prev_year,
     d.from_date,
     d.to_date,
     d.no_days

  FROM DATA_GRP a
left join AO_CTRY_RANK b on a.adep_name = b.adep_name AND a.ao_grp_code = b.ao_grp_code
left join AO_CTRY_RANK_PREV c on a.adep_name = c.adep_name AND a.ao_grp_code = c.ao_grp_code
left join REF_DATES d on a.year=d.year
where r_rank<='10'
order by ao_grp_name, year desc, r_rank
  "
  )
}

# ao airport pair ----
## day ----
query_ao_apt_pair_data_day_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 (
"with

LIST_AO AS (
SELECT ao_code,ao_name,ao_grp_code,ao_grp_name, ao_grp_level
FROM prudev.v_covid_dim_ao
where ao_grp_code in ('RYR_GRP',
                      'EZY_GRP',
                      'THY_GRP',
                      'DLH_GRP',
                      'AFR_GRP',
                      'WZZ_GRP',
                      'KLM_GRP',
                      'BAW_GRP',
                      'SAS_GRP',
                      'VLG',
                      'PGT',
                      'EWG_GRP',
                      'SWR_GRP',
                      'NAX_GRP',
                      'IBE_GRP',
                      'ITY',
                      'TUI_GRP',
                      'AEE_GRP',
                      'TAP_GRP',
                      'WIF',
                      'AUA',
                      'LOT',
                      'EXS',
                      'FIN',
                      'BCS_GRP',
                      'SXS',
                      'QTR',
                      'ANE',
                      'UAE',
                      'EIN_GRP',
                      'VOE',
                      'AEA',
                      'RAM',
                      'BEL',
                      'NJE',
                      'UAL',
                      'LOG',
                      'SEH',
                      'DAL',
                      'ASL'
                      )
 ),

DIM_COUNTRY as (
select iso_ct_code, ct_name
from covid_dim_country
),

DIM_AP
 as ( select cfmu_ap_code , pru_dashboard_ap_name  as airport_name,  iso_ct_name as country_name  from  v_covid_rel_airport_area),


DATA_FLIGHT as (

     SELECT
        b.ao_grp_code,
        b.ao_grp_name ,
       case when  coalesce(c.airport_name,'Unspecified') <= coalesce(d.airport_name,'Unspecified') then
                   coalesce(c.airport_name,'Unspecified')|| '<->' || coalesce(d.airport_name,'Unspecified')
              else  d.airport_name || '<->' || c.airport_name
         end airport_pair ,
        trunc(A.flt_a_asp_prof_time_entry ) as entry_day,
        extract (year from A.flt_a_asp_prof_time_entry) year,
        A.flt_uid
FROM v_aiu_flt A
     inner join  LIST_AO b ON   (a.ao_icao_id = b.ao_code)
      left join  DIM_AP c  on ( a.flt_dep_ad  = c.cfmu_ap_code)
      left join  DIM_AP d on (a.flt_ctfm_ades = d.cfmu_ap_code)
WHERE
    (
    (     A.flt_lobt >= ", mydate, " -1 -1
                        AND A.flt_lobt < ", mydate, "-0
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -1
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0
                        )
                        or
                      (     A.flt_lobt >= ", mydate, " -1 -1 -7
                        AND A.flt_lobt < ", mydate, "-0 -7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -1 -7
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-0 - 7
                        )
                        or
                        (     A.flt_lobt >= ", mydate, " -364 -1 -1
                        AND A.flt_lobt < ", mydate, "-364
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " -364-1
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "-364
                        )
                        or
                        (     A.flt_lobt >= ", mydate, " - ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7 -1 -1
                        AND A.flt_lobt < ", mydate, "- ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        AND A.flt_a_asp_prof_time_entry >=  ", mydate, " - ((extract (year from (", mydate, "-1))-2019) *364) - floor((extract (year from (", mydate, "-1))-2019)/4)*7 -1
                        AND A.flt_a_asp_prof_time_entry < ", mydate, "- ((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
                        )

)
    AND A.flt_state IN ('TE','TA','AA')

),

DATA_PERIOD as
 (
 SELECT
      a.entry_day,
      a.airport_pair,
      a.ao_grp_code, a.ao_grp_name,
         case when entry_day >= ", mydate, " -1 and a.entry_day < ", mydate, "  then 'CURRENT_DAY'
      when a.entry_day >= ", mydate, " -364-1 and a.entry_day < ", mydate, " -364  then 'DAY_PREV_YEAR'
      when a.entry_day >= ", mydate, " -1-7 and a.entry_day < ", mydate, " -7  then 'DAY_PREV_WEEK'
      when a.entry_day >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7-1
              and a.entry_day < ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
              then 'DAY_2019'
         else '-'
    end  flag_day,
    flt_uid
  FROM DATA_FLIGHT a
),

DATA_GRP as
(
 SELECT
      a.flag_day,
      a.airport_pair,
      a.ao_grp_code, a.ao_grp_name,
     count(flt_uid) as flight,
      min(entry_Day) as from_date_with_data,
      max(entry_day) as to_date_with_data

  FROM DATA_PERIOD a
  GROUP BY
       a.flag_day,
      a.airport_pair,
       a.ao_grp_code, a.ao_grp_name
),

AO_CTRY_RANK as
(
SELECT
  ao_grp_code, flag_day, airport_pair,
        ROW_NUMBER() OVER (PARTITION BY  ao_grp_code, flag_day
                ORDER BY flight DESC, airport_pair) as R_RANK,
       RANK() OVER (PARTITION BY  ao_grp_code, flag_day
                ORDER BY flight DESC, airport_pair) as RANK

FROM DATA_GRP
where flag_day = 'CURRENT_DAY'
),

AO_CTRY_RANK_PREV as
(
SELECT
  ao_grp_code, flag_day, airport_pair,
        RANK() OVER (PARTITION BY  ao_grp_code, flag_day
                ORDER BY flight DESC, airport_pair) as RANK_PREV_WEEK
FROM DATA_GRP
where  flag_day = 'DAY_PREV_WEEK'
)


SELECT
      a.ao_grp_name,
      a.ao_grp_code,
      a.flag_day,
      a.airport_pair,
      flight,
      r_rank,
     rank,
     rank_prev_week,
     to_date(  TO_CHAR (", mydate, "-1, 'dd-mm-yyyy'),'dd-mm-yyyy') as to_date

  FROM DATA_GRP a
left join AO_CTRY_RANK b on a.airport_pair = b.airport_pair AND a.ao_grp_code = b.ao_grp_code
left join AO_CTRY_RANK_PREV c on a.airport_pair = c.airport_pair AND a.ao_grp_code = c.ao_grp_code
where r_rank<='10'
order by ao_grp_name, flag_day, r_rank
"
)
}

## week ----
query_ao_apt_pair_data_week_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 (
  "with

LIST_AO AS (
SELECT ao_code,ao_name,ao_grp_code,ao_grp_name, ao_grp_level
FROM prudev.v_covid_dim_ao
where ao_grp_code in ('RYR_GRP',
                      'EZY_GRP',
                      'THY_GRP',
                      'DLH_GRP',
                      'AFR_GRP',
                      'WZZ_GRP',
                      'KLM_GRP',
                      'BAW_GRP',
                      'SAS_GRP',
                      'VLG',
                      'PGT',
                      'EWG_GRP',
                      'SWR_GRP',
                      'NAX_GRP',
                      'IBE_GRP',
                      'ITY',
                      'TUI_GRP',
                      'AEE_GRP',
                      'TAP_GRP',
                      'WIF',
                      'AUA',
                      'LOT',
                      'EXS',
                      'FIN',
                      'BCS_GRP',
                      'SXS',
                      'QTR',
                      'ANE',
                      'UAE',
                      'EIN_GRP',
                      'VOE',
                      'AEA',
                      'RAM',
                      'BEL',
                      'NJE',
                      'UAL',
                      'LOG',
                      'SEH',
                      'DAL',
                      'ASL'
                      )
 ),

DIM_COUNTRY as (
select iso_ct_code, ct_name
from covid_dim_country
),

DIM_AP
 as ( select cfmu_ap_code , pru_dashboard_ap_name  as airport_name,  iso_ct_name as country_name  from  v_covid_rel_airport_area),


DATA_FLIGHT as (

     SELECT
        b.ao_grp_code,
        b.ao_grp_name ,
       case when  coalesce(c.airport_name,'Unspecified') <= coalesce(d.airport_name,'Unspecified') then
                   coalesce(c.airport_name,'Unspecified')|| '<->' || coalesce(d.airport_name,'Unspecified')
              else  d.airport_name || '<->' || c.airport_name
         end airport_pair ,
        trunc(A.flt_a_asp_prof_time_entry ) as entry_day,
        extract (year from A.flt_a_asp_prof_time_entry) year,
        A.flt_uid
FROM v_aiu_flt A
     inner join  LIST_AO b ON   (a.ao_icao_id = b.ao_code)
      left join  DIM_AP c  on ( a.flt_dep_ad  = c.cfmu_ap_code)
      left join  DIM_AP d on (a.flt_ctfm_ades = d.cfmu_ap_code)
WHERE
    (
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
    AND A.flt_state IN ('TE','TA','AA')

),

DATA_PERIOD as
 (
 SELECT
      a.entry_day,
      a.airport_pair,
      a.ao_grp_code, a.ao_grp_name,
      case when entry_day >= ", mydate, " -7 and entry_day < ", mydate, "  then 'CURRENT_ROLLING_WEEK'
        when entry_day >= ", mydate, " -364-7 and entry_day < ", mydate, " -364  then 'ROLLING_WEEK_PREV_YEAR'
        when entry_day >= ", mydate, " -7-7 and entry_day < ", mydate, " -7  then 'PREV_ROLLING_WEEK'
        when entry_day >= ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7-7
              and entry_day < ", mydate, " -((extract (year from (", mydate, "-1))-2019) *364)- floor((extract (year from (", mydate, "-1))-2019)/4)*7
              then 'ROLLING_WEEK_2019'
        else '-'
      end flag_rolling_week,
    flt_uid
  FROM DATA_FLIGHT a
),

DATA_GRP as
(
 SELECT
      a.flag_rolling_week,
      a.airport_pair,
      a.ao_grp_code, a.ao_grp_name,
     count(flt_uid) as flight

  FROM DATA_PERIOD a
  GROUP BY
       a.flag_rolling_week,
      a.airport_pair,
       a.ao_grp_code, a.ao_grp_name
),

AO_CTRY_RANK as
(
SELECT
  ao_grp_code, flag_rolling_week, airport_pair,
        ROW_NUMBER() OVER (PARTITION BY  ao_grp_code, flag_rolling_week
                ORDER BY flight DESC, airport_pair) as R_RANK,
       RANK() OVER (PARTITION BY  ao_grp_code, flag_rolling_week
                ORDER BY flight DESC, airport_pair) as RANK

FROM DATA_GRP
where flag_rolling_week = 'CURRENT_ROLLING_WEEK'
),

AO_CTRY_RANK_PREV as
(
SELECT
  ao_grp_code, flag_rolling_week, airport_pair,
        RANK() OVER (PARTITION BY  ao_grp_code, flag_rolling_week
                ORDER BY flight DESC, airport_pair) as RANK_PREV_WEEK
FROM DATA_GRP
where  flag_rolling_week = 'PREV_ROLLING_WEEK'
)


SELECT
      a.ao_grp_name,
      a.ao_grp_code,
      a.flag_rolling_week,
      a.airport_pair,
      flight,
      r_rank,
     rank,
     rank_prev_week,
     to_date(  TO_CHAR (", mydate, "-7, 'dd-mm-yyyy'),'dd-mm-yyyy') as from_date,
     to_date(  TO_CHAR (", mydate, "-1, 'dd-mm-yyyy'),'dd-mm-yyyy') as to_date

  FROM DATA_GRP a
left join AO_CTRY_RANK b on a.airport_pair = b.airport_pair AND a.ao_grp_code = b.ao_grp_code
left join AO_CTRY_RANK_PREV c on a.airport_pair = c.airport_pair AND a.ao_grp_code = c.ao_grp_code
where r_rank<='10'
order by ao_grp_name, flag_rolling_week, r_rank
  "
)
}

## y2d ----
query_ao_apt_pair_data_y2d_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 (
  "with

LIST_AO AS (
SELECT ao_code,ao_name,ao_grp_code,ao_grp_name, ao_grp_level
FROM prudev.v_covid_dim_ao
where ao_grp_code in ('RYR_GRP',
                      'EZY_GRP',
                      'THY_GRP',
                      'DLH_GRP',
                      'AFR_GRP',
                      'WZZ_GRP',
                      'KLM_GRP',
                      'BAW_GRP',
                      'SAS_GRP',
                      'VLG',
                      'PGT',
                      'EWG_GRP',
                      'SWR_GRP',
                      'NAX_GRP',
                      'IBE_GRP',
                      'ITY',
                      'TUI_GRP',
                      'AEE_GRP',
                      'TAP_GRP',
                      'WIF',
                      'AUA',
                      'LOT',
                      'EXS',
                      'FIN',
                      'BCS_GRP',
                      'SXS',
                      'QTR',
                      'ANE',
                      'UAE',
                      'EIN_GRP',
                      'VOE',
                      'AEA',
                      'RAM',
                      'BEL',
                      'NJE',
                      'UAL',
                      'LOG',
                      'SEH',
                      'DAL',
                      'ASL'
                      )
 ),

DIM_COUNTRY as (
select iso_ct_code, ct_name
from covid_dim_country
),

DIM_AP
 as ( select cfmu_ap_code , pru_dashboard_ap_name  as airport_name,  iso_ct_name as country_name  from  v_covid_rel_airport_area),

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

DATA_FLIGHT as (

     SELECT
        b.ao_grp_code,
        b.ao_grp_name ,
       case when  coalesce(c.airport_name,'Unspecified') <= coalesce(d.airport_name,'Unspecified') then
                   coalesce(c.airport_name,'Unspecified')|| '<->' || coalesce(d.airport_name,'Unspecified')
              else  d.airport_name || '<->' || c.airport_name
         end airport_pair ,
        trunc(A.flt_a_asp_prof_time_entry ) as entry_day,
        extract (year from A.flt_a_asp_prof_time_entry) year,
        A.flt_uid
FROM v_aiu_flt A
     inner join  LIST_AO b ON   (a.ao_icao_id = b.ao_code)
      left join  DIM_AP c  on ( a.flt_dep_ad  = c.cfmu_ap_code)
      left join  DIM_AP d on (a.flt_ctfm_ades = d.cfmu_ap_code)
WHERE
    A.flt_lobt>= TO_DATE ('01-01-2019', 'dd-mm-yyyy') -1
        AND A.flt_lobt < ", mydate, "
        AND flt_a_asp_prof_time_entry >= TO_DATE ('01-01-2019', 'dd-mm-yyyy')
        AND TO_NUMBER (TO_CHAR (TRUNC (flt_a_asp_prof_time_entry), 'mmdd')) <=   TO_NUMBER (TO_CHAR (", mydate, "-1, 'mmdd'))
        AND extract (year from flt_a_asp_prof_time_entry) <= extract(year from (", mydate, "-1))
        AND A.flt_state IN ('TE','TA','AA')

),

DATA_GRP as
(
 SELECT
      a.year,
      a.airport_pair,
      a.ao_grp_code, a.ao_grp_name,
     count(flt_uid) as flight

  FROM DATA_FLIGHT a
  GROUP BY
       a.year,
      a.airport_pair,
       a.ao_grp_code, a.ao_grp_name
),

AO_CTRY_RANK as
(
SELECT
  ao_grp_code, year, airport_pair,
        ROW_NUMBER() OVER (PARTITION BY  ao_grp_code, year
                ORDER BY flight DESC, airport_pair) as R_RANK,
       RANK() OVER (PARTITION BY  ao_grp_code, year
                ORDER BY flight DESC, airport_pair) as RANK

FROM DATA_GRP
where year = extract (year from (", mydate, "-1))
),

AO_CTRY_RANK_PREV as
(
SELECT
  ao_grp_code, year, airport_pair,
        RANK() OVER (PARTITION BY  ao_grp_code, year
                ORDER BY flight DESC, airport_pair) as RANK_PREV_YEAR
FROM DATA_GRP
where  year = extract (year from (", mydate, "-1)) - 1
)


SELECT
      a.ao_grp_name,
      a.ao_grp_code,
      a.year,
      a.airport_pair,
      flight,
      flight/d.no_days as avg_flight,
      r_rank,
     rank,
     rank_prev_year,
     d.from_date,
     d.to_date,
     d.no_days

  FROM DATA_GRP a
left join AO_CTRY_RANK b on a.airport_pair = b.airport_pair AND a.ao_grp_code = b.ao_grp_code
left join AO_CTRY_RANK_PREV c on a.airport_pair = c.airport_pair AND a.ao_grp_code = c.ao_grp_code
left join REF_DATES d on a.year=d.year
where r_rank<='10'
order by ao_grp_name, year, r_rank
  "
  )
}

# ao apt delay ----
query_ao_apt_arr_delay_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0 (
"
WITH


   Base_date AS(SELECT CAST(", mydate, "-1 AS DATE) Bd FROM DUAL),

   Ref_dates AS
        (SELECT 'D' Type_ref, Bd Ref_date FROM Base_date
         UNION ALL
         SELECT 'C'
              , NEXT_DAY(ADD_MONTHS(Bd, -12) - 1, TO_CHAR(Bd, 'DAY'))
           FROM Base_date)
    ,

    Ref_dates_YTD AS
        (SELECT 'D' Type_ref, Bd Ref_date FROM Base_date
         UNION ALL
         SELECT 'C'
              , ADD_MONTHS(BD,-12)
           FROM Base_date) ,


    Prds AS
        (SELECT 'YTD'                        Prd
              , Type_ref
              , TRUNC(Ref_date, 'YEAR')     Start_d
              , Ref_date                    End_d
           FROM Ref_dates_YTD
         UNION ALL
         SELECT 'WK'
              , Type_ref
              ,   Ref_date
                - INTERVAL '6' DAY
              , Ref_date
           FROM Ref_dates
         UNION ALL
         SELECT '1D', Type_ref, Ref_date, Ref_date FROM Ref_dates)



         ,
    Ao_list AS
        (SELECT Ao_code
              , Ao_name
              , Ao_nm_group_code
           FROM Prudev.V_covid_dim_ao
          WHERE Ao_grp_code
                      IN ('RYR_GRP'
                              , 'EZY_GRP'
                              , 'THY_GRP'
                              , 'DLH_GRP'
                              , 'AFR_GRP'
                              , 'WZZ_GRP'
                              , 'KLM_GRP'
                              , 'BAW_GRP'
                              , 'SAS_GRP'
                              , 'VLG'
                              , 'PGT'
                              , 'EWG_GRP'
                              , 'SWR_GRP'
                              , 'NAX_GRP'
                              , 'IBE_GRP'
                              , 'ITY'
                              , 'TUI_GRP'
                              , 'AEE_GRP'
                              , 'TAP_GRP'
                              , 'WIF'
                              , 'AUA'
                              , 'LOT'
                              , 'EXS'
                              , 'FIN'
                              , 'BCS_GRP'
                              , 'SXS'
                              , 'QTR'
                              , 'ANE'
                              , 'UAE'
                              , 'EIN_GRP'
                              , 'VOE'
                              , 'AEA'
                              , 'RAM'
                              , 'BEL'
                              , 'NJE'
                              , 'UAL'
                              , 'LOG'
                              , 'SEH'
                              , 'DAL'
                              , 'ASL')
                              ),
    Apt_arr_reg AS
        (SELECT DISTINCT
                Agg_flt_lobt     Flts_lobt
              , Agg_flt_mp_regu_id
              , Ref_loc_id       Reg_arpt
           FROM Prudev.V_aiu_agg_flt_flow
          WHERE Mp_regu_loc_cat = 'Arrival'
            AND Agg_flt_mp_regu_loc_ty = 'Airport'
            and agg_flt_lobt >= (SELECT MIN(Start_d) FROM Prds)-1
            ),


    Apt_arr_delay_ao AS
        (SELECT DISTINCT
                TRUNC(A.Arvt_3)       AS Arr_day
              , C.Ao_nm_group_code    Ao_group
              , A.Flt_ctfm_ades      Apt_Code
              , COUNT(*)
                    OVER(
                        PARTITION BY TRUNC(A.Arvt_3)
                                   , C.Ao_nm_group_code
                                   , A.Flt_ctfm_ades
                    )                 Flts
              , SUM(NVL2(Agg_flt_mp_regu_id, decode(atfm_delay,NULL,0,0,0, 1),0))
                    OVER(
                        PARTITION BY TRUNC(A.Arvt_3)
                                   , C.Ao_nm_group_code
                                   , A.Flt_ctfm_ades
                    )                 AS Delayed_flts
              , SUM(NVL2(Agg_flt_mp_regu_id, Atfm_delay, 0))
                    OVER(
                        PARTITION BY TRUNC(A.Arvt_3)
                                   , C.Ao_nm_group_code
                                   , A.Flt_ctfm_ades
                    )                 AS Delay_amnt
           FROM V_aiu_flt  A
                INNER JOIN Ao_list C ON (A.Ao_icao_id = C.Ao_code)
                LEFT JOIN Apt_arr_reg B
                    ON (TRUNC(A.Flt_lobt) = B.Flts_lobt
                    AND A.Flt_most_penal_regu_id =
                        B.Agg_flt_mp_regu_id
                    AND A.Flt_ftfm_ades = B.Reg_arpt)
          WHERE A.Flt_state IN ('TE', 'TA', 'AA')
            AND Flt_lobt >= (SELECT MIN(Start_d) FROM Prds) -1
            AND Flt_lobt < (SELECT MAX(End_d) FROM Prds)+1)

,
    All_trf AS
        (SELECT DISTINCT P.*
                       , Ao_group
                       , Apt_Code
                       , SUM(Flts)
                             OVER(
                                 PARTITION BY Prd
                                            , Type_ref
                                            , Ao_group
                                            , Apt_Code
                             )    Flts
                       , SUM(Delayed_flts)
                             OVER(
                                 PARTITION BY Prd
                                            , Type_ref
                                            , Ao_group
                                            , Apt_Code
                             )    Delayed_flts
                       , SUM(Delay_amnt)
                             OVER(
                                 PARTITION BY Prd
                                            , Type_ref
                                            , Ao_group
                                            , Apt_Code
                             )    Delay_amnt
           FROM Prds  P
                JOIN Apt_arr_delay_ao
                    ON (Arr_day BETWEEN Start_d AND End_d)),


Period_Rank as
(SELECT D.*
                      , ROW_NUMBER()
                            OVER(
                                PARTITION BY Prd, Type_ref, Ao_group
                                ORDER BY Flts DESC, Apt_Code
                            )    Rnk
                   FROM All_trf D
                  WHERE D.Type_ref = 'D'),

Top_10 AS
        (SELECT *
           FROM Period_Rank
          WHERE Rnk <= 10),

Prev_Period as
        (SELECT * from all_trf where type_ref = 'C')


  SELECT
         t.prd as Period, t.start_d as start_date, t.end_d as end_date, t.ao_group as ao_group_code,
         t.Apt_Code  , t.flts as arr_flight, t.delayed_flts as arr_delayed_flight, t.delay_amnt as arr_atfm_delay, t.rnk as rank_by_flight,
          a.start_d as prev_start_date, a.end_d as prev_end_date,  a.flts as prev_arr_flight, a.delayed_flts as prev_delayed_flight, a.delay_amnt as prev_arr_atfm_delay
    FROM Top_10 T
         left JOIN Prev_Period A  on  (t.Prd =a.Prd and t.Ao_group = a.Ao_group and t.Apt_Code = a.Apt_Code)
"
  )
  }
