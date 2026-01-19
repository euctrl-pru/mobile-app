# ansp traffic  ----
query_sp_traffic_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0("
WITH LIST_AUA as (
SELECT
AUA_CODE,
ANSP_NAME, WEF, TILL , ANSP_ID
FROM PRUDEV.V_PRU_REL_CFMU_AUA_ANSP
WHERE ANSP_ID  in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,26,27,28,29,30,31,32,33,39,42,44,45,46,50,53,56,57)

),

LIST_ANSP as (
select ANSP_ID, ANSP_NAME FROM LIST_AUA
group by  ANSP_ID, ANSP_NAME
),

ANSP_DAY AS (
SELECT  a.ansp_id,
        a.ANSP_NAME,
        t.day_date,
        t.month,
        t.week,
        t.week_nb_year,
        t.day_type,
        t.day_of_week_nb AS day_of_week,
        t.year
FROM LIST_ANSP a ,  prudev.pru_time_references t
WHERE
   t.day_date >=  to_date('01-01-2019', 'dd-mm-yyyy')-10 
   AND t.day_date <  to_date('31-12-' || EXTRACT(YEAR FROM (", mydate, "-1)), 'dd-mm-yyyy')
 ),

OP_AUA_DATA as
(
SELECT
FLT_MODEL_FLT_UID flight_sk, c.ansp_id,  c.ansp_name,
ASP_PROF_TIME_ENTRY as asp_time_entry
FROM prudev.aiu_Asp_Prof_calc a
inner join prudev.v_aiu_flt b on (a.FLT_MODEL_FLT_UID = b.flt_uid)
inner join list_aua C ON (a.ASP_PROF_ID = c.aua_code)
where a.FLT_MODEL_TY =3
 and a.flt_model_lobt >= ", mydate, "-8 and a.flt_model_lobt < ", mydate, "+1
   and b.flt_lobt >= ", mydate, "-8 and b.flt_lobt < ", mydate, "+1
and a.asp_prof_time_entry >= ", mydate, "-8  and a.asp_prof_time_entry < ", mydate, "+1   and asp_prof_ty = 'AUA'
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

ANSP_DATA as (
SELECT ansp_name, ansp_id,
       entry_date,
       flt_daio
FROM op_ansp_data2
where entry_date >=", mydate, "-7 and entry_date < ", mydate, "

UNION
SELECT unit_name as ansp_name, unit_id as ansp_id,
       FLIGHT_DATE as entry_date,
       TTF_FLT as FLT_DAIO
FROM  PRUDEV.V_PRU_FAC_TD_DD
WHERE  unit_kind = 'ANSP' and
      flight_date >=to_date('01-01-2019', 'dd-mm-yyyy')-10 and
      flight_date < ", mydate, "-7
),


ANSP_DAY_DATA as (
 SELECT a.ansp_name,a.ansp_id,
    a.year, a.month,
    a.day_date as entry_date,
    a.week,
    a.week_nb_year,
    a.day_type,
    a.day_of_week,
    coalesce( flt_daio,0) as flt_daio
FROM  ANSP_DAY a left join ANSP_DATA b
on  (a.day_date = b.entry_DATE and a.ansp_id = b.ansp_id)

),

ANSP_Y2D_DATA as (
select
        entry_date ,
        ansp_name,
       SUM (flt_daio) OVER (PARTITION BY ansp_name ORDER BY entry_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(entry_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) y2d_flt_daio_year,
       SUM (flt_daio) OVER (PARTITION BY ansp_name ORDER BY entry_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(entry_date, 'DDD'))-1) PRECEDING AND CURRENT ROW)/
                Count (entry_date) OVER (PARTITION BY ansp_name ORDER BY entry_date ROWS BETWEEN (TO_NUMBER(TO_CHAR(entry_date, 'DDD'))-1) PRECEDING AND CURRENT ROW) y2d_avg_flt_daio_year


FROM ANSP_DAY_DATA
),

ANSP_PERIOD_DATA AS (
SELECT
       a.ansp_name,
       a.ansp_id,
       year,
       month,
       week,
       week_nb_year,
       day_type,
       day_of_week,

 ---- date
       a.entry_date,
       LAG (a.entry_date, 7) OVER (PARTITION BY a.ansp_name ORDER BY a.entry_date) entry_date_prev_week,
       LAG (a.entry_date, 364) OVER (PARTITION BY a.ansp_name ORDER BY a.entry_date) entry_date_prev_year,
       LAG (a.entry_date,  greatest((extract (year from a.entry_date)-2020) *364+ floor((extract (year from a.entry_date)-2020)/4)*7,0))
                OVER (PARTITION BY a.ansp_name ORDER BY a.entry_date) entry_date_2020,
       LAG (a.entry_date,  greatest((extract (year from a.entry_date)-2019) *364+ floor((extract (year from a.entry_date)-2019)/4)*7,0))
                OVER (PARTITION BY a.ansp_name ORDER BY a.entry_date) entry_date_2019,

---- traffic
       --day
       flt_daio AS day_flt_daio,
       LAG (flt_daio, 7) OVER (PARTITION BY a.ansp_name ORDER BY a.entry_date)  day_flt_daio_prev_week,
       LAG (flt_daio, 364) OVER (PARTITION BY a.ansp_name ORDER BY a.entry_date)  day_flt_daio_prev_year,
       LAG (flt_daio,  greatest((extract (year from a.entry_date)-2019) *364+ floor((extract (year from a.entry_date)-2019)/4)*7,0))
                OVER (PARTITION BY a.ansp_name ORDER BY a.entry_date)  day_flt_daio_2019,

       --rolling week
       AVG (flt_daio)  OVER (PARTITION BY a.ansp_name ORDER BY a.entry_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rw_avg_flt_daio,
       AVG (flt_daio)  OVER (PARTITION BY a.ansp_name ORDER BY a.entry_date ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) AS rw_avg_flt_daio_prev_week,

       --year to date
       b.y2d_flt_daio_year,
       c.y2d_flt_daio_year as y2d_flt_daio_prev_year,
       d.y2d_flt_daio_year as y2d_flt_daio_2019,
       b.y2d_avg_flt_daio_year,
       c.y2d_avg_flt_daio_year as y2d_avg_flt_daio_prev_year,
       d.y2d_avg_flt_daio_year as y2d_avg_flt_daio_2019

FROM ANSP_DAY_DATA a
  left join ANSP_Y2D_DATA b on a.entry_date = b.entry_date and a.ansp_name = b.ansp_name
  left join ANSP_Y2D_DATA c on add_months(a.entry_date,-12) = c.entry_date and a.ansp_name = c.ansp_name
  left join ANSP_Y2D_DATA d on add_months(a.entry_date,-12*(extract (year from a.entry_date)-2019)) = d.entry_date and a.ansp_name = d.ansp_name
),

ANSP_PERIOD_DATA2 AS (
SELECT
	a.*,
	LAG (rw_avg_flt_daio,364) OVER (PARTITION BY ansp_name ORDER BY entry_date) as rw_avg_flt_daio_prev_year,
    LAG (rw_avg_flt_daio, greatest((extract (year from entry_date)-2020) *364+ floor((extract (year from entry_date)-2020)/4)*7,0)  ) OVER (PARTITION BY ansp_name ORDER BY entry_date) as rw_avg_flt_daio_2020,
    LAG (rw_avg_flt_daio, greatest((extract (year from entry_date)-2019) *364+ floor((extract (year from entry_date)-2019)/4)*7,0)  ) OVER (PARTITION BY ansp_name ORDER BY entry_date) as rw_avg_flt_daio_2019

FROM ANSP_PERIOD_DATA a
)

-- iceland exception
SELECT
	ansp_name,
	year,
	month,
	week,
	week_nb_year,
	day_type,
	day_of_week,

	entry_date,
	entry_date_prev_week,
	entry_date_prev_year,
	entry_date_2020,
	entry_date_2019,

	-- day
	CASE WHEN ansp_id = 46 AND entry_date < TO_DATE('01-01-2024', 'dd-mm-yyyy') THEN NULL
		ELSE day_flt_daio
	END day_flt_daio,

	CASE WHEN ansp_id = 46 AND entry_date < TO_DATE('01-01-2024', 'dd-mm-yyyy') THEN NULL
		ELSE day_flt_daio_prev_week
	END day_flt_daio_prev_week,

	CASE WHEN ansp_id = 46 AND entry_date < TO_DATE('01-01-2025', 'dd-mm-yyyy') THEN NULL
		ELSE day_flt_daio_prev_year
	END day_flt_daio_prev_year,

	CASE WHEN ansp_id = 46 THEN NULL
		ELSE day_flt_daio_2019
	END day_flt_daio_2019,

	-- week
	CASE WHEN (ansp_id = 46 AND entry_date < TO_DATE('01-01-2024', 'dd-mm-yyyy')) OR (entry_date >= ", mydate, ") THEN NULL
		ELSE rw_avg_flt_daio
	END rw_avg_flt_daio,

	CASE WHEN (ansp_id = 46 AND entry_date < TO_DATE('01-01-2024', 'dd-mm-yyyy')) OR (entry_date >= ", mydate, ") THEN NULL
		ELSE rw_avg_flt_daio_prev_week
	END rw_avg_flt_daio_prev_week,

	CASE WHEN ansp_id = 46 AND entry_date < TO_DATE('01-01-2025', 'dd-mm-yyyy') THEN NULL
		ELSE rw_avg_flt_daio_prev_year
	END rw_avg_flt_daio_prev_year,

	CASE WHEN ansp_id = 46 THEN NULL
		ELSE rw_avg_flt_daio_2020
	END rw_avg_flt_daio_2020,

	CASE WHEN ansp_id = 46 THEN NULL
		ELSE rw_avg_flt_daio_2019
	END rw_avg_flt_daio_2019,

	-- y2d total
	CASE WHEN (ansp_id = 46 AND entry_date < TO_DATE('01-01-2024', 'dd-mm-yyyy')) OR (entry_date >= ", mydate, ") THEN NULL
		ELSE y2d_flt_daio_year
	END y2d_flt_daio_year,

	CASE WHEN ansp_id = 46 AND entry_date < TO_DATE('01-01-2025', 'dd-mm-yyyy') THEN NULL
		ELSE y2d_flt_daio_prev_year
	END y2d_flt_daio_prev_year,

	CASE WHEN ansp_id = 46 THEN NULL
		ELSE y2d_flt_daio_2019
	END y2d_flt_daio_2019,

	-- y2d avg
	CASE WHEN ansp_id = 46 AND entry_date < TO_DATE('01-01-2024', 'dd-mm-yyyy') THEN NULL
		ELSE y2d_avg_flt_daio_year
	END y2d_avg_flt_daio_year,

	CASE WHEN ansp_id = 46 AND entry_date < TO_DATE('01-01-2025', 'dd-mm-yyyy') THEN NULL
		ELSE y2d_avg_flt_daio_prev_year
	END y2d_avg_flt_daio_prev_year,

	CASE WHEN ansp_id = 46 THEN NULL
		ELSE y2d_avg_flt_daio_2019
	END y2d_avg_flt_daio_2019,

	", mydate, " -1 as last_data_day

FROM ANSP_PERIOD_DATA2
WHERE entry_date >=to_date('01-01-2024','dd-mm-yyyy')
"
)
}


# ansp delay  ----
query_sp_delay_raw <- function(mydate_string) {
  mydate <- date_sql_string(mydate_string)
  paste0(
"
WITH
LIST_AUA as (
SELECT
AUA_CODE,
ANSP_NAME, WEF, TILL , ANSP_ID
FROM PRUDEV.V_PRU_REL_CFMU_AUA_ANSP
WHERE ANSP_ID  in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,26,27,28,29,30,31,32,33,39,42,44,45,46,50,53,56,57)

),

LIST_ANSP AS (
select ANSP_ID, ANSP_NAME FROM LIST_AUA
group by  ANSP_ID, ANSP_NAME
),

ANSP_DAY AS (
	SELECT
		a.ansp_id,
		a.ansp_name,
		t.day_date,
		t.month,
		t.week,
		t.week_nb_year,
		t.day_type,
		t.day_of_week_nb AS day_of_week,
		t.year
	FROM
		LIST_ANSP a ,
		prudev.pru_time_references t
	WHERE
		t.day_date >= to_date('01-01-2019', 'dd-mm-yyyy')-10
			AND t.day_date < ", mydate, "
),

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
inner join list_aua C ON (a.ASP_PROF_ID = c.aua_code)
where a.FLT_MODEL_TY =3
 and a.flt_model_lobt >= ", mydate, "-8 and a.flt_model_lobt < ", mydate, "+1
   and b.flt_lobt >= ", mydate, "-8 and b.flt_lobt < ", mydate, "+1
and a.asp_prof_time_entry >= ", mydate, "-8  and a.asp_prof_time_entry < ", mydate, "+1   and asp_prof_ty = 'AUA'
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
where entry_date >=", mydate, "-7 and entry_date < ", mydate, "

UNION
SELECT unit_name as ansp_name, unit_id as ansp_id,
       FLIGHT_DATE as entry_date,
       TTF_FLT as FLT_DAIO
FROM  PRUDEV.V_PRU_FAC_TD_DD
WHERE  unit_kind = 'ANSP' and
      flight_date >=to_date('01-01-2019', 'dd-mm-yyyy')-10 and
      flight_date < ", mydate, "-7
)

SELECT
	a.ansp_name,
	a.ansp_id,
	a.year,
	a.month,
	a.week,
	a.week_nb_year,
	a.day_type,
	a.day_of_week,
	a.day_date AS entry_date,
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
FROM
	ANSP_DAY a
LEFT JOIN ANSP_DATA b
ON
	(
		a.day_date = b.entry_DATE
			AND a.ansp_id = b.unit_id
	)
LEFT JOIN FLT_ANSP_DATA c
ON (
		a.day_date = c.entry_DATE
			AND a.ansp_id = c.ansp_id
)
ORDER BY a.ansp_name, a.day_date
"
  )}
