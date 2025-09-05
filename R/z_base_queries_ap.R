# STATE ----
## st_ao ----
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