# ap_ao_day_base_query ----
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

# ap_st_des_day_base_query ----
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

# ap_ap_des_day_base_query ----
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

# ap_ms_day_base_query ----
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


