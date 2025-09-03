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