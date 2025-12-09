# AIRLINE----
## dim ao grp ----  
# same as v_covid_dim_ao but adding ao_id and removing old aos
dim_ao_grp_query <- "
WITH
        data_ao_grp1_and_oth
        AS
            (SELECT AO_ID
                   ,TRIM (AO_CODE)
                        AS ao_code
                   ,TRIM (AO_NAME)
                        AS ao_name
                   ,AO_GRP_ID
                   ,CASE
                        WHEN AO_GRP_level = 'GROUP1'
                        THEN
                            ao_grp_code
                        ELSE
                            ao_code
                    END
                        AS ao_grp_code
                   ,CASE
                        WHEN AO_GRP_level = 'GROUP1'
                        THEN
                            ao_grp_name
                        ELSE
                            ao_name
                    END
                        AS ao_grp_name
                   ,CASE WHEN ao_grp_level = 'GROUP1' THEN 'Y' ELSE 'N' END
                        AS AO_NM_GROUP_FLAG
                   ,CASE
                        WHEN ao_grp_level = 'GROUP1'
                        THEN
                            ao_grp_level
                        ELSE
                            'GROUP1_OTHER'
                    END
                        AS AO_GRP_LEVEL
                   ,WEF
                   ,TIL
                   ,AO_ISO_CTRY
               FROM ldw_acc.AO_GROUPS_ASSOCIATION
              WHERE     CRCO_DUPLICATE_FLAG IS NULL
                    AND ao_code IS NOT NULL
                    AND ao_grp_level IN ('GROUP1', 'OTHER')),
        data_assoc
        AS
            (SELECT AO_ID
                   ,TRIM (AO_CODE) AS ao_code
                   ,TRIM (AO_NAME) AS ao_name
                   ,AO_GRP_ID
                   ,ao_code        AS ao_grp_code
                   ,ao_name        AS ao_grp_name
                   ,'N'            AS AO_NM_GROUP_FLAG
                   ,'GROUP1_OTHER' AS AO_GRP_LEVEL
                   ,WEF
                   ,TIL
                   ,AO_ISO_CTRY
               FROM ldw_acc.AO_GROUPS_ASSOCIATION a
              WHERE     CRCO_DUPLICATE_FLAG IS NULL
                    AND ao_code IS NOT NULL
                    AND ao_grp_level IN ('ASSOCIATION')
                    AND NOT EXISTS
                            (SELECT NULL
                               FROM data_ao_grp1_and_oth b
                              WHERE a.ao_id = b.ao_id)),
--        data_old_ao
--        AS
--            (SELECT 0                AS ao_id
--                   ,ao_code
--                   ,ao_name
--                   ,0                AS ao_grp_id
--                   ,ao_code          AS ao_grp_code
--                   ,ao_name          AS ao_grp_name
--                   ,'N'              AS AO_NM_GROUP_FLAG
--                   ,'GROUP1_OTHER'   AS AO_GRP_LEVEL
--                   ,wef
--                   ,til
--                   ,ao_iso_ctry_code AS ao_iso_ctry
--               FROM PRUDEV.COVID_DIM_AO_BEF_2019),
        data_union_ao
        AS
            (SELECT AO_ID
                   ,ao_code
                   ,ao_name
                   ,AO_GRP_ID
                   ,ao_grp_code
                   ,ao_grp_name
                   ,AO_NM_GROUP_FLAG
                   ,AO_GRP_LEVEL
                   ,WEF
                   ,TIL
                   ,AO_ISO_CTRY
               FROM data_ao_grp1_and_oth
             UNION ALL
             SELECT AO_ID
                   ,ao_code
                   ,ao_name
                   ,AO_GRP_ID
                   ,ao_grp_code
                   ,ao_grp_name
                   ,AO_NM_GROUP_FLAG
                   ,AO_GRP_LEVEL
                   ,WEF
                   ,TIL
                   ,AO_ISO_CTRY
               FROM data_assoc
--             UNION ALL
--             SELECT AO_ID
--                   ,ao_code
--                   ,ao_name
--                   ,AO_GRP_ID
--                   ,ao_grp_code
--                   ,ao_grp_name
--                   ,AO_NM_GROUP_FLAG
--                   ,AO_GRP_LEVEL
--                   ,WEF
--                   ,TIL
--                   ,AO_ISO_CTRY
--               FROM data_old_ao
               ),
        data_all_ao
        AS
            (SELECT ao_id
                   ,ao_code
                   ,ao_name
                   ,wef
                   ,til
                   ,ao_iso_ctry
                   ,ao_grp_code
                   ,ao_grp_name
                   ,ao_grp_level
                   ,ROW_NUMBER ()
                    OVER (PARTITION BY ao_code ORDER BY til DESC, ao_id DESC)
                        rn
               FROM data_union_ao),
        data_all_ao_uniq
        AS
            (SELECT ao_id
                   ,ao_code
                   ,ao_name
                   ,wef
                   ,til
                   ,ao_iso_ctry
                   ,ao_grp_code
                   ,ao_grp_name
                   ,ao_grp_level
                   ,CASE WHEN ao_grp_level = 'GROUP1' THEN 'Y' ELSE 'N' END
                        AS AO_NM_GROUP_FLAG
               FROM data_all_ao
              WHERE rn = 1),
        data_ao_group2
        AS
            (SELECT ao_id
                   ,ao_code
                   ,ao_name
                   ,wef
                   ,til
                   ,ao_iso_ctry
                   ,ao_grp_code
                   ,ao_grp_name
                   ,ao_grp_level
                   ,'Y'
                        AS AO_GROUP2_COVID_LIST
                   ,ROW_NUMBER ()
                    OVER (PARTITION BY ao_code ORDER BY til DESC, ao_id DESC)
                        rn
               FROM ldw_acc.AO_GROUPS_ASSOCIATION
              WHERE     CRCO_DUPLICATE_FLAG IS NULL
                    AND ao_code IS NOT NULL
                    AND ao_grp_level IN ('GROUP2')),
        data_ao_group2_uniq
        AS
            (SELECT ao_id
                   ,ao_code
                   ,ao_name
                   ,wef
                   ,til
                   ,ao_iso_ctry
                   ,ao_grp_code
                   ,ao_grp_name
                   ,ao_grp_level
                   ,AO_GROUP2_COVID_LIST
               FROM data_ao_group2
              WHERE rn = 1),
   data_all_ao_incl_dupl as (
    SELECT a.til,
          a.ao_id
    	  ,a.ao_code
          ,a.ao_name
          ,a.ao_name                               AS nm_ao_name
          ,a.ao_grp_code                           AS ao_nm_group_code
          ,a.ao_grp_name                           AS ao_nm_group_name
          ,a.ao_grp_code
          ,a.ao_grp_name
          ,a.AO_NM_GROUP_FLAG
          ,COALESCE (c.LIST_DSH, 'N')              AS AO_NM_LIST
          ,COALESCE (c.LIST_DSH, 'N')              AS LIST_DSH
          ,COALESCE (c.LIST_DENIS, 'N')            AS LIST_DENIS
          ,COALESCE (b.AO_GRP_CODE, a.AO_GRP_CODE) AS AO_GROUP2_CODE
          ,COALESCE (b.AO_GRP_NAME, a.AO_GRP_NAME) AS AO_GROUP2_NAME
          ,COALESCE (b.AO_GROUP2_COVID_LIST, 'N')  AS AO_GROUP2_COVID_LIST
          ,a.ao_grp_level
          ,a.ao_iso_ctry                           AS ao_iso_ctry_code
      FROM data_all_ao_uniq  a
           LEFT JOIN data_ao_group2_uniq b
               ON (a.ao_id = b.ao_id AND a.til = b.til)
           LEFT JOIN PRUDEV.V_COVID_DSH_LIST_AO C ON (a.ao_code = c.ao_code)
   )

   select  a.til
          ,a.ao_id
    	    ,a.ao_code
          ,a.ao_name
          ,a.ao_grp_code
          ,a.ao_grp_name
          ,AO_GROUP2_CODE
          ,AO_GROUP2_NAME
          ,a.ao_grp_level
          ,ao_iso_ctry_code

   from data_all_ao_incl_dupl a
--   WHERE (a.ao_id, a.til) IN (
--		  SELECT ao_id, MAX(til)
--  			FROM data_all_ao_incl_dupl
--  				GROUP BY ao_id)
"





## list ao ----
list_ao_query <-"
SELECT a.ao_code,
		a.ao_name
		, b.ao_id
FROM pruprod.v_aiu_app_dim_ao_grp a
LEFT JOIN 
	(SELECT * FROM ldw_acc.AO_GROUPS_ASSOCIATION) b ON a.ao_code = b.ao_code
GROUP BY a.ao_code, a.ao_name,
		b.ao_id
ORDER BY ao_id
"

list_ao_query_new <-"
SELECT 		ao_id,
    ao_code,
		ao_name,
		wef,
		til,
  	ao_grp_code,
		ao_grp_name
FROM pruread.v_aiu_app_list_ao_grp 
ORDER BY ao_id
"


## list ao group ----
list_ao_grp_query <-"select ao_grp_code, 
ao_grp_name
 , flag_top_ao
from pruprod.v_aiu_app_dim_ao_grp
group by ao_grp_code, ao_grp_name
, flag_top_ao
order by ao_grp_code
"


## market segment ---- 
dim_ms_query <- "
select 
  SK_FLT_TYPE_RULE_ID as ms_id,
  rule_description as MARKET_SEGMENT
from  SWH_FCT.DIM_FLIGHT_TYPE_RULE
"
# AIRPORT ----
## dim airport ----
dim_ap_query <- "
  select
    id as apt_id,
    code as apt_icao_code,
    lat as latitude,
    lon as longitude,
    country_id,
    
    pru_name as apt_name
    
  from prudev.pru_airport
"

dim_ap_query_new <- "
select * from pruread.v_aiu_dim_airport

"

## list airport new ----
list_ap_query_new <- "
select a.*,
	latitude, longitude
from pruread.v_aiu_app_list_airport a
INNER JOIN pruread.v_aiu_dim_airport b ON a.ec_ap_code = b.ec_ap_code
GROUP BY 	a.EC_AP_CODE, a.BK_AP_ID, a.EC_AP_NAME, a.icao2letter, a.flag_top_apt, latitude, longitude
"


## list airport ----
list_ap_query <- "SELECT
arp_id as apt_id,
arp_code AS APT_ICAO_CODE,
arp_name AS apt_name,
flag_top_apt,
latitude,
longitude

FROM pruprod.v_aiu_app_dim_airport a
INNER JOIN (
  SELECT ec_ap_code, latitude, longitude
  FROM (
    SELECT ec_ap_code, latitude, longitude,
           ROW_NUMBER() OVER (PARTITION BY ec_ap_code ORDER BY sk_ap_id DESC) AS rn
    FROM swh_fct.dim_airport
  ) t
  WHERE rn = 1
) b ON a.arp_code = b.ec_ap_code
"



## list airport extended new ----
list_ap_ext_query_new <- "
select 
	a.ec_ap_code,
	a.bk_ap_id,
	a.EC_AP_NAME,
	icao2letter,
	flag_top_apt,
	latitude, longitude
from pruread.v_aiu_app_list_airport a
INNER JOIN pruread.v_aiu_dim_airport b ON a.ec_ap_code = b.ec_ap_code
GROUP BY 	a.EC_AP_CODE, a.BK_AP_ID, a.EC_AP_NAME, a.icao2letter, a.flag_top_apt, latitude, longitude

UNION ALL
SELECT
	ec_ap_code,
	bk_ap_id,
	ec_ap_name,
	SUBSTR(EC_AP_CODE, 1 ,2 ) AS icao2letter,
	'N' AS flag_top_apt,
	latitude,
	longitude

FROM pruread.v_aiu_dim_airport
 where ec_ap_code in (
  'BIKF','BIRK','EBCI','EBLG','EDDN','EDDP','EDDS','EDDV','EGAA','EGBB','EGGD','EGLC','EGNX','EGPD','EGPF','ENBR','ENVA','ENZV',
  'EPKK','ESGG','ESSB','GCRR','GCTS','GCXO','LBSF','LEAL','LEBB','LEIB','LEVC','LEZL','LFBD','LFBO','LFLL','LFML','LFPB','LFRS',
  'LFSB','LGIR','LGTS','LICC','LICJ','LIME','LIML','LIPE','LIPZ','LIRA','LIRN','LPFR','LPPR','LTAC','LTBA','LTBJ','UKBB'
	)
	AND EC_AP_CODE NOT IN (SELECT EC_AP_CODE FROM pruread.v_aiu_app_list_airport)
	AND VALID_TO >= trunc(sysdate)
	 GROUP BY 	EC_AP_CODE, BK_AP_ID, EC_AP_NAME, SUBSTR (EC_AP_CODE, 1, 2), latitude, longitude
"

## list airport extended ----
list_ap_ext_query <- "
SELECT
arp_id as apt_id,
arp_code AS APT_ICAO_CODE,
arp_name AS apt_name,
flag_top_apt,
latitude,
longitude

FROM pruprod.v_aiu_app_dim_airport a
INNER JOIN (
  SELECT ec_ap_code, latitude, longitude
  FROM (
    SELECT ec_ap_code, latitude, longitude,
           ROW_NUMBER() OVER (PARTITION BY ec_ap_code ORDER BY sk_ap_id DESC) AS rn
    FROM swh_fct.dim_airport
  ) t
  WHERE rn = 1
) b ON a.arp_code = b.ec_ap_code

UNION ALL
SELECT
a.id as apt_id,
a.code AS APT_ICAO_CODE,
a.DASHBOARD_NAME AS apt_name,
'N' flag_top_apt,
latitude,
longitude
 from prudev.pru_airport a 
INNER JOIN (
  SELECT ec_ap_code, latitude, longitude
  FROM (
    SELECT ec_ap_code, latitude, longitude,
           ROW_NUMBER() OVER (PARTITION BY ec_ap_code ORDER BY sk_ap_id DESC) AS rn
    FROM swh_fct.dim_airport
  ) t
  WHERE rn = 1
) b ON a.code = b.ec_ap_code
 where a.code in (
  'BIKF','BIRK','EBCI','EBLG','EDDN','EDDP','EDDS','EDDV','EGAA','EGBB','EGGD','EGLC','EGNX','EGPD','EGPF','ENBR','ENVA','ENZV',
  'EPKK','ESGG','ESSB','GCRR','GCTS','GCXO','LBSF','LEAL','LEBB','LEIB','LEVC','LEZL','LFBD','LFBO','LFLL','LFML','LFPB','LFRS',
  'LFSB','LGIR','LGTS','LICC','LICJ','LIME','LIML','LIPE','LIPZ','LIRA','LIRN','LPFR','LPPR','LTAC','LTBA','LTBJ','UKBB'
	)
	AND a.code NOT IN (SELECT arp_code FROM pruprod.v_aiu_app_dim_airport)

"

# COUNTRY----
##dim iso country ----
dim_iso_st_query <- "
  select 
    AIU_ISO_COUNTRY_CODE as iso_country_code,
    AIU_ISO_COUNTRY_NAME as country_name
  from prudev.pru_country_iso
  group by AIU_ISO_COUNTRY_CODE, AIU_ISO_COUNTRY_NAME
  UNION ALL
  select 
    'RSME' as iso_country_code,
    'Serbia/Montenegro' as country_name
  from dual
order by iso_country_code
"

##list iso country ----
list_iso_st_query <-"
    SELECT 
    AIU_ISO_COUNTRY_CODE as iso_country_code,
    AIU_ISO_COUNTRY_NAME as country_name
  from prudev.pru_country_iso
  WHERE AIU_ISO_COUNTRY_CODE IN (
  'AL', 'AM', 'AT', 'BE', 'BA', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'GE', 'DE', 'GR', 'HU', 'IS', 'IE', 'IL'
, 'IT', 'LV', 'LT', 'LU', 'MT', 'MD', 'ME', 'MA', 'NL', 'MK', 'NO', 'PL', 'PT', 'RO', 'RS', 'SK', 'SI',       'SE', 'CH'
, 'TR', 'UA', 'GB'
)
  group by AIU_ISO_COUNTRY_CODE, AIU_ISO_COUNTRY_NAME
  UNION ALL 
  SELECT 
  	'IC' AS iso_country_code,
  	'Spain Canaries' AS country_name
  FROM dual
    UNION ALL 
  SELECT 
  	'ES' AS iso_country_code,
  	'Spain Continental' AS country_name
  FROM dual
  ORDER BY ISO_COUNTRY_CODE 
"

##list icao country ----
list_icao_st_query <-"
 select distinct
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
"


# ANSP----
## dim ansp ----  
dim_ansp_query <- " 
SELECT 
ANSP_ID, ANSP_NAME 
FROM PRUDEV.V_PRU_REL_CFMU_AUA_ANSP
--WHERE ANSP_ID  in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,26,27,28,29,30,31,32,33,39,42,44,45,46,50,53,56,57)
group by  ANSP_ID, ANSP_NAME
"

