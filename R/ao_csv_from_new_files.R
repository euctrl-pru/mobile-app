library(arrow)
library(duckdb)
library(duckplyr)
library(lubridate)
library(here)
library(RODBC)

source(here::here("..", "mobile-app", "R", "helpers.R"))
source(here::here("..", "mobile-app", "R", "params.R"))

# parameters ----
if (!exists("data_day_date")) {current_day <- today() - days(1)} else {current_day <- data_day_date}

# DIMENSIONS ----
## ao group ----  
# same as v_covid_dim_ao but adding ao_id and removing old aos

query <- "
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

   select 
             a.ao_id
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

dim_ao_group <- export_query(query) 

query <-"
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

list_ao <- export_query(query) %>% 
  left_join(dim_ao_group, by = c("AO_ID", "AO_CODE", "AO_NAME")) %>% 
  select (AO_ID, AO_CODE, AO_NAME, AO_GRP_CODE, AO_GRP_NAME)

list_ao_group <- list_ao %>% group_by(AO_GRP_CODE, AO_GRP_NAME) %>% 
  summarise(AO_GRP_CODE =  max(AO_GRP_CODE), AO_GRP_NAME =  max(AO_GRP_NAME)) %>%
  arrange(AO_GRP_CODE) %>% ungroup()

## airport ----
query <- "
  select
    id as apt_id,
    code as apt_icao_code,
    lat as latitude,
    lon as longitude,
    country_id,
    
    pru_name as apt_name
    
  from prudev.pru_airport
"

dim_airport <- export_query(query) 


# airport app list table 
query <- "SELECT
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

list_airport <- export_query(query) 

## iso country ----
query <- "
  select 
    AIU_ISO_COUNTRY_CODE as iso_country_code,
    AIU_ISO_COUNTRY_NAME as country_name
  from prudev.pru_country_iso
  group by AIU_ISO_COUNTRY_CODE, AIU_ISO_COUNTRY_NAME
"

dim_iso_country <- export_query(query) 


## market segment ---- 
query <- "
select 
  SK_FLT_TYPE_RULE_ID as ms_id,
  rule_description as MARKET_SEGMENT
from  SWH_FCT.DIM_FLIGHT_TYPE_RULE
"

dim_marktet_segment <- export_query(query) 



# prep data functions ----
import_dataframe <- function(dfname) {
  # import data 
  # mydataframe <- "ao_ap_arr_delay"
  mydataframe <- dfname
  myparquetfile <- paste0(mydataframe, "_day_base.parquet")
  
  df_base <- read_parquet_duckdb(here(archive_dir_raw, 
                                      myparquetfile)
  ) 
  
  # filter to keep days and airports needed
  df_alldays <- df_base %>% 
      filter(AO_ID %in% list_ao$AO_ID)  

  # pre-process data
  if (dfname == "ao_st_des") {
    df_app <- df_alldays %>% 
      compute(prudence = "lavish") %>%
      left_join(list_ao, by = c("AO_ID", "AO_CODE")) %>%
      summarise(
        FLIGHT = sum(FLIGHT, na.rm = TRUE),
        .by = c(ENTRY_DATE, AO_GRP_CODE, AO_GRP_NAME, ARR_ISO_CTY_CODE)
      )  
    
    } else if (dfname == "ao_ap_dep"){
      
      df_app <- df_alldays %>% 
        compute(prudence = "lavish") %>%
        left_join(list_ao, by = c("AO_ID", "AO_CODE")) %>%
        summarise(
          FLIGHT = sum(FLIGHT, na.rm = TRUE),
          .by = c(ENTRY_DATE, AO_GRP_CODE, AO_GRP_NAME, DEP_ARP_PRU_ID)
        )
    } else if (dfname == "ao_ap_pair"){
      
      df_app <- df_alldays %>% 
        compute(prudence = "lavish") %>%
        left_join(list_ao, by = c("AO_ID", "AO_CODE")) %>%
        summarise(
          FLIGHT = sum(FLIGHT, na.rm = TRUE),
          .by = c(ENTRY_DATE, AO_GRP_CODE, AO_GRP_NAME, ARP_PRU_ID_1, ARP_PRU_ID_2)
        )
    } else if (dfname == "ao_ap_arr_delay"){
      
      df_app <- df_alldays %>% 
        compute(prudence = "lavish") %>%
        left_join(list_ao, by = c("AO_ID", "AO_CODE")) %>%
        summarise(
          FLIGHT = sum(FLTS, na.rm = TRUE),
          ARR_DELAYED_FLIGHT = sum(DELAYED_FLTS, na.rm = TRUE),
          ARR_ATFM_DELAY = sum(DELAY_AMNT, na.rm = TRUE),
          .by = c(ARR_DATE, AO_GRP_CODE, AO_GRP_NAME, ARR_ARP_PRU_ID)
        ) %>% 
        rename (ENTRY_DATE = ARR_DATE)
      
      } else if (dfname == "ao_traffic_delay"){
      
      df_app <- df_alldays %>% 
        compute(prudence = "lavish") %>%
        left_join(list_ao, by = c("AO_ID", "AO_CODE")) %>%
        summarise(
          DAY_TFC = sum(DAY_TFC, na.rm = TRUE),
          DAY_DLY = sum(DAY_DLY, na.rm = TRUE),
          DAY_DLY_15 = sum(DAY_DLY_15, na.rm = TRUE),
          DAY_DELAYED_TFC = sum(DAY_DELAYED_TFC, na.rm = TRUE),
          DAY_DELAYED_TFC_15 = sum(DAY_DELAYED_TFC_15, na.rm = TRUE),
          .by = c(YEAR, MONTH, WEEK,	WEEK_NB_YEAR,	DAY_TYPE,	DAY_OF_WEEK, FLIGHT_DATE, AO_GRP_CODE, AO_GRP_NAME)
        )
    } else {
      df_app <- df_alldays
  }

  return(df_app) 
}

# ao day ----
mydataframe <-  "ao_traffic_delay"
mydatafile <- paste0(mydataframe, "_day_raw.parquet")
stakeholder <- str_sub(mydataframe, 1, 2)

df_app <- import_dataframe(mydataframe)

mydate <- max(df_app$FLIGHT_DATE, na.rm = TRUE)
current_year = year(mydate)

#### create date sequence
year_from <- paste0(2018, "-12-24")
year_til <- paste0(current_year, "-12-31")
days_sequence <- seq(ymd(year_from), ymd(year_til), by = "1 day") %>%
  as_tibble() %>%
  select(FLIGHT_DATE = value) 

#### combine with ansp list to get full sequence
days_ao_grp <- crossing(days_sequence, list_ao_group) %>% 
  arrange(AO_GRP_CODE, FLIGHT_DATE)%>% 
  mutate(YEAR = year(FLIGHT_DATE))



df_day_year <- days_ao_grp %>%
  left_join(df_app, by = c("YEAR", "FLIGHT_DATE", "AO_GRP_CODE", "AO_GRP_NAME")) %>% 
  arrange(AO_GRP_CODE, FLIGHT_DATE) %>%
  # filter(AO_GRP_CODE == "NAX_GRP")
  group_by(AO_GRP_CODE) %>% 
  mutate(
    DAY_DLY_FLT = if_else(DAY_TFC == 0, NA, DAY_DLY/ DAY_TFC),
    DAY_DELAYED_TFC_PERC = if_else(DAY_TFC == 0, NA, DAY_DELAYED_TFC/ DAY_TFC),
    DAY_DELAYED_TFC_15_PERC = if_else(DAY_TFC == 0, NA, DAY_DELAYED_TFC_15/ DAY_TFC),
    
    #rolling week
    RWK_AVG_TFC = rollsum(DAY_TFC, 7, fill = NA, align = "right") / 7,
    RWK_AVG_DLY = rollsum(DAY_DLY, 7, fill = NA, align = "right") / 7,
    RWK_DLY_FLT = if_else(RWK_AVG_TFC == 0, NA, RWK_AVG_DLY/ RWK_AVG_TFC),
    RWK_AVG_DELAYED_TFC = rollsum(DAY_DELAYED_TFC, 7, fill = NA, align = "right") / 7,
    RWK_AVG_DELAYED_15_TFC = rollsum(DAY_DELAYED_TFC_15, 7, fill = NA, align = "right") / 7,
    
    RWK_DELAYED_TFC_PERC = if_else(RWK_AVG_TFC == 0, NA, RWK_AVG_DELAYED_TFC/ RWK_AVG_TFC),
    RWK_DELAYED_TFC_15_PERC = if_else(RWK_AVG_TFC == 0, NA, RWK_AVG_DELAYED_15_TFC/ RWK_AVG_TFC),
    
  )  %>% 
  group_by(AO_GRP_CODE, YEAR) %>% 
  # arrange(AO_GRP_CODE, FLIGHT_DATE) %>% 
  mutate(
    # year to date
    Y2D_TFC_YEAR = cumsum(coalesce(DAY_TFC, 0)),
    Y2D_AVG_TFC_YEAR = cumsum(coalesce(DAY_TFC, 0)) / row_number(),
    Y2D_DLY_YEAR = cumsum(coalesce(DAY_DLY, 0)),
    Y2D_AVG_DLY_YEAR = cumsum(coalesce(DAY_DLY, 0)) / row_number(),
    Y2D_DELAYED_TFC = cumsum(coalesce(DAY_DELAYED_TFC, 0)),
    Y2D_DELAYED_TFC_15 = cumsum(coalesce(DAY_DELAYED_TFC_15, 0)),
    
    Y2D_DLY_FLT_YEAR = if_else(Y2D_TFC_YEAR == 0, NA, Y2D_DLY_YEAR/ Y2D_TFC_YEAR),
    Y2D_DELAYED_TFC_PERC = if_else(Y2D_TFC_YEAR == 0, NA, Y2D_DELAYED_TFC/ Y2D_TFC_YEAR),
    Y2D_DELAYED_TFC_15_PERC = if_else(Y2D_TFC_YEAR == 0, NA, Y2D_DELAYED_TFC_15/ Y2D_TFC_YEAR)
  ) %>% 
  ungroup ()

## split table ----
df_day <- df_day_year %>% 
  mutate(
    FLIGHT_DATE_2019 = FLIGHT_DATE - days((YEAR-2019)*364+ floor((YEAR - 2019) / 4) * 7),
    FLIGHT_DATE_2020 = FLIGHT_DATE - days((YEAR-2020)*364+ floor((YEAR - 2020) / 4) * 7),
    FLIGHT_DATE_2019_SD = FLIGHT_DATE %m-% years(YEAR-2019),
    FLIGHT_DATE_PREV_YEAR_SD = FLIGHT_DATE %m-% years(1) 
    ) %>% 
  left_join(select(df_day_year, AO_GRP_CODE, YEAR, FLIGHT_DATE, starts_with(c("Y2D"))), by = c("AO_GRP_CODE", "FLIGHT_DATE_PREV_YEAR_SD" = "FLIGHT_DATE"), suffix = c("","_PREV_YEAR")) %>% 
  left_join(select(df_day_year, AO_GRP_CODE, YEAR, FLIGHT_DATE, starts_with(c("Y2D"))), by = c("AO_GRP_CODE", "FLIGHT_DATE_2019_SD" = "FLIGHT_DATE"), suffix = c("","_2019")) %>% 
  left_join(select(df_day_year, AO_GRP_CODE, YEAR, FLIGHT_DATE, starts_with(c("DAY", "RWK"))), by = c("AO_GRP_CODE", "FLIGHT_DATE_2019" = "FLIGHT_DATE"), suffix = c("","_2019")) %>% 
  left_join(select(df_day_year, AO_GRP_CODE, YEAR, FLIGHT_DATE, RWK_AVG_TFC), by = c("AO_GRP_CODE", "FLIGHT_DATE_2020" = "FLIGHT_DATE"), suffix = c("","_2020")) %>% 
  arrange(AO_GRP_CODE, FLIGHT_DATE) %>%
  # filter(FLIGHT_DATE == ymd(20250228))  %>%
  # filter(AO_GRP_CODE == 'AEA') %>%
  # select(AO_GRP_CODE, FLIGHT_DATE,FLIGHT_DATE_PREV_YEAR_SD, FLIGHT_DATE_2019_SD, Y2D_TFC_YEAR,	Y2D_TFC_YEAR_PREV_YEAR,	Y2D_TFC_YEAR_2019)
  group_by(AO_GRP_CODE) %>% 
  mutate(
    # prev week
    FLIGHT_DATE_PREV_WEEK = lag(FLIGHT_DATE, 7),
    DAY_TFC_PREV_WEEK = lag(DAY_TFC , 7),
    RWK_AVG_TFC_PREV_WEEK = lag(RWK_AVG_TFC, 7),
    
    DAY_DLY_PREV_WEEK = lag(DAY_DLY , 7),
    RWK_AVG_DLY_PREV_WEEK = lag(RWK_AVG_DLY, 7),
    
    DAY_DLY_FLT_PREV_WEEK = lag(DAY_DLY_FLT, 7),
    RWK_DLY_FLT_PREV_WEEK = lag(RWK_DLY_FLT, 7),
    
    DAY_DELAYED_TFC_PERC_PREV_WEEK = lag(DAY_DELAYED_TFC_PERC, 7),
    RWK_DELAYED_TFC_PERC_PREV_WEEK = lag(RWK_DELAYED_TFC_PERC, 7),
    
    DAY_DELAYED_TFC_15_PERC_PREV_WEEK = lag(DAY_DELAYED_TFC_15_PERC, 7),
    RWK_DELAYED_TFC_15_PERC_PREV_WEEK = lag(RWK_DELAYED_TFC_15_PERC, 7),
    
    # dif prev week
    DAY_TFC_DIF_PREV_WEEK = coalesce(DAY_TFC,0) - coalesce(DAY_TFC_PREV_WEEK, 0),
    DAY_TFC_DIF_PREV_WEEK_PERC = if_else(DAY_TFC_PREV_WEEK == 0, NA, DAY_TFC/ DAY_TFC_PREV_WEEK) -1,
    
    RWK_TFC_DIF_PREV_WEEK_PERC = if_else(RWK_AVG_TFC_PREV_WEEK == 0, NA, RWK_AVG_TFC/ RWK_AVG_TFC_PREV_WEEK)-1,
    
    DAY_DLY_DIF_PREV_WEEK_PERC = if_else(DAY_DLY_PREV_WEEK == 0, NA, DAY_DLY/ DAY_DLY_PREV_WEEK)-1,
    RWK_DLY_DIF_PREV_WEEK_PERC = if_else(RWK_AVG_DLY_PREV_WEEK == 0, NA, RWK_AVG_DLY/ RWK_AVG_DLY_PREV_WEEK)-1,
    
    DAY_DLY_FLT_DIF_PREV_WEEK_PERC = if_else(DAY_DLY_FLT_PREV_WEEK == 0, NA, DAY_DLY_FLT/ DAY_DLY_FLT_PREV_WEEK)-1,
    
    DAY_DELAYED_TFC_PERC_DIF_PREV_WEEK = DAY_DELAYED_TFC_PERC - DAY_DELAYED_TFC_PERC_PREV_WEEK,
    RWK_DELAYED_TFC_PERC_DIF_PREV_WEEK = RWK_DELAYED_TFC_PERC - RWK_DELAYED_TFC_PERC_PREV_WEEK, 
    
    DAY_DELAYED_TFC_15_PERC_DIF_PREV_WEEK = DAY_DELAYED_TFC_15_PERC - DAY_DELAYED_TFC_15_PERC_PREV_WEEK,
    RWK_DELAYED_TFC_15_PERC_DIF_PREV_WEEK = RWK_DELAYED_TFC_15_PERC - RWK_DELAYED_TFC_15_PERC_PREV_WEEK, 

    # prev year
    FLIGHT_DATE_PREV_YEAR = lag(FLIGHT_DATE, 364),
    DAY_TFC_PREV_YEAR = lag(DAY_TFC , 364),
    RWK_AVG_TFC_PREV_YEAR = lag(RWK_AVG_TFC, 364),
    Y2D_TFC_PREV_YEAR = Y2D_TFC_YEAR_PREV_YEAR,
    Y2D_AVG_TFC_PREV_YEAR = Y2D_AVG_TFC_YEAR_PREV_YEAR,
    
    DAY_DLY_PREV_YEAR = lag(DAY_DLY , 364),
    RWK_AVG_DLY_PREV_YEAR = lag(RWK_AVG_DLY, 364),
    Y2D_AVG_DLY_PREV_YEAR = Y2D_AVG_DLY_YEAR_PREV_YEAR,
    
    DAY_DLY_FLT_PREV_YEAR = lag(DAY_DLY_FLT, 364),
    RWK_DLY_FLT_PREV_YEAR = lag(RWK_DLY_FLT, 364),
    Y2D_DLY_FLT_PREV_YEAR = Y2D_DLY_FLT_YEAR_PREV_YEAR,
    
    DAY_DELAYED_TFC_PERC_PREV_YEAR = lag(DAY_DELAYED_TFC_PERC, 364),
    RWK_DELAYED_TFC_PERC_PREV_YEAR = lag(RWK_DELAYED_TFC_PERC, 364),
    Y2D_DELAYED_TFC_PERC_PREV_YEAR = Y2D_DELAYED_TFC_PERC_PREV_YEAR,
    
    DAY_DELAYED_TFC_15_PERC_PREV_YEAR = lag(DAY_DELAYED_TFC_15_PERC, 364),
    RWK_DELAYED_TFC_15_PERC_PREV_YEAR = lag(RWK_DELAYED_TFC_15_PERC, 364),
    Y2D_DELAYED_TFC_15_PERC_PREV_YEAR = Y2D_DELAYED_TFC_15_PERC_PREV_YEAR,
    
    # dif prev year
    DAY_TFC_DIF_PREV_YEAR = coalesce(DAY_TFC, 0) - coalesce(DAY_TFC_PREV_YEAR, 0),
    DAY_TFC_DIF_PREV_YEAR_PERC = if_else(DAY_TFC_PREV_YEAR == 0, NA, DAY_TFC/ DAY_TFC_PREV_YEAR)-1,
    RWK_TFC_DIF_PREV_YEAR_PERC = if_else(RWK_AVG_TFC_PREV_YEAR == 0, NA, RWK_AVG_TFC/ RWK_AVG_TFC_PREV_YEAR)-1,
    Y2D_TFC_DIF_PREV_YEAR_PERC = if_else(Y2D_AVG_TFC_PREV_YEAR == 0, NA, Y2D_AVG_TFC_YEAR/ Y2D_AVG_TFC_PREV_YEAR)-1,
    
    DAY_DLY_DIF_PREV_YEAR_PERC = if_else(DAY_DLY_PREV_YEAR == 0, NA, DAY_DLY/ DAY_DLY_PREV_YEAR)-1,
    RWK_DLY_DIF_PREV_YEAR_PERC = if_else(RWK_AVG_DLY_PREV_YEAR == 0, NA, RWK_AVG_DLY/ RWK_AVG_DLY_PREV_YEAR)-1,
    Y2D_DLY_DIF_PREV_YEAR_PERC = if_else(Y2D_AVG_DLY_PREV_YEAR == 0, NA, Y2D_AVG_DLY_YEAR/ Y2D_AVG_DLY_PREV_YEAR)-1,
    
    DAY_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(DAY_DLY_FLT_PREV_YEAR == 0, NA, DAY_DLY_FLT/ DAY_DLY_FLT_PREV_YEAR)-1,
    RWK_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(RWK_DLY_FLT_PREV_YEAR == 0, NA, RWK_DLY_FLT/ RWK_DLY_FLT_PREV_YEAR)-1,
    Y2D_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(Y2D_DLY_FLT_PREV_YEAR == 0, NA, Y2D_DLY_FLT_YEAR/ Y2D_DLY_FLT_PREV_YEAR)-1,
    
    DAY_DELAYED_TFC_PERC_DIF_PREV_YEAR = DAY_DELAYED_TFC_PERC - DAY_DELAYED_TFC_PERC_PREV_YEAR,
    RWK_DELAYED_TFC_PERC_DIF_PREV_YEAR = RWK_DELAYED_TFC_PERC - RWK_DELAYED_TFC_PERC_PREV_YEAR,
    Y2D_DELAYED_TFC_PERC_DIF_PREV_YEAR = Y2D_DELAYED_TFC_PERC - Y2D_DELAYED_TFC_PERC_PREV_YEAR, 
    
    DAY_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = DAY_DELAYED_TFC_15_PERC - DAY_DELAYED_TFC_15_PERC_PREV_YEAR,
    RWK_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = RWK_DELAYED_TFC_15_PERC - RWK_DELAYED_TFC_15_PERC_PREV_YEAR, 
    Y2D_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = Y2D_DELAYED_TFC_15_PERC - Y2D_DELAYED_TFC_15_PERC_PREV_YEAR, 
    
    # 2020
    FLIGHT_DATE_2020 = FLIGHT_DATE_2020,
    RWK_AVG_TFC_2020 = RWK_AVG_TFC_2020,
    
    # 2019
    FLIGHT_DATE_2019 = FLIGHT_DATE_2019,

    DAY_TFC_2019 = DAY_TFC_2019,
    RWK_AVG_TFC_2019 = RWK_AVG_TFC_2019,
    Y2D_TFC_2019 = Y2D_TFC_YEAR_2019,
    Y2D_AVG_TFC_2019 = Y2D_AVG_TFC_YEAR_2019,
    
    DAY_DLY_2019 = DAY_DLY_2019,
    RWK_AVG_DLY_2019 = RWK_AVG_DLY_2019,
    Y2D_AVG_DLY_2019 = Y2D_AVG_DLY_YEAR_2019,
    
    DAY_DLY_FLT_2019 = DAY_DLY_FLT_2019,
    RWK_DLY_FLT_2019 = RWK_DLY_FLT_2019,
    Y2D_DLY_FLT_2019 = Y2D_DLY_FLT_YEAR_2019,
    
    DAY_DELAYED_TFC_PERC_2019 = DAY_DELAYED_TFC_PERC_2019,
    RWK_DELAYED_TFC_PERC_2019 = RWK_DELAYED_TFC_PERC_2019,
    Y2D_DELAYED_TFC_PERC_2019 = Y2D_DELAYED_TFC_PERC_2019,
    
    DAY_DELAYED_TFC_15_PERC_2019 = DAY_DELAYED_TFC_15_PERC_2019,
    RWK_DELAYED_TFC_15_PERC_2019 = RWK_DELAYED_TFC_15_PERC_2019,
    Y2D_DELAYED_TFC_15_PERC_2019 = Y2D_DELAYED_TFC_15_PERC_2019,

    # dif 2019
    DAY_TFC_DIF_2019 = coalesce(DAY_TFC, 0) - coalesce(DAY_TFC_2019, 0),
    DAY_TFC_DIF_2019_PERC = if_else(DAY_TFC_2019 == 0, NA, DAY_TFC/ DAY_TFC_2019)-1,
    RWK_TFC_DIF_2019_PERC = if_else(RWK_AVG_TFC_2019 == 0, NA, RWK_AVG_TFC/ RWK_AVG_TFC_2019)-1,
    Y2D_TFC_DIF_2019_PERC = if_else(Y2D_AVG_TFC_2019 == 0, NA, Y2D_AVG_TFC_YEAR/ Y2D_AVG_TFC_2019)-1,
    
    DAY_DLY_DIF_2019_PERC = if_else(DAY_DLY_2019 == 0, NA, DAY_DLY/ DAY_DLY_2019)-1,
    RWK_DLY_DIF_2019_PERC = if_else(RWK_AVG_DLY_2019 == 0, NA, RWK_AVG_DLY/ RWK_AVG_DLY_2019)-1,
    Y2D_DLY_DIF_2019_PERC = if_else(Y2D_AVG_DLY_2019 == 0, NA, Y2D_AVG_DLY_YEAR/ Y2D_AVG_DLY_2019)-1,
    
    DAY_DLY_FLT_DIF_2019_PERC = if_else(DAY_DLY_FLT_2019 == 0, NA, DAY_DLY_FLT/ DAY_DLY_FLT_2019)-1,
    RWK_DLY_FLT_DIF_2019_PERC = if_else(RWK_DLY_FLT_2019 == 0, NA, RWK_DLY_FLT/ RWK_DLY_FLT_2019)-1,
    Y2D_DLY_FLT_DIF_2019_PERC = if_else(Y2D_DLY_FLT_2019 == 0, NA, Y2D_DLY_FLT_YEAR/ Y2D_DLY_FLT_2019)-1,
    
    DAY_DELAYED_TFC_PERC_DIF_2019 = coalesce(DAY_DELAYED_TFC_PERC, 0) - coalesce(DAY_DELAYED_TFC_PERC_2019, 0),
    RWK_DELAYED_TFC_PERC_DIF_2019 = coalesce(RWK_DELAYED_TFC_PERC, 0) - coalesce(RWK_DELAYED_TFC_PERC_2019, 0),
    Y2D_DELAYED_TFC_PERC_DIF_2019 = coalesce(Y2D_DELAYED_TFC_PERC, 0) - coalesce(Y2D_DELAYED_TFC_PERC_2019, 0), 
    
    DAY_DELAYED_TFC_15_PERC_DIF_2019 = coalesce(DAY_DELAYED_TFC_15_PERC, 0) - coalesce(DAY_DELAYED_TFC_15_PERC_2019, 0),
    RWK_DELAYED_TFC_15_PERC_DIF_2019 = coalesce(RWK_DELAYED_TFC_15_PERC, 0) - coalesce(RWK_DELAYED_TFC_15_PERC_2019, 0), 
    Y2D_DELAYED_TFC_15_PERC_DIF_2019 = coalesce(Y2D_DELAYED_TFC_15_PERC, 0) - coalesce(Y2D_DELAYED_TFC_15_PERC_2019, 0),
    
    LAST_DATA_DAY = mydate
    ) %>% 
  select(
    AO_GRP_CODE,
    AO_GRP_NAME,
    YEAR,
    MONTH,
    WEEK,
    WEEK_NB_YEAR,
    DAY_TYPE,
    DAY_OF_WEEK,
    
    FLIGHT_DATE,
    FLIGHT_DATE_PREV_WEEK,
    FLIGHT_DATE_PREV_YEAR,
    FLIGHT_DATE_2020,
    FLIGHT_DATE_2019,
    
    DAY_TFC,
    DAY_TFC_PREV_WEEK,
    DAY_TFC_PREV_YEAR,
    DAY_TFC_2019,
    DAY_TFC_DIF_PREV_WEEK,
    DAY_TFC_DIF_PREV_YEAR,
    DAY_TFC_DIF_2019,
    DAY_TFC_DIF_PREV_WEEK_PERC,
    DAY_TFC_DIF_PREV_YEAR_PERC,
    DAY_TFC_DIF_2019_PERC,
    
    RWK_AVG_TFC,
    RWK_AVG_TFC_PREV_WEEK,
    RWK_AVG_TFC_PREV_YEAR,
    RWK_AVG_TFC_2020,
    RWK_AVG_TFC_2019,
    RWK_TFC_DIF_PREV_YEAR_PERC,
    RWK_TFC_DIF_2019_PERC,
    
    Y2D_TFC_YEAR,
    Y2D_TFC_PREV_YEAR,
    Y2D_TFC_2019,
    Y2D_AVG_TFC_YEAR,
    Y2D_AVG_TFC_PREV_YEAR,
    Y2D_AVG_TFC_2019,
    Y2D_TFC_DIF_PREV_YEAR_PERC,
    Y2D_TFC_DIF_2019_PERC,
    
    DAY_DLY,
    DAY_DLY_PREV_WEEK,
    DAY_DLY_PREV_YEAR,
    DAY_DLY_2019,
    DAY_DLY_DIF_PREV_WEEK_PERC,
    DAY_DLY_DIF_PREV_YEAR_PERC,
    DAY_DLY_DIF_2019_PERC,
    
    RWK_AVG_DLY,
    RWK_AVG_DLY_PREV_WEEK,
    RWK_AVG_DLY_PREV_YEAR,
    RWK_AVG_DLY_2019,
    RWK_DLY_DIF_PREV_YEAR_PERC,
    RWK_DLY_DIF_2019_PERC,
    
    Y2D_AVG_DLY_YEAR,
    Y2D_AVG_DLY_PREV_YEAR,
    Y2D_AVG_DLY_2019,
    Y2D_DLY_DIF_PREV_YEAR_PERC,
    Y2D_DLY_DIF_2019_PERC,
    
    DAY_DLY_FLT,
    DAY_DLY_FLT_PREV_WEEK,
    DAY_DLY_FLT_PREV_YEAR,
    DAY_DLY_FLT_2019,
    DAY_DLY_FLT_DIF_PREV_WEEK_PERC,
    DAY_DLY_FLT_DIF_PREV_YEAR_PERC,
    DAY_DLY_FLT_DIF_2019_PERC,
    
    RWK_DLY_FLT,
    RWK_DLY_FLT_PREV_WEEK,
    RWK_DLY_FLT_PREV_YEAR,
    RWK_DLY_FLT_2019,
    RWK_DLY_FLT_DIF_PREV_YEAR_PERC,
    RWK_DLY_FLT_DIF_2019_PERC,
    
    Y2D_DLY_FLT_YEAR,
    Y2D_DLY_FLT_PREV_YEAR,
    Y2D_DLY_FLT_2019,
    Y2D_DLY_FLT_DIF_PREV_YEAR_PERC,
    Y2D_DLY_FLT_DIF_2019_PERC,
    
    DAY_DELAYED_TFC_PERC,
    DAY_DELAYED_TFC_PERC_PREV_WEEK,
    DAY_DELAYED_TFC_PERC_PREV_YEAR,
    DAY_DELAYED_TFC_PERC_2019,
    DAY_DELAYED_TFC_PERC_DIF_PREV_WEEK,
    DAY_DELAYED_TFC_PERC_DIF_PREV_YEAR,
    DAY_DELAYED_TFC_PERC_DIF_2019,
    
    RWK_DELAYED_TFC_PERC,
    RWK_DELAYED_TFC_PERC_PREV_WEEK,
    RWK_DELAYED_TFC_PERC_PREV_YEAR,
    RWK_DELAYED_TFC_PERC_2019,
    RWK_DELAYED_TFC_PERC_DIF_PREV_WEEK,
    RWK_DELAYED_TFC_PERC_DIF_PREV_YEAR,
    RWK_DELAYED_TFC_PERC_DIF_2019,
    
    Y2D_DELAYED_TFC_PERC,
    Y2D_DELAYED_TFC_PERC_PREV_YEAR,
    Y2D_DELAYED_TFC_PERC_2019,
    Y2D_DELAYED_TFC_PERC_DIF_PREV_YEAR,
    Y2D_DELAYED_TFC_PERC_DIF_2019,
    
    DAY_DELAYED_TFC_15_PERC,
    DAY_DELAYED_TFC_15_PERC_PREV_WEEK,
    DAY_DELAYED_TFC_15_PERC_PREV_YEAR,
    DAY_DELAYED_TFC_15_PERC_2019,
    DAY_DELAYED_TFC_15_PERC_DIF_PREV_WEEK,
    DAY_DELAYED_TFC_15_PERC_DIF_PREV_YEAR,
    DAY_DELAYED_TFC_15_PERC_DIF_2019,
    
    RWK_DELAYED_TFC_15_PERC,
    RWK_DELAYED_TFC_15_PERC_PREV_WEEK,
    RWK_DELAYED_TFC_15_PERC_PREV_YEAR,
    RWK_DELAYED_TFC_15_PERC_2019,
    RWK_DELAYED_TFC_15_PERC_DIF_PREV_WEEK,
    RWK_DELAYED_TFC_15_PERC_DIF_PREV_YEAR,
    RWK_DELAYED_TFC_15_PERC_DIF_2019,
    
    Y2D_DELAYED_TFC_15_PERC,
    Y2D_DELAYED_TFC_15_PERC_PREV_YEAR,
    Y2D_DELAYED_TFC_15_PERC_2019,
    Y2D_DELAYED_TFC_15_PERC_DIF_PREV_YEAR,
    Y2D_DELAYED_TFC_15_PERC_DIF_2019,
    
    LAST_DATA_DAY
    
  ) %>% 
  ungroup() %>% 
  arrange(AO_GRP_CODE, FLIGHT_DATE)

df_day %>% write_parquet(here(archive_dir_raw, stakeholder, mydatafile))

print(paste(mydataframe, mydate))

# test <- df_day %>% filter(YEAR == 2024) %>% 
#   filter(FLIGHT_DATE >= ymd(20240106)) # %>%
#   # filter(AO_GRP_CODE == 'AEA') %>%
#   # select(AO_GRP_CODE, FLIGHT_DATE,FLIGHT_DATE_PREV_YEAR, FLIGHT_DATE_2019, Y2D_TFC_YEAR,	Y2D_TFC_PREV_YEAR,	Y2D_TFC_2019)
# 
# 
# test %>% write_csv(here(archive_dir_raw,"test.csv"))

# # dcheck
# dfc <- read_xlsx(
#   path  = fs::path_abs(
#     str_glue(ao_base_file),
#     start = ao_base_dir),
#   sheet = "ao_traffic_delay",
#   range = cell_limits(c(1, 1), c(NA, NA))) |>
#   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
# 
# dfc <- read_csv(here(archive_dir_raw, stakeholder, "20241231_ao_traffic_delay_raw.csv"), show_col_types = FALSE)
# 
# df <- dfc %>% arrange(AO_GRP_CODE, FLIGHT_DATE) %>%
#   filter(YEAR == 2024) %>% 
#   filter(FLIGHT_DATE >= ymd(20240106))
#   # filter(AO_GRP_CODE == 'AEA') %>%
#   # select(AO_GRP_CODE, FLIGHT_DATE,FLIGHT_DATE_PREV_YEAR, FLIGHT_DATE_2019, Y2D_TFC_YEAR,	Y2D_TFC_PREV_YEAR,	Y2D_TFC_2019)


# for (i in 1:nrow(list_ao_group)) {
#   df1 <- df %>% filter(AO_GRP_CODE == list_ao_group$AO_GRP_CODE[i])
# 
#   df_day1 <- test %>% filter(AO_GRP_CODE == list_ao_group$AO_GRP_CODE[i])
# 
#   # print(paste(list_ao_group$AO_GRP_CODE[i], all.equal(df1, df_day1)))
#   # nrow(df1)
#   for (j in 1:nrow(df1)) {
#     for (t in 14:112){
#       if (((coalesce(df1[[j,t]],0) - coalesce(df_day1[[j,t]],0))<10^-8) == FALSE) {
#         print(paste(t,df1[[j,1]], df1[[j,9]], (df1[[j,t]] - df_day1[[j,t]])<10^-8))
#         break}
#       # print(paste(t,df1[[j,1]], df1[[j,9]], (df1[[j,t]] - df_day1[[j,t]])<10^-8))
#     }
#   }
#   print(paste(t,df1[[j,1]], df1[[j,9]], (df1[[j,t]] - df_day1[[j,t]])<10^-8))
#   
# }


# ao st des ----
ao_st_des <- function(mydate =  current_day) {
  mydataframe <-  "ao_st_des"
  
  df_app <- import_dataframe(mydataframe)
  
  # mydate <- today()- days(1)
  data_day_text <- mydate %>% format("%Y%m%d")
  day_prev_week <- mydate + days(-7)
  day_prev_year <- mydate + days(-364)
  day_2019 <- mydate - days(364 * (year(mydate) - 2019) + floor((year(mydate) - 2019) / 4) * 7)
  current_year = year(mydate)
  
  stakeholder <- str_sub(mydataframe, 1, 2)
  
  ## day ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_day_raw.csv")

  df_day <- df_app %>%
    filter(ENTRY_DATE %in% c(mydate, day_prev_week, day_2019, day_prev_year)) %>% 
    left_join(dim_iso_country, by = c("ARR_ISO_CTY_CODE" = "ISO_COUNTRY_CODE")) %>% 
    group_by(AO_GRP_CODE, ENTRY_DATE) %>%
    arrange(AO_GRP_CODE, ENTRY_DATE, desc(FLIGHT), COUNTRY_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        ENTRY_DATE == mydate ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        ENTRY_DATE == mydate ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        ENTRY_DATE == day_prev_week ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      FLAG_PERIOD = case_when( 
        ENTRY_DATE == mydate ~ "CURRENT_DAY",
        ENTRY_DATE == day_prev_week ~ "DAY_PREV_WEEK",
        ENTRY_DATE == day_2019 ~ "DAY_2019",
        ENTRY_DATE == day_prev_year ~ "DAY_PREV_YEAR",
      )
    ) %>% 
    ungroup() %>% 
    group_by(AO_GRP_CODE, COUNTRY_NAME) %>% 
    arrange(AO_GRP_CODE, COUNTRY_NAME, desc(ENTRY_DATE)) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(TO_DATE = max(ENTRY_DATE)) %>% 
    select(
      AO_GRP_NAME,
      AO_GRP_CODE,
      FLAG_DAY = FLAG_PERIOD,
      ISO_CT_NAME_ARR = COUNTRY_NAME,
      ISO_CT_CODE_ARR = ARR_ISO_CTY_CODE,
      FLIGHT,
      R_RANK,
      RANK,
      RANK_PREV_WEEK = RANK_PREV,
      TO_DATE,
    ) %>% 
    arrange(AO_GRP_CODE, FLAG_DAY, R_RANK, ISO_CT_NAME_ARR)
  
  df_day %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ao_base_file),
  #     start = ao_base_dir),
  #   sheet = "ao_state_des_day",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(AO_GRP_CODE, FLAG_DAY, R_RANK, ISO_CT_NAME_ARR)
  # 
  # 
  # for (i in 1:nrow(list_ao_group)) {
  #   # i <- 1
  #   df1 <- df %>% filter(AO_GRP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   df_day1 <- df_day %>% filter(AO_GRP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   print(paste(list_ao_group$AO_GRP_CODE[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   # 
  #   #   print(paste(j, df1[[j,1]] == df_day1[[j,1]]))
  #   # 
  #   # }
  # }

## week ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_week_raw.csv")

  df_week <- df_app %>%
    filter(
      ENTRY_DATE %in% c(seq.Date(mydate-6, mydate)) |
        ENTRY_DATE %in% c(seq.Date(day_prev_week-6, day_prev_week))|
        ENTRY_DATE %in% c(seq.Date(day_2019-6, day_2019)) |
        ENTRY_DATE %in% c(seq.Date(day_prev_year-6, day_prev_year))
    ) %>% 
    compute(prudence = "lavish") %>%
    mutate(
      FLAG_PERIOD = case_when( 
        (ENTRY_DATE >= mydate - days(6) & ENTRY_DATE <= mydate) ~ "CURRENT_ROLLING_WEEK",
        (ENTRY_DATE >= day_prev_week - days(6) & ENTRY_DATE <= day_prev_week) ~ "PREV_ROLLING_WEEK",
        (ENTRY_DATE >= day_2019 - days(6) & ENTRY_DATE <= day_2019) ~ "ROLLING_WEEK_2019",
        (ENTRY_DATE >= day_prev_year - days(6) & ENTRY_DATE <= day_prev_year) ~ "ROLLING_WEEK_PREV_YEAR"
      )
    ) %>% 
    summarise(
      FLIGHT = sum(FLIGHT, na.rm = TRUE),
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE),
      .by = c(FLAG_PERIOD, AO_GRP_CODE, AO_GRP_NAME, ARR_ISO_CTY_CODE)
    ) %>% 
    ungroup() %>% 
    left_join(dim_iso_country, by = c("ARR_ISO_CTY_CODE" = "ISO_COUNTRY_CODE")) %>% 
    group_by(AO_GRP_CODE, FLAG_PERIOD) %>%
    arrange(AO_GRP_CODE, FLAG_PERIOD, desc(FLIGHT), COUNTRY_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        FLAG_PERIOD == "CURRENT_ROLLING_WEEK" ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        FLAG_PERIOD == "CURRENT_ROLLING_WEEK" ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        FLAG_PERIOD == "PREV_ROLLING_WEEK" ~ min_rank(desc(FLIGHT)),
        .default = NA
      )
    ) %>% 
    ungroup() %>% 
    group_by(AO_GRP_NAME, COUNTRY_NAME) %>% 
    arrange(AO_GRP_NAME, COUNTRY_NAME, FLAG_PERIOD) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(
      TO_DATE = max(TO_DATE, na.rm = TRUE),
      FROM_DATE = TO_DATE - days(6),
      ) %>% 
    select(
      AO_GRP_NAME,
      AO_GRP_CODE,
      FLAG_ROLLING_WEEK = FLAG_PERIOD,
      ISO_CT_NAME_ARR = COUNTRY_NAME,
      ISO_CT_CODE_ARR = ARR_ISO_CTY_CODE,
      FLIGHT,
      R_RANK,
      RANK,
      RANK_PREV_WEEK = RANK_PREV,
      FROM_DATE,
      TO_DATE
    ) %>% 
    arrange(AO_GRP_CODE, FLAG_ROLLING_WEEK, R_RANK, ISO_CT_NAME_ARR)
  
  df_week %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ao_base_file),
  #     start = ao_base_dir),
  #   sheet = "ao_state_des_week",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(AO_GRP_CODE, FLAG_ROLLING_WEEK, R_RANK, ISO_CT_NAME_ARR)
  # 
  # for (i in 1:nrow(list_ao_group)) {
  #   df1 <- df %>% filter(AO_GRP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   df_day1 <- df_week %>% filter(AO_GRP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   print(paste(list_ao_group$AO_GRP_CODE[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   #
  #   #   print(paste(j, df1[[j,4]] == df_day1[[j,4]]))
  #   #
  #   # }
  # }

  
  ## year ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_y2d_raw.csv")

  y2d_dates <- seq.Date(ymd(paste0(2019,"01","01")),
                        mydate) %>% as_tibble() %>%
    filter(year(value) %in% c(2019, current_year-1, current_year)) %>%
    filter(format(value, "%m-%d") <= format(mydate, "%m-%d")) %>%
    pull()

  df_y2d <- df_app %>%
    compute(prudence = "lavish") %>%
    mutate(
      YEAR = year(ENTRY_DATE)
    ) %>%
    filter(YEAR %in% c(current_year, current_year-1, 2019)) %>%
    filter(ENTRY_DATE %in% y2d_dates) %>%
    # filter(DEP_ARP_PRU_ID == 4467) %>%
    summarise(
      FLIGHT = sum(FLIGHT, na.rm = TRUE),
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE),
      .by = c(YEAR, AO_GRP_CODE, AO_GRP_NAME, ARR_ISO_CTY_CODE)
    ) %>%
    ungroup() %>%
    reframe(
      AO_GRP_CODE, AO_GRP_NAME, ARR_ISO_CTY_CODE, FLIGHT,
      TO_DATE = max(TO_DATE, na.rm = TRUE),
      FROM_DATE = min(FROM_DATE, na.rm = TRUE),
      .by = c(YEAR)
    ) %>% 
    left_join(dim_iso_country, by = c("ARR_ISO_CTY_CODE" = "ISO_COUNTRY_CODE")) %>%
    group_by(AO_GRP_CODE, YEAR) %>%
    arrange(AO_GRP_CODE, YEAR, desc(FLIGHT), COUNTRY_NAME) %>%
    mutate(
      R_RANK = case_when(
        YEAR == current_year ~ row_number(),
        .default = NA
      ),
      RANK = case_when(
        YEAR == current_year ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      RANK_PREV = case_when(
        YEAR == current_year-1 ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      NO_DAYS = as.numeric(TO_DATE - FROM_DATE) + 1,
      AVG_FLIGHT = FLIGHT / NO_DAYS
    ) %>%
    ungroup() %>%
    group_by(AO_GRP_NAME, COUNTRY_NAME) %>%
    arrange(AO_GRP_NAME, COUNTRY_NAME, YEAR) %>%
    fill(R_RANK, .direction = "down") %>%
    fill(R_RANK, .direction = "up") %>%

    fill(RANK, .direction = "down") %>%
    fill(RANK, .direction = "up") %>%
    fill(RANK_PREV, .direction = "down") %>%
    fill(RANK_PREV, .direction = "up") %>%
    ungroup() %>%
    filter(R_RANK < 11) %>%
    select(
      AO_GRP_NAME,
      AO_GRP_CODE,
      YEAR,
      ISO_CT_NAME_ARR = COUNTRY_NAME,
      ISO_CT_CODE_ARR = ARR_ISO_CTY_CODE,
      FLIGHT,
      AVG_FLIGHT,
      R_RANK,
      RANK,
      RANK_PREV_YEAR = RANK_PREV,
      FROM_DATE,
      TO_DATE,
      NO_DAYS
    ) %>% 
    arrange(AO_GRP_CODE, desc(YEAR), R_RANK, ISO_CT_NAME_ARR)


  df_y2d %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ao_base_file),
  #     start = ao_base_dir),
  #   sheet = "ao_state_des_y2d",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(AO_GRP_CODE, desc(YEAR), R_RANK, ISO_CT_NAME_ARR)
  # 
  # list_ao_group <- unique(df$AO_GRP_CODE)
  # for (i in 1:nrow(list_ao_group)) {
  #   df1 <- df %>% filter(AO_GRP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   df_day1 <- df_y2d %>% filter(AO_GRP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   print(paste(list_ao_group$AO_GRP_CODE[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   #
  #   #   print(paste(j, df1[[j,4]] == df_day1[[j,4]]))
  #   #
  #   # }
  # }
  
  print(paste(mydataframe, mydate))
}
  
# ao ap dep ----
ao_ap_dep <- function(mydate =  current_day) {
  mydataframe <-  "ao_ap_dep"
  
  df_app <- import_dataframe(mydataframe)
  
  # mydate <- today()- days(1)
  data_day_text <- mydate %>% format("%Y%m%d")
  day_prev_week <- mydate + days(-7)
  day_prev_year <- mydate + days(-364)
  day_2019 <- mydate - days(364 * (year(mydate) - 2019) + floor((year(mydate) - 2019) / 4) * 7)
  current_year = year(mydate)
  
  stakeholder <- str_sub(mydataframe, 1, 2)
  
  ## day ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_day_raw.csv")
  
  df_day <- df_app %>%
    filter(ENTRY_DATE %in% c(mydate, day_prev_week, day_2019, day_prev_year)) %>% 
    left_join(dim_airport, by = c("DEP_ARP_PRU_ID" = "APT_ID")) %>% 
    group_by(AO_GRP_CODE, ENTRY_DATE) %>%
    arrange(AO_GRP_CODE, ENTRY_DATE, desc(FLIGHT), APT_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        ENTRY_DATE == mydate ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        ENTRY_DATE == mydate ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        ENTRY_DATE == day_prev_week ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      FLAG_PERIOD = case_when( 
        ENTRY_DATE == mydate ~ "CURRENT_DAY",
        ENTRY_DATE == day_prev_week ~ "DAY_PREV_WEEK",
        ENTRY_DATE == day_2019 ~ "DAY_2019",
        ENTRY_DATE == day_prev_year ~ "DAY_PREV_YEAR",
      )
    ) %>% 
    ungroup() %>% 
    group_by(AO_GRP_CODE, APT_NAME) %>% 
    arrange(AO_GRP_CODE, APT_NAME, desc(ENTRY_DATE)) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(TO_DATE = max(ENTRY_DATE)) %>% 
    select(
      AO_GRP_NAME,
      AO_GRP_CODE,
      FLAG_DAY = FLAG_PERIOD,
      ADEP_CODE = APT_ICAO_CODE,
      ADEP_NAME = APT_NAME,
      FLIGHT,
      R_RANK,
      RANK,
      RANK_PREV_WEEK = RANK_PREV,
      TO_DATE
    ) %>% 
    arrange(AO_GRP_CODE, FLAG_DAY, R_RANK, ADEP_NAME)
  
  df_day %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ao_base_file),
  #     start = ao_base_dir),
  #   sheet = "ao_apt_dep_day",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(AO_GRP_CODE, FLAG_DAY, R_RANK, ADEP_NAME)
  # 
  # list_ao_group <- unique(df$AO_GRP_CODE)
  # for (i in 1:nrow(list_ao_group)) {
  #   df1 <- df %>% filter(AO_GRP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   df_day1 <- df_day %>% filter(AO_GRP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   print(paste(list_ao_group$AO_GRP_CODE[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   #
  #   #   print(paste(j, df1[[j,4]] == df_day1[[j,4]]))
  #   #
  #   # }
  # }
  
  ## week ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_week_raw.csv")
  
  df_week <- df_app %>%
    filter(
      ENTRY_DATE %in% c(seq.Date(mydate-6, mydate)) |
        ENTRY_DATE %in% c(seq.Date(day_prev_week-6, day_prev_week))|
        ENTRY_DATE %in% c(seq.Date(day_2019-6, day_2019)) |
        ENTRY_DATE %in% c(seq.Date(day_prev_year-6, day_prev_year))
    ) %>% 
    compute(prudence = "lavish") %>%
    mutate(
      FLAG_PERIOD = case_when( 
        (ENTRY_DATE >= mydate - days(6) & ENTRY_DATE <= mydate) ~ "CURRENT_ROLLING_WEEK",
        (ENTRY_DATE >= day_prev_week - days(6) & ENTRY_DATE <= day_prev_week) ~ "PREV_ROLLING_WEEK",
        (ENTRY_DATE >= day_2019 - days(6) & ENTRY_DATE <= day_2019) ~ "ROLLING_WEEK_2019",
        (ENTRY_DATE >= day_prev_year - days(6) & ENTRY_DATE <= day_prev_year) ~ "ROLLING_WEEK_PREV_YEAR"
      )
    ) %>% 
    summarise(
      FLIGHT = sum(FLIGHT, na.rm = TRUE),
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE),
      .by = c(FLAG_PERIOD, AO_GRP_CODE, AO_GRP_NAME, DEP_ARP_PRU_ID)
    ) %>% 
    ungroup() %>% 
    left_join(dim_airport, by = c("DEP_ARP_PRU_ID" = "APT_ID")) %>% 
    group_by(AO_GRP_CODE, FLAG_PERIOD) %>%
    arrange(AO_GRP_CODE, FLAG_PERIOD, desc(FLIGHT), APT_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        FLAG_PERIOD == "CURRENT_ROLLING_WEEK" ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        FLAG_PERIOD == "CURRENT_ROLLING_WEEK" ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        FLAG_PERIOD == "PREV_ROLLING_WEEK" ~ min_rank(desc(FLIGHT)),
        .default = NA
      )
    ) %>% 
    ungroup() %>% 
    group_by(AO_GRP_NAME, APT_NAME) %>% 
    arrange(AO_GRP_NAME, APT_NAME, FLAG_PERIOD) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(
      TO_DATE = max(TO_DATE, na.rm = TRUE),
      FROM_DATE = TO_DATE - days(6),
    ) %>% 
    select(
      AO_GRP_NAME,
      AO_GRP_CODE,
      FLAG_ROLLING_WEEK = FLAG_PERIOD,
      ADEP_CODE = APT_ICAO_CODE,
      ADEP_NAME = APT_NAME,
      FLIGHT,
      R_RANK,
      RANK,
      RANK_PREV_WEEK = RANK_PREV,
      FROM_DATE,
      TO_DATE
    ) %>% 
    arrange(AO_GRP_CODE, FLAG_ROLLING_WEEK, R_RANK, ADEP_NAME)
  
  df_week %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ao_base_file),
  #     start = ao_base_dir),
  #   sheet = "ao_apt_dep_week",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(AO_GRP_CODE, FLAG_ROLLING_WEEK, R_RANK, ADEP_NAME)
  # 
  # list_ao_group <- unique(df$AO_GRP_CODE)
  # for (i in 1:nrow(list_ao_group)) {
  #   df1 <- df %>% filter(AO_GRP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   df_day1 <- df_week %>% filter(AO_GRP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   print(paste(list_ao_group$AO_GRP_CODE[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   #
  #   #   print(paste(j, df1[[j,4]] == df_day1[[j,4]]))
  #   #
  #   # }
  # }
  
  
  ## year ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_y2d_raw.csv")
  
  y2d_dates <- seq.Date(ymd(paste0(2019,"01","01")),
                        mydate) %>% as_tibble() %>%
    filter(year(value) %in% c(2019, current_year-1, current_year)) %>%
    filter(format(value, "%m-%d") <= format(mydate, "%m-%d")) %>%
    pull()
  
  df_y2d <- df_app %>%
    compute(prudence = "lavish") %>%
    mutate(
      YEAR = year(ENTRY_DATE)
    ) %>%
    filter(YEAR %in% c(current_year, current_year-1, 2019)) %>%
    filter(ENTRY_DATE %in% y2d_dates) %>%
    # filter(DEP_ARP_PRU_ID == 4467) %>%
    summarise(
      FLIGHT = sum(FLIGHT, na.rm = TRUE),
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE),
      .by = c(YEAR, AO_GRP_CODE, AO_GRP_NAME, DEP_ARP_PRU_ID)
    ) %>%
    ungroup() %>%
    reframe(
      AO_GRP_CODE, AO_GRP_NAME, DEP_ARP_PRU_ID, FLIGHT,
      TO_DATE = max(TO_DATE, na.rm = TRUE),
      FROM_DATE = min(FROM_DATE, na.rm = TRUE),
      .by = c(YEAR)
    ) %>% 
    left_join(dim_airport, by = c("DEP_ARP_PRU_ID" = "APT_ID")) %>% 
    group_by(AO_GRP_CODE, YEAR) %>%
    arrange(AO_GRP_CODE, YEAR, desc(FLIGHT), APT_NAME) %>%
    mutate(
      R_RANK = case_when(
        YEAR == current_year ~ row_number(),
        .default = NA
      ),
      RANK = case_when(
        YEAR == current_year ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      RANK_PREV = case_when(
        YEAR == current_year-1 ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      NO_DAYS = as.numeric(TO_DATE - FROM_DATE) + 1,
      AVG_FLIGHT = FLIGHT / NO_DAYS
    ) %>%
    ungroup() %>%
    group_by(AO_GRP_NAME, APT_NAME) %>%
    arrange(AO_GRP_NAME, APT_NAME, YEAR) %>%
    fill(R_RANK, .direction = "down") %>%
    fill(R_RANK, .direction = "up") %>%
    
    fill(RANK, .direction = "down") %>%
    fill(RANK, .direction = "up") %>%
    fill(RANK_PREV, .direction = "down") %>%
    fill(RANK_PREV, .direction = "up") %>%
    ungroup() %>%
    filter(R_RANK < 11) %>%
    select(
      AO_GRP_NAME,
      AO_GRP_CODE,
      YEAR,
      ADEP_CODE = APT_ICAO_CODE,
      ADEP_NAME = APT_NAME,
      FLIGHT,
      AVG_FLIGHT,
      R_RANK,
      RANK,
      RANK_PREV_YEAR = RANK_PREV,
      FROM_DATE,
      TO_DATE,
      NO_DAYS
    ) %>% 
    arrange(AO_GRP_CODE, desc(YEAR), R_RANK, ADEP_NAME)
  
  
  df_y2d %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ao_base_file),
  #     start = ao_base_dir),
  #   sheet = "ao_apt_dep_y2d",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(AO_GRP_CODE, desc(YEAR), R_RANK, ADEP_NAME)
  # 
  # list_ao_group <- unique(df$AO_GRP_CODE)
  # for (i in 1:nrow(list_ao_group)) {
  #   df1 <- df %>% filter(AO_GRP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   df_day1 <- df_y2d %>% filter(AO_GRP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   print(paste(list_ao_group$AO_GRP_CODE[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   #
  #   #   print(paste(j, df1[[j,4]] == df_day1[[j,4]]))
  #   #
  #   # }
  # }
  
  print(paste(mydataframe, mydate))
}


# ao ap pair ----
ao_ap_pair <- function(mydate =  current_day) {
  mydataframe <-  "ao_ap_pair"
  
  df_app <- import_dataframe(mydataframe)
  
  # mydate <- today()- days(1)
  data_day_text <- mydate %>% format("%Y%m%d")
  day_prev_week <- mydate + days(-7)
  day_prev_year <- mydate + days(-364)
  day_2019 <- mydate - days(364 * (year(mydate) - 2019) + floor((year(mydate) - 2019) / 4) * 7)
  current_year = year(mydate)
  
  stakeholder <- str_sub(mydataframe, 1, 2)
  
  ## day ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_day_raw.csv")
  
  df_day <- df_app %>%
    filter(ENTRY_DATE %in% c(mydate, day_prev_week, day_2019, day_prev_year)) %>% 
    left_join(dim_airport, by = c("ARP_PRU_ID_1" = "APT_ID")) %>% 
    left_join(dim_airport, by = c("ARP_PRU_ID_2" = "APT_ID"), 
              suffix = c("_1", "_2")) %>% 
    mutate(AIRPORT_PAIR = case_when(
      APT_NAME_1 <= APT_NAME_2 ~ paste0(APT_NAME_1, "<->", APT_NAME_2),
      .default = paste0(APT_NAME_2, "<->", APT_NAME_1)
                                    )
      ) %>% 
    group_by(AO_GRP_CODE, ENTRY_DATE) %>%
    arrange(AO_GRP_CODE, ENTRY_DATE, desc(FLIGHT), AIRPORT_PAIR) %>% 
    mutate(
      R_RANK = case_when( 
        ENTRY_DATE == mydate ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        ENTRY_DATE == mydate ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        ENTRY_DATE == day_prev_week ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      FLAG_PERIOD = case_when( 
        ENTRY_DATE == mydate ~ "CURRENT_DAY",
        ENTRY_DATE == day_prev_week ~ "DAY_PREV_WEEK",
        ENTRY_DATE == day_2019 ~ "DAY_2019",
        ENTRY_DATE == day_prev_year ~ "DAY_PREV_YEAR",
      )
    ) %>% 
    ungroup() %>% 
    group_by(AO_GRP_CODE, AIRPORT_PAIR) %>% 
    arrange(AO_GRP_CODE, AIRPORT_PAIR, desc(ENTRY_DATE)) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(TO_DATE = max(ENTRY_DATE)) %>% 
    select(
      AO_GRP_NAME,
      AO_GRP_CODE,
      FLAG_DAY = FLAG_PERIOD,
      AIRPORT_PAIR,
      FLIGHT,
      R_RANK,
      RANK,
      RANK_PREV_WEEK = RANK_PREV,
      TO_DATE
    ) %>% 
    arrange(AO_GRP_CODE, FLAG_DAY, R_RANK, AIRPORT_PAIR)
  
  df_day %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ao_base_file),
  #     start = ao_base_dir),
  #   sheet = "ao_apt_pair_day",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(AO_GRP_CODE, FLAG_DAY, R_RANK, AIRPORT_PAIR)
  # 
  # list_ao_group <- unique(df$AO_GRP_CODE)
  # nrow(list_ao_group)
  # for (i in 2:2) {
  #   df1 <- df %>% filter(AO_GRP_CODE == list_ao_group$AO_GRP_CODE[i]) 
  # 
  #   df_day1 <- df_day %>% filter(AO_GRP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   print(paste(list_ao_group$AO_GRP_CODE[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   # 
  #   #   print(paste(j, df1[[j,4]] == df_day1[[j,4]]))
  #   # 
  #   # }
  # }
  
  ## week ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_week_raw.csv")
  
  df_week <- df_app %>%
    filter(
      ENTRY_DATE %in% c(seq.Date(mydate-6, mydate)) |
        ENTRY_DATE %in% c(seq.Date(day_prev_week-6, day_prev_week))|
        ENTRY_DATE %in% c(seq.Date(day_2019-6, day_2019)) |
        ENTRY_DATE %in% c(seq.Date(day_prev_year-6, day_prev_year))
    ) %>% 
    compute(prudence = "lavish") %>%
    mutate(
      FLAG_PERIOD = case_when( 
        (ENTRY_DATE >= mydate - days(6) & ENTRY_DATE <= mydate) ~ "CURRENT_ROLLING_WEEK",
        (ENTRY_DATE >= day_prev_week - days(6) & ENTRY_DATE <= day_prev_week) ~ "PREV_ROLLING_WEEK",
        (ENTRY_DATE >= day_2019 - days(6) & ENTRY_DATE <= day_2019) ~ "ROLLING_WEEK_2019",
        (ENTRY_DATE >= day_prev_year - days(6) & ENTRY_DATE <= day_prev_year) ~ "ROLLING_WEEK_PREV_YEAR"
      )
    ) %>% 
    summarise(
      FLIGHT = sum(FLIGHT, na.rm = TRUE),
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE),
      .by = c(FLAG_PERIOD, AO_GRP_CODE, AO_GRP_NAME, ARP_PRU_ID_1, ARP_PRU_ID_2)
    ) %>% 
    ungroup() %>% 
    left_join(dim_airport, by = c("ARP_PRU_ID_1" = "APT_ID")) %>% 
    left_join(dim_airport, by = c("ARP_PRU_ID_2" = "APT_ID"), 
              suffix = c("_1", "_2")) %>% 
    mutate(AIRPORT_PAIR = case_when(
      APT_NAME_1 <= APT_NAME_2 ~ paste0(APT_NAME_1, "<->", APT_NAME_2),
      .default = paste0(APT_NAME_2, "<->", APT_NAME_1)
    )
    ) %>% 
    group_by(AO_GRP_CODE, FLAG_PERIOD) %>%
    arrange(AO_GRP_CODE, FLAG_PERIOD, desc(FLIGHT), AIRPORT_PAIR) %>% 
    mutate(
      R_RANK = case_when( 
        FLAG_PERIOD == "CURRENT_ROLLING_WEEK" ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        FLAG_PERIOD == "CURRENT_ROLLING_WEEK" ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        FLAG_PERIOD == "PREV_ROLLING_WEEK" ~ min_rank(desc(FLIGHT)),
        .default = NA
      )
    ) %>% 
    ungroup() %>% 
    group_by(AO_GRP_NAME, AIRPORT_PAIR) %>% 
    arrange(AO_GRP_NAME, AIRPORT_PAIR, FLAG_PERIOD) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(
      TO_DATE = max(TO_DATE, na.rm = TRUE),
      FROM_DATE = TO_DATE - days(6),
    ) %>% 
    select(
      AO_GRP_NAME,
      AO_GRP_CODE,
      FLAG_ROLLING_WEEK = FLAG_PERIOD,
      AIRPORT_PAIR,
      FLIGHT,
      R_RANK,
      RANK,
      RANK_PREV_WEEK = RANK_PREV,
      FROM_DATE,
      TO_DATE
    ) %>% 
    arrange(AO_GRP_CODE, FLAG_ROLLING_WEEK, R_RANK, AIRPORT_PAIR)
  
  df_week %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ao_base_file),
  #     start = ao_base_dir),
  #   sheet = "ao_apt_pair_week",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(AO_GRP_CODE, FLAG_ROLLING_WEEK, R_RANK, AIRPORT_PAIR)
  # 
  # list_ao_group <- unique(df$AO_GRP_CODE)
  # 
  # for (i in 2:nrow(list_ao_group)) {
  #   df1 <- df %>% filter(AO_GRP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   df_day1 <- df_week %>% filter(AO_GRP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   print(paste(list_ao_group$AO_GRP_CODE[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   # 
  #   #   print(paste(j, df1[[j,5]] == df_day1[[j,5]]))
  #   # 
  #   # }
  # }
  
  
  ## year ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_y2d_raw.csv")
  
  y2d_dates <- seq.Date(ymd(paste0(2019,"01","01")),
                        mydate) %>% as_tibble() %>%
    filter(year(value) %in% c(2019, current_year-1, current_year)) %>%
    filter(format(value, "%m-%d") <= format(mydate, "%m-%d")) %>%
    pull()
  
  df_y2d <- df_app %>%
    compute(prudence = "lavish") %>%
    mutate(
      YEAR = year(ENTRY_DATE)
    ) %>%
    filter(YEAR %in% c(current_year, current_year-1, 2019)) %>%
    filter(ENTRY_DATE %in% y2d_dates) %>%
    # filter(DEP_ARP_PRU_ID == 4467) %>%
    summarise(
      FLIGHT = sum(FLIGHT, na.rm = TRUE),
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE),
      .by = c(YEAR, AO_GRP_CODE, AO_GRP_NAME, ARP_PRU_ID_1, ARP_PRU_ID_2)
    ) %>%
    ungroup() %>%
    reframe(
      AO_GRP_CODE, AO_GRP_NAME, ARP_PRU_ID_1, ARP_PRU_ID_2, FLIGHT,
      TO_DATE = max(TO_DATE, na.rm = TRUE),
      FROM_DATE = min(FROM_DATE, na.rm = TRUE),
      .by = c(YEAR)
    ) %>% 
    left_join(dim_airport, by = c("ARP_PRU_ID_1" = "APT_ID")) %>% 
    left_join(dim_airport, by = c("ARP_PRU_ID_2" = "APT_ID"), 
              suffix = c("_1", "_2")) %>% 
    mutate(AIRPORT_PAIR = case_when(
      APT_NAME_1 <= APT_NAME_2 ~ paste0(APT_NAME_1, "<->", APT_NAME_2),
      .default = paste0(APT_NAME_2, "<->", APT_NAME_1)
    )
    ) %>% 
    group_by(AO_GRP_CODE, YEAR) %>%
    arrange(AO_GRP_CODE, YEAR, desc(FLIGHT), AIRPORT_PAIR) %>%
    mutate(
      R_RANK = case_when(
        YEAR == current_year ~ row_number(),
        .default = NA
      ),
      RANK = case_when(
        YEAR == current_year ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      RANK_PREV = case_when(
        YEAR == current_year-1 ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      NO_DAYS = as.numeric(TO_DATE - FROM_DATE) + 1,
      AVG_FLIGHT = FLIGHT / NO_DAYS
    ) %>%
    ungroup() %>%
    group_by(AO_GRP_NAME, AIRPORT_PAIR) %>%
    arrange(AO_GRP_NAME, AIRPORT_PAIR, YEAR) %>%
    fill(R_RANK, .direction = "down") %>%
    fill(R_RANK, .direction = "up") %>%
    
    fill(RANK, .direction = "down") %>%
    fill(RANK, .direction = "up") %>%
    fill(RANK_PREV, .direction = "down") %>%
    fill(RANK_PREV, .direction = "up") %>%
    ungroup() %>%
    filter(R_RANK < 11) %>%
    select(
      AO_GRP_NAME,
      AO_GRP_CODE,
      YEAR,
      AIRPORT_PAIR,
      FLIGHT,
      AVG_FLIGHT,
      R_RANK,
      RANK,
      RANK_PREV_YEAR = RANK_PREV,
      FROM_DATE,
      TO_DATE,
      NO_DAYS
    ) %>% 
    arrange(AO_GRP_CODE, desc(YEAR), R_RANK, AIRPORT_PAIR)
  
  
  df_y2d %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ao_base_file),
  #     start = ao_base_dir),
  #   sheet = "ao_apt_pair_y2d",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(AO_GRP_CODE, desc(YEAR), R_RANK, AIRPORT_PAIR)
  # 
  # list_ao_group <- unique(df$AO_GRP_CODE)
  # for (i in 1:nrow(list_ao_group)) {
  #   df1 <- df %>% filter(AO_GRP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   df_day1 <- df_y2d %>% filter(AO_GRP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   print(paste(list_ao_group$AO_GRP_CODE[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   #
  #   #   print(paste(j, df1[[j,4]] == df_day1[[j,4]]))
  #   #
  #   # }
  # }
  
  print(paste(mydataframe, mydate))
}

# ao ap arr delay ----
ao_ap_arr_delay <- function(mydate =  current_day) {
  mydataframe <-  "ao_ap_arr_delay"
  
  df_app <- import_dataframe(mydataframe)
  
  # mydate <- today()- days(1)
  data_day_text <- mydate %>% format("%Y%m%d")
  day_prev_week <- mydate + days(-7)
  day_prev_year <- mydate + days(-364)
  day_2019 <- mydate - days(364 * (year(mydate) - 2019) + floor((year(mydate) - 2019) / 4) * 7)
  current_year = year(mydate)
  
  stakeholder <- str_sub(mydataframe, 1, 2)
  
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_day_raw.csv")

  ## day ----
  df_day <- df_app %>%
    filter(ENTRY_DATE %in% c(mydate, day_prev_year)) %>% 
    left_join(dim_airport, by = c("ARR_ARP_PRU_ID" = "APT_ID")) %>% 
    mutate(
      FLAG_PERIOD = if_else(ENTRY_DATE == mydate, "", "PREV_")
    ) %>% 
    select(
      FLAG_PERIOD,
      ENTRY_DATE,
      AO_GRP_CODE,
      APT_ICAO_CODE,
      APT_NAME,
      FLIGHT,
      ARR_DELAYED_FLIGHT,
      ARR_ATFM_DELAY
    ) %>% 
    pivot_wider(
      names_from  = FLAG_PERIOD,
      values_from = c(ENTRY_DATE, FLIGHT, ARR_DELAYED_FLIGHT, ARR_ATFM_DELAY),
      names_glue  = "{FLAG_PERIOD}{.value}"
    ) %>% 
    group_by(AO_GRP_CODE, ENTRY_DATE) %>%
    arrange(AO_GRP_CODE, ENTRY_DATE, desc(FLIGHT), APT_ICAO_CODE) %>% 
    mutate(
      RANK_BY_FLIGHT = row_number()
    ) %>% 
    ungroup() %>% 
    filter(RANK_BY_FLIGHT < 11 & !is.na(ENTRY_DATE)) %>% 
    mutate(
      PERIOD = "1D",
      START_DATE = ENTRY_DATE + days(0),
      END_DATE = ENTRY_DATE,
      PREV_START_DATE = PREV_ENTRY_DATE + days(0),
      PREV_END_DATE = PREV_ENTRY_DATE
    ) %>% 
    select(
      PERIOD,
      START_DATE,
      END_DATE,
      AO_GROUP_CODE = AO_GRP_CODE,
      APT_CODE = APT_ICAO_CODE,
      APT_NAME,
      ARR_FLIGHT = FLIGHT,
      ARR_DELAYED_FLIGHT,
      ARR_ATFM_DELAY,
      RANK_BY_FLIGHT,
      PREV_START_DATE,
      PREV_END_DATE,
      PREV_ARR_FLIGHT = PREV_FLIGHT,
      PREV_DELAYED_FLIGHT = PREV_ARR_DELAYED_FLIGHT,
      PREV_ARR_ATFM_DELAY
    ) %>% 
    arrange(AO_GROUP_CODE, PERIOD, START_DATE, RANK_BY_FLIGHT, APT_NAME)
  
  # df_day %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ao_base_file),
  #     start = ao_base_dir),
  #   sheet = "ao_apt_arr_delay",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>% 
  #   filter(PERIOD == "1D")
  # 
  # df <- df %>%
  #   arrange(AO_GROUP_CODE, PERIOD, START_DATE, RANK_BY_FLIGHT, APT_NAME)
  # 
  # # nrow(list_ao_group)
  # for (i in 1:nrow(list_ao_group)) {
  #   df1 <- df %>% filter(AO_GROUP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   df_day1 <- df_day %>% filter(AO_GROUP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   print(paste(list_ao_group$AO_GRP_CODE[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   #
  #   #   print(paste(j, df1[[j,4]] == df_day1[[j,4]]))
  #   #
  #   # }
  # }
  
  ## week ----
  df_week <- df_app %>%
    filter(
      ENTRY_DATE %in% c(seq.Date(mydate-6, mydate)) |
      ENTRY_DATE %in% c(seq.Date(day_prev_year-6, day_prev_year))
    ) %>% 
    compute(prudence = "lavish") %>%
    mutate(
      FLAG_PERIOD = case_when( 
        (ENTRY_DATE >= mydate - days(6) & ENTRY_DATE <= mydate) ~ "",
        (ENTRY_DATE >= day_prev_year - days(6) & ENTRY_DATE <= day_prev_year) ~ "PREV_"
      )
    ) %>% 
    summarise(
      FLIGHT = sum(FLIGHT, na.rm = TRUE),
      ARR_DELAYED_FLIGHT = sum(ARR_DELAYED_FLIGHT, na.rm = TRUE),
      ARR_ATFM_DELAY = sum(ARR_ATFM_DELAY, na.rm = TRUE),
      .by = c(FLAG_PERIOD, AO_GRP_CODE, AO_GRP_NAME, ARR_ARP_PRU_ID)
    ) %>% 
    ungroup() %>% 
    left_join(dim_airport, by = c("ARR_ARP_PRU_ID" = "APT_ID")) %>% 
    select(
      FLAG_PERIOD,
      AO_GRP_CODE,
      APT_ICAO_CODE,
      APT_NAME,
      FLIGHT,
      ARR_DELAYED_FLIGHT,
      ARR_ATFM_DELAY
    ) %>% 
    pivot_wider(
      names_from  = FLAG_PERIOD,
      values_from = c(FLIGHT, ARR_DELAYED_FLIGHT, ARR_ATFM_DELAY),
      names_glue  = "{FLAG_PERIOD}{.value}"
    ) %>% 
    group_by(AO_GRP_CODE) %>%
    arrange(AO_GRP_CODE, desc(FLIGHT), APT_ICAO_CODE) %>% 
    mutate(
      RANK_BY_FLIGHT = row_number(),
      START_DATE = mydate - days(6),
      END_DATE = mydate,
      PREV_START_DATE = day_prev_year - days(6),
      PREV_END_DATE = day_prev_year
    ) %>% 
    ungroup() %>% 
    filter(RANK_BY_FLIGHT < 11 & !is.na(END_DATE)) %>% 
    mutate(
      PERIOD = "WK") %>% 
    select(
      PERIOD,
      START_DATE,
      END_DATE,
      AO_GROUP_CODE = AO_GRP_CODE,
      APT_CODE = APT_ICAO_CODE,
      APT_NAME,
      ARR_FLIGHT = FLIGHT,
      ARR_DELAYED_FLIGHT,
      ARR_ATFM_DELAY,
      RANK_BY_FLIGHT,
      PREV_START_DATE,
      PREV_END_DATE,
      PREV_ARR_FLIGHT = PREV_FLIGHT,
      PREV_DELAYED_FLIGHT = PREV_ARR_DELAYED_FLIGHT,
      PREV_ARR_ATFM_DELAY
    ) %>% 
    arrange(AO_GROUP_CODE, PERIOD, START_DATE, RANK_BY_FLIGHT, APT_NAME)
  
  unique(df_week$START_DATE)
  # df_day %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ao_base_file),
  #     start = ao_base_dir),
  #   sheet = "ao_apt_arr_delay",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
  #   filter(PERIOD == "WK")
  # 
  # df <- df %>%
  #   arrange(AO_GROUP_CODE, PERIOD, START_DATE, RANK_BY_FLIGHT, APT_NAME)
  # 
  # # nrow(list_ao_group)
  # for (i in 1:nrow(list_ao_group)) {
  #   df1 <- df %>% filter(AO_GROUP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   df_day1 <- df_week %>% filter(AO_GROUP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   print(paste(list_ao_group$AO_GRP_CODE[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   #
  #   #   print(paste(j, df1[[j,4]] == df_day1[[j,4]]))
  #   #
  #   # }
  # }
  
  ## year ----
  y2d_dates <- seq.Date(ymd(paste0(current_year-1,"01","01")),
                        mydate) %>% as_tibble() %>%
    filter(format(value, "%m-%d") <= format(mydate, "%m-%d")) %>%
    pull()
  
  day_prev_year_sd <- max(y2d_dates[year(y2d_dates) == current_year - 1], na.rm = TRUE)
  
  
  df_y2d <- df_app %>%
    compute(prudence = "lavish") %>%
    mutate(
      YEAR = year(ENTRY_DATE)
    ) %>%
    filter(YEAR %in% c(current_year, current_year-1)) %>%
    filter(ENTRY_DATE %in% y2d_dates) %>% 
    compute(prudence = "lavish") %>%
    summarise(
      FLIGHT = sum(FLIGHT, na.rm = TRUE),
      ARR_DELAYED_FLIGHT = sum(ARR_DELAYED_FLIGHT, na.rm = TRUE),
      ARR_ATFM_DELAY = sum(ARR_ATFM_DELAY, na.rm = TRUE),
      .by = c(YEAR, AO_GRP_CODE, AO_GRP_NAME, ARR_ARP_PRU_ID)
    ) %>% 
    ungroup() %>% 
    left_join(dim_airport, by = c("ARR_ARP_PRU_ID" = "APT_ID")) %>% 
    mutate(
      FLAG_PERIOD = if_else(YEAR == current_year, "", "PREV_")
    ) %>%
    select(
      FLAG_PERIOD,
      AO_GRP_CODE,
      APT_ICAO_CODE,
      APT_NAME,
      FLIGHT,
      ARR_DELAYED_FLIGHT,
      ARR_ATFM_DELAY
    ) %>% 
    pivot_wider(
      names_from  = FLAG_PERIOD,
      values_from = c(FLIGHT, ARR_DELAYED_FLIGHT, ARR_ATFM_DELAY),
      names_glue  = "{FLAG_PERIOD}{.value}"
    ) %>% 
    group_by(AO_GRP_CODE) %>%
    arrange(AO_GRP_CODE, desc(FLIGHT), APT_ICAO_CODE) %>% 
    mutate(
      RANK_BY_FLIGHT = row_number(),
      START_DATE = ymd(paste0(current_year,"0101")),
      END_DATE = mydate,
      PREV_START_DATE = ymd(paste0(current_year-1,"0101")),
      PREV_END_DATE = day_prev_year_sd
    ) %>% 
    ungroup() %>% 
    filter(RANK_BY_FLIGHT < 11) %>% 
    mutate(
      PERIOD = "YTD") %>% 
    select(
      PERIOD,
      START_DATE,
      END_DATE,
      AO_GROUP_CODE = AO_GRP_CODE,
      APT_CODE = APT_ICAO_CODE,
      APT_NAME,
      ARR_FLIGHT = FLIGHT,
      ARR_DELAYED_FLIGHT,
      ARR_ATFM_DELAY,
      RANK_BY_FLIGHT,
      PREV_START_DATE,
      PREV_END_DATE,
      PREV_ARR_FLIGHT = PREV_FLIGHT,
      PREV_DELAYED_FLIGHT = PREV_ARR_DELAYED_FLIGHT,
      PREV_ARR_ATFM_DELAY
    ) %>% 
    arrange(AO_GROUP_CODE, PERIOD, START_DATE, RANK_BY_FLIGHT, APT_NAME)

  # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ao_base_file),
  #     start = ao_base_dir),
  #   sheet = "ao_apt_arr_delay",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
  #   filter(PERIOD == "YTD")
  # 
  # df <- df %>%
  #   arrange(AO_GROUP_CODE, PERIOD, START_DATE, RANK_BY_FLIGHT, APT_NAME)
  # 
  # # nrow(list_ao_group)
  # for (i in 1:nrow(list_ao_group)) {
  #   df1 <- df %>% filter(AO_GROUP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   df_day1 <- df_y2d %>% filter(AO_GROUP_CODE == list_ao_group$AO_GRP_CODE[i])
  # 
  #   print(paste(list_ao_group$AO_GRP_CODE[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   #
  #   #   print(paste(j, df1[[j,4]] == df_day1[[j,4]]))
  #   #
  #   # }
  # }
  
  df_all_periods <- df_day %>% 
    rbind(df_week) %>% 
    rbind(df_y2d)
  
  df_all_periods %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  
  print(paste(mydataframe, mydate))
}



# execute functions ----
# wef <- "2024-01-01"  #included in output
# til <- "2024-05-17"  #included in output
# current_day <- seq(ymd(til), ymd(wef), by = "-1 day")

purrr::walk(current_day, ao_st_des)
purrr::walk(current_day, ao_ap_dep)
purrr::walk(current_day, ao_ap_pair)
purrr::walk(current_day, ao_ap_arr_delay)

