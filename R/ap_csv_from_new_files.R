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
  mydataframe <- dfname
  myparquetfile <- paste0(mydataframe, "_day_base.parquet")
  
  df_base <- read_parquet_duckdb(here(archive_dir_raw, 
                                      myparquetfile)
  ) 
  
  # filter to keep days and airports needed
  if ("ARP_PRU_ID" %in% names(df_base)) {
    df_alldays <- df_base %>% 
      filter(ARP_PRU_ID %in% list_airport$APT_ID)  
  } else if ("DEP_ARP_PRU_ID" %in% names(df_base)) {
    df_alldays <- df_base %>% 
      filter(DEP_ARP_PRU_ID %in% list_airport$APT_ID)  
    
  }

  # pre-process data
  if (dfname == "ap_ao") {
    df_app <- df_alldays %>% 
      filter(AO_ID != 1777) %>%  # undefined
      compute(prudence = "lavish") %>%
      left_join(dim_ao_group, by = c("AO_ID", "AO_CODE")) %>% 
      summarise(
        DEP_ARR = sum(DEP_ARR, na.rm = TRUE),
        .by = c(ENTRY_DATE, ARP_PRU_ID, AO_GRP_CODE, AO_GRP_NAME)
      )
    
  } else if (dfname == "ap_ms") {
    df_app <- df_alldays %>% collect() %>% 
      mutate(MS_ID = case_when(
        MS_ID == -1 | MS_ID ==  1 | MS_ID == 5 ~ 0,
        .default = MS_ID)
      ) %>% 
      group_by(ENTRY_DATE, ARP_PRU_ID, MS_ID) %>%
      summarise(DEP_ARR = sum(DEP_ARR, na.rm = TRUE)) %>% 
      ungroup() %>% 
      left_join(dim_marktet_segment, by = "MS_ID") %>% 
      left_join(dim_airport, by = c("ARP_PRU_ID" = "APT_ID"))
    
  } else {
    df_app <- df_alldays
  }

  return(df_app) 
}


# ap ao grp ----
ap_ao <- function(mydate =  current_day) {
  mydataframe <-  "ap_ao"
  
  df_app <- import_dataframe(mydataframe)

    # mydate <- current_day
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
    left_join(dim_airport, by = c("ARP_PRU_ID" = "APT_ID")) %>% 
    group_by(ARP_PRU_ID, ENTRY_DATE) %>%
    arrange(APT_ICAO_CODE, ENTRY_DATE, desc(DEP_ARR), AO_GRP_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        ENTRY_DATE == mydate ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        ENTRY_DATE == mydate ~ min_rank(desc(DEP_ARR)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        ENTRY_DATE == day_prev_week ~ min_rank(desc(DEP_ARR)),
        .default = NA
      ),
      FLAG_DAY = case_when( 
        ENTRY_DATE == mydate ~ "CURRENT_DAY",
        ENTRY_DATE == day_prev_week ~ "DAY_PREV_WEEK",
        ENTRY_DATE == day_2019 ~ "DAY_2019",
        ENTRY_DATE == day_prev_year ~ "DAY_PREV_YEAR",
      )
    ) %>% 
    ungroup() %>% 
    group_by(APT_NAME, AO_GRP_NAME) %>% 
    arrange(APT_NAME, AO_GRP_NAME, desc(ENTRY_DATE)) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(LAST_DATA_DAY = max(ENTRY_DATE)) %>% 
    select(
      ARP_CODE = APT_ICAO_CODE,
      ARP_NAME = APT_NAME,
      FLAG_DAY,
      TO_DATE = ENTRY_DATE,
      LAST_DATA_DAY,
      AO_GRP_CODE,
      AO_GRP_NAME,
      DEP_ARR,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>% 
    arrange(ARP_CODE, FLAG_DAY, R_RANK, AO_GRP_NAME)
  
  df_day %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  # test <- df_day %>% 
  #   filter(ARP_CODE == "EFHK") 
  
  # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_ao_day",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE, FLAG_DAY, R_RANK, AO_GRP_NAME)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   df_day1 <- df_day %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, df_day1)))
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
      PERIOD_TYPE = case_when( 
        (ENTRY_DATE >= mydate - days(6) & ENTRY_DATE <= mydate) ~ "CURRENT_ROLLING_WEEK",
        (ENTRY_DATE >= day_prev_week - days(6) & ENTRY_DATE <= day_prev_week) ~ "PREV_ROLLING_WEEK",
        (ENTRY_DATE >= day_2019 - days(6) & ENTRY_DATE <= day_2019) ~ "ROLLING_WEEK_2019",
        (ENTRY_DATE >= day_prev_year - days(6) & ENTRY_DATE <= day_prev_year) ~ "ROLLING_WEEK_PREV_YEAR"
      )
    ) %>% 
    summarise(
      DEP_ARR = sum(DEP_ARR, na.rm = TRUE),
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE),
      .by = c(PERIOD_TYPE, ARP_PRU_ID, AO_GRP_NAME, AO_GRP_CODE)
    ) %>% 
    left_join(dim_airport, by = c("ARP_PRU_ID" = "APT_ID")) %>% 
    ungroup() %>% 
    group_by(ARP_PRU_ID, PERIOD_TYPE) %>%
    arrange(APT_ICAO_CODE, PERIOD_TYPE, desc(DEP_ARR), AO_GRP_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        PERIOD_TYPE == "CURRENT_ROLLING_WEEK" ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        PERIOD_TYPE == "CURRENT_ROLLING_WEEK" ~ min_rank(desc(DEP_ARR)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        PERIOD_TYPE == "PREV_ROLLING_WEEK" ~ min_rank(desc(DEP_ARR)),
        .default = NA
      )
    ) %>% 
    ungroup() %>% 
    group_by(APT_NAME, AO_GRP_NAME) %>% 
    arrange(APT_NAME, AO_GRP_NAME, PERIOD_TYPE) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(LAST_DATA_DAY = max(TO_DATE)) %>% 
    select(
      ARP_CODE = APT_ICAO_CODE,
      ARP_NAME = APT_NAME,
      PERIOD_TYPE,
      FROM_DATE,
      TO_DATE,
      LAST_DATA_DAY,
      AO_GRP_CODE,
      AO_GRP_NAME,
      DEP_ARR,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>% 
    arrange(ARP_CODE, PERIOD_TYPE, R_RANK, AO_GRP_NAME)
  
  df_week %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  test <- df_week %>%
    filter(ARP_CODE == "LLBG")

  # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_ao_week",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE, PERIOD_TYPE, R_RANK, AO_GRP_NAME)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   df_day1 <- df_week %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df)) {
  #   #
  #   #   print(paste(j, df1[[j,1]] == df_day1[[j,1]]))
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
      DEP_ARR = sum(DEP_ARR, na.rm = TRUE),
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE),
      .by = c(YEAR, ARP_PRU_ID, AO_GRP_NAME, AO_GRP_CODE)
    ) %>% 
    ungroup() %>% 
    left_join(dim_airport, by = c("ARP_PRU_ID" = "APT_ID")) %>% 
    group_by(ARP_PRU_ID, YEAR) %>%
    arrange(ARP_PRU_ID, YEAR, desc(DEP_ARR), AO_GRP_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        YEAR == current_year ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        YEAR == current_year ~ min_rank(desc(DEP_ARR)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        YEAR == current_year-1 ~ min_rank(desc(DEP_ARR)),
        .default = NA
      )
    ) %>% 
    ungroup() %>% 
    group_by(APT_NAME, AO_GRP_NAME) %>% 
    arrange(APT_NAME, AO_GRP_NAME, YEAR) %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "up") %>% 
    
    fill(RANK, .direction = "down") %>% 
    fill(RANK, .direction = "up") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(LAST_DATA_DAY = max(TO_DATE)) %>% 
    select(
      ARP_CODE = APT_ICAO_CODE,
      ARP_NAME = APT_NAME,
      YEAR,
      AO_GRP_CODE,
      AO_GRP_NAME,
      DEP_ARR,
      FROM_DATE,
      TO_DATE,
      LAST_DATA_DATE = LAST_DATA_DAY,
      R_RANK,
      RANK,
      RANK_PY = RANK_PREV
    ) %>% 
    arrange(ARP_CODE, desc(YEAR), R_RANK, AO_GRP_NAME)
  
  
  df_y2d %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
# dcheck
#   df <- read_xlsx(
#     path  = fs::path_abs(
#       str_glue(ap_base_file),
#       start = ap_base_dir),
#     sheet = "apt_ao_y2d",
#     range = cell_limits(c(1, 1), c(NA, NA))) |>
#     mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
# 
#   df <- df %>% arrange(ARP_CODE, desc(YEAR), R_RANK, AO_GRP_NAME)
# 
#   for (i in 1:nrow(list_airport)) {
#     df1 <- df %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
#     df_y2d1 <- df_y2d %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
# 
#     print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, df_y2d1)))
# 
# #
# #       for (j in 1:1) {
# #
# #         print(paste(j, df1[[j,6]], df_y2d1[[j,6]]))
# #
# #       }
# 
#   }
  
  print(mydate)
}

# ap st des ----
ap_st_des <- function(mydate =  current_day) {
  mydataframe <-  "ap_st_des"
  
  df_app <- import_dataframe(mydataframe)
  
    # mydate <- current_day
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
    left_join(dim_iso_country, by = c("ARR_ISO_CTY_CODE" = "ISO_COUNTRY_CODE")) %>% 
    group_by(DEP_ARP_PRU_ID, ENTRY_DATE) %>%
    arrange(APT_ICAO_CODE, ENTRY_DATE, desc(DEP), COUNTRY_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        ENTRY_DATE == mydate ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        ENTRY_DATE == mydate ~ min_rank(desc(DEP)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        ENTRY_DATE == day_prev_week ~ min_rank(desc(DEP)),
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
    group_by(APT_NAME, COUNTRY_NAME) %>% 
    arrange(APT_NAME, COUNTRY_NAME, desc(ENTRY_DATE)) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(LAST_DATA_DAY = max(ENTRY_DATE)) %>% 
    select(
      FLAG_PERIOD,
      ARP_CODE = APT_ICAO_CODE,
      ARP_NAME = APT_NAME,
      ISO_CT_NAME_ARR = COUNTRY_NAME,
      ISO_CT_CODE_ARR = ARR_ISO_CTY_CODE,
      DEP,
      TO_DATE = ENTRY_DATE,
      LAST_DATA_DAY,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>% 
    arrange(ARP_CODE, FLAG_PERIOD, R_RANK, ISO_CT_NAME_ARR)
  
  df_day %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_state_des_day",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE, FLAG_PERIOD, R_RANK, ISO_CT_NAME_ARR)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   df_day1 <- df_day %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, df_day1)))
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
      DEP = sum(DEP, na.rm = TRUE),
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE),
      .by = c(FLAG_PERIOD, DEP_ARP_PRU_ID, ARR_ISO_CTY_CODE)
    ) %>% 
    ungroup() %>% 
    left_join(dim_airport, by = c("DEP_ARP_PRU_ID" = "APT_ID")) %>% 
    left_join(dim_iso_country, by = c("ARR_ISO_CTY_CODE" = "ISO_COUNTRY_CODE")) %>% 
    group_by(DEP_ARP_PRU_ID, FLAG_PERIOD) %>%
    arrange(APT_ICAO_CODE, FLAG_PERIOD, desc(DEP), COUNTRY_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        FLAG_PERIOD == "CURRENT_ROLLING_WEEK" ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        FLAG_PERIOD == "CURRENT_ROLLING_WEEK" ~ min_rank(desc(DEP)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        FLAG_PERIOD == "PREV_ROLLING_WEEK" ~ min_rank(desc(DEP)),
        .default = NA
      )
    ) %>% 
    ungroup() %>% 
    group_by(APT_NAME, COUNTRY_NAME) %>% 
    arrange(APT_NAME, COUNTRY_NAME, FLAG_PERIOD) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(LAST_DATA_DAY = max(TO_DATE)) %>% 
    select(
      FLAG_PERIOD,
      ARP_CODE = APT_ICAO_CODE,
      ARP_NAME = APT_NAME,
      ISO_CT_NAME_ARR = COUNTRY_NAME,
      ISO_CT_CODE_ARR = ARR_ISO_CTY_CODE,
      DEP,
      FROM_DATE,
      TO_DATE,
      LAST_DATA_DAY,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>% 
    arrange(ARP_CODE, FLAG_PERIOD, R_RANK, ISO_CT_NAME_ARR)
  
  df_week %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_state_des_week",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE, FLAG_PERIOD, R_RANK, ISO_CT_NAME_ARR)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   df_day1 <- df_week %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, df_day1)))
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
      DEP = sum(DEP, na.rm = TRUE),
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE),
      .by = c(YEAR, DEP_ARP_PRU_ID, ARR_ISO_CTY_CODE)
    ) %>%
    ungroup() %>%
    left_join(dim_airport, by = c("DEP_ARP_PRU_ID" = "APT_ID")) %>%
    left_join(dim_iso_country, by = c("ARR_ISO_CTY_CODE" = "ISO_COUNTRY_CODE")) %>%
    group_by(DEP_ARP_PRU_ID, YEAR) %>%
    arrange(DEP_ARP_PRU_ID, YEAR, desc(DEP), COUNTRY_NAME) %>%
    mutate(
      R_RANK = case_when(
        YEAR == current_year ~ row_number(),
        .default = NA
      ),
      RANK = case_when(
        YEAR == current_year ~ min_rank(desc(DEP)),
        .default = NA
      ),
      RANK_PREV = case_when(
        YEAR == current_year-1 ~ min_rank(desc(DEP)),
        .default = NA
      )
    ) %>%
    ungroup() %>%
    group_by(APT_NAME, COUNTRY_NAME) %>%
    arrange(APT_NAME, COUNTRY_NAME, YEAR) %>%
    fill(R_RANK, .direction = "down") %>%
    fill(R_RANK, .direction = "up") %>%

    fill(RANK, .direction = "down") %>%
    fill(RANK, .direction = "up") %>%
    fill(RANK_PREV, .direction = "down") %>%
    fill(RANK_PREV, .direction = "up") %>%
    ungroup() %>%
    filter(R_RANK < 11) %>%
    mutate(LAST_DATA_DAY = max(TO_DATE)) %>%
    select(
      YEAR,
      ARP_CODE = APT_ICAO_CODE,
      ARP_NAME = APT_NAME,
      ISO_CT_NAME_ARR = COUNTRY_NAME,
      ISO_CT_CODE_ARR = ARR_ISO_CTY_CODE,
      DEP,
      FROM_DATE,
      TO_DATE,
      LAST_DATA_DAY,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>%
    arrange(ARP_CODE, desc(YEAR), R_RANK, ISO_CT_NAME_ARR)


  df_y2d %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck 
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_state_des_y2d",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE, desc(YEAR), R_RANK, ISO_CT_NAME_ARR)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  #   ap_ms_app_day1 <- df_y2d %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, ap_ms_app_day1)))
  # }
  
  print(mydate)
}
  
# ap ap des ----
ap_ap_des <- function(mydate =  current_day) {
    
  mydataframe <-  "ap_ap_des"
  df_app <- import_dataframe(mydataframe)
  
  # mydate <- current_day
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
    left_join(dim_airport, by = c("ARR_ARP_PRU_ID" = "APT_ID"),
              suffix = c("_DEP", "_ARR")) %>% 
    group_by(DEP_ARP_PRU_ID, ENTRY_DATE) %>%
    arrange(APT_ICAO_CODE_DEP, ENTRY_DATE, desc(DEP), APT_NAME_ARR) %>% 
    mutate(
      R_RANK = case_when( 
        ENTRY_DATE == mydate ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        ENTRY_DATE == mydate ~ min_rank(desc(DEP)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        ENTRY_DATE == day_prev_week ~ min_rank(desc(DEP)),
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
    group_by(APT_NAME_DEP, APT_NAME_ARR) %>% 
    arrange(APT_NAME_DEP, APT_NAME_ARR, desc(ENTRY_DATE)) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(LAST_DATA_DAY = max(ENTRY_DATE)) %>% 
    select(
      FLAG_PERIOD,
      ARP_CODE_DEP = APT_ICAO_CODE_DEP,
      ARP_NAME_DEP = APT_NAME_DEP,
      ARP_CODE_ARR = APT_ICAO_CODE_ARR,
      ARP_NAME_ARR = APT_NAME_ARR,
      DEP,
      TO_DATE = ENTRY_DATE,
      LAST_DATA_DAY,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>% 
    arrange(ARP_CODE_DEP, FLAG_PERIOD, R_RANK, ARP_NAME_ARR)
  
  df_day %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_apt_des_day",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE_DEP, FLAG_PERIOD, R_RANK, ARP_NAME_ARR)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE_DEP == list_airport$APT_ICAO_CODE[i])
  # 
  #   df_day1 <- df_day %>% filter(ARP_CODE_DEP == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, df_day1)))
  #   #
  #   # for (j in 1:nrow(df)) {
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
      DEP = sum(DEP, na.rm = TRUE),
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE),
      .by = c(FLAG_PERIOD, DEP_ARP_PRU_ID, ARR_ARP_PRU_ID)
    ) %>% 
    ungroup() %>% 
    left_join(dim_airport, by = c("DEP_ARP_PRU_ID" = "APT_ID")) %>% 
    left_join(dim_airport, by = c("ARR_ARP_PRU_ID" = "APT_ID"),
              suffix = c("_DEP", "_ARR")) %>% 
    group_by(DEP_ARP_PRU_ID, FLAG_PERIOD) %>%
    arrange(APT_ICAO_CODE_DEP, FLAG_PERIOD, desc(DEP), APT_NAME_ARR) %>% 
    mutate(
      R_RANK = case_when( 
        FLAG_PERIOD == "CURRENT_ROLLING_WEEK" ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        FLAG_PERIOD == "CURRENT_ROLLING_WEEK" ~ min_rank(desc(DEP)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        FLAG_PERIOD == "PREV_ROLLING_WEEK" ~ min_rank(desc(DEP)),
        .default = NA
      )
    ) %>% 
    ungroup() %>% 
    group_by(APT_NAME_DEP, APT_NAME_ARR) %>% 
    arrange(APT_NAME_DEP, APT_NAME_ARR, FLAG_PERIOD) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(LAST_DATA_DAY = max(TO_DATE)) %>% 
    select(
      FLAG_PERIOD,
      ARP_CODE_DEP = APT_ICAO_CODE_DEP,
      ARP_NAME_DEP = APT_NAME_DEP,
      ARP_CODE_ARR = APT_ICAO_CODE_ARR,
      ARP_NAME_ARR = APT_NAME_ARR,
      DEP,
      FROM_DATE,
      TO_DATE,
      LAST_DATA_DAY,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>% 
    arrange(ARP_CODE_DEP, FLAG_PERIOD, R_RANK, ARP_NAME_ARR)
  
  df_week %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_apt_des_week",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE_DEP, FLAG_PERIOD, R_RANK, ARP_NAME_ARR)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE_DEP == list_airport$APT_ICAO_CODE[i])
  # 
  #   df_day1 <- df_week %>% filter(ARP_CODE_DEP == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, df_day1)))
  #   #
  #   # for (j in 1:nrow(df)) {
  #   #
  #   #   print(paste(j, df1[[j,1]] == df_day1[[j,1]]))
  #   #
  #   # }
  # }
  # 
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
      DEP = sum(DEP, na.rm = TRUE),
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE),
      .by = c(YEAR, DEP_ARP_PRU_ID, ARR_ARP_PRU_ID)
    ) %>% 
    ungroup() %>% 
    left_join(dim_airport, by = c("DEP_ARP_PRU_ID" = "APT_ID")) %>% 
    left_join(dim_airport, by = c("ARR_ARP_PRU_ID" = "APT_ID"),
              suffix = c("_DEP", "_ARR")) %>% 
    group_by(DEP_ARP_PRU_ID, YEAR) %>%
    arrange(DEP_ARP_PRU_ID, YEAR, desc(DEP), APT_NAME_ARR) %>% 
    mutate(
      R_RANK = case_when( 
        YEAR == current_year ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        YEAR == current_year ~ min_rank(desc(DEP)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        YEAR == current_year-1 ~ min_rank(desc(DEP)),
        .default = NA
      )
    ) %>% 
    ungroup() %>% 
    group_by(APT_NAME_DEP, APT_NAME_ARR) %>% 
    arrange(APT_NAME_DEP, APT_NAME_ARR, YEAR) %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "up") %>% 
  
    fill(RANK, .direction = "down") %>% 
    fill(RANK, .direction = "up") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(LAST_DATA_DAY = max(TO_DATE)) %>% 
    select(
      YEAR,
      ARP_CODE_DEP = APT_ICAO_CODE_DEP,
      ARP_NAME_DEP = APT_NAME_DEP,
      ARP_CODE_ARR = APT_ICAO_CODE_ARR,
      ARP_NAME_ARR = APT_NAME_ARR,
      DEP,
      FROM_DATE,
      TO_DATE,
      LAST_DATA_DAY,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>% 
    arrange(ARP_CODE_DEP, desc(YEAR), R_RANK, ARP_NAME_ARR)
  
  
  df_y2d %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck 
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_apt_des_y2d",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE_DEP, desc(YEAR), R_RANK, ARP_NAME_ARR)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE_DEP == list_airport$APT_ICAO_CODE[i])
  #   ap_ms_app_day1 <- df_y2d %>% filter(ARP_CODE_DEP == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, ap_ms_app_day1)))
  # }

  
  print(mydate)
  
}

# ap ms ----
ap_ms <- function(mydate =  current_day) {
  mydataframe <-  "ap_ms"
  df_app <- import_dataframe(mydataframe)
  
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
    group_by(APT_ICAO_CODE, ENTRY_DATE) %>% 
    arrange(APT_ICAO_CODE, ENTRY_DATE, desc(DEP_ARR), MARKET_SEGMENT) %>% 
    mutate(
      R_RANK = case_when( 
        ENTRY_DATE == mydate ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        ENTRY_DATE == mydate ~ min_rank(desc(DEP_ARR)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        ENTRY_DATE == day_prev_week ~ min_rank(desc(DEP_ARR)),
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
    group_by(APT_NAME, MARKET_SEGMENT) %>% 
    arrange(APT_NAME, MARKET_SEGMENT, desc(ENTRY_DATE)) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    mutate(LAST_DATA_DAY = max(ENTRY_DATE)) %>% 
    select(
      ARP_CODE = APT_ICAO_CODE,
      ARP_NAME = APT_NAME,
      FLAG_PERIOD,
      TO_DATE = ENTRY_DATE,
      LAST_DATA_DAY,
      MARKET_SEGMENT,
      DEP_ARR,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>% 
    arrange(ARP_CODE, FLAG_PERIOD, R_RANK, MARKET_SEGMENT)
  
  df_day %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  
  # # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_ms_day",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE, FLAG_PERIOD, R_RANK, MARKET_SEGMENT)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   ap_ms_app_day1 <- df_day %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, ap_ms_app_day1)))
  # }

  
  ## week ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_week_raw.csv")

  df_week <- df_app %>% 
    filter(
      (ENTRY_DATE >= mydate - days(6) & ENTRY_DATE <= mydate) | 
        (ENTRY_DATE >= day_prev_week - days(6) & ENTRY_DATE <= day_prev_week) | 
        (ENTRY_DATE >= day_prev_year - days(6) & ENTRY_DATE <= day_prev_year) | 
        (ENTRY_DATE >= day_2019 - days(6) & ENTRY_DATE <= day_2019)  
    ) %>% 
    mutate(
      FLAG_PERIOD = case_when( 
        (ENTRY_DATE >= mydate - days(6) & ENTRY_DATE <= mydate) ~ "CURRENT_ROLLING_WEEK",
        (ENTRY_DATE >= day_prev_week - days(6) & ENTRY_DATE <= day_prev_week) ~ "PREV_ROLLING_WEEK",
        (ENTRY_DATE >= day_2019 - days(6) & ENTRY_DATE <= day_2019) ~ "ROLLING_WEEK_2019",
        (ENTRY_DATE >= day_prev_year - days(6) & ENTRY_DATE <= day_prev_year) ~ "ROLLING_WEEK_PREV_YEAR"
      )
    ) %>% 
    group_by(APT_ICAO_CODE, APT_NAME, MARKET_SEGMENT, FLAG_PERIOD) %>%
    summarise(
      DEP_ARR = sum(DEP_ARR, na.rm = TRUE),
      TO_DATE = as_date(max(ENTRY_DATE, na.rm = TRUE)),
      FROM_DATE = as_date(min(ENTRY_DATE, na.rm = TRUE)),
    ) %>% 
    ungroup() %>% 
    group_by(APT_ICAO_CODE, APT_NAME, FLAG_PERIOD) %>%
    arrange(APT_ICAO_CODE, FLAG_PERIOD, desc(DEP_ARR)) %>% 
    mutate(
      R_RANK = case_when( 
        FLAG_PERIOD == "CURRENT_ROLLING_WEEK" ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        FLAG_PERIOD == "CURRENT_ROLLING_WEEK" ~ min_rank(desc(DEP_ARR)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        FLAG_PERIOD == "PREV_ROLLING_WEEK" ~ min_rank(desc(DEP_ARR)),
        .default = NA
      )
    ) %>% 
    ungroup() %>% 
    group_by(APT_NAME, MARKET_SEGMENT) %>% 
    arrange(APT_NAME, MARKET_SEGMENT, FLAG_PERIOD) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(RANK, .direction = "up") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "up") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    mutate(LAST_DATA_DAY = max(TO_DATE)) %>% 
    select(
      ARP_CODE = APT_ICAO_CODE,
      ARP_NAME = APT_NAME,
      FLAG_PERIOD,
      FROM_DATE,
      TO_DATE,
      LAST_DATA_DAY,
      MARKET_SEGMENT,
      DEP_ARR,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>% 
    arrange(ARP_CODE, FLAG_PERIOD, R_RANK, MARKET_SEGMENT)
  
  df_week %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck 
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_ms_week",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE, FLAG_PERIOD, R_RANK, MARKET_SEGMENT)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  #   ap_ms_app_day1 <- df_week %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, ap_ms_app_day1)))
  # }
  
  ## year ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_y2d_raw.csv")
  
  df_y2d <- df_app %>% 
    filter(format(ENTRY_DATE, "%m-%d") <= format(mydate, "%m-%d")) %>%
    filter(year(ENTRY_DATE) %in% c(current_year, current_year-1, 2019)) %>%
    mutate(
      YEAR = year(ENTRY_DATE)
    ) %>% 
    group_by(APT_ICAO_CODE, APT_NAME, MARKET_SEGMENT, YEAR) %>%
    summarise(
      DEP_ARR = sum(DEP_ARR, na.rm = TRUE),
      TO_DATE = as_date(max(ENTRY_DATE, na.rm = TRUE)),
      FROM_DATE = as_date(min(ENTRY_DATE, na.rm = TRUE))
    ) %>% 
    ungroup() %>% 
    group_by(APT_ICAO_CODE, APT_NAME, YEAR) %>%
    arrange(APT_ICAO_CODE, YEAR, desc(DEP_ARR)) %>% 
    mutate(
      R_RANK = case_when( 
        YEAR == current_year ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        YEAR == current_year ~ min_rank(desc(DEP_ARR)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        YEAR == current_year-1 ~ min_rank(desc(DEP_ARR)),
        .default = NA
      )
    ) %>% 
    ungroup() %>% 
    group_by(APT_NAME, MARKET_SEGMENT) %>% 
    arrange(APT_NAME, MARKET_SEGMENT, YEAR) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(RANK, .direction = "up") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "up") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    mutate(LAST_DATA_DAY = max(TO_DATE)) %>% 
    select(
      ARP_CODE = APT_ICAO_CODE,
      ARP_NAME = APT_NAME,
      YEAR,
      FROM_DATE,
      TO_DATE,
      LAST_DATA_DAY,
      MARKET_SEGMENT,
      DEP_ARR,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>% 
    arrange(ARP_CODE, desc(YEAR), R_RANK, MARKET_SEGMENT)
  
  df_y2d %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck 
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_ms_y2d",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE, desc(YEAR), R_RANK, MARKET_SEGMENT)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  #   ap_ms_app_day1 <- df_y2d %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, ap_ms_app_day1)))
  # }
  
  print(mydate)
}

# execute functions ----
# purrr::walk(seq(ymd(til), ymd(wef), by = "-1 day"), ap_ms)

purrr::walk(current_day, ap_ao)
purrr::walk(current_day, ap_st_des)
purrr::walk(current_day, ap_ap_des)
purrr::walk(current_day, ap_ms)

