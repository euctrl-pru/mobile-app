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
  # mydataframe <- "ao_st_des"
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
    
    } else {
    df_app <- df_alldays
  }

  return(df_app) 
}

df_alldays %>% filter(AO_CODE %in% ("AEA"))

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
  # list_ao_group <- unique(df$AO_GRP_CODE)
  # for (i in 1:length(list_ao_group)) {
  #   df1 <- df %>% filter(AO_GRP_CODE == list_ao_group[[i]])
  # 
  #   df_day1 <- df_day %>% filter(AO_GRP_CODE == list_ao_group[[i]])
  # 
  #   print(paste(list_ao_group[[i]], all.equal(df1, df_day1)))
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
  # list_ao_group <- unique(df$AO_GRP_CODE)
  # for (i in 1:length(list_ao_group)) {
  #   df1 <- df %>% filter(AO_GRP_CODE == list_ao_group[[i]])
  # 
  #   df_day1 <- df_week %>% filter(AO_GRP_CODE == list_ao_group[[i]])
  # 
  #   print(paste(list_ao_group[[i]], all.equal(df1, df_day1)))
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
  # for (i in 1:length(list_ao_group)) {
  #   df1 <- df %>% filter(AO_GRP_CODE == list_ao_group[[i]])
  # 
  #   df_day1 <- df_y2d %>% filter(AO_GRP_CODE == list_ao_group[[i]])
  # 
  #   print(paste(list_ao_group[[i]], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   #
  #   #   print(paste(j, df1[[j,4]] == df_day1[[j,4]]))
  #   #
  #   # }
  # }
  
  print(paste(mydataframe, mydate))
}
  

# execute functions ----
# wef <- "2024-01-01"  #included in output
# til <- "2024-05-17"  #included in output
# current_day <- seq(ymd(til), ymd(wef), by = "-1 day")

# purrr::walk(current_day, ap_ao)
purrr::walk(current_day, ao_st_des)
# purrr::walk(current_day, ap_ap_des)
# purrr::walk(current_day, ap_ms)

