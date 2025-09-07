library(arrow)
library(duckdb)
library(duckplyr)
library(lubridate)
library(here)
library(RODBC)
library(DBI)

source(here::here("..", "mobile-app", "R", "helpers.R"))
source(here::here("..", "mobile-app", "R", "params.R"))
source(here::here("..", "mobile-app", "R", "dimension_queries.R"))
source(here::here("..", "mobile-app", "R", "duckdb_functions.R"))

# parameters ----
if (!exists("data_day_date")) {current_day <- today() - days(1)} else {current_day <- data_day_date}

# DIMENSIONS ----
## ao group ----  
# same as v_covid_dim_ao but adding ao_id and removing old aos

dim_ao_group <- export_query(dim_ao_grp_query) 

list_ao <- export_query(list_ao_query) %>% 
  left_join(dim_ao_group, by = c("AO_ID", "AO_CODE", "AO_NAME")) %>% 
  select (AO_ID, AO_CODE, AO_NAME, AO_GRP_CODE, AO_GRP_NAME)

list_ao_group <- list_ao %>% group_by(AO_GRP_CODE, AO_GRP_NAME) %>% 
  summarise(AO_GRP_CODE =  max(AO_GRP_CODE), AO_GRP_NAME =  max(AO_GRP_NAME)) %>%
  arrange(AO_GRP_CODE) %>% ungroup()

## airport ----
dim_airport <- export_query(dim_ap_query) 

## iso country ----
dim_iso_country <- export_query(dim_iso_st_query) 


# prep data functions ----
import_dataframe <- function(dfname, con) {
  # import data 
  # dfname <- "ao_traffic_delay"
  mydataframe <- dfname
  myparquetfile <- paste0(mydataframe, "_day_base.parquet")
  
  con <- duck_open()
  df_base <- duck_ingest_parquet(con, here::here(archive_dir_raw, myparquetfile))  # now eager by default
  
  df_alldays <- df_base
  
  rm(df_base)
  
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
      # compute(prudence = "lavish") %>%
      left_join(dim_ao_group, by = c("AO_ID", "AO_CODE")) %>%
      summarise(
        FLIGHT = sum(FLTS, na.rm = TRUE),
        ARR_DELAYED_FLIGHT = sum(DELAYED_FLTS, na.rm = TRUE),
        ARR_ATFM_DELAY = sum(DELAY_AMNT, na.rm = TRUE),
        .by = c(ARR_DATE, AO_GRP_CODE, AO_GRP_NAME, ARR_ARP_PRU_ID)
      ) %>% 
      rename (ENTRY_DATE = ARR_DATE)
    
  } else if (dfname == "ao_traffic_delay"){
    
    df_app <- df_alldays %>% 
      filter(AO_ID != 1777) %>%  # undefined
      # compute(prudence = "lavish") %>%
      left_join(dim_ao_group, by = c("AO_ID", "AO_CODE")) %>%
      summarise(
        FLIGHT = sum(DAY_TFC, na.rm = TRUE),
        .by = c(YEAR, FLIGHT_DATE, AO_GRP_CODE, AO_GRP_NAME)
      )
  } else {
    df_app <- df_alldays
  }
  
  rm(df_alldays)
  
  return(df_app) 
}

# nw day ----
nw_traffic_delay <- function() {
  mydataframe <-  "ao_traffic_delay"
  mydatafile <- paste0(mydataframe, "_day_raw.parquet")
  stakeholder <- str_sub(mydataframe, 1, 2)
  
  con <- duck_open()
  df_app <- import_dataframe(mydataframe, con = con)
  
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
  duck_close(con, clean = TRUE)
  
  
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
}

# nw ao grp ----
nw_ao <- function(mydate =  current_day) {
  mydatasource <-  "ao_traffic_delay"
  
  con <- duck_open()
  df_app <- import_dataframe(mydatasource, con = con)
  
  mydataframe <-  "nw_ao"
  
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
    filter(FLIGHT_DATE %in% c(mydate, day_prev_week, day_2019, day_prev_year)) %>% 
    group_by(FLIGHT_DATE) %>%
    arrange(FLIGHT_DATE, desc(FLIGHT), AO_GRP_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        FLIGHT_DATE == mydate ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        FLIGHT_DATE == mydate ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        FLIGHT_DATE == day_prev_week ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      FLAG_PERIOD = case_when( 
        FLIGHT_DATE == mydate ~ "CURRENT_DAY",
        FLIGHT_DATE == day_prev_week ~ "DAY_PREV_WEEK",
        FLIGHT_DATE == day_2019 ~ "DAY_2019",
        FLIGHT_DATE == day_prev_year ~ "DAY_PREV_YEAR",
      )
    ) %>% 
    ungroup() %>% 
    group_by(AO_GRP_NAME) %>% 
    arrange(AO_GRP_NAME, desc(FLIGHT_DATE)) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 41) %>% 
    select(
      FLAG_PERIOD,
      AO_GRP_CODE,
      AO_GRP_NAME,
      FLIGHT,
      R_RANK,
      RANK,
      RANK_PREV,
      TO_DATE = FLIGHT_DATE
      
    ) %>% 
    arrange(FLAG_PERIOD, R_RANK, AO_GRP_NAME)
  
  
  
  df_day %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  ## week ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_week_raw.csv")
  
  df_week <- df_app %>%
    filter(
      FLIGHT_DATE %in% c(seq.Date(mydate-6, mydate)) |
        FLIGHT_DATE %in% c(seq.Date(day_prev_week-6, day_prev_week))|
        FLIGHT_DATE %in% c(seq.Date(day_2019-6, day_2019)) |
        FLIGHT_DATE %in% c(seq.Date(day_prev_year-6, day_prev_year))
    ) %>% 
    compute(prudence = "lavish") %>%
    mutate(
      FLAG_PERIOD = case_when( 
        (FLIGHT_DATE >= mydate - days(6) & FLIGHT_DATE <= mydate) ~ "CURRENT_ROLLING_WEEK",
        (FLIGHT_DATE >= day_prev_week - days(6) & FLIGHT_DATE <= day_prev_week) ~ "PREV_ROLLING_WEEK",
        (FLIGHT_DATE >= day_2019 - days(6) & FLIGHT_DATE <= day_2019) ~ "ROLLING_WEEK_2019",
        (FLIGHT_DATE >= day_prev_year - days(6) & FLIGHT_DATE <= day_prev_year) ~ "ROLLING_WEEK_PREV_YEAR"
      )
    ) %>% 
    summarise(
      FLIGHT = sum(FLIGHT, na.rm = TRUE),
      .by = c(FLAG_PERIOD, AO_GRP_NAME, AO_GRP_CODE)
    ) %>% 
    ungroup() %>% 
    group_by(FLAG_PERIOD) %>%
    arrange(FLAG_PERIOD, desc(FLIGHT), AO_GRP_NAME) %>% 
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
    group_by(AO_GRP_NAME) %>% 
    arrange(AO_GRP_NAME, FLAG_PERIOD) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 41) %>% 
    mutate(
      FROM_DATE = mydate - days(6),
      TO_DATE = mydate
    ) %>% 
    select(
      FLAG_PERIOD,
      AO_GRP_CODE,
      AO_GRP_NAME,
      FLIGHT,
      R_RANK,
      RANK,
      RANK_PREV,
      FROM_DATE,
      TO_DATE
      
    ) %>% 
    arrange(FLAG_PERIOD, R_RANK, AO_GRP_NAME)
  
  df_week %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
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
      YEAR = year(FLIGHT_DATE)
    ) %>% 
    filter(YEAR %in% c(current_year, current_year-1, 2019)) %>% 
    filter(FLIGHT_DATE %in% y2d_dates) %>% 
    group_by(YEAR) %>% 
    mutate(
      TO_DATE = max(FLIGHT_DATE, na.rm = TRUE),
      FROM_DATE = min(FLIGHT_DATE, na.rm = TRUE)
    ) %>% 
    ungroup() %>% 
    summarise(
      FLIGHT = sum(FLIGHT, na.rm = TRUE),
      .by = c(YEAR, AO_GRP_NAME, AO_GRP_CODE, TO_DATE, FROM_DATE)
    ) %>% 
    group_by(YEAR) %>%
    arrange(YEAR, desc(FLIGHT), AO_GRP_NAME) %>% 
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
      )
    ) %>% 
    ungroup() %>% 
    group_by(AO_GRP_NAME) %>% 
    arrange(AO_GRP_NAME, YEAR) %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "up") %>% 
    
    fill(RANK, .direction = "down") %>% 
    fill(RANK, .direction = "up") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 41) %>% 
    mutate(
      NO_DAYS = as.numeric(TO_DATE - FROM_DATE) +1,
      AVG_FLIGHT = FLIGHT/NO_DAYS
    ) %>%
    select(
      YEAR,
      AO_GRP_CODE,
      AO_GRP_NAME,
      FLIGHT,	
      AVG_FLIGHT,
      R_RANK,
      RANK,
      RANK_PREV,
      FROM_DATE,
      TO_DATE
      
    ) %>% 
    arrange(desc(YEAR), R_RANK, AO_GRP_NAME)
  
  
  df_y2d %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  print(paste(format(now(), "%H:%M:%S"), mydataframe, mydate))
  
  duck_close(con, clean = TRUE)
}

# execute functions ----
# wef <- "2024-01-01"  #included in output
# til <- "2024-01-03"  #included in output
# current_day <- seq(ymd(til), ymd(wef), by = "-1 day")


# ao_traffic_delay()
purrr::walk(current_day, nw_ao)
# purrr::walk(current_day, ao_ap_dep)
# purrr::walk(current_day, ao_ap_pair)
# purrr::walk(current_day, ao_ap_arr_delay)
# 
