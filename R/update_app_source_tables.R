library(arrow)
library(duckdb)
library(duckplyr)
library(lubridate)
library(here)
library(RODBC)
library(DBI)
library(tidyverse)

source(here::here("..", "mobile-app", "R", "helpers.R"), local = TRUE)
source(here::here("..", "mobile-app", "R", "params.R"), local = TRUE)
source(here::here("..", "mobile-app", "R", "dimensions.R"), local = TRUE)
source(here::here("..", "mobile-app", "R", "duckdb_functions.R"), local = TRUE)

# parameters ----
if (!exists("data_day_date")) {current_day <- today() - days(1)} else {current_day <- data_day_date}

# prep data functions ----
import_dataframe <- function(dfname) {
  # print(paste(format(now(), "%H:%M:%S")))
  
  # import data 
  # dfname <- "ao_traffic_delay"
  stakeholder <- substr(dfname, 1,2)
  # for nw we use aggregations at nw level
  if (stakeholder == 'nw') {
    if (substr(dfname, 4,5) == 'ao'){
      mydataframe <- "ao_traffic_delay"
    } else if (substr(dfname, 4,5) == 'ap') {
      mydataframe <- "ap_traffic_delay"
    } else if (substr(dfname, 4,5) == 'st') {
      mydataframe <- "st_dai"
    } else {
      mydataframe <- dfname
    }
    
  } else {
    # for the other stkh we have double aggregations
    mydataframe <- dfname
  }
    
  # df_base <- read_parquet_duckdb(here(archive_dir_raw, myparquetfile))
  
  con = DBI::dbConnect(duckdb::duckdb())
  myyears <- c(2018:year(current_day))
  
  df_base <- read_partitioned_parquet_duckdb (con = con,
                                              mydataframe = mydataframe,,
                                              years = myyears)
  

  # register dim tables in the connection so they can be joined
  exists <- DBI::dbExistsTable(con, "list_ao_reg")
  if (!exists) duckdb::duckdb_register(con, "list_ao_reg", as.data.frame(list_ao))
  
  exists <- DBI::dbExistsTable(con, "dim_ao_group_reg")
  if (!exists) duckdb::duckdb_register(con, "dim_ao_group_reg", as.data.frame(dim_ao_group))
  
  exists <- DBI::dbExistsTable(con, "list_airport_reg")
  if (!exists) duckdb::duckdb_register(con, "list_airport_reg", as.data.frame(list_airport))
  
  exists <- DBI::dbExistsTable(con, "list_airport_extended_reg")
  if (!exists) duckdb::duckdb_register(con, "list_airport_extended_reg", as.data.frame(list_airport_extended))
  
  exists <- DBI::dbExistsTable(con, "list_iso_country_reg")
  if (!exists) duckdb::duckdb_register(con, "list_iso_country_reg", as.data.frame(list_iso_country))

  exists <- DBI::dbExistsTable(con, "dim_iso_country_reg")
  if (!exists) duckdb::duckdb_register(con, "dim_iso_country_reg", as.data.frame(dim_iso_country))
  
  exists <- DBI::dbExistsTable(con, "list_icao_country_spain_reg")
  if (!exists) duckdb::duckdb_register(con, "list_icao_country_spain_reg", as.data.frame(list_icao_country_spain))

  exists <- DBI::dbExistsTable(con, "list_marktet_segment_app_reg")
  if (!exists) duckdb::duckdb_register(con, "list_marktet_segment_app_reg", as.data.frame(list_marktet_segment_app))
  
  
  # filter to keep days and stakeholders needed for the app
  if (stakeholder == "ao") {
    df_alldays <- df_base %>% 
      filter(AO_ID %in% list_ao$AO_ID)  
    
  } else if (stakeholder == "ap") {
    if ("BK_AP_ID" %in% colnames(df_base)) {
      df_alldays <- df_base %>% 
        filter(BK_AP_ID %in% list_airport_extended$BK_AP_ID)  
      if ("ADEP_CODE" %in% colnames(df_base)) {
        df_alldays <- df_alldays %>% 
          rename(AP_CODE = ADEP_CODE)  
      }
    }
  } else if (stakeholder == "st") {
    if (mydataframe == "st_dai") {
      df_alldays <- df_base %>%
        filter(COUNTRY_CODE %in% list_icao_country_spain$COUNTRY_CODE)  
      
      
    } else {
      df_alldays <- df_base %>% 
        filter(ISO_CT_CODE %in% list_iso_country$ISO_COUNTRY_CODE)  %>% 
        left_join(tbl(con, "list_iso_country_reg"),
                  by = c("ISO_CT_CODE" = "ISO_COUNTRY_CODE"), 
                  keep = FALSE )
      }

  } else if (stakeholder == "nw") {
    df_alldays <- df_base %>% 
      mutate(UNIT = 'NM')
    
  } else {
    df_alldays <- df_base
  }
  
  
  rm(df_base)

  # pre-process data
  if (dfname == "ao_st_des") {
      df_app <- df_alldays %>% 
      filter(ARR_ISO_CT_CODE != "##") %>%  # filter unknown
      left_join(tbl(con, "list_ao_reg"), by = c("AO_ID", "AO_CODE")) %>% 
      summarise(
        FLIGHT = sum(FLIGHT, na.rm = TRUE),
        .by = c(ENTRY_DATE, AO_GRP_CODE, AO_GRP_NAME, ARR_ISO_CT_CODE)
      )  
    
  } else if (dfname == "ao_ap_dep"){
    df_app <- df_alldays %>% 
      filter(BK_AP_ID != -1) %>%  # filter unknown
      left_join(tbl(con, "list_ao_reg"), by = c("AO_ID", "AO_CODE")) %>% 
      summarise(
        FLIGHT = sum(FLIGHT, na.rm = TRUE),
        .by = c(ENTRY_DATE, AO_GRP_CODE, AO_GRP_NAME, BK_AP_ID)
      )
  } else if (dfname == "ao_ap_pair"){
    df_app <- df_alldays %>% 
      filter(BK_AP_ID_1 != -1) %>%  # filter unknown
      filter(BK_AP_ID_2 != -1) %>%  # filter unknown
      left_join(tbl(con, "list_ao_reg"), by = c("AO_ID", "AO_CODE")) %>% 
      summarise(
        FLIGHT = sum(FLIGHT, na.rm = TRUE),
        .by = c(ENTRY_DATE, AO_GRP_CODE, AO_GRP_NAME, ARP_PAIR_ID)
      )
  } else if (dfname == "ao_ap_arr_delay"){
    df_app <- df_alldays %>% 
      filter(BK_AP_ID != -1) %>%  # filter unknown
      left_join(tbl(con, "list_ao_reg"), by = c("AO_ID", "AO_CODE")) %>% 
      summarise(
        FLIGHT = sum(FLTS, na.rm = TRUE),
        ARR_DELAYED_FLIGHT = sum(DELAYED_FLTS, na.rm = TRUE),
        ARR_ATFM_DELAY = sum(DELAY_AMNT, na.rm = TRUE),
        .by = c(ARR_DATE, AO_GRP_CODE, AO_GRP_NAME, BK_AP_ID)
      )
    
  # } else if (dfname == "ao_traffic_delay"){
  #   
  #   df_app <- df_alldays %>% 
  #     # left_join(tbl(con, "list_ao_reg"), by = c("AO_ID", "AO_CODE")) %>% 
  #     left_join(list_ao, by = c("AO_ID", "AO_CODE")) %>%
  #     summarise(
  #       DAY_TFC = sum(DAY_TFC, na.rm = TRUE),
  #       DAY_DLY = sum(DAY_DLY, na.rm = TRUE),
  #       DAY_DLY_15 = sum(DAY_DLY_15, na.rm = TRUE),
  #       DAY_DELAYED_TFC = sum(DAY_DELAYED_TFC, na.rm = TRUE),
  #       DAY_DELAYED_TFC_15 = sum(DAY_DELAYED_TFC_15, na.rm = TRUE),
  #       .by = c(YEAR, MONTH, WEEK,	WEEK_NB_YEAR,	DAY_TYPE,	DAY_OF_WEEK, FLIGHT_DATE, AO_GRP_CODE, AO_GRP_NAME)
  #     )
  } else if (dfname == "ap_ao") {
        df_app <- df_alldays %>% 
        filter(AO_ID != 1777) %>%  # undefined
        left_join(tbl(con, "dim_ao_group_reg"), by = c("AO_ID", "AO_CODE")) %>% 
        summarise(
          DEP_ARR = sum(DEP_ARR, na.rm = TRUE),
          .by = c(ENTRY_DATE, BK_AP_ID, AP_CODE, AO_GRP_CODE, AO_GRP_NAME)
        ) %>% 
        left_join(tbl(con, "list_airport_extended_reg"), by = c("BK_AP_ID")) %>% 
        rename(AP_NAME = EC_AP_NAME)
      
  } else if (dfname == "ap_st_des" | dfname == "ap_ap_des" | dfname == "ap_ms") {
    df_app <- df_alldays %>% 
      left_join(tbl(con, "list_airport_extended_reg"), by = c("BK_AP_ID")) %>%
      select(-EC_AP_CODE) %>% 
      rename(AP_NAME = EC_AP_NAME)
    
    if (dfname == "ap_ms") {
      df_app <- df_app %>% 
        mutate(MS_ID = if_else(MS_ID %in% c(2,3,4, 6, 7,8), MS_ID, 9)) %>% 
        summarise(
          DEP_ARR = sum(DEP_ARR, na.rm = TRUE),
          .by = c(ENTRY_DATE, BK_AP_ID, AP_CODE, AP_NAME, MS_ID)
        ) 
    }
  
  } else if (dfname == "st_ao") {
    df_app <- df_alldays %>% 
      filter(AO_ID != 1777) %>%  # undefined
      left_join(tbl(con, "dim_ao_group_reg"), by = c("AO_ID", "AO_CODE")) %>% 
      summarise(
        FLIGHT = sum(FLIGHT, na.rm = TRUE),
        .by = c(ENTRY_DATE, ISO_CT_CODE, COUNTRY_NAME, AO_GRP_CODE, AO_GRP_NAME)
      ) 
    
  } else if (dfname == "nw_ao") {
    df_app <- df_alldays %>% 
      filter(AO_ID != 1777) %>%  # undefined
      left_join(tbl(con, "dim_ao_group_reg"), by = c("AO_ID", "AO_CODE")) %>% 
      summarise(
        FLIGHT = sum(DAY_TFC, na.rm = TRUE),
        .by = c(FLIGHT_DATE, AO_GRP_CODE, AO_GRP_NAME, UNIT)
      ) 
    
  } else if (dfname == "nw_st_dai") {
    df_app <- df_alldays %>% 
      filter(!(COUNTRY_CODE %in% c('LE', 'GC'))) %>%  # spain separated
      left_join(tbl(con, "list_icao_country_spain_reg"), by = c("COUNTRY_CODE")) %>% 
      left_join(tbl(con, "dim_iso_country_reg"), by = c("COUNTRY_NAME")) 
      
  } else {
    df_app <- df_alldays
  }
  
  rm(df_alldays)
  
  # df_app_c <- collect(df_app)
  # print(paste(format(now(), "%H:%M:%S")))
  
  return(df_app) 
  DBI::dbDisconnect(con, shutdown = TRUE)
  
}


stk_daily <- function(df, 
                      mydate =  today()- days(1),
                      stk_id = AO_GRP_NAME,
                      date_field = ENTRY_DATE,
                      tfc_field = FLIGHT,
                      dly_field = NULL,
                      dlyed_field = NULL,
                      dlyed15_field = NULL,
                      dly_ert_field = NULL,
                      dly_arp_field = NULL
) 
{
  # df            = "ao_traffic_delay"
  # mydate        = current_day
  # stk_id        = quo(AO_ID)
  # date_field    = quo(FLIGHT_DATE)
  # tfc_field     = quo(DAY_TFC)
  # dly_field     = quo(DAY_DLY)
  # dlyed_field   = quo(DAY_DELAYED_TFC)
  # dlyed15_field = quo(DAY_DELAYED_TFC_15)

  
  #### define stakeholder
  stakeholder <- str_sub(df, 1, 2)
  
  #### import data
  df_raw <- import_dataframe(df)

  #### preprocess delay fields
  if (df == "sp_traffic_delay") {
    df_pp <- df_raw %>% 
      mutate(
        TDM_ERT_CS = TDM_ERT_C + TDM_ERT_S,
        TDM_ERT_IT = TDM_ERT_I + TDM_ERT_T,
        TDM_ERT_WD = TDM_ERT_W + TDM_ERT_D,
        TDM_ERT_OTHER = TDM_ERT - TDM_ERT_CS - TDM_ERT_IT - TDM_ERT_WD,
        TDM_ERT_G = 0
        )
    
    
  } else if (df == "ao_traffic_delay") {
    df_pp <- df_raw %>% 
      # to ensure the structure is the same
      mutate(
        DAY_DLY_CS = NA,
        DAY_DLY_IT = NA,
        DAY_DLY_WD = NA,
        DAY_DLY_OTHER = NA,
        DAY_DLY_G = NA
      )
    
  } else if (df == "ap_traffic_delay") {
    df_pp <- df_raw %>% 
      # to ensure the structure is the same
      mutate(
        TDM_ARP_ARR_G = TDM_ARP_ARR_G,
        TDM_ARP_ARR_CS = TDM_ARP_ARR_C + TDM_ARP_ARR_S,
        TDM_ARP_ARR_IT = TDM_ARP_ARR_I + TDM_ARP_ARR_T,
        TDM_ARP_ARR_WD = TDM_ARP_ARR_W + TDM_ARP_ARR_D,
        TDM_ARP_ARR_OTHER = TDM_ARP_ARR - TDM_ARP_ARR_CS - TDM_ARP_ARR_IT - TDM_ARP_ARR_WD - TDM_ARP_ARR_G,
      )
    
  } else if (df == "nw_delay_cause") {
    df_pp <- df_raw %>% 
      # to ensure the structure is the same
      mutate(
        TDM_G = TDM_ARP_G + TDM_ERT_G,
        TDM_CS = TDM_ARP_C + TDM_ERT_C + TDM_ARP_S + TDM_ERT_S, 
        TDM_IT = TDM_ARP_I + TDM_ERT_I + TDM_ARP_T + TDM_ERT_T, 
        TDM_WD = TDM_ARP_W + TDM_ERT_W + TDM_ARP_D + TDM_ERT_D, 
        TDM_OTHER = TDM - TDM_CS - TDM_IT - TDM_WD - TDM_G,
        AREA = "NW"
      )
    
  } else {
    df_pp <- df_raw
    
  }
  
  ### capture field names
  dly_quo   <- rlang::enquo(dly_field)
  if (!rlang::quo_is_null(dly_quo)) {dly_name <- rlang::as_name(dly_quo)}
  dlyed15_quo <- rlang::enquo(dlyed15_field) 
  dly_ert_quo <- rlang::enquo(dly_ert_field) 
  dly_arp_quo <- rlang::enquo(dly_arp_field) 
  
  ### rename fields for data processing  
  if (rlang::quo_is_null(dly_quo)) {
    df_app <- df_pp %>%
      mutate(
        FLIGHT_DATE = {{ date_field }},
        STK_ID = {{ stk_id }},
        TFC = {{ tfc_field }},
        TFC_DLY = NA,
        DLY = NA,
        DLY_G = NA,
        DLY_CS = NA,
        DLY_IT = NA,
        DLY_WD = NA,
        DLY_OTHER = NA,
        DLYED = NA,
        DLYED_15 = NA,
        DLY_ERT = NA,
        DLY_ARP = NA
      ) %>%
      select(STK_ID, FLIGHT_DATE, TFC, TFC_DLY, DLY, DLY_G, DLY_CS, DLY_IT, DLY_WD, DLY_OTHER, DLYED, DLYED_15, DLY_ERT, DLY_ARP) %>% 
    collect()
  } else {
    
    if(df == "ap_traffic_delay") {
      df_pp <- df_pp %>% mutate(TFC_DLY = ARR)
    } else {
      df_pp <- df_pp %>% mutate(TFC_DLY = {{ tfc_field }})
      }
    
    df_app <- df_pp %>%
      mutate(
        FLIGHT_DATE = {{ date_field }},
        STK_ID = {{ stk_id }},
        TFC = {{ tfc_field }},
        DLY = {{ dly_field }},
        DLY_G = .data[[paste0(dly_name, "_G")]],
        DLY_CS = .data[[paste0(dly_name, "_CS")]],
        DLY_IT = .data[[paste0(dly_name, "_IT")]],
        DLY_WD = .data[[paste0(dly_name, "_WD")]],
        DLY_OTHER = .data[[paste0(dly_name, "_OTHER")]],
        DLYED = {{ dlyed_field }}

      ) %>%
      collect() %>% 
      mutate(
        DLYED_15 = if (rlang::quo_is_null(dlyed15_quo)) NA else (!!dlyed15_quo),
        DLY_ERT = if (rlang::quo_is_null(dly_ert_quo)) NA else (!!dly_ert_quo),
        DLY_ARP = if (rlang::quo_is_null(dly_arp_quo)) NA else (!!dly_arp_quo)
      ) %>% 
      select(STK_ID, FLIGHT_DATE, TFC, TFC_DLY, DLY, DLY_G, DLY_CS, DLY_IT, DLY_WD, DLY_OTHER, DLYED, DLYED_15, DLY_ERT, DLY_ARP) 
    
  }
  
  ### group at ao group level for aos
  if (stakeholder == "ao") {
    df_app_sum <- df_app %>% 
      left_join(list_ao, by = c("STK_ID" = "AO_ID"), keep = FALSE) %>% 
      filter(FLIGHT_DATE >= WEF & FLIGHT_DATE <= TIL) %>% 
      summarise(
        TFC = sum(TFC, na.rm = TRUE),
        TFC_DLY = sum(TFC_DLY, na.rm = TRUE),
        DLY = sum(DLY, na.rm = TRUE),
        DLY_G = sum(DLY_G, na.rm = TRUE),
        DLY_CS = sum(DLY_CS, na.rm = TRUE),
        DLY_IT = sum(DLY_IT, na.rm = TRUE),
        DLY_WD = sum(DLY_WD, na.rm = TRUE),
        DLY_OTHER = sum(DLY_OTHER, na.rm = TRUE),
        DLYED = sum(DLYED, na.rm = TRUE),
        DLYED_15= sum(DLYED_15, na.rm = TRUE),
        DLY_ERT= sum(DLY_ERT, na.rm = TRUE),
        DLY_ARP= sum(DLY_ARP, na.rm = TRUE),
        .by = c(AO_GRP_CODE, AO_GRP_NAME, FLIGHT_DATE)
      ) %>% 
      mutate(
        STK_ID = AO_GRP_NAME
      )
  } else {
    df_app_sum <- df_app
  }
  
  
  #### find max date in dataset and define current year
  mydate <- df_app_sum %>% summarise(max(FLIGHT_DATE, na.rm = TRUE)) %>% pull()
  current_year = year(mydate)
  
  #### create date sequence
  year_from <- paste0(2018, "-12-24")
  year_til <- paste0(current_year, "-12-31")
  days_sequence <- seq(ymd(year_from), ymd(year_til), by = "1 day") %>%
    as_tibble() %>%
    select(FLIGHT_DATE = value) 
  
  #### rename fields in list tables to facilitate joins
  list_airport_extended_f <- list_airport_extended %>% 
    rename(STK_ID = BK_AP_ID, STK_CODE = EC_AP_CODE, STK_NAME = EC_AP_NAME)
  
  list_ao_group_f <- list_ao_group %>% 
    mutate(STK_ID = AO_GRP_NAME, STK_CODE = AO_GRP_CODE, STK_NAME = AO_GRP_NAME) %>% 
    select(STK_ID, STK_CODE, STK_NAME)
  
  list_ansp_f <- list_ansp %>% 
    rename(STK_ID = ANSP_ID, STK_CODE = ANSP_CODE, STK_NAME = ANSP_NAME)
  
  list_icao_country_f <- list_icao_country %>% 
    mutate(STK_ID = COUNTRY_CODE, STK_CODE = COUNTRY_CODE, STK_NAME = COUNTRY_NAME) %>% 
    select(STK_ID, STK_CODE, STK_NAME)
  
  list_nw_f <- data.frame(STK_ID = c("NW"), STK_CODE = c("NW"), STK_NAME = c("Total Network Management Area"))
  
  #### combine with ap list to get full sequence
  list_stk <- case_when(
    stakeholder == "nw" ~ "list_nw_f",
    stakeholder == "ao" ~ "list_ao_group_f",
    stakeholder == "ap" ~ "list_airport_extended_f",
    stakeholder == "sp" ~ "list_ansp_f",
    stakeholder == "st" ~ "list_icao_country_f" # only for dai. daio comes processed already
    
  )
  
  days_stk <- crossing(days_sequence, get(list_stk)) %>% 
    arrange(STK_ID, FLIGHT_DATE) 
  
  
  df_day_year <- days_stk %>%
    left_join(df_app_sum, by = c("FLIGHT_DATE", "STK_ID")) %>% 
    arrange(STK_ID, FLIGHT_DATE) %>% 
    group_by(STK_ID) %>% 
    # day/week calculations
    mutate (
      YEAR =  year(FLIGHT_DATE),
      # traffic
      DAY_TFC = coalesce(TFC,0),
      RWK_AVG_TFC = rollsum(DAY_TFC, 7, fill = NA, align = "right") / 7,
      DAY_TFC_DLY = coalesce(TFC_DLY,0),
      RWK_AVG_TFC_DLY = rollsum(DAY_TFC_DLY, 7, fill = NA, align = "right") / 7,
      
      # delay
      DAY_DLY = coalesce(DLY,0),
      DAY_DLY_ERT = coalesce(DLY_ERT,0),
      DAY_DLY_ARP = coalesce(DLY_ARP,0),
      RWK_AVG_DLY = rollsum(DAY_DLY, 7, fill = NA, align = "right") / 7,
      RWK_AVG_DLY_ERT = rollsum(DAY_DLY_ERT, 7, fill = NA, align = "right") / 7,
      RWK_AVG_DLY_ARP = rollsum(DAY_DLY_ARP, 7, fill = NA, align = "right") / 7,
      
      # delay cause
      DAY_DLY_G = coalesce(DLY_G,0),
      DAY_DLY_CS = coalesce(DLY_CS,0),
      DAY_DLY_IT = coalesce(DLY_IT,0),
      DAY_DLY_WD = coalesce(DLY_WD,0),
      DAY_DLY_OTHER = coalesce(DLY_OTHER,0),
      
      RWK_AVG_DLY_G = rollsum(DAY_DLY_G, 7, fill = NA, align = "right") / 7,
      RWK_AVG_DLY_CS = rollsum(DAY_DLY_CS, 7, fill = NA, align = "right") / 7,
      RWK_AVG_DLY_IT = rollsum(DAY_DLY_IT, 7, fill = NA, align = "right") / 7,
      RWK_AVG_DLY_WD = rollsum(DAY_DLY_WD, 7, fill = NA, align = "right") / 7,
      RWK_AVG_DLY_OTHER = rollsum(DAY_DLY_OTHER, 7, fill = NA, align = "right") / 7,
      
      # delay/flight
      DAY_DLY_FLT = if_else(DAY_TFC_DLY == 0, 0, DAY_DLY / DAY_TFC_DLY),
      DAY_DLY_ERT_FLT = if_else(DAY_TFC_DLY == 0, 0, DAY_DLY_ERT / DAY_TFC_DLY),
      DAY_DLY_ARP_FLT = if_else(DAY_TFC_DLY == 0, 0, DAY_DLY_ARP / DAY_TFC_DLY),
      RWK_DLY_FLT = if_else(RWK_AVG_TFC_DLY == 0, 0, RWK_AVG_DLY / RWK_AVG_TFC_DLY),
      RWK_DLY_ERT_FLT = if_else(RWK_AVG_TFC_DLY == 0, 0, RWK_AVG_DLY_ERT / RWK_AVG_TFC_DLY),
      RWK_DLY_ARP_FLT = if_else(RWK_AVG_TFC_DLY == 0, 0, RWK_AVG_DLY_ARP / RWK_AVG_TFC_DLY),
      
      # delayed flights
      DAY_DLYED = coalesce(DLYED,0),
      RWK_AVG_DLYED = rollsum(DAY_DLYED, 7, fill = NA, align = "right") / 7,
      DAY_DLYED_PERC = if_else(DAY_TFC_DLY == 0, 0, DAY_DLYED / DAY_TFC_DLY),
      RWK_DLYED_PERC = if_else(RWK_AVG_TFC_DLY == 0, 0, RWK_AVG_DLYED / RWK_AVG_TFC_DLY),
      
      # delayed flights 15 min
      DAY_DLYED_15 = coalesce(DLYED_15,0),
      RWK_AVG_DLYED_15 = rollsum(DAY_DLYED_15, 7, fill = NA, align = "right") / 7,
      DAY_DLYED_15_PERC = if_else(DAY_TFC_DLY == 0, 0, DAY_DLYED_15 / DAY_TFC_DLY),
      RWK_DLYED_15_PERC = if_else(RWK_AVG_TFC_DLY == 0, 0, RWK_AVG_DLYED_15 / RWK_AVG_TFC_DLY)
    ) %>% 
    # ensure week values beyond data day are NA 
    mutate(
      mutate(across(starts_with("RWK"), ~ replace(.x, FLIGHT_DATE > mydate, NA)))
    ) %>% 
    # y2d calculations
    group_by(STK_ID, YEAR) %>%
    mutate(
      # traffic
      Y2D_TFC = cumsum(DAY_TFC),
      Y2D_AVG_TFC = cumsum(DAY_TFC) / row_number(),
      Y2D_TFC_DLY = cumsum(DAY_TFC_DLY),
      Y2D_AVG_TFC_DLY = cumsum(DAY_TFC_DLY) / row_number(),
      
      # delay
      Y2D_DLY = cumsum(DAY_DLY),
      Y2D_DLY_ERT = cumsum(DAY_DLY_ERT),
      Y2D_DLY_ARP = cumsum(DAY_DLY_ARP),
      Y2D_AVG_DLY = cumsum(DAY_DLY) / row_number(),
      Y2D_AVG_DLY_ERT = cumsum(DAY_DLY_ERT) / row_number(),
      Y2D_AVG_DLY_ARP = cumsum(DAY_DLY_ARP) / row_number(),
      
      # delay/flight
      Y2D_DLY_FLT = if_else(Y2D_TFC_DLY == 0, 0, Y2D_DLY / Y2D_TFC_DLY),
      Y2D_DLY_ERT_FLT = if_else(Y2D_TFC_DLY == 0, 0, Y2D_DLY_ERT / Y2D_TFC_DLY),
      Y2D_DLY_ARP_FLT = if_else(Y2D_TFC_DLY == 0, 0, Y2D_DLY_ARP / Y2D_TFC_DLY),
      
      # delayed flights
      Y2D_DLYED = cumsum(DAY_DLYED),
      Y2D_DLYED_PERC = if_else(Y2D_TFC_DLY == 0, 0, Y2D_DLYED / Y2D_TFC_DLY),
      
      # delayed flights
      Y2D_DLYED_15 = cumsum(DAY_DLYED_15),
      Y2D_DLYED_15_PERC = if_else(Y2D_TFC_DLY == 0, 0, Y2D_DLYED_15 / Y2D_TFC_DLY)
      
    )%>% 
    ungroup () 
  
  ## split table ----
  df_day <- df_day_year %>% 
    mutate(
      FLIGHT_DATE_PREV_YEAR = FLIGHT_DATE - days(364),
      FLIGHT_DATE_2019 = FLIGHT_DATE - days((YEAR-2019)*364+ floor((YEAR - 2019) / 4) * 7),
      FLIGHT_DATE_2020 = FLIGHT_DATE - days((YEAR-2020)*364+ floor((YEAR - 2020) / 4) * 7),
      FLIGHT_DATE_2019_SD = FLIGHT_DATE %m-% years(YEAR-2019),
      FLIGHT_DATE_PREV_YEAR_SD = FLIGHT_DATE %m-% years(1) 
    ) %>% 
    left_join(select(df_day_year, STK_ID, YEAR, FLIGHT_DATE, starts_with(c("Y2D"))), by = c("STK_ID", "FLIGHT_DATE_PREV_YEAR_SD" = "FLIGHT_DATE"), suffix = c("","_PREV_YEAR")) %>% 
    left_join(select(df_day_year, STK_ID, YEAR, FLIGHT_DATE, starts_with(c("Y2D"))), by = c("STK_ID", "FLIGHT_DATE_2019_SD" = "FLIGHT_DATE"), suffix = c("","_2019")) %>% 
    left_join(select(df_day_year, STK_ID, YEAR, FLIGHT_DATE, starts_with(c("DAY", "RWK"))), by = c("STK_ID", "FLIGHT_DATE_2019" = "FLIGHT_DATE"), suffix = c("","_2019")) %>% 
    left_join(select(df_day_year, STK_ID, YEAR, FLIGHT_DATE, starts_with(c("DAY", "RWK"))), by = c("STK_ID", "FLIGHT_DATE_PREV_YEAR" = "FLIGHT_DATE"), suffix = c("","_PREV_YEAR")) %>%
    left_join(select(df_day_year, STK_ID, YEAR, FLIGHT_DATE, RWK_AVG_TFC), by = c("STK_ID", "FLIGHT_DATE_2020" = "FLIGHT_DATE"), suffix = c("","_2020")) %>% 
    arrange(STK_ID, FLIGHT_DATE) %>% 
    group_by(STK_ID) %>% 
    mutate(
      # prev week
      ##date
      FLIGHT_DATE_PREV_WEEK = lag(FLIGHT_DATE, 7),
      ##traffic
      DAY_TFC_PREV_WEEK = lag(DAY_TFC, 7),
      RWK_AVG_TFC_PREV_WEEK = lag(RWK_AVG_TFC, 7),
      ##delay
      DAY_DLY_PREV_WEEK = lag(DAY_DLY, 7),
      DAY_DLY_ERT_PREV_WEEK = lag(DAY_DLY_ERT, 7),
      DAY_DLY_ARP_PREV_WEEK = lag(DAY_DLY_ARP, 7),
      RWK_AVG_DLY_PREV_WEEK = lag(RWK_AVG_DLY, 7),
      RWK_AVG_DLY_ERT_PREV_WEEK = lag(RWK_AVG_DLY_ERT, 7),
      RWK_AVG_DLY_ARP_PREV_WEEK = lag(RWK_AVG_DLY_ARP, 7),
      ##delay/flight
      DAY_DLY_FLT_PREV_WEEK = lag(DAY_DLY_FLT, 7),
      DAY_DLY_ERT_FLT_PREV_WEEK = lag(DAY_DLY_ERT_FLT, 7),
      DAY_DLY_ARP_FLT_PREV_WEEK = lag(DAY_DLY_ARP_FLT, 7),
      RWK_DLY_FLT_PREV_WEEK = lag(RWK_DLY_FLT, 7),
      RWK_DLY_ERT_FLT_PREV_WEEK = lag(RWK_DLY_ERT_FLT, 7),
      RWK_DLY_ARP_FLT_PREV_WEEK = lag(RWK_DLY_ARP_FLT, 7),
      ##delayed flights
      DAY_DLYED_PERC_PREV_WEEK = lag(DAY_DLYED_PERC, 7),
      RWK_DLYED_PERC_PREV_WEEK = lag(RWK_DLYED_PERC, 7),
      ##delayed flights 15 min
      DAY_DLYED_15_PERC_PREV_WEEK = lag(DAY_DLYED_15_PERC, 7),
      RWK_DLYED_15_PERC_PREV_WEEK = lag(RWK_DLYED_15_PERC, 7),
      
      # dif prev week
      ##traffic
      DAY_TFC_DIF_PREV_WEEK = coalesce(DAY_TFC,0) - coalesce(DAY_TFC_PREV_WEEK, 0),
      DAY_TFC_DIF_PREV_WEEK_PERC = if_else(DAY_TFC_PREV_WEEK == 0, NA, DAY_TFC/ DAY_TFC_PREV_WEEK) -1,
      RWK_TFC_DIF_PREV_WEEK_PERC = if_else(RWK_AVG_TFC_PREV_WEEK == 0, NA, RWK_AVG_TFC/ RWK_AVG_TFC_PREV_WEEK)-1,
      ##delay
      DAY_DLY_DIF_PREV_WEEK_PERC = if_else(DAY_DLY_PREV_WEEK == 0, NA, DAY_DLY/ DAY_DLY_PREV_WEEK) -1,
      RWK_DLY_DIF_PREV_WEEK_PERC = if_else(RWK_AVG_DLY_PREV_WEEK == 0, NA, RWK_AVG_DLY/ RWK_AVG_DLY_PREV_WEEK)-1,
      ##delay/flight
      DAY_DLY_FLT_DIF_PREV_WEEK_PERC = if_else(DAY_DLY_FLT_PREV_WEEK == 0, NA, DAY_DLY_FLT/ DAY_DLY_FLT_PREV_WEEK) -1,
      RWK_DLY_FLT_DIF_PREV_WEEK_PERC = if_else(RWK_DLY_FLT_PREV_WEEK == 0, NA, RWK_DLY_FLT/ RWK_DLY_FLT_PREV_WEEK)-1,
      ##delayed flights
      DAY_DLYED_PERC_DIF_PREV_WEEK = DAY_DLYED_PERC - DAY_DLYED_PERC_PREV_WEEK,
      RWK_DLYED_PERC_DIF_PREV_WEEK = RWK_DLYED_PERC - RWK_DLYED_PERC_PREV_WEEK,
      ##delayed flights 15 min
      DAY_DLYED_15_PERC_DIF_PREV_WEEK = DAY_DLYED_15_PERC - DAY_DLYED_15_PERC_PREV_WEEK,
      RWK_DLYED_15_PERC_DIF_PREV_WEEK = RWK_DLYED_15_PERC - RWK_DLYED_15_PERC_PREV_WEEK,
      
      # # prev year
      # ##date
      # FLIGHT_DATE_PREV_YEAR = lag(FLIGHT_DATE, 364),
      # ##traffic
      # DAY_TFC_PREV_YEAR = lag(DAY_TFC , 364),
      # RWK_AVG_TFC_PREV_YEAR = lag(RWK_AVG_TFC, 364),
      # ##delay
      # DAY_DLY_PREV_YEAR = lag(DAY_DLY, 364),
      # RWK_AVG_DLY_PREV_YEAR = lag(RWK_AVG_DLY, 364),
      # ##delay ert
      # DAY_DLY_ERT_PREV_YEAR = lag(DAY_DLY_ERT, 364),
      # RWK_AVG_DLY_ERT_PREV_YEAR = lag(RWK_AVG_DLY_ERT, 364),
      # ##delay arp
      # DAY_DLY_ARP_PREV_YEAR = lag(DAY_DLY, 364),
      # RWK_AVG_DLY_ARP_PREV_YEAR = lag(RWK_AVG_DLY_ARP, 364),
      # ##delay/flight
      # DAY_DLY_ERT_FLT_PREV_YEAR = lag(DAY_DLY_ERT_FLT, 364),
      # RWK_DLY_ERT_FLT_PREV_YEAR = lag(RWK_DLY_ERT_FLT, 364),
      # ##delay/flight ert
      # DAY_DLY_ARP_FLT_PREV_YEAR = lag(DAY_DLY_ARP_FLT, 364),
      # RWK_DLY_ARP_FLT_PREV_YEAR = lag(RWK_DLY_ARP_FLT, 364),
      # ##delay/flight arp
      # DAY_DLY_FLT_PREV_YEAR = lag(DAY_DLY_FLT, 364),
      # RWK_DLY_FLT_PREV_YEAR = lag(RWK_DLY_FLT, 364),
      # ##delayed flights
      # DAY_DLYED_PERC_PREV_YEAR = lag(DAY_DLYED_PERC, 364),
      # RWK_DLYED_PERC_PREV_YEAR = lag(RWK_DLYED_PERC, 364),
      # ##delayed flights 15 min
      # DAY_DLYED_15_PERC_PREV_YEAR = lag(DAY_DLYED_15_PERC, 364),
      # RWK_DLYED_15_PERC_PREV_YEAR = lag(RWK_DLYED_15_PERC, 364),
      
      # dif prev year
      ##traffic
      DAY_TFC_DIF_PREV_YEAR = coalesce(DAY_TFC, 0) - coalesce(DAY_TFC_PREV_YEAR, 0),
      DAY_TFC_DIF_PREV_YEAR_PERC = if_else(DAY_TFC_PREV_YEAR == 0, NA, DAY_TFC/ DAY_TFC_PREV_YEAR)-1,
      RWK_TFC_DIF_PREV_YEAR_PERC = if_else(RWK_AVG_TFC_PREV_YEAR == 0, NA, RWK_AVG_TFC/ RWK_AVG_TFC_PREV_YEAR)-1,
      Y2D_TFC_DIF_PREV_YEAR_PERC = if_else(Y2D_AVG_TFC_PREV_YEAR == 0, NA, Y2D_AVG_TFC/ Y2D_AVG_TFC_PREV_YEAR)-1,
      ##delay
      DAY_DLY_DIF_PREV_YEAR_PERC = if_else(DAY_DLY_PREV_YEAR == 0, NA, DAY_DLY/ DAY_DLY_PREV_YEAR)-1,
      RWK_DLY_DIF_PREV_YEAR_PERC = if_else(RWK_AVG_DLY_PREV_YEAR == 0, NA, RWK_AVG_DLY/ RWK_AVG_DLY_PREV_YEAR)-1,
      Y2D_DLY_DIF_PREV_YEAR_PERC = if_else(Y2D_AVG_DLY_PREV_YEAR == 0, NA, Y2D_AVG_DLY/ Y2D_AVG_DLY_PREV_YEAR)-1,
      ##delay ert
      DAY_DLY_ERT_DIF_PREV_YEAR_PERC = if_else(DAY_DLY_ERT_PREV_YEAR == 0, NA, DAY_DLY_ERT/ DAY_DLY_ERT_PREV_YEAR)-1,
      RWK_DLY_ERT_DIF_PREV_YEAR_PERC = if_else(RWK_AVG_DLY_ERT_PREV_YEAR == 0, NA, RWK_AVG_DLY_ERT/ RWK_AVG_DLY_ERT_PREV_YEAR)-1,
      Y2D_DLY_ERT_DIF_PREV_YEAR_PERC = if_else(Y2D_AVG_DLY_ERT_PREV_YEAR == 0, NA, Y2D_AVG_DLY_ERT/ Y2D_AVG_DLY_ERT_PREV_YEAR)-1,
      ##delay arp
      DAY_DLY_ARP_DIF_PREV_YEAR_PERC = if_else(DAY_DLY_ARP_PREV_YEAR == 0, NA, DAY_DLY_ARP/ DAY_DLY_ARP_PREV_YEAR)-1,
      RWK_DLY_ARP_DIF_PREV_YEAR_PERC = if_else(RWK_AVG_DLY_ARP_PREV_YEAR == 0, NA, RWK_AVG_DLY_ARP/ RWK_AVG_DLY_ARP_PREV_YEAR)-1,
      Y2D_DLY_ARP_DIF_PREV_YEAR_PERC = if_else(Y2D_AVG_DLY_ARP_PREV_YEAR == 0, NA, Y2D_AVG_DLY_ARP/ Y2D_AVG_DLY_ARP_PREV_YEAR)-1,
      ##delay/flight
      DAY_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(DAY_DLY_FLT_PREV_YEAR == 0, NA, DAY_DLY_FLT/ DAY_DLY_FLT_PREV_YEAR)-1,
      RWK_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(RWK_DLY_FLT_PREV_YEAR == 0, NA, RWK_DLY_FLT/ RWK_DLY_FLT_PREV_YEAR)-1,
      Y2D_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(Y2D_DLY_FLT_PREV_YEAR == 0, NA, Y2D_DLY_FLT/ Y2D_DLY_FLT_PREV_YEAR)-1,
      ##delay/flight ert
      DAY_DLY_ERT_FLT_DIF_PREV_YEAR_PERC = if_else(DAY_DLY_ERT_FLT_PREV_YEAR == 0, NA, DAY_DLY_ERT_FLT/ DAY_DLY_ERT_FLT_PREV_YEAR)-1,
      RWK_DLY_ERT_FLT_DIF_PREV_YEAR_PERC = if_else(RWK_DLY_ERT_FLT_PREV_YEAR == 0, NA, RWK_DLY_ERT_FLT/ RWK_DLY_ERT_FLT_PREV_YEAR)-1,
      Y2D_DLY_ERT_FLT_DIF_PREV_YEAR_PERC = if_else(Y2D_DLY_ERT_FLT_PREV_YEAR == 0, NA, Y2D_DLY_ERT_FLT/ Y2D_DLY_ERT_FLT_PREV_YEAR)-1,
      ##delay/flight arp
      DAY_DLY_ARP_FLT_DIF_PREV_YEAR_PERC = if_else(DAY_DLY_ARP_FLT_PREV_YEAR == 0, NA, DAY_DLY_ARP_FLT/ DAY_DLY_ARP_FLT_PREV_YEAR)-1,
      RWK_DLY_ARP_FLT_DIF_PREV_YEAR_PERC = if_else(RWK_DLY_ARP_FLT_PREV_YEAR == 0, NA, RWK_DLY_ARP_FLT/ RWK_DLY_ARP_FLT_PREV_YEAR)-1,
      Y2D_DLY_ARP_FLT_DIF_PREV_YEAR_PERC = if_else(Y2D_DLY_ARP_FLT_PREV_YEAR == 0, NA, Y2D_DLY_ARP_FLT/ Y2D_DLY_ARP_FLT_PREV_YEAR)-1,
      ##delayed flights
      DAY_DLYED_PERC_DIF_PREV_YEAR = DAY_DLYED_PERC - DAY_DLYED_PERC_PREV_YEAR,
      RWK_DLYED_PERC_DIF_PREV_YEAR = RWK_DLYED_PERC - RWK_DLYED_PERC_PREV_YEAR,
      Y2D_DLYED_PERC_DIF_PREV_YEAR = Y2D_DLYED_PERC - Y2D_DLYED_PERC_PREV_YEAR,
      ##delayed flights 15 min
      DAY_DLYED_15_PERC_DIF_PREV_YEAR = DAY_DLYED_15_PERC - DAY_DLYED_15_PERC_PREV_YEAR,
      RWK_DLYED_15_PERC_DIF_PREV_YEAR = RWK_DLYED_15_PERC - RWK_DLYED_15_PERC_PREV_YEAR,
      Y2D_DLYED_15_PERC_DIF_PREV_YEAR = Y2D_DLYED_15_PERC - Y2D_DLYED_15_PERC_PREV_YEAR,
      
      # dif 2019
      ##traffic
      DAY_TFC_DIF_2019 = coalesce(DAY_TFC, 0) - coalesce(DAY_TFC_2019, 0),
      DAY_TFC_DIF_2019_PERC = if_else(DAY_TFC_2019 == 0, NA, DAY_TFC/ DAY_TFC_2019)-1,
      RWK_TFC_DIF_2019_PERC = if_else(RWK_AVG_TFC_2019 == 0, NA, RWK_AVG_TFC/ RWK_AVG_TFC_2019)-1,
      Y2D_TFC_DIF_2019_PERC = if_else(Y2D_AVG_TFC_2019 == 0, NA, Y2D_AVG_TFC/ Y2D_AVG_TFC_2019)-1,
      ##delay
      DAY_DLY_DIF_2019_PERC = if_else(DAY_DLY_2019 == 0, NA, DAY_DLY/ DAY_DLY_2019)-1,
      RWK_DLY_DIF_2019_PERC = if_else(RWK_AVG_DLY_2019 == 0, NA, RWK_AVG_DLY/ RWK_AVG_DLY_2019)-1,
      Y2D_DLY_DIF_2019_PERC = if_else(Y2D_AVG_DLY_2019 == 0, NA, Y2D_AVG_DLY/ Y2D_AVG_DLY_2019)-1,
      ##delay ert
      DAY_DLY_ERT_DIF_2019_PERC = if_else(DAY_DLY_ERT_2019 == 0, NA, DAY_DLY_ERT/ DAY_DLY_ERT_2019)-1,
      RWK_DLY_ERT_DIF_2019_PERC = if_else(RWK_AVG_DLY_ERT_2019 == 0, NA, RWK_AVG_DLY_ERT/ RWK_AVG_DLY_ERT_2019)-1,
      Y2D_DLY_ERT_DIF_2019_PERC = if_else(Y2D_AVG_DLY_ERT_2019 == 0, NA, Y2D_AVG_DLY_ERT/ Y2D_AVG_DLY_ERT_2019)-1,
      ##delay arp
      DAY_DLY_ARP_DIF_2019_PERC = if_else(DAY_DLY_ARP_2019 == 0, NA, DAY_DLY_ARP/ DAY_DLY_ARP_2019)-1,
      RWK_DLY_ARP_DIF_2019_PERC = if_else(RWK_AVG_DLY_ARP_2019 == 0, NA, RWK_AVG_DLY_ARP/ RWK_AVG_DLY_ARP_2019)-1,
      Y2D_DLY_ARP_DIF_2019_PERC = if_else(Y2D_AVG_DLY_ARP_2019 == 0, NA, Y2D_AVG_DLY_ARP/ Y2D_AVG_DLY_ARP_2019)-1,
      ##delay/flight
      DAY_DLY_FLT_DIF_2019_PERC = if_else(DAY_DLY_FLT_2019 == 0, NA, DAY_DLY_FLT/ DAY_DLY_FLT_2019)-1,
      RWK_DLY_FLT_DIF_2019_PERC = if_else(RWK_DLY_FLT_2019 == 0, NA, RWK_DLY_FLT/ RWK_DLY_FLT_2019)-1,
      Y2D_DLY_FLT_DIF_2019_PERC = if_else(Y2D_DLY_FLT_2019 == 0, NA, Y2D_DLY_FLT/ Y2D_DLY_FLT_2019)-1,
      ##delay/flight ert
      DAY_DLY_ERT_FLT_DIF_2019_PERC = if_else(DAY_DLY_ERT_FLT_2019 == 0, NA, DAY_DLY_ERT_FLT/ DAY_DLY_ERT_FLT_2019)-1,
      RWK_DLY_ERT_FLT_DIF_2019_PERC = if_else(RWK_DLY_ERT_FLT_2019 == 0, NA, RWK_DLY_ERT_FLT/ RWK_DLY_ERT_FLT_2019)-1,
      Y2D_DLY_ERT_FLT_DIF_2019_PERC = if_else(Y2D_DLY_ERT_FLT_2019 == 0, NA, Y2D_DLY_ERT_FLT/ Y2D_DLY_ERT_FLT_2019)-1,
      ##delay/flight arp
      DAY_DLY_ARP_FLT_DIF_2019_PERC = if_else(DAY_DLY_ARP_FLT_2019 == 0, NA, DAY_DLY_ARP_FLT/ DAY_DLY_ARP_FLT_2019)-1,
      RWK_DLY_ARP_FLT_DIF_2019_PERC = if_else(RWK_DLY_ARP_FLT_2019 == 0, NA, RWK_DLY_ARP_FLT/ RWK_DLY_ARP_FLT_2019)-1,
      Y2D_DLY_ARP_FLT_DIF_2019_PERC = if_else(Y2D_DLY_ARP_FLT_2019 == 0, NA, Y2D_DLY_ARP_FLT/ Y2D_DLY_ARP_FLT_2019)-1,
      ##delayed flights
      DAY_DLYED_PERC_DIF_2019 = DAY_DLYED_PERC - DAY_DLYED_PERC_2019,
      RWK_DLYED_PERC_DIF_2019 = RWK_DLYED_PERC - RWK_DLYED_PERC_2019,
      Y2D_DLYED_PERC_DIF_2019 = Y2D_DLYED_PERC - Y2D_DLYED_PERC_2019,
      ##delayed flights
      DAY_DLYED_15_PERC_DIF_2019 = DAY_DLYED_15_PERC - DAY_DLYED_15_PERC_2019,
      RWK_DLYED_15_PERC_DIF_2019 = RWK_DLYED_15_PERC - RWK_DLYED_15_PERC_2019,
      Y2D_DLYED_15_PERC_DIF_2019 = Y2D_DLYED_15_PERC - Y2D_DLYED_15_PERC_2019,
      
      DATA_DAY = mydate
      
    )  %>%  
    ungroup() %>% 
    select(
      STK_CODE,
      STK_NAME,

      YEAR,
      FLIGHT_DATE,
      FLIGHT_DATE_PREV_WEEK,
      FLIGHT_DATE_PREV_YEAR,
      FLIGHT_DATE_2019,
      
      #traffic
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
      RWK_TFC_DIF_2019_PERC	,
      
      Y2D_TFC,
      Y2D_AVG_TFC,
      Y2D_TFC_PREV_YEAR,
      Y2D_AVG_TFC_PREV_YEAR,
      Y2D_TFC_2019,
      Y2D_AVG_TFC_2019,
      Y2D_TFC_DIF_PREV_YEAR_PERC,
      Y2D_TFC_DIF_2019_PERC,
      
      #delay
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
      RWK_DLY_DIF_2019_PERC	,
      
      Y2D_DLY,
      Y2D_AVG_DLY,
      Y2D_DLY_PREV_YEAR,
      Y2D_AVG_DLY_PREV_YEAR,
      Y2D_DLY_2019,
      Y2D_AVG_DLY_2019,
      Y2D_DLY_DIF_PREV_YEAR_PERC,
      Y2D_DLY_DIF_2019_PERC,
      
      #delay ert
      DAY_DLY_ERT,
      DAY_DLY_ERT_PREV_WEEK,
      DAY_DLY_ERT_PREV_YEAR,
      DAY_DLY_ERT_2019,
      DAY_DLY_ERT_DIF_PREV_YEAR_PERC,
      DAY_DLY_ERT_DIF_2019_PERC,
      
      RWK_AVG_DLY_ERT,
      RWK_AVG_DLY_ERT_PREV_WEEK,
      RWK_AVG_DLY_ERT_PREV_YEAR,
      RWK_AVG_DLY_ERT_2019,
      RWK_DLY_ERT_DIF_PREV_YEAR_PERC,
      RWK_DLY_ERT_DIF_2019_PERC	,
      
      Y2D_DLY_ERT,
      Y2D_AVG_DLY_ERT,
      Y2D_DLY_ERT_PREV_YEAR,
      Y2D_AVG_DLY_ERT_PREV_YEAR,
      Y2D_DLY_ERT_2019,
      Y2D_AVG_DLY_ERT_2019,
      Y2D_DLY_ERT_DIF_PREV_YEAR_PERC,
      Y2D_DLY_ERT_DIF_2019_PERC,
      
      #delay arp
      DAY_DLY_ARP,
      DAY_DLY_ARP_PREV_WEEK,
      DAY_DLY_ARP_PREV_YEAR,
      DAY_DLY_ARP_2019,
      DAY_DLY_ARP_DIF_PREV_YEAR_PERC,
      DAY_DLY_ARP_DIF_2019_PERC,
      
      RWK_AVG_DLY_ARP,
      RWK_AVG_DLY_ARP_PREV_WEEK,
      RWK_AVG_DLY_ARP_PREV_YEAR,
      RWK_AVG_DLY_ARP_2019,
      RWK_DLY_ARP_DIF_PREV_YEAR_PERC,
      RWK_DLY_ARP_DIF_2019_PERC	,
      
      Y2D_DLY_ARP,
      Y2D_AVG_DLY_ARP,
      Y2D_DLY_ARP_PREV_YEAR,
      Y2D_AVG_DLY_ARP_PREV_YEAR,
      Y2D_DLY_ARP_2019,
      Y2D_AVG_DLY_ARP_2019,
      Y2D_DLY_ARP_DIF_PREV_YEAR_PERC,
      Y2D_DLY_ARP_DIF_2019_PERC,
      
      # delay cause
      DAY_DLY_G,
      DAY_DLY_CS,
      DAY_DLY_IT,
      DAY_DLY_WD,
      DAY_DLY_OTHER,
      
      RWK_AVG_DLY_G,
      RWK_AVG_DLY_CS,
      RWK_AVG_DLY_IT,
      RWK_AVG_DLY_WD,
      RWK_AVG_DLY_OTHER,
      
      #delay/flight
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
      RWK_DLY_FLT_DIF_2019_PERC	,
      
      Y2D_DLY_FLT,
      Y2D_DLY_FLT_PREV_YEAR,
      Y2D_DLY_FLT_PREV_YEAR,
      Y2D_DLY_FLT_2019,
      Y2D_DLY_FLT_DIF_PREV_YEAR_PERC,
      Y2D_DLY_FLT_DIF_2019_PERC,

      #delay/flight ert
      DAY_DLY_ERT_FLT,
      DAY_DLY_ERT_FLT_PREV_WEEK,
      DAY_DLY_ERT_FLT_PREV_YEAR,
      DAY_DLY_ERT_FLT_2019,
      DAY_DLY_ERT_FLT_DIF_PREV_YEAR_PERC,
      DAY_DLY_ERT_FLT_DIF_2019_PERC,
      
      RWK_DLY_ERT_FLT,
      RWK_DLY_ERT_FLT_PREV_WEEK,
      RWK_DLY_ERT_FLT_PREV_YEAR,
      RWK_DLY_ERT_FLT_2019,
      RWK_DLY_ERT_FLT_DIF_PREV_YEAR_PERC,
      RWK_DLY_ERT_FLT_DIF_2019_PERC	,
      
      Y2D_DLY_ERT_FLT,
      Y2D_DLY_ERT_FLT_PREV_YEAR,
      Y2D_DLY_ERT_FLT_PREV_YEAR,
      Y2D_DLY_ERT_FLT_2019,
      Y2D_DLY_ERT_FLT_DIF_PREV_YEAR_PERC,
      Y2D_DLY_ERT_FLT_DIF_2019_PERC,

      #delay/flight arp
      DAY_DLY_ARP_FLT,
      DAY_DLY_ARP_FLT_PREV_WEEK,
      DAY_DLY_ARP_FLT_PREV_YEAR,
      DAY_DLY_ARP_FLT_2019,
      DAY_DLY_ARP_FLT_DIF_PREV_YEAR_PERC,
      DAY_DLY_ARP_FLT_DIF_2019_PERC,
      
      RWK_DLY_ARP_FLT,
      RWK_DLY_ARP_FLT_PREV_WEEK,
      RWK_DLY_ARP_FLT_PREV_YEAR,
      RWK_DLY_ARP_FLT_2019,
      RWK_DLY_ARP_FLT_DIF_PREV_YEAR_PERC,
      RWK_DLY_ARP_FLT_DIF_2019_PERC	,
      
      Y2D_DLY_ARP_FLT,
      Y2D_DLY_ARP_FLT_PREV_YEAR,
      Y2D_DLY_ARP_FLT_PREV_YEAR,
      Y2D_DLY_ARP_FLT_2019,
      Y2D_DLY_ARP_FLT_DIF_PREV_YEAR_PERC,
      Y2D_DLY_ARP_FLT_DIF_2019_PERC,
      
      #delayed flights
      DAY_DLYED_PERC,
      DAY_DLYED_PERC_PREV_WEEK,
      DAY_DLYED_PERC_PREV_YEAR,
      DAY_DLYED_PERC_2019,
      DAY_DLYED_PERC_DIF_PREV_WEEK,
      DAY_DLYED_PERC_DIF_PREV_YEAR,
      DAY_DLYED_PERC_DIF_2019,
      
      RWK_DLYED_PERC,
      RWK_DLYED_PERC_PREV_WEEK,
      RWK_DLYED_PERC_PREV_YEAR,
      RWK_DLYED_PERC_2019,
      RWK_DLYED_PERC_DIF_PREV_YEAR,
      RWK_DLYED_PERC_DIF_2019,
      
      Y2D_DLYED_PERC,
      Y2D_DLYED_PERC_PREV_YEAR,
      Y2D_DLYED_PERC_2019,
      Y2D_DLYED_PERC_DIF_PREV_YEAR,
      Y2D_DLYED_PERC_DIF_2019,
      
      #delayed flights 15 min
      DAY_DLYED_15_PERC,
      DAY_DLYED_15_PERC_PREV_WEEK,
      DAY_DLYED_15_PERC_PREV_YEAR,
      DAY_DLYED_15_PERC_2019,
      DAY_DLYED_15_PERC_DIF_PREV_WEEK,
      DAY_DLYED_15_PERC_DIF_PREV_YEAR,
      DAY_DLYED_15_PERC_DIF_2019,
      
      RWK_DLYED_15_PERC,
      RWK_DLYED_15_PERC_PREV_WEEK,
      RWK_DLYED_15_PERC_PREV_YEAR,
      RWK_DLYED_15_PERC_2019,
      RWK_DLYED_15_PERC_DIF_PREV_YEAR,
      RWK_DLYED_15_PERC_DIF_2019,
      
      Y2D_DLYED_15_PERC,
      Y2D_DLYED_15_PERC_PREV_YEAR,
      Y2D_DLYED_15_PERC_2019,
      Y2D_DLYED_15_PERC_DIF_PREV_YEAR,
      Y2D_DLYED_15_PERC_DIF_2019,
      
      DATA_DAY
      
    ) %>% 
    # iceland exception 2019-2020
    mutate(across(
      c(
        # traffic
        DAY_TFC_2019,
        DAY_TFC_DIF_2019, 
        DAY_TFC_DIF_2019_PERC,
        RWK_AVG_TFC_2019, 
        RWK_AVG_TFC_2020,
        RWK_TFC_DIF_2019_PERC,
        Y2D_TFC_2019,
        Y2D_AVG_TFC_2019,
        Y2D_TFC_DIF_2019_PERC,
        
        # delay
        DAY_DLY_2019,
        DAY_DLY_DIF_2019_PERC,
        RWK_AVG_DLY_2019,
        RWK_DLY_DIF_2019_PERC,
        Y2D_DLY_2019,
        Y2D_AVG_DLY_2019,
        Y2D_DLY_DIF_2019_PERC,
        
        # delay ert
        DAY_DLY_ERT_2019,
        DAY_DLY_ERT_DIF_2019_PERC,
        RWK_AVG_DLY_ERT_2019,
        RWK_DLY_ERT_DIF_2019_PERC,
        Y2D_DLY_ERT_2019,
        Y2D_AVG_DLY_ERT_2019,
        Y2D_DLY_ERT_DIF_2019_PERC,
        
        # delay arp
        DAY_DLY_ARP_2019,
        DAY_DLY_ARP_DIF_2019_PERC,
        RWK_AVG_DLY_ARP_2019,
        RWK_DLY_ARP_DIF_2019_PERC,
        Y2D_DLY_ARP_2019,
        Y2D_AVG_DLY_ARP_2019,
        Y2D_DLY_ARP_DIF_2019_PERC,
        
        #delay/flight
        DAY_DLY_FLT_2019,
        DAY_DLY_FLT_DIF_2019_PERC,
        RWK_DLY_FLT_2019,
        RWK_DLY_FLT_DIF_2019_PERC,
        Y2D_DLY_FLT_2019,
        Y2D_DLY_FLT_DIF_2019_PERC,
        
        #delay/flight ert
        DAY_DLY_ERT_FLT_2019,
        DAY_DLY_ERT_FLT_DIF_2019_PERC,
        RWK_DLY_ERT_FLT_2019,
        RWK_DLY_ERT_FLT_DIF_2019_PERC,
        Y2D_DLY_ERT_FLT_2019,
        Y2D_DLY_ERT_FLT_DIF_2019_PERC,
        
        #delay/flight arp
        DAY_DLY_ARP_FLT_2019,
        DAY_DLY_ARP_FLT_DIF_2019_PERC,
        RWK_DLY_ARP_FLT_2019,
        RWK_DLY_ARP_FLT_DIF_2019_PERC,
        Y2D_DLY_ARP_FLT_2019,
        Y2D_DLY_ARP_FLT_DIF_2019_PERC,
        
        #delayed flights
        DAY_DLYED_PERC_2019,
        DAY_DLYED_PERC_DIF_2019,
        RWK_DLYED_PERC_2019,
        RWK_DLYED_PERC_DIF_2019,
        Y2D_DLYED_PERC_2019,
        Y2D_DLYED_PERC_DIF_2019,
        
        #delayed flights 15'
        DAY_DLYED_15_PERC_2019,
        DAY_DLYED_15_PERC_DIF_2019,
        RWK_DLYED_15_PERC_2019,
        RWK_DLYED_15_PERC_DIF_2019,
        Y2D_DLYED_15_PERC_2019,
        Y2D_DLYED_15_PERC_DIF_2019
        
      ),
      ~ if_else((STK_CODE == "IS_ANSP" | substr(STK_CODE, 1, 2) == "BI"), NA, .x)
      )
    ) %>% 
    # iceland exception prev year
    mutate(across(
      c(
        # traffic
        DAY_TFC_PREV_YEAR,
        DAY_TFC_DIF_PREV_YEAR, 
        DAY_TFC_DIF_PREV_YEAR_PERC,
        RWK_AVG_TFC_PREV_YEAR, 
        RWK_TFC_DIF_PREV_YEAR_PERC,
        Y2D_TFC_PREV_YEAR,
        Y2D_AVG_TFC_PREV_YEAR,
        Y2D_TFC_DIF_PREV_YEAR_PERC,
        
        # delay
        DAY_DLY_PREV_YEAR,
        DAY_DLY_DIF_PREV_YEAR_PERC,
        RWK_AVG_DLY_PREV_YEAR,
        RWK_DLY_DIF_PREV_YEAR_PERC,
        Y2D_DLY_PREV_YEAR,
        Y2D_AVG_DLY_PREV_YEAR,
        Y2D_DLY_DIF_PREV_YEAR_PERC,
        
        # delay ert
        DAY_DLY_ERT_PREV_YEAR,
        DAY_DLY_ERT_DIF_PREV_YEAR_PERC,
        RWK_AVG_DLY_ERT_PREV_YEAR,
        RWK_DLY_ERT_DIF_PREV_YEAR_PERC,
        Y2D_DLY_ERT_PREV_YEAR,
        Y2D_AVG_DLY_ERT_PREV_YEAR,
        Y2D_DLY_ERT_DIF_PREV_YEAR_PERC,
        
        # delay arp
        DAY_DLY_ARP_PREV_YEAR,
        DAY_DLY_ARP_DIF_PREV_YEAR_PERC,
        RWK_AVG_DLY_ARP_PREV_YEAR,
        RWK_DLY_ARP_DIF_PREV_YEAR_PERC,
        Y2D_DLY_ARP_PREV_YEAR,
        Y2D_AVG_DLY_ARP_PREV_YEAR,
        Y2D_DLY_ARP_DIF_PREV_YEAR_PERC,
        
        #delay/flight
        DAY_DLY_FLT_PREV_YEAR,
        DAY_DLY_FLT_DIF_PREV_YEAR_PERC,
        RWK_DLY_FLT_PREV_YEAR,
        RWK_DLY_FLT_DIF_PREV_YEAR_PERC,
        Y2D_DLY_FLT_PREV_YEAR,
        Y2D_DLY_FLT_DIF_PREV_YEAR_PERC,
        
        #delay/flight ert
        DAY_DLY_ERT_FLT_PREV_YEAR,
        DAY_DLY_ERT_FLT_DIF_PREV_YEAR_PERC,
        RWK_DLY_ERT_FLT_PREV_YEAR,
        RWK_DLY_ERT_FLT_DIF_PREV_YEAR_PERC,
        Y2D_DLY_ERT_FLT_PREV_YEAR,
        Y2D_DLY_ERT_FLT_DIF_PREV_YEAR_PERC,
        
        #delay/flight arp
        DAY_DLY_ARP_FLT_PREV_YEAR,
        DAY_DLY_ARP_FLT_DIF_PREV_YEAR_PERC,
        RWK_DLY_ARP_FLT_PREV_YEAR,
        RWK_DLY_ARP_FLT_DIF_PREV_YEAR_PERC,
        Y2D_DLY_ARP_FLT_PREV_YEAR,
        Y2D_DLY_ARP_FLT_DIF_PREV_YEAR_PERC,
        
        #delayed flights
        DAY_DLYED_PERC_PREV_YEAR,
        DAY_DLYED_PERC_DIF_PREV_YEAR,
        RWK_DLYED_PERC_PREV_YEAR,
        RWK_DLYED_PERC_DIF_PREV_YEAR,
        Y2D_DLYED_PERC_PREV_YEAR,
        Y2D_DLYED_PERC_DIF_PREV_YEAR,
        
        #delayed flights 15'
        DAY_DLYED_15_PERC_PREV_YEAR,
        DAY_DLYED_15_PERC_DIF_PREV_YEAR,
        RWK_DLYED_15_PERC_PREV_YEAR,
        RWK_DLYED_15_PERC_DIF_PREV_YEAR,
        Y2D_DLYED_15_PERC_PREV_YEAR,
        Y2D_DLYED_15_PERC_DIF_PREV_YEAR
        
      ),
      ~ if_else((STK_CODE == "IS_ANSP" | substr(STK_CODE, 1, 2) == "BI") & YEAR < 2025, NA, .x)
      )
    ) %>% 
    # iceland exception current year
    mutate(across(
      c(
        # traffic
        DAY_TFC,
        DAY_TFC_PREV_WEEK, 
        DAY_TFC_DIF_PREV_WEEK,
        DAY_TFC_DIF_PREV_WEEK_PERC,
        RWK_AVG_TFC, 
        RWK_AVG_TFC_PREV_WEEK,
        Y2D_TFC,
        Y2D_AVG_TFC,

        # delay
        DAY_DLY,
        DAY_DLY_PREV_WEEK,
        DAY_DLY_DIF_PREV_WEEK_PERC,
        RWK_AVG_DLY,
        RWK_AVG_DLY_PREV_WEEK,
        Y2D_DLY,
        Y2D_AVG_DLY,

        # delay ert
        DAY_DLY_ERT,
        DAY_DLY_ERT_PREV_WEEK,
        RWK_AVG_DLY_ERT,
        RWK_AVG_DLY_ERT_PREV_WEEK,
        Y2D_DLY_ERT,
        Y2D_AVG_DLY_ERT,
        
        # delay arp
        DAY_DLY_ARP,
        DAY_DLY_ARP_PREV_WEEK,
        RWK_AVG_DLY_ARP,
        RWK_AVG_DLY_ARP_PREV_WEEK,
        Y2D_DLY_ARP,
        Y2D_AVG_DLY_ARP,
        
        #delay/flight
        DAY_DLY_FLT,
        DAY_DLY_FLT_PREV_WEEK,
        DAY_DLY_FLT_DIF_PREV_WEEK_PERC,
        RWK_DLY_FLT,
        RWK_DLY_FLT_PREV_WEEK,
        Y2D_DLY_FLT,
        
        #delay/flight ert
        DAY_DLY_ERT_FLT,
        DAY_DLY_ERT_FLT_PREV_WEEK,
        RWK_DLY_ERT_FLT,
        RWK_DLY_ERT_FLT_PREV_WEEK,
        Y2D_DLY_ERT_FLT,
        
        #delay/flight arp
        DAY_DLY_ARP_FLT,
        DAY_DLY_ARP_FLT_PREV_WEEK,
        RWK_DLY_ARP_FLT,
        RWK_DLY_ARP_FLT_PREV_WEEK,
        Y2D_DLY_ARP_FLT,
        
        #delayed flights
        DAY_DLYED_PERC,
        DAY_DLYED_PERC_PREV_WEEK,
        DAY_DLYED_PERC_DIF_PREV_WEEK,
        RWK_DLYED_PERC,
        RWK_DLYED_PERC_PREV_WEEK,
        Y2D_DLYED_PERC,

        #delayed flights 15'
        DAY_DLYED_15_PERC,
        DAY_DLYED_15_PERC_PREV_WEEK,
        DAY_DLYED_15_PERC_DIF_PREV_WEEK,
        RWK_DLYED_15_PERC,
        RWK_DLYED_15_PERC_PREV_WEEK,
        Y2D_DLYED_15_PERC
        
      ),
      ~ if_else((STK_CODE == "IS_ANSP" | substr(STK_CODE, 1, 2) == "BI") & YEAR < 2024, NA, .x)
      )
    ) %>% 
    arrange(STK_NAME, FLIGHT_DATE)
  
    
}


stk_aggregate <- function(df, 
                           period_type, 
                           mydate =  today()- days(1),
                           stk_id = AO_GRP_NAME,
                           stk_code = AO_GRP_CODE,
                           stk_name = AO_GRP_NAME,
                           date_field = ENTRY_DATE,
                           metric1 = FLIGHT,
                           metric2 = NULL,
                           agg_id = ARP_PAIR_ID,
                           agg_code = AIRPORT_PAIR_CODE,
                           agg_name = AIRPORT_PAIR_NAME,
                           agg_stk = 'airport_pair'
) 
{
  # mydate <- data_day_date
  # agg_stk <- "airport_pair"
  # df <- import_dataframe("st_ap_agg")
  # df <- df_app
  # period_type <- 'D'
  # print(paste(format(now(), "%H:%M:%S")))
  
  data_day_text <- mydate %>% format("%Y%m%d")
  day_prev_week <- mydate + days(-7)
  day_prev_year <- mydate + days(-364)
  day_2019 <- mydate - days(364 * (year(mydate) - 2019) + floor((year(mydate) - 2019) / 4) * 7)
  current_year = year(mydate)
  
  if (period_type == 'D') {
    flag_curr   <- "CURRENT_DAY"
    flag_prev_w <- "DAY_PREV_WEEK"
    flag_prev_y <- "DAY_PREV_YEAR"
    flag_2019   <- "DAY_2019"
    
    my_dates <- c(mydate, day_prev_week, day_2019, day_prev_year)
    
  } else if (period_type == 'W') {
    flag_curr   <- "CURRENT_ROLLING_WEEK"
    flag_prev_w <- "PREV_ROLLING_WEEK"
    flag_prev_y <- "ROLLING_WEEK_PREV_YEAR"
    flag_2019   <- "ROLLING_WEEK_2019"
    
    my_dates <- c(
      seq.Date(mydate - 6, mydate, by = "day"),
      seq.Date(day_prev_week - 6, day_prev_week, by = "day"),
      seq.Date(day_2019 - 6, day_2019, by = "day"),
      seq.Date(day_prev_year - 6, day_prev_year, by = "day")
    )    
    
    
  } else if (period_type == 'Y') {
    flag_curr   <- "CURRENT_YEAR"
    flag_prev_w <- NA
    flag_prev_y <- "PREV_YEAR"
    flag_2019   <- "2019"
    
    my_dates <- seq.Date(ymd(paste0(2019,"01","01")),
                         mydate) %>% as_tibble() %>%
      filter(year(value) %in% c(2019, current_year-1, current_year)) %>%
      filter(format(value, "%m-%d") <= format(mydate, "%m-%d")) %>%
      pull()
    
  }
  
  #capture metric names to create averages and avoid issue when me is null
  m1_q <- enquo(metric1)
  avg_name <- paste0("AVG_", as_name(m1_q))
  avg_m1_sym <- sym(avg_name)
  
  m2_q <- enquo(metric2)

  if(rlang::quo_is_null(m2_q)) {m2_sym <- sym("FAKE")}
  else {
    m2_sym <- ensym(m2_q)
    }
  
  df <- df %>% 
    # collect() %>% 
    mutate(
      DATE_FIELD = {{ date_field }},
      STK_ID = {{ stk_id }},
      STK_CODE = {{ stk_code }},
      STK_NAME = {{ stk_name }},
      AGG_ID = {{ agg_id }},
      # AGG_CODE = {{ agg_code }},
      METRIC1 = {{ metric1 }}
    ) %>%
    # mutate(
    #   DATE_FIELD = ENTRY_DATE,
    #   STK_ID = AO_GRP_NAME,
    #   STK_CODE = AO_GRP_CODE,
    #   STK_NAME = AO_GRP_NAME,
    #   AGG_ID = ARP_PAIR_ID,
    #   METRIC1 = FLIGHT,
    #   METRIC2 = NULL
    # ) %>%
    select(
      DATE_FIELD,
      STK_ID,
      STK_CODE,
      STK_NAME,
      AGG_ID,
      METRIC1,
      {{ metric2 }}
    ) %>% 
    filter(DATE_FIELD %in% my_dates) %>% 
    mutate(
      FLAG_PERIOD = case_when(
        !!(period_type == "Y") ~ case_when(
          lubridate::year(DATE_FIELD) == !!current_year      ~ "CURRENT_YEAR",
          lubridate::year(DATE_FIELD) == !!(current_year-1)  ~ "PREV_YEAR",
          lubridate::year(DATE_FIELD) == 2019                ~ "2019",
          TRUE                                               ~ NA_character_
        ),
        !!(period_type == "W") ~ case_when(
          between(DATE_FIELD, !!(mydate - lubridate::days(6)), !!mydate)                     ~ "CURRENT_ROLLING_WEEK",
          between(DATE_FIELD, !!(day_prev_week - lubridate::days(6)), !!day_prev_week)      ~ "PREV_ROLLING_WEEK",
          between(DATE_FIELD, !!(day_2019 - lubridate::days(6)), !!day_2019)                ~ "ROLLING_WEEK_2019",
          between(DATE_FIELD, !!(day_prev_year - lubridate::days(6)), !!day_prev_year)      ~ "ROLLING_WEEK_PREV_YEAR",
          TRUE                                                                               ~ NA_character_
        ),
        !!(period_type == "D") ~ case_when(
          DATE_FIELD == !!mydate        ~ "CURRENT_DAY",
          DATE_FIELD == !!day_prev_week ~ "DAY_PREV_WEEK",
          DATE_FIELD == !!day_2019      ~ "DAY_2019",
          DATE_FIELD == !!day_prev_year ~ "DAY_PREV_YEAR",   # <- fixed: removed stray ')'
          TRUE                          ~ NA_character_
        ),
        TRUE ~ NA_character_
      )
    ) %>% 
    summarise(
      METRIC1 = sum(METRIC1, na.rm = TRUE),
      !!m2_sym := sum({{ metric2 }}, na.rm = TRUE),
      TO_DATE = max(DATE_FIELD, na.rm = TRUE),
      FROM_DATE = min(DATE_FIELD, na.rm = TRUE),
      .by = c(FLAG_PERIOD, STK_ID, STK_CODE, STK_NAME, AGG_ID)
    ) %>% 
    ungroup() %>%
    group_by(FLAG_PERIOD) %>% 
    mutate(
      TO_DATE = max(TO_DATE, na.rm = TRUE),
      FROM_DATE = min(FROM_DATE, na.rm = TRUE)
    ) %>% 
    ungroup() %>%
    mutate(
      PERIOD_TYPE = case_when(
        period_type == 'D' ~ 'DAY',
        period_type == 'W' ~ 'WEEK',
        period_type == 'Y' ~ 'Y2D'
      ),
      NO_DAYS = as.numeric(TO_DATE - FROM_DATE) + 1,
      AVG_METRIC1 = METRIC1/NO_DAYS,
      DATA_DATE = max(TO_DATE, na.rm = TRUE),
      YEAR = year(TO_DATE)
    )  %>% 
    collect()
  
  if (agg_stk == 'iso_country') {
    df_joined <- df %>% 
      left_join(dim_iso_country, by = join_by(AGG_ID == ISO_COUNTRY_CODE)) %>% 
      rename(AGG_NAME = COUNTRY_NAME) %>% 
      mutate(AGG_CODE = AGG_ID) %>% 
      filter(AGG_CODE != '##')
    
  } else if (agg_stk == 'iso_country_spain') {
    df_joined <- df %>% 
      left_join(
        rename(dim_iso_country_spain, COUNTRY_NAME2 = COUNTRY_NAME),
        by = join_by(AGG_ID == ISO_COUNTRY_CODE)) %>% 
      rename(AGG_NAME = COUNTRY_NAME2) %>% 
      mutate(AGG_CODE = AGG_ID) %>% 
      filter(AGG_CODE != '##')
    
  } else if (agg_stk == 'airport') {
    df_joined <- df %>% 
      mutate(
        .rid  = row_number(),
      ) %>%
      left_join(dim_airport, by = join_by(AGG_ID == BK_AP_ID)) %>% 
      rename(AGG_NAME = EC_AP_NAME, AGG_CODE = EC_AP_CODE) %>% 
      filter(VALID_TO >= TO_DATE) %>%
      group_by(.rid) %>%
      slice_min(VALID_TO, n = 1, with_ties = FALSE) %>%            # nearest future change
      ungroup() %>% 
      select(-.rid)
    
    
  } else if (agg_stk == 'airport_pair') {
    df_joined <- df %>% 
      # rename(AGG_ID = ARP_PAIR_ID) %>% 
      separate_wider_delim(
        AGG_ID, delim = "-", 
        names = c("BK_AP_ID_1", "BK_AP_ID_2"),
        too_many = "merge", too_few = "align_start") %>% 
      mutate(across(c(BK_AP_ID_1, BK_AP_ID_2), ~as.integer(.x)))  %>% 
      #keep only last icao code
      mutate(
        .rid  = row_number(),
      ) %>%
      left_join(dim_airport, by = c("BK_AP_ID_1" = "BK_AP_ID"), keep = FALSE)  %>% 
      filter(VALID_TO >= TO_DATE) %>%
      group_by(.rid) %>%
      slice_min(VALID_TO, n = 1, with_ties = FALSE) %>%            
      ungroup() %>% 
      left_join(dim_airport, by = c("BK_AP_ID_2" = "BK_AP_ID"),
                suffix = c("_1", "_2"),
                keep = FALSE)  %>% 
      filter(VALID_TO_2 >= TO_DATE) %>%
      group_by(.rid) %>%
      slice_min(VALID_TO_2, n = 1, with_ties = FALSE) %>%            
      ungroup()  %>% 
      select(-.rid) %>% 
      mutate(
        AGG_NAME = case_when(
          EC_AP_NAME_1 <= EC_AP_NAME_2 ~ paste0(EC_AP_NAME_1, "<->", EC_AP_NAME_2),
          .default = paste0(EC_AP_NAME_2, "<->", EC_AP_NAME_1)
        ),
        AGG_CODE = case_when(
          EC_AP_NAME_1 <= EC_AP_NAME_2 ~ paste0(EC_AP_CODE_1, "<->", EC_AP_CODE_2),
          .default = paste0(EC_AP_CODE_2, "<->", EC_AP_CODE_1)
        ),
        AGG_ID = case_when(
          EC_AP_NAME_1 <= EC_AP_NAME_2 ~ paste0(BK_AP_ID_1, "<->", BK_AP_ID_2),
          .default = paste0(BK_AP_ID_2, "<->", BK_AP_ID_1)
        )
      )
    
  } else if (agg_stk == 'airline') {
    df_joined <- df %>% 
      mutate(
        .rid  = dplyr::row_number(),
      ) %>%
      left_join(dim_ao_group1,
                by = join_by(AGG_ID == AO_GRP_NAME)) %>% 
      mutate(AGG_NAME = AGG_ID, 
             AGG_CODE = AO_GRP_CODE
      ) %>% 
      filter(TIL >= TO_DATE) %>%
      group_by(.rid) %>%
      slice_min(TIL, n = 1, with_ties = FALSE) %>%            # nearest future change
      ungroup() %>% 
      select(-.rid)
    
    
  } else if (agg_stk == 'market_segment') {
    df_joined <- df %>% 
      left_join(list_marktet_segment_app,
                by = join_by(AGG_ID == MS_ID)) %>% 
      mutate(AGG_CODE = AGG_ID, AGG_NAME = MS_NAME)
    
  }  
  
  
  df_period <- df_joined %>% 
    group_by(STK_ID, FLAG_PERIOD) %>%
    arrange(STK_ID, FLAG_PERIOD, desc(METRIC1), AGG_NAME) %>%
    mutate(
      R_RANK = case_when( 
        FLAG_PERIOD == flag_curr ~ row_number(),
        .default = NA_integer_
      ),
      RANK = case_when( 
        FLAG_PERIOD == flag_curr  ~ min_rank(desc(METRIC1)),
        .default = NA_integer_
      ),
      RANK_PREV_WEEK = case_when( 
        FLAG_PERIOD == flag_prev_w  &!!(period_type != "Y") ~ min_rank(desc(METRIC1)),
        .default = NA_integer_
      ),
      RANK_PREV_YEAR = case_when( 
        FLAG_PERIOD == flag_prev_y ~ min_rank(desc(METRIC1)),
        .default = NA_integer_
      ),
      RANK_2019 = case_when( 
        FLAG_PERIOD == flag_2019 ~ min_rank(desc(METRIC1)),
        .default = NA_integer_
      )
    ) %>% 
    ungroup() %>% 
    group_by(STK_ID, AGG_ID) %>% 
    arrange(STK_ID, AGG_ID, FLAG_PERIOD) %>%
    fill(RANK, .direction = "down") %>% 
    fill(RANK, .direction = "up") %>% 
    fill(R_RANK, .direction = "up") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV_WEEK, .direction = "down") %>% 
    fill(RANK_PREV_WEEK, .direction = "up") %>% 
    fill(RANK_PREV_YEAR, .direction = "down") %>% 
    fill(RANK_PREV_YEAR, .direction = "up") %>% 
    fill(RANK_2019, .direction = "down") %>% 
    fill(RANK_2019, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 41) 

  
  df_out <- df_period %>% 
    arrange(STK_CODE, FLAG_PERIOD, R_RANK, AGG_NAME) %>% 
    mutate (
      YEAR_DATA = as.integer(year(DATA_DATE)),
      ) %>% 
    ## iceland exception
    mutate(
      across(c(RANK_2019),   
             ~ if_else(STK_CODE %in% c('IS', 'BIRK', 'BIFK', 'ICE', 'FPY_GRP', "IS_ANSP"), NA, .x)),
      
      across(c(RANK_PREV_YEAR),   
             ~ if_else(STK_CODE %in% c('IS', 'BIRK', 'BIFK', 'ICE', 'FPY_GRP', "IS_ANSP") & YEAR_DATA < 2025, NA, .x)),
      
      across(c(RANK, RANK_PREV_WEEK),   
             ~ if_else(STK_CODE %in% c('IS', 'BIRK', 'BIFK', 'ICE', 'FPY_GRP', "IS_ANSP") & YEAR_DATA < 2024, NA, .x)),
      
      across(c(METRIC1, AVG_METRIC1, {{ metric2 }}),   
             ~ if_else(STK_CODE %in% c('IS', 'BIRK', 'BIFK', 'ICE', 'FPY_GRP', "IS_ANSP") & YEAR < 2024, NA, .x))
    ) %>% 
    select(
        STK_NAME,
        STK_CODE,
        PERIOD_TYPE,
        FLAG_PERIOD,
        YEAR,
        FROM_DATE,
        TO_DATE,
        NO_DAYS,
        # AGG_NAME,
        # AGG_CODE,
        # METRIC1,
        # AVG_METRIC1,
        {{ agg_name }} := AGG_NAME,
        {{ agg_code }} := AGG_CODE,
        {{ metric1 }} := METRIC1,
        !!avg_m1_sym := AVG_METRIC1,
        {{ metric2 }},
        R_RANK,
        RANK,
        RANK_PREV_WEEK,
        RANK_PREV_YEAR,
        RANK_2019,
        DATA_DATE,
        YEAR_DATA
    ) 

  print(paste(format(now(), "%H:%M:%S")))
  return(df_out)
  
}

# aggregation functions ----
stk_aggregate_period <- function(granularity, date, params) {
  args <- c(
    list(df = df_app, period_type = granularity, mydate = date),
    params
  )
  do.call(stk_aggregate, args)
}


run_for_date <- function(day_seq) {

  con = DBI::dbConnect(duckdb::duckdb())
  # day <- ymd(20251030)
  # ingest only partitions defined in myyears
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  years_needed <- unique(year(date_seq))

  df_source <- tryCatch(
    {
      read_partitioned_parquet_duckdb(
        con = con,
        mydataframe = paste0(mydataframe, "_agg"),
        years = as.integer(years_needed),
        subpattern = NULL,
        year_col = "YEAR_DATA"
      ) %>%
        filter(!(DATA_DATE %in% date_seq)) %>%
        collect()
    },
    error = function(e) {
      message("Source partition not found (or unreadable); continuing without df_source.")
      dplyr::tibble()  # empty fallback
    }
  )

  new_rows <- imap_dfr(
    date_seq,
    function(day, i) {
      message(day)
      bind_rows(
        stk_aggregate_period("D", day, params),
        stk_aggregate_period("W", day, params),
        stk_aggregate_period("Y", day, params)
      )
    }
  )

  # Combine once.
  out <- bind_rows(df_source, new_rows)
  out <- out %>% mutate(YEAR_DATA = as.integer(YEAR_DATA))
  return(out)
}

# NETWORK ----
## nw day ----
params_nw_day <- list(
  df            = "nw_delay_cause",
  mydate        = current_day,
  stk_id        = quo(AREA),
  date_field    = quo(FLIGHT_DATE),
  tfc_field     = quo(FLT),
  dly_field     = quo(TDM),
  dlyed_field   = quo(TDF),
  dlyed15_field = NULL,
  dly_ert_field = quo(TDM_ERT),
  dly_arp_field = quo(TDM_ARP)
)

## nw ap ----
params_nw_ap <- list(
  stk_id     = quo(UNIT),
  stk_code   = quo(UNIT),
  stk_name   = quo(UNIT),
  date_field = quo(FLIGHT_DATE),
  metric1    = quo(DEP_ARR),
  metric2    = NULL,            # explicitly pass NULL
  agg_id     = quo(BK_AP_ID),
  agg_code   = quo(AP_CODE),
  agg_name   = quo(AP_NAME),
  agg_stk    = "airport"
)

## nw ao ----
params_nw_ao <- list(
  stk_id     = quo(UNIT),
  stk_code   = quo(UNIT),
  stk_name   = quo(UNIT),
  date_field = quo(FLIGHT_DATE),
  metric1    = quo(FLIGHT),
  metric2    = NULL,            # explicitly pass NULL
  agg_id     = quo(AO_GRP_NAME),
  agg_code   = quo(AO_GRP_CODE),
  agg_name   = quo(AO_GRP_NAME),
  agg_stk    = "airline"
)

## nw st dai ----
params_nw_st_dai <- list(
  stk_id     = quo(UNIT),
  stk_code   = quo(UNIT),
  stk_name   = quo(UNIT),
  date_field = quo(FLIGHT_DATE),
  metric1    = quo(DAY_TFC),
  metric2    = NULL,            # explicitly pass NULL
  agg_id     = quo(ISO_COUNTRY_CODE),
  agg_code   = quo(ISO_COUNTRY_CODE),
  agg_name   = quo(COUNTRY_NAME),
  agg_stk    = "iso_country"
)

# COUNTRY ----
## st dai day ----
params_st_day <- list(
  df            = "st_dai",
  mydate        = current_day,
  stk_id        = quo(COUNTRY_CODE),
  date_field    = quo(FLIGHT_DATE),
  tfc_field     = quo(DAY_TFC)
)

## st ao ----
params_st_ao <- list(
  stk_id     = quo(ISO_CT_CODE),
  stk_code   = quo(ISO_CT_CODE),
  stk_name   = quo(COUNTRY_NAME),
  date_field = quo(ENTRY_DATE),
  metric1    = quo(FLIGHT),
  metric2    = NULL,            # explicitly pass NULL
  agg_id     = quo(AO_GRP_NAME),
  agg_code   = quo(AO_GRP_CODE),
  agg_name   = quo(AO_GRP_NAME),
  agg_stk    = "airline"
)

## st ap ----
params_st_ap <- list(
  stk_id     = quo(ISO_CT_CODE),
  stk_code   = quo(ISO_CT_CODE),
  stk_name   = quo(COUNTRY_NAME),
  date_field = quo(FLIGHT_DATE),
  metric1    = quo(DEP_ARR),
  metric2    = NULL,            # explicitly pass NULL
  agg_id     = quo(BK_AP_ID),
  agg_code   = quo(AP_CODE),
  agg_name   = quo(AP_NAME),
  agg_stk    = "airport"
)

## st st ----
params_st_st <- list(
  stk_id     = quo(ISO_CT_CODE),
  stk_code   = quo(ISO_CT_CODE),
  stk_name   = quo(COUNTRY_NAME),
  date_field = quo(ENTRY_DATE),
  metric1    = quo(FLIGHT),
  metric2    = NULL,            # explicitly pass NULL
  agg_id     = quo(ISO_CT_CODE2),
  agg_code   = quo(ISO_CT_CODE2),
  agg_name   = quo(COUNTRY_NAME2),
  agg_stk    = "iso_country_spain"
)

# AIRPORT ----
## ap day ----
params_ap_day <- list(
  df            = "ap_traffic_delay",
  mydate        = current_day,
  stk_id        = quo(BK_AP_ID),
  date_field    = quo(FLIGHT_DATE),
  tfc_field     = quo(DEP_ARR),
  dly_field     = quo(TDM_ARP_ARR),
  dlyed_field   = quo(TDF_ARP_ARR),
  dlyed15_field = quo(TDF_15_ARP_ARR)
)

## ap ao ----
params_ap_ao <- list(
  stk_id     = quo(BK_AP_ID),
  stk_code   = quo(AP_CODE),
  stk_name   = quo(AP_NAME),
  date_field = quo(ENTRY_DATE),
  metric1    = quo(DEP_ARR),
  metric2    = NULL,            # explicitly pass NULL
  agg_id     = quo(AO_GRP_NAME),
  agg_code   = quo(AO_GRP_CODE),
  agg_name   = quo(AO_GRP_NAME),
  agg_stk    = "airline"
)

## ap st des  ----
params_ap_st_des <- list(
  stk_id     = quo(BK_AP_ID),
  stk_code   = quo(AP_CODE),
  stk_name   = quo(AP_NAME),
  date_field = quo(ENTRY_DATE),
  metric1    = quo(DEP),
  metric2    = NULL,            # explicitly pass NULL
  agg_id     = quo(ARR_ISO_CT_CODE),
  agg_code   = quo(ISO_CT_CODE_ARR),
  agg_name   = quo(ISO_CT_NAME_ARR),
  agg_stk    = "iso_country"
)

## ap ap des  ----
params_ap_ap_des <- list(
  stk_id     = quo(BK_AP_ID),
  stk_code   = quo(AP_CODE),
  stk_name   = quo(AP_NAME),
  date_field = quo(ENTRY_DATE),
  metric1    = quo(DEP),
  metric2    = NULL,            # explicitly pass NULL
  agg_id     = quo(ARR_BK_AP_ID),
  agg_code   = quo(ADES_CODE),
  agg_name   = quo(ADES_NAME),
  agg_stk    = "airport"
)

## ap ms  ----
params_ap_ms <- list(
  stk_id     = quo(BK_AP_ID),
  stk_code   = quo(AP_CODE),
  stk_name   = quo(AP_NAME),
  date_field = quo(ENTRY_DATE),
  metric1    = quo(DEP_ARR),
  metric2    = NULL,            # explicitly pass NULL
  agg_id     = quo(MS_ID),
  agg_code   = quo(MS_CODE),
  agg_name   = quo(MS_NAME),
  agg_stk    = "market_segment"
)

# AIRCRAFT OPERATOR ----
## ao day ----
params_ao_day <- list(
  df            = "ao_traffic_delay",
  mydate        = current_day,
  stk_id        = quo(AO_ID),
  date_field    = quo(FLIGHT_DATE),
  tfc_field     = quo(DAY_TFC),
  dly_field     = quo(DAY_DLY),
  dlyed_field   = quo(DAY_DELAYED_TFC),
  dlyed15_field = quo(DAY_DELAYED_TFC_15)
)

## ao st des ----
params_ao_st_des <- list(
  stk_id     = quo(AO_GRP_NAME),
  stk_code   = quo(AO_GRP_CODE),
  stk_name   = quo(AO_GRP_NAME),
  date_field = quo(ENTRY_DATE),
  metric1    = quo(FLIGHT),
  metric2    = NULL,            # explicitly pass NULL
  agg_id     = quo(ARR_ISO_CT_CODE),
  agg_code   = quo(ISO_CT_CODE_ARR),
  agg_name   = quo(ISO_CT_NAME_ARR),
  agg_stk    = "iso_country"
)  

## ao ap dep ----
params_ao_ap_dep <- list(
  stk_id     = quo(AO_GRP_NAME),
  stk_code   = quo(AO_GRP_CODE),
  stk_name   = quo(AO_GRP_NAME),
  date_field = quo(ENTRY_DATE),
  metric1    = quo(FLIGHT),
  metric2    = NULL,            # explicitly pass NULL
  agg_id     = quo(BK_AP_ID),
  agg_code   = quo(AP_CODE),
  agg_name   = quo(AP_NAME),
  agg_stk    = "airport"
) 

## ao ap pair ----
params_ao_ap_pair <- list(
  stk_id     = quo(AO_GRP_NAME),
  stk_code   = quo(AO_GRP_CODE),
  stk_name   = quo(AO_GRP_NAME),
  date_field = quo(ENTRY_DATE),
  metric1    = quo(FLIGHT),
  metric2    = NULL,            # explicitly pass NULL
  agg_id     = quo(ARP_PAIR_ID),
  agg_code   = quo(AIRPORT_PAIR_CODE),
  agg_name   = quo(AIRPORT_PAIR_NAME),
  agg_stk    = "airport_pair"
) 

## ao ap arr delay ----
params_ao_ap_arr_delay <- list(
  stk_id     = quo(AO_GRP_NAME),
  stk_code   = quo(AO_GRP_CODE),
  stk_name   = quo(AO_GRP_NAME),
  date_field = quo(ARR_DATE),
  metric1    = quo(FLIGHT),
  metric2    = quo(ARR_ATFM_DELAY),           
  agg_id     = quo(BK_AP_ID),
  agg_code   = quo(APT_CODE),
  agg_name   = quo(APT_NAME),
  agg_stk    = "airport"
) 

# ANSP ----
# sp day ----
params_sp_day <- list(
  df            = "sp_traffic_delay",
  mydate        = current_day,
  stk_id        = quo(ANSP_ID),
  date_field    = quo(ENTRY_DATE),
  tfc_field     = quo(FLT_DAIO),
  dly_field     = quo(TDM_ERT),
  dlyed_field   = quo(TDF_ERT),
  dlyed15_field = quo(TDF_15_ERT)
)
 
# EXECUTE FUNCTIONS ----
## stk daily aggregates ----

stk_day_save <- function(stk) {
  message(paste(format(now(), "%H:%M:%S")))
  params <- get(paste0("params_", stk, "_day"))
  
  df_day <- do.call(stk_daily, params)
  
  if(stk == "nw") {
    df_day <- df_day %>% select(-(contains("15")))
    mydatafile <- paste0("nw_traffic_delay_day.parquet")
    
  } else if (stk == "st") {
    df_day <- df_day %>% select(-(contains("DLY")))
    mydatafile <- paste0("st_dai_day.parquet")
    
  } else if (stk == "ap") {
    df_day <- df_day %>% select(-(contains("_ERT")), -(contains("_ARP")))
    mydatafile <- paste0("ap_traffic_delay_day.parquet") 
    
  } else if (stk == "ao") {
    df_day <- df_day %>% select(-(contains("_ERT")), -(contains("_ARP")))
    mydatafile <- paste0("ao_traffic_delay_day.parquet")
    
  } else if (stk == "sp") {
    df_day <- df_day %>% select(-(contains("_ERT")), -(contains("_ARP")))
    mydatafile <- paste0("sp_traffic_delay_day.parquet")

  }
  
  stakeholder <- stk
  
  df_day %>% write_parquet(here(archive_dir_raw, stakeholder, mydatafile))
  print(paste0(mydatafile, " saved"))
  message(paste(format(now(), "%H:%M:%S")))
  
}

stk_day_list <- c(
  "nw",
  "st",
  "ap",
  "ao",
  "sp",
  NULL
)

walk(stk_day_list, stk_day_save)

## stk - stk daily aggregates ----
stk_agg_list <- c(
  "nw_ap", "nw_ao", "nw_st_dai",
  "st_ao", "st_ap", "st_st",
  "ap_ao", "ap_st_des", "ap_ap_des", "ap_ms",
  "ao_st_des", "ao_ap_dep", "ao_ap_pair", "ao_ap_arr_delay",
  NULL
)

# date_seq <- seq.Date(ymd(20260101), current_day)
date_seq <- seq.Date(current_day, current_day)

  
stk_agg_save <- function(stk_stk) {
  # stk_stk <- "nw_ao"
  mydataframe <- paste0(stk_stk, "")
  # expose globally, but guarantee removal when done
  assign("mydataframe", mydataframe, envir = .GlobalEnv)
  on.exit(rm("mydataframe", envir = .GlobalEnv), add = TRUE)
  
  message(mydataframe)
  df_app <- import_dataframe(mydataframe) 

  # expose globally, but guarantee removal when done
  assign("df_app", df_app, envir = .GlobalEnv)
  on.exit(rm("df_app", envir = .GlobalEnv), add = TRUE)
  
  params <- get(paste0("params_", stk_stk))
  assign("params", params, envir = .GlobalEnv)
  on.exit(rm("params", envir = .GlobalEnv), add = TRUE)
  
  df_agg <- run_for_date(date_seq)
  
  myyears <- distinct(df_agg, YEAR_DATA) %>% pull() %>% as.integer()
  
  con = DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  save_partitions_single_copy (con = con, 
                               df = df_agg, 
                               paste0(mydataframe, "_agg"),
                               years = myyears, 
                               year_col = "YEAR_DATA")
}  

walk(stk_agg_list, stk_agg_save)

# stk_agg_save("nw_st_dai")

# stakeholder <- str_sub(mydataframe, 1, 2)
# 
# 
# con = DBI::dbConnect(duckdb::duckdb())
# df <- read_partitioned_parquet_duckdb(con = con,
#                                       mydataframe = "nw_st_dai_new_agg",
#                                       years= c(2025),
#                                       subpattern = NULL,
#                                       year_col = "YEAR_DATA") %>%
#   # filter(DATA_DATE == data_day_date) %>%
#   # filter(PERIOD_TYPE == period_type) %>%
#   collect()
# DBI::dbDisconnect(con, shutdown = TRUE)


# test <- df_agg %>% filter(STK_CODE == "IS")

# mdt <- unique(df_agg$DATA_DATE) %>% sort()
# mdt2 <- seq.Date(ymd(20250101), ymd(20251114))
# 
# all.equal(mdt, mdt2)

# df_agg %>% filter(R_RANK >40) %>% summarise(max(DATA_DATE)) %>% pull()


# df            <- "sp_traffic_delay_new"
# df_raw <- import_dataframe(df)
