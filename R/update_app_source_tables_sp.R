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
## ansp ----  
# same as v_covid_dim_ao but adding ao_id and removing old aos

dim_ansp <- export_query(dim_ansp_query) 

list_ansp <-  read_xlsx(
  here("stakeholder_lists.xlsx"),
  sheet = "ansp_lists",
  range = cell_limits(c(1, 1), c(NA, 3))) %>%
  select(-ANSP_NAME) %>% 
  as_tibble() %>% 
  left_join(dim_ansp, by = "ANSP_ID")


# prep data functions ----
import_dataframe <- function(dfname) {
  # import data 
  # dfname <- "sp_traffic_delay"
  mydataframe <- dfname
  myparquetfile <- paste0(mydataframe, "_day_base.parquet")
  
  df_base <- read_parquet_duckdb(here(archive_dir_raw, myparquetfile))
  
  
  # filter to keep days and airports needed
  df_alldays <- df_base %>% 
    filter(ANSP_ID %in% list_ansp$ANSP_ID) 
  
  rm(df_base)
  
  df_alldays %>% arrange(ANSP_ID, ENTRY_DATE)
  
  # pre-process data

  if (dfname == "sp_traffic_delay"){
    
    df_app <- df_alldays %>% 
      compute(prudence = "lavish") %>%
      left_join(list_ansp, by = "ANSP_ID")
  } 
  
  rm(df_alldays)
  
  return(df_app) 
}

# sp day ----
sp_traffic_delay <- function() {
  mydatasource <-  "sp_traffic_delay"
  mydatafile <- paste0(mydatasource, "_day_raw.parquet")
  stakeholder <- str_sub(mydatasource, 1, 2)
  
  df_app <- import_dataframe(mydatasource)
  
  mydate <- df_app %>% summarise(max(ENTRY_DATE, na.rm = TRUE)) %>% pull()
  current_year = year(mydate)
  
  #### create date sequence
  year_from <- paste0(2018, "-12-24")
  year_til <- paste0(current_year, "-12-31")
  days_sequence <- seq(ymd(year_from), ymd(year_til), by = "1 day") %>%
    as_tibble() %>%
    select(ENTRY_DATE = value) 
  
  #### combine with ansp list to get full sequence
  days_sp <- crossing(days_sequence, select(list_ansp, ANSP_ID, ANSP_NAME, ANSP_CODE)) %>% 
    arrange(ANSP_ID, ENTRY_DATE)%>% 
    mutate(YEAR = year(ENTRY_DATE))
  
  df_day_year <- days_sp %>%
    left_join(df_app, by = c("ENTRY_DATE", "ANSP_ID","ANSP_NAME", "ANSP_CODE")) %>% 
    arrange(ANSP_ID, ENTRY_DATE) %>%
    group_by(ANSP_ID)  %>% 
    mutate(
      DAY_FLT_DAIO = if_else(ENTRY_DATE >mydate, NA, coalesce(FLT_DAIO, 0)), 
      
      TDM = coalesce(TDM, 0),
      TDM_ERT = coalesce(TDM_ERT, 0),
      TDM_15_ERT = coalesce(TDM_15_ERT, 0),
      TDF_15_ERT = coalesce(TDF_15_ERT, 0),
      TDM_ERT_A = coalesce(TDM_ERT_A, 0),
      TDM_ERT_C = coalesce(TDM_ERT_C, 0),
      TDM_ERT_D = coalesce(TDM_ERT_D, 0),
      TDM_ERT_E = coalesce(TDM_ERT_E, 0),
      TDM_ERT_G = coalesce(TDM_ERT_G, 0),
      TDM_ERT_I = coalesce(TDM_ERT_I, 0),
      TDM_ERT_M = coalesce(TDM_ERT_M, 0),
      TDM_ERT_N = coalesce(TDM_ERT_N, 0),
      TDM_ERT_O = coalesce(TDM_ERT_O, 0),
      TDM_ERT_P = coalesce(TDM_ERT_P, 0),
      TDM_ERT_R = coalesce(TDM_ERT_R, 0),
      TDM_ERT_S = coalesce(TDM_ERT_S, 0),
      TDM_ERT_T = coalesce(TDM_ERT_T, 0),
      TDM_ERT_V = coalesce(TDM_ERT_V, 0),
      TDM_ERT_W = coalesce(TDM_ERT_W, 0),
      TDM_ERT_NA = coalesce(TDM_ERT_NA, 0),
      
      TDF_ERT = coalesce(TDF_ERT, 0),
      
      #rolling week
      RW_AVG_FLT_DAIO = rollsum(DAY_FLT_DAIO, 7, fill = NA, align = "right") / 7

    )  %>% 
    ungroup() %>% 
    group_by(ANSP_ID, YEAR) %>% 
    mutate(
      # year to date
      Y2D_FLT_DAIO_YEAR = cumsum(DAY_FLT_DAIO),
      Y2D_AVG_FLT_DAIO_YEAR = Y2D_FLT_DAIO_YEAR / row_number()
    ) %>% 
    ungroup ()
  
  ## split table ----
  df_day <- df_day_year %>% 
    select(-starts_with("TDM_")) %>% 
    mutate(
      ENTRY_DATE_2019 = ENTRY_DATE - days((YEAR-2019)*364+ floor((YEAR - 2019) / 4) * 7),
      ENTRY_DATE_2020 = ENTRY_DATE - days((YEAR-2020)*364+ floor((YEAR - 2020) / 4) * 7),
      ENTRY_DATE_2019_SD = ENTRY_DATE %m-% years(YEAR-2019),
      ENTRY_DATE_PREV_YEAR_SD = ENTRY_DATE %m-% years(1) 
    ) %>% 
    left_join(select(df_day_year, ANSP_ID, ENTRY_DATE, starts_with(c("Y2D"))), by = c("ANSP_ID", "ENTRY_DATE_PREV_YEAR_SD" = "ENTRY_DATE"), suffix = c("","_PREV_YEAR")) %>% 
    left_join(select(df_day_year, ANSP_ID, ENTRY_DATE, starts_with(c("Y2D"))), by = c("ANSP_ID", "ENTRY_DATE_2019_SD" = "ENTRY_DATE"), suffix = c("","_2019")) %>% 
    left_join(select(df_day_year, ANSP_ID, ENTRY_DATE, starts_with(c("DAY", "RW"))), by = c("ANSP_ID", "ENTRY_DATE_2019" = "ENTRY_DATE"), suffix = c("","_2019")) %>% 
    left_join(select(df_day_year, ANSP_ID, ENTRY_DATE, RW_AVG_FLT_DAIO), by = c("ANSP_ID", "ENTRY_DATE_2020" = "ENTRY_DATE"), suffix = c("","_2020")) %>% 
    arrange(ANSP_ID, ENTRY_DATE)  %>% 
    group_by(ANSP_ID) %>% 
    mutate(
      # prev week
      ENTRY_DATE_PREV_WEEK = lag(ENTRY_DATE, 7),
      DAY_FLT_DAIO_PREV_WEEK = lag(DAY_FLT_DAIO , 7),
      RW_AVG_FLT_DAIO_PREV_WEEK = lag(RW_AVG_FLT_DAIO, 7),
      
      # dif prev week
      DAY_FLT_DAIO_DIF_PREV_WEEK = coalesce(DAY_FLT_DAIO,0) - coalesce(DAY_FLT_DAIO_PREV_WEEK, 0),
      DAY_FLT_DAIO_DIF_PREV_WEEK_PERC = if_else(DAY_FLT_DAIO_PREV_WEEK == 0, NA, DAY_FLT_DAIO/ DAY_FLT_DAIO_PREV_WEEK -1 ),
      
      RW_TFC_DIF_PREV_WEEK_PERC = if_else(RW_AVG_FLT_DAIO_PREV_WEEK == 0, NA, RW_AVG_FLT_DAIO/ RW_AVG_FLT_DAIO_PREV_WEEK -1 ),
      
      # prev year
      ENTRY_DATE_PREV_YEAR = lag(ENTRY_DATE, 364),
      DAY_FLT_DAIO_PREV_YEAR = lag(DAY_FLT_DAIO , 364),
      RW_AVG_FLT_DAIO_PREV_YEAR = lag(RW_AVG_FLT_DAIO, 364),
      Y2D_FLT_DAIO_PREV_YEAR = Y2D_FLT_DAIO_YEAR_PREV_YEAR,
      Y2D_AVG_FLT_DAIO_PREV_YEAR = Y2D_AVG_FLT_DAIO_YEAR_PREV_YEAR,

      # dif prev year
      DAY_FLT_DAIO_DIF_PREV_YEAR = coalesce(DAY_FLT_DAIO, 0) - coalesce(DAY_FLT_DAIO_PREV_YEAR, 0),
      DAY_FLT_DAIO_DIF_PREV_YEAR_PERC = if_else(DAY_FLT_DAIO_PREV_YEAR == 0, NA, DAY_FLT_DAIO/ DAY_FLT_DAIO_PREV_YEAR-1),
      RW_FLT_DAIO_DIF_PREV_YEAR_PERC = if_else(RW_AVG_FLT_DAIO_PREV_YEAR == 0, NA, RW_AVG_FLT_DAIO/ RW_AVG_FLT_DAIO_PREV_YEAR-1),
      Y2D_FLT_DAIO_DIF_PREV_YEAR_PERC = if_else(Y2D_AVG_FLT_DAIO_PREV_YEAR == 0, NA, Y2D_AVG_FLT_DAIO_YEAR/ Y2D_AVG_FLT_DAIO_PREV_YEAR-1),
      
      # 2020
      ENTRY_DATE_2020 = ENTRY_DATE_2020,
      RW_AVG_FLT_DAIO_2020 = RW_AVG_FLT_DAIO_2020,
      
      # 2019
      ENTRY_DATE_2019 = ENTRY_DATE_2019,
      DAY_FLT_DAIO_2019 = DAY_FLT_DAIO_2019,
      RW_AVG_FLT_DAIO_2019 = RW_AVG_FLT_DAIO_2019,
      Y2D_FLT_DAIO_2019 = Y2D_FLT_DAIO_YEAR_2019,
      Y2D_AVG_FLT_DAIO_2019 = Y2D_AVG_FLT_DAIO_YEAR_2019,
      
      # dif 2019
      DAY_FLT_DAIO_DIF_2019 = coalesce(DAY_FLT_DAIO, 0) - coalesce(DAY_FLT_DAIO_2019, 0),
      DAY_FLT_DAIO_DIF_2019_PERC = if_else(DAY_FLT_DAIO_2019 == 0, NA, DAY_FLT_DAIO/ DAY_FLT_DAIO_2019-1),
      RW_FLT_DAIO_DIF_2019_PERC = if_else(RW_AVG_FLT_DAIO_2019 == 0, NA, RW_AVG_FLT_DAIO/ RW_AVG_FLT_DAIO_2019 -1 ),
      Y2D_FLT_DAIO_DIF_2019_PERC = if_else(Y2D_AVG_FLT_DAIO_2019 == 0, NA, Y2D_AVG_FLT_DAIO_YEAR/ Y2D_AVG_FLT_DAIO_2019 -1 ),

      LAST_DATA_DAY = mydate
    ) %>% 
    # iceland (ansp_id = 46) case
    mutate(
      DAY_FLT_DAIO = if_else(ANSP_ID == 46 & YEAR < 2024, NA, DAY_FLT_DAIO),
      DAY_FLT_DAIO_PREV_WEEK = if_else(ANSP_ID == 46 & YEAR < 2024, NA, DAY_FLT_DAIO_PREV_WEEK),
      DAY_FLT_DAIO_PREV_YEAR = if_else(ANSP_ID == 46 & YEAR < 2025, NA, DAY_FLT_DAIO_PREV_YEAR),
      DAY_FLT_DAIO_2019 = if_else(ANSP_ID == 46, NA, DAY_FLT_DAIO_2019),
      DAY_FLT_DAIO_DIF_PREV_WEEK = if_else(ANSP_ID == 46 & YEAR < 2024, NA, DAY_FLT_DAIO_DIF_PREV_WEEK),
      DAY_FLT_DAIO_DIF_PREV_YEAR = if_else(ANSP_ID == 46 & YEAR < 2025, NA, DAY_FLT_DAIO_DIF_PREV_YEAR),
      DAY_FLT_DAIO_DIF_2019 = if_else(ANSP_ID == 46, NA, DAY_FLT_DAIO_DIF_2019),
      DAY_FLT_DAIO_DIF_PREV_WEEK_PERC = if_else(ANSP_ID == 46 & YEAR < 2024, NA, DAY_FLT_DAIO_DIF_PREV_WEEK_PERC),
      DAY_FLT_DAIO_DIF_PREV_YEAR_PERC = if_else(ANSP_ID == 46 & YEAR < 2025, NA, DAY_FLT_DAIO_DIF_PREV_YEAR_PERC),
      DAY_FLT_DAIO_DIF_2019_PERC = if_else(ANSP_ID == 46, NA, DAY_FLT_DAIO_DIF_2019_PERC),
      
      RW_AVG_FLT_DAIO = if_else(ANSP_ID == 46 & YEAR < 2024, NA, RW_AVG_FLT_DAIO),
      RW_AVG_FLT_DAIO_PREV_WEEK = if_else(ANSP_ID == 46 & YEAR < 2024, NA, RW_AVG_FLT_DAIO_PREV_WEEK),
      RW_AVG_FLT_DAIO_PREV_YEAR = if_else(ANSP_ID == 46 & YEAR < 2025, NA, RW_AVG_FLT_DAIO_PREV_YEAR),
      RW_AVG_FLT_DAIO_2020 = if_else(ANSP_ID == 46, NA, RW_AVG_FLT_DAIO_2020),
      RW_AVG_FLT_DAIO_2019 = if_else(ANSP_ID == 46, NA, RW_AVG_FLT_DAIO_2019),
      RW_FLT_DAIO_DIF_PREV_YEAR_PERC = if_else(ANSP_ID == 46 & YEAR < 2025, NA, RW_FLT_DAIO_DIF_PREV_YEAR_PERC),
      RW_FLT_DAIO_DIF_2019_PERC = if_else(ANSP_ID == 46, NA, RW_FLT_DAIO_DIF_2019_PERC),
      
      Y2D_FLT_DAIO_YEAR = if_else(ANSP_ID == 46 & YEAR < 2024, NA, Y2D_FLT_DAIO_YEAR),
      Y2D_FLT_DAIO_PREV_YEAR = if_else(ANSP_ID == 46 & YEAR < 2025, NA, Y2D_FLT_DAIO_PREV_YEAR),
      Y2D_FLT_DAIO_2019 = if_else(ANSP_ID == 46, NA, Y2D_FLT_DAIO_2019),
      Y2D_AVG_FLT_DAIO_YEAR = if_else(ANSP_ID == 46 & YEAR < 2024, NA, Y2D_AVG_FLT_DAIO_YEAR),
      Y2D_AVG_FLT_DAIO_PREV_YEAR = if_else(ANSP_ID == 46 & YEAR < 2025, NA, Y2D_AVG_FLT_DAIO_PREV_YEAR),
      Y2D_AVG_FLT_DAIO_2019 = if_else(ANSP_ID == 46, NA, Y2D_AVG_FLT_DAIO_2019),
      Y2D_FLT_DAIO_DIF_PREV_YEAR_PERC = if_else(ANSP_ID == 46 & YEAR < 2025, NA, Y2D_FLT_DAIO_DIF_PREV_YEAR_PERC),
      Y2D_FLT_DAIO_DIF_2019_PERC = if_else(ANSP_ID == 46, NA, Y2D_FLT_DAIO_DIF_2019_PERC),
      
    ) %>% 
    select(
      ANSP_ID,
      ANSP_CODE,
      ANSP_NAME,
      YEAR,

      ENTRY_DATE,
      ENTRY_DATE_PREV_WEEK,
      ENTRY_DATE_PREV_YEAR,
      ENTRY_DATE_2020,
      ENTRY_DATE_2019,
      
      DAY_FLT_DAIO,
      DAY_FLT_DAIO_PREV_WEEK,
      DAY_FLT_DAIO_PREV_YEAR,
      DAY_FLT_DAIO_2019,
      DAY_FLT_DAIO_DIF_PREV_WEEK,
      DAY_FLT_DAIO_DIF_PREV_YEAR,
      DAY_FLT_DAIO_DIF_2019,
      DAY_FLT_DAIO_DIF_PREV_WEEK_PERC,
      DAY_FLT_DAIO_DIF_PREV_YEAR_PERC,
      DAY_FLT_DAIO_DIF_2019_PERC,
      
      RW_AVG_FLT_DAIO,
      RW_AVG_FLT_DAIO_PREV_WEEK,
      RW_AVG_FLT_DAIO_PREV_YEAR,
      RW_AVG_FLT_DAIO_2020,
      RW_AVG_FLT_DAIO_2019,
      RW_FLT_DAIO_DIF_PREV_YEAR_PERC,
      RW_FLT_DAIO_DIF_2019_PERC,
      
      Y2D_FLT_DAIO_YEAR,
      Y2D_FLT_DAIO_PREV_YEAR,
      Y2D_FLT_DAIO_2019,
      Y2D_AVG_FLT_DAIO_YEAR,
      Y2D_AVG_FLT_DAIO_PREV_YEAR,
      Y2D_AVG_FLT_DAIO_2019,
      Y2D_FLT_DAIO_DIF_PREV_YEAR_PERC,
      Y2D_FLT_DAIO_DIF_2019_PERC,

      LAST_DATA_DAY
      
    ) %>% 
    ungroup() %>% 
    arrange(ANSP_NAME, ENTRY_DATE)
  
  mydatafile <- paste0("sp_delay_day_raw.parquet")
  df_day_year %>% filter(ENTRY_DATE <= mydate) %>% 
    write_parquet(here(archive_dir_raw, stakeholder, mydatafile))
  
  mydatafile <- paste0("sp_traffic_day_raw.parquet")
  df_day %>% write_parquet(here(archive_dir_raw, stakeholder, mydatafile))
  
  # df_day %>% filter(ARP_CODE == "EBCI") %>% filter(FLIGHT_DATE == current_day) %>% select(DAY_ARR)
  
  print(paste(format(now(), "%H:%M:%S"), mydatasource, mydate))
  
}

# execute functions ----
# wef <- "2024-01-01"  #included in output
# til <- "2024-01-03"  #included in output
# current_day <- seq(ymd(til), ymd(wef), by = "-1 day")


sp_traffic_delay()
# purrr::walk(current_day, ao_st_des)
