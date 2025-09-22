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
list_airport_ext <- export_query(list_ap_ext_query) 

## iso country ----
dim_iso_country <- export_query(dim_iso_st_query) 

## icao country ----
list_icao_country <- export_query(list_icao_st_query) %>% 
  add_row(COUNTRY_CODE = 'LEGC', COUNTRY_NAME = 'Spain')


# prep data functions ----
import_dataframe <- function(dfname) {
  # import data 
  # dfname <- "ap_traffic_delay"
  mydataframe <- dfname
  myparquetfile <- paste0(mydataframe, "_day_base.parquet")
  
  df_base <- read_parquet_duckdb(here(archive_dir_raw,myparquetfile))
  
  df_alldays <- df_base
  
  rm(df_base)
  
  # pre-process data
  if (dfname == "ap_traffic_delay"){
    
    df_app <- df_alldays %>% 
      filter(ARP_ID != 1618) %>%  # undefined
      compute(prudence = "lavish") %>% 
      filter(ARP_ID %in% list_airport_ext$APT_ID) 

  } else if (dfname == "ao_traffic_delay"){
    
    df_app <- df_alldays %>% 
      filter(AO_ID != 1777) %>%  # undefined
      # compute(prudence = "lavish") %>%
      left_join(dim_ao_group, by = c("AO_ID", "AO_CODE")) %>%
      summarise(
        FLIGHT = sum(DAY_TFC, na.rm = TRUE),
        .by = c(YEAR, FLIGHT_DATE, AO_GRP_CODE, AO_GRP_NAME)
      )
  } else if (dfname == "st_dai"){
    
    df_app <- df_alldays %>% 
      filter(!(COUNTRY_CODE %in% c('LE', 'GC'))) %>%  # spain separated
      # compute(prudence = "lavish") %>%
      left_join(list_icao_country, by = c("COUNTRY_CODE")) %>% 
      left_join(dim_iso_country, by = c("COUNTRY_NAME")) 
    
    
  } else {
    df_app <- df_alldays
  }
  
  rm(df_alldays)
  
  return(df_app) 
}



# nw delay cause ----
nw_delay_cause <- function() {
  mydatasource <-  "nw_delay_cause"
  mydatafile <- paste0(mydatasource, "_day_raw.parquet")
  stakeholder <- str_sub(mydatasource, 1, 2)
  
  df_app <- import_dataframe(mydatasource)
  
  mydate <- df_app %>% summarise(max(FLIGHT_DATE, na.rm = TRUE)) %>% pull()
  current_year = year(mydate)

  df_day_year <- df_app %>%
    arrange(FLIGHT_DATE) %>%
    mutate(
      DAY_FLT = FLT, 
      
      DAY_DLY = TDM,
      DAY_DLY_ERT = TDM_ERT,	
      DAY_DLY_APT	= TDM_ARP,
      
      DAY_DLY_PER_FLT	= if_else(FLT == 0, 0, TDM/FLT),
      
      DAY_DLY_CAP_STAF_NOG = TDM_ERT_C + TDM_ERT_S + TDM_ARP_C + TDM_ARP_S,
      DAY_DLY_DISR = TDM_ERT_I + TDM_ERT_T + TDM_ARP_I + TDM_ARP_T,
      DAY_DLY_WTH	= TDM_ERT_W + TDM_ERT_D + TDM_ARP_W + TDM_ARP_D,
      DAY_DLY_APT_CAP = TDM_ERT_G + TDM_ARP_G,
      DAY_DLY_OTH	= TDM - DAY_DLY_CAP_STAF_NOG - DAY_DLY_DISR - DAY_DLY_WTH - DAY_DLY_APT_CAP,

      #rolling week
      ROLL_WK_AVG_FLT = rollsum(DAY_FLT, 7, fill = NA, align = "right") / 7,
      ROLL_WK_AVG_DLY = rollsum(DAY_DLY, 7, fill = NA, align = "right") / 7,
      ROLL_WK_AVG_DLY_ERT = rollsum(DAY_DLY_ERT, 7, fill = NA, align = "right") / 7,
      ROLL_WK_AVG_DLY_APT	= rollsum(DAY_DLY_APT, 7, fill = NA, align = "right") / 7,
      ROLL_WK_AVG_DLY_PER_FLT  = if_else(ROLL_WK_AVG_FLT == 0, 0, ROLL_WK_AVG_DLY/ROLL_WK_AVG_FLT),
      
      ROLL_WK_AVG_DLY_CAP_STAF_NOG = rollsum(DAY_DLY_CAP_STAF_NOG, 7, fill = NA, align = "right") / 7, 	
      ROLL_WK_AVG_DLY_DISR = rollsum(DAY_DLY_DISR, 7, fill = NA, align = "right") / 7,
      ROLL_WK_AVG_DLY_WTH = rollsum(DAY_DLY_WTH, 7, fill = NA, align = "right") / 7,	
      ROLL_WK_AVG_DLY_APT_CAP = rollsum(DAY_DLY_APT_CAP, 7, fill = NA, align = "right") / 7,	
      ROLL_WK_AVG_DLY_OTH = rollsum(DAY_DLY_OTH, 7, fill = NA, align = "right") / 7,	
	
      LAST_DAY = mydate
      
    )  %>% 
    select(
      AREA,
      FLIGHT_DATE,
      
      DAY_FLT, 
      
      DAY_DLY,
      DAY_DLY_ERT,	
      DAY_DLY_APT,
      
      DAY_DLY_PER_FLT,
      
      DAY_DLY_CAP_STAF_NOG,
      DAY_DLY_DISR,
      DAY_DLY_WTH,
      DAY_DLY_APT_CAP,
      DAY_DLY_OTH,
      
      #rolling week
      ROLL_WK_AVG_FLT,
      ROLL_WK_AVG_DLY,
      ROLL_WK_AVG_DLY_ERT,
      ROLL_WK_AVG_DLY_APT,
      ROLL_WK_AVG_DLY_PER_FLT,
      
      ROLL_WK_AVG_DLY_CAP_STAF_NOG, 	
      ROLL_WK_AVG_DLY_DISR,
      ROLL_WK_AVG_DLY_WTH,	
      ROLL_WK_AVG_DLY_APT_CAP,	
      ROLL_WK_AVG_DLY_OTH,	
      
      LAST_DAY
      
    ) 
  
  mydatafile <- paste0("nw_delay_cause_day_raw.parquet")
  df_day_year %>% write_parquet(here(archive_dir_raw, stakeholder, mydatafile))
  
  print(paste(format(now(), "%H:%M:%S"), mydatasource, mydate))
  
}

# nw ao grp ----
nw_ao <- function(mydate =  current_day) {
  mydatasource <-  "ao_traffic_delay"
  
  df_app <- import_dataframe(mydatasource)
  
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
  
}

# nw ap ----
nw_ap <- function(mydate =  current_day) {
  mydatasource <-  "ap_traffic_delay"
  
  df_app <- import_dataframe(mydatasource)
  
  mydataframe <-  "nw_ap"
  
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
    left_join(list_airport_ext, by = c("ARP_ID" = "APT_ID")) %>% 
    group_by(FLIGHT_DATE) %>%
    arrange(FLIGHT_DATE, desc(DEP_ARR), APT_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        FLIGHT_DATE == mydate ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        FLIGHT_DATE == mydate ~ min_rank(desc(DEP_ARR)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        FLIGHT_DATE == day_prev_week ~ min_rank(desc(DEP_ARR)),
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
    group_by(ARP_ID) %>% 
    arrange(ARP_ID, desc(FLIGHT_DATE)) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 41) %>% 
    select(
      FLAG_PERIOD,
      APT_CODE = ARP_CODE,
      APT_NAME,
      DEP_ARR,
      R_RANK,
      RANK,
      RANK_PREV,
      TO_DATE = FLIGHT_DATE
      
    ) %>% 
    arrange(FLAG_PERIOD, R_RANK, APT_NAME)

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
      DEP_ARR = sum(DEP_ARR, na.rm = TRUE),
      .by = c(FLAG_PERIOD, ARP_ID)
    ) %>% 
    ungroup() %>% 
    left_join(list_airport_ext, by = c("ARP_ID" = "APT_ID")) %>% 
    group_by(FLAG_PERIOD) %>%
    arrange(FLAG_PERIOD, desc(DEP_ARR), APT_NAME) %>% 
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
    group_by(ARP_ID) %>% 
    arrange(ARP_ID, FLAG_PERIOD) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 41) %>% 
    mutate(
      FROM_DATE = mydate - days(6),
      TO_DATE = mydate,
      AVG_DEP_ARR = DEP_ARR/7
    ) %>% 
    select(
      FLAG_PERIOD,
      APT_CODE = APT_ICAO_CODE,
      APT_NAME,
      DEP_ARR,
      AVG_DEP_ARR,
      R_RANK,
      RANK,
      RANK_PREV,
      FROM_DATE,
      TO_DATE
      
    ) %>% 
    arrange(FLAG_PERIOD, R_RANK, APT_NAME)
  
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
    filter(FLIGHT_DATE %in% y2d_dates) %>% 
    group_by(YEAR) %>% 
    mutate(
      TO_DATE = max(FLIGHT_DATE, na.rm = TRUE),
      FROM_DATE = min(FLIGHT_DATE, na.rm = TRUE)
    ) %>% 
    ungroup() %>% 
    summarise(
      DEP_ARR = sum(DEP_ARR, na.rm = TRUE),
      .by = c(YEAR, ARP_ID, TO_DATE, FROM_DATE)
    ) %>% 
    left_join(list_airport_ext, by = c("ARP_ID" = "APT_ID")) %>% 
    group_by(YEAR) %>%
    arrange(YEAR, desc(DEP_ARR), APT_NAME) %>% 
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
    group_by(ARP_ID) %>% 
    arrange(ARP_ID, YEAR) %>% 
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
      AVG_DEP_ARR = DEP_ARR/NO_DAYS
    ) %>%
    select(
      YEAR,
      APT_CODE = APT_ICAO_CODE,
      APT_NAME,
      DEP_ARR,	
      AVG_DEP_ARR,
      R_RANK,
      RANK,
      RANK_PREV,
      FROM_DATE,
      TO_DATE
      
    ) %>% 
    arrange(desc(YEAR), R_RANK, APT_NAME)
  
  df_y2d %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  print(paste(format(now(), "%H:%M:%S"), mydataframe, mydate))
  
}


# nw st dai ----
nw_st_dai <- function(mydate =  current_day) {
  mydatasource <-  "st_dai"
  
  df_app <- import_dataframe(mydatasource)
  
  mydataframe <-  "nw_st_dai"
  
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
    summarise(DEP_ARR = sum(DAY_TFC, na.rm = TRUE), .by = c(FLIGHT_DATE, COUNTRY_CODE, COUNTRY_NAME, ISO_COUNTRY_CODE)) %>% 
    group_by(FLIGHT_DATE) %>%
    arrange(FLIGHT_DATE, desc(DEP_ARR), COUNTRY_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        FLIGHT_DATE == mydate ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        FLIGHT_DATE == mydate ~ min_rank(desc(DEP_ARR)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        FLIGHT_DATE == day_prev_week ~ min_rank(desc(DEP_ARR)),
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
    group_by(COUNTRY_CODE) %>% 
    arrange(COUNTRY_CODE, desc(FLIGHT_DATE)) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    # filter(R_RANK < 41) %>% 
    select(
      FLAG_PERIOD,
      COUNTRY_CODE,
      ISO_COUNTRY_CODE,
      COUNTRY_NAME,
      DEP_ARR,
      R_RANK,
      RANK,
      RANK_PREV,
      TO_DATE = FLIGHT_DATE
      
    ) %>% 
    arrange(FLAG_PERIOD, R_RANK, COUNTRY_NAME)
  
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
      DEP_ARR = sum(DAY_TFC, na.rm = TRUE),
      .by = c(FLAG_PERIOD, COUNTRY_CODE, COUNTRY_NAME, ISO_COUNTRY_CODE)
    ) %>% 
    ungroup() %>% 
    group_by(FLAG_PERIOD) %>%
    arrange(FLAG_PERIOD, desc(DEP_ARR), COUNTRY_NAME) %>% 
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
    group_by(COUNTRY_CODE) %>% 
    arrange(COUNTRY_CODE, FLAG_PERIOD) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    mutate(
      FROM_DATE = mydate - days(6),
      TO_DATE = mydate,
      AVG_DEP_ARR = DEP_ARR/7
    ) %>% 
    select(
      FLAG_PERIOD,
      COUNTRY_CODE,
      ISO_COUNTRY_CODE,
      COUNTRY_NAME,
      DEP_ARR,
      AVG_DEP_ARR,
      R_RANK,
      RANK,
      RANK_PREV,
      FROM_DATE,
      TO_DATE
      
    ) %>% 
    arrange(FLAG_PERIOD, R_RANK, COUNTRY_NAME)
  
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
    filter(FLIGHT_DATE %in% y2d_dates) %>% 
    group_by(YEAR) %>% 
    mutate(
      TO_DATE = max(FLIGHT_DATE, na.rm = TRUE),
      FROM_DATE = min(FLIGHT_DATE, na.rm = TRUE)
    ) %>% 
    ungroup() %>% 
    summarise(DEP_ARR = sum(DAY_TFC, na.rm = TRUE), 
              .by = c(YEAR, COUNTRY_CODE, COUNTRY_NAME, ISO_COUNTRY_CODE, FROM_DATE, TO_DATE)) %>% 
    group_by(YEAR) %>%
    arrange(YEAR, desc(DEP_ARR), COUNTRY_NAME) %>% 
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
    group_by(COUNTRY_CODE) %>% 
    arrange(COUNTRY_CODE, YEAR) %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "up") %>% 
    
    fill(RANK, .direction = "down") %>% 
    fill(RANK, .direction = "up") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    mutate(
      NO_DAYS = as.numeric(TO_DATE - FROM_DATE) +1,
      AVG_DEP_ARR = DEP_ARR/NO_DAYS
    ) %>%
    select(
      YEAR,
      COUNTRY_CODE,
      ISO_COUNTRY_CODE,
      COUNTRY_NAME,
      DEP_ARR,	
      AVG_DEP_ARR,
      R_RANK,
      RANK,
      RANK_PREV,
      FROM_DATE,
      TO_DATE
      
    ) %>% 
    arrange(desc(YEAR), R_RANK, COUNTRY_NAME)
  

  df_y2d %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  print(paste(format(now(), "%H:%M:%S"), mydataframe, mydate))
  
}


# execute functions ----
# wef <- "2024-01-01"  #included in output
# til <- "2025-09-11"  #included in output
# current_day <- seq(ymd(til), ymd(wef), by = "-1 day")

nw_delay_cause()
purrr::walk(current_day, nw_ao)
purrr::walk(current_day, nw_ap)
purrr::walk(current_day, nw_st_dai)
