## libraries
library(arrow)
library(fs)
library(tibble)
library(dplyr)
library(tidyr)
library(stringr)
library(readxl)
library(DBI)
library(ROracle)
library(lubridate)
library(zoo)
library(jsonlite)
library(here)
library(RODBC)


# functions ----
source(here::here("..", "mobile-app", "R", "helpers.R"))



# Parameters ----
source(here("..", "mobile-app", "R", "params.R"))


# Queries ----
source(here("..", "mobile-app", "R", "queries_ap.R"))


# Dimensions ----
source(here("..", "mobile-app", "R", "dimension_queries.R"))


# # airport dimension table (lists the airports and their ICAO codes)
# query <- "SELECT
# arp_code AS apt_icao_code,
# arp_name AS apt_name,
# flag_top_apt,
# latitude,
# longitude
# 
# FROM pruprod.v_aiu_app_dim_airport a
# INNER JOIN (
#   SELECT ec_ap_code, latitude, longitude
#   FROM (
#     SELECT ec_ap_code, latitude, longitude,
#            ROW_NUMBER() OVER (PARTITION BY ec_ap_code ORDER BY sk_ap_id DESC) AS rn
#     FROM swh_fct.dim_airport
#   ) t
#   WHERE rn = 1
# ) b ON a.arp_code = b.ec_ap_code
# "
# 
# apt_icao_full <- export_query(query) %>%
#   janitor::clean_names()
# 
# apt_icao <- apt_icao_full %>% select (apt_icao_code, apt_name)

list_airport_extended <- export_query(list_ap_ext_query) %>% 
  janitor::clean_names() %>% select(-apt_id)


# archive mode for past dates
if (exists("archive_mode") == FALSE) {archive_mode <- FALSE}
if (exists("data_day_date") == FALSE) {
  data_day_date <- lubridate::today(tzone = "") +  days(-1)
}

data_day_text <- data_day_date %>% format("%Y%m%d")
data_day_year <- as.numeric(format(data_day_date,'%Y'))


apt_json_app <-""

# ____________________________________________________________________________________________
#
#    APT landing page -----
#
# ____________________________________________________________________________________________

#### Traffic data ----
mydataframe <- "ap_traffic_day_raw"
stakeholder <- "ap"

if (!exists("apt_traffic_data_all")) {
  apt_traffic_data_all <- read_parquet(here(archive_dir_raw, stakeholder, paste0(mydataframe, ".parquet"))) 
}

apt_traffic_data <- apt_traffic_data_all %>% filter(YEAR == data_day_year) %>% 
  # iceland exception 
  mutate(across(contains("2019"), ~ replace(.x, substr(ARP_CODE, 1,2) == "BI", NA))) %>% 
  mutate(across(contains("2020"), ~ replace(.x, substr(ARP_CODE, 1,2) == "BI", NA))) 


#getting the latest date's traffic data
apt_traffic_last_day <- apt_traffic_data %>%
  filter(FLIGHT_DATE == min(data_day_date,
                            max(LAST_DATA_DAY, na.rm = TRUE),
                            na.rm = TRUE)
  ) %>%
  arrange(ARP_NAME, FLIGHT_DATE)

#selecting columns and renaming
apt_traffic_for_json <- apt_traffic_last_day %>%
  right_join(list_airport_extended, by = c("ARP_CODE" = "apt_icao_code", "ARP_NAME" = "apt_name")) %>%
  group_by(flag_top_apt) %>%
  mutate(
    DY_TFC_RANK = if_else(flag_top_apt == "N", NA, min_rank(desc(DAY_DEP_ARR))),
    WK_TFC_RANK = if_else(flag_top_apt == "N", NA, min_rank(desc(RWK_AVG_DEP_ARR))),
    Y2D_TFC_RANK = if_else(flag_top_apt == "N", NA, min_rank(desc(Y2D_DEP_ARR_YEAR))),
    
    TFC_RANK_TEXT = "*Rank within top 40 airports\nTop rank for highest."
    
  ) %>%
  ungroup() %>%
  select(
    ARP_NAME,
    ARP_CODE,
    FLIGHT_DATE,
    #day
    DY_TFC_RANK,
    DY_TFC = DAY_DEP_ARR, #arrival and departure traffic
    DY_TFC_DIF_PREV_YEAR_PERC = DAY_DEP_ARR_DIF_PREV_YEAR_PERC,
    DY_TFC_DIF_2019_PERC = DAY_DEP_ARR_DIF_2019_PERC,
    #week average
    WK_TFC_RANK,
    WK_TFC_AVG_ROLLING = RWK_AVG_DEP_ARR,
    WK_TFC_DIF_PREV_YEAR_PERC = RWK_DEP_ARR_DIF_PREV_YEAR_PERC,
    WK_TFC_DIF_2019_PERC = RWK_DEP_ARR_DIF_2019_PERC,
    #year to date
    Y2D_TFC_RANK,
    Y2D_TFC = Y2D_DEP_ARR_YEAR,
    Y2D_TFC_AVG = Y2D_AVG_DEP_ARR_YEAR,
    Y2D_TFC_DIF_PREV_YEAR_PERC = Y2D_DEP_ARR_DIF_PREV_YEAR_PERC,
    Y2D_TFC_DIF_2019_PERC = Y2D_DEP_ARR_DIF_2019_PERC,
    
    TFC_RANK_TEXT
  ) 


#### Delay data ----
mydataframe <- "ap_delay_day_raw"
stakeholder <- "ap"

if (!exists("apt_delay_data")) {
  apt_delay_data <- read_parquet(here(archive_dir_raw, stakeholder, paste0(mydataframe, ".parquet")))
}

#adding rolling week average and year-to-date(Y2D)
apt_delay_data_calc <- apt_delay_data %>%
  arrange(FLIGHT_DATE) %>%
  group_by(ARP_CODE) %>%
  mutate(#delay minutes
    DAY_DLY = TDM_ARP_ARR,
    AVG_DLY_ROLLING_WEEK = rollmean( DAY_DLY , 7, align = "right", fill = NA),
    #number of flights
    DAY_TFC = DAY_ARR,
    AVG_TFC_ROLLING_WEEK = rollmean( DAY_TFC , 7, align = "right", fill = NA),
    #number of delayed flights
    DAY_DELAYED_TFC = TDF_ARP_ARR,
    AVG_DELAYED_TFC_ROLLING_WEEK = rollmean( TDF_ARP_ARR , 7, align = "right", fill = NA),
    #number delayed flights >15 minutes'
    DAY_DELAYED_TFC_15 = TDF_15_ARP_ARR,
    AVG_DELAYED_TFC_15_ROLLING_WEEK = rollmean( TDF_15_ARP_ARR , 7, align = "right", fill = NA)
  ) %>%
  group_by( year( FLIGHT_DATE), ARP_CODE) %>%
  mutate(#year to date
    #delay
    Y2D_DLY_YEAR = cumsum( coalesce( DAY_DLY , 0)),
    Y2D_AVG_DLY = cummean( coalesce( DAY_DLY , 0)),
    #number of flights
    Y2D_TFC_YEAR = cumsum( coalesce( DAY_TFC , 0)),
    Y2D_AVG_TFC = cummean( coalesce( DAY_TFC , 0)),
    #number of delayed flights
    Y2D_DELAYED_TFC_YEAR = cumsum( coalesce( DAY_DELAYED_TFC , 0)),
    Y2D_AVG_DELAYED_TFC = cummean( coalesce( DAY_DELAYED_TFC , 0)),
    #number delayed flights >15 minutes'
    Y2D_DELAYED_TFC_15_YEAR = cumsum( coalesce( DAY_DELAYED_TFC_15 , 0)),
    Y2D_AVG_DELAYED_TFC_15 = cummean( coalesce( DAY_DELAYED_TFC_15 , 0))) %>%
  ungroup()

#adding previous year and 2019 values
apt_delay_data_full <- apt_delay_data_calc %>%
  #2019, and previous year values
  mutate(DATE_PREV_YEAR = FLIGHT_DATE-364,
         DATE_2019 = FLIGHT_DATE - days((YEAR-2019)*364+ floor((YEAR - 2019) / 4) * 7),
         DATE_2019_SD = FLIGHT_DATE %m-% years(YEAR-2019),
         DATE_PREV_YEAR_SD = FLIGHT_DATE %m-% years(1)) %>%
  arrange(ARP_CODE, FLIGHT_DATE) %>%
  # ensuring no nas get in the way
  mutate(
    #delay minutes
    coalesce(DAY_DLY, 0),
    coalesce(AVG_DLY_ROLLING_WEEK, 0),
    coalesce(Y2D_DLY_YEAR, 0),
    coalesce(Y2D_AVG_DLY, 0),
    
    #number of flights
    coalesce(DAY_TFC, 0),
    coalesce(AVG_TFC_ROLLING_WEEK, 0),
    coalesce(Y2D_TFC_YEAR, 0),
    coalesce(Y2D_AVG_TFC, 0),
    
    #number of delayed flights
    coalesce(DAY_DELAYED_TFC, 0),
    coalesce(AVG_DELAYED_TFC_ROLLING_WEEK, 0),
    coalesce(Y2D_DELAYED_TFC_YEAR, 0),
    coalesce(Y2D_AVG_DELAYED_TFC, 0),
    
    #number delayed flights >15 minutes'
    coalesce(DAY_DELAYED_TFC_15, 0),
    coalesce(AVG_DELAYED_TFC_15_ROLLING_WEEK, 0),
    coalesce(Y2D_DELAYED_TFC_15_YEAR, 0),
    coalesce(Y2D_AVG_DELAYED_TFC_15, 0)
    ) %>% 
   #obtaining data for previous year op day
  left_join(apt_delay_data_calc %>% select(
                                      ARP_CODE,
                                      FLIGHT_DATE,
                                      #delay minutes
                                      DAY_DLY,
                                      AVG_DLY_ROLLING_WEEK,

                                      #number of flights
                                      DAY_TFC,
                                      AVG_TFC_ROLLING_WEEK,

                                      #number of delayed flights
                                      DAY_DELAYED_TFC,
                                      AVG_DELAYED_TFC_ROLLING_WEEK,

                                      #number delayed flights >15 minutes'
                                      DAY_DELAYED_TFC_15,
                                      AVG_DELAYED_TFC_15_ROLLING_WEEK,

  ),
  by = join_by(DATE_PREV_YEAR == FLIGHT_DATE, ARP_CODE == ARP_CODE),
  suffix = c("", "_PREV_YEAR"))  %>%
  #obtaining data for previous year same date
  left_join(apt_delay_data_calc %>% select(
    ARP_CODE,
    FLIGHT_DATE,
    #delay minutes
    Y2D_DLY_YEAR,
    Y2D_AVG_DLY,
    
    #number of flights
    Y2D_TFC_YEAR,
    Y2D_AVG_TFC,
    
    #number of delayed flights
    Y2D_DELAYED_TFC_YEAR,
    Y2D_AVG_DELAYED_TFC,
    
    #number delayed flights >15 minutes'
    Y2D_DELAYED_TFC_15_YEAR,
    Y2D_AVG_DELAYED_TFC_15
  ),
  by = join_by(DATE_PREV_YEAR_SD == FLIGHT_DATE, ARP_CODE == ARP_CODE),
  suffix = c("", "_PREV_YEAR"))  %>%
  #obtaining data for 2019 op date
  left_join(apt_delay_data_calc %>% select(ARP_CODE,
                                      FLIGHT_DATE,
                                      #delay minutes
                                      DAY_DLY,
                                      AVG_DLY_ROLLING_WEEK,

                                      #number of flights
                                      DAY_TFC,
                                      AVG_TFC_ROLLING_WEEK,

                                      #number of delayed flights
                                      DAY_DELAYED_TFC,
                                      AVG_DELAYED_TFC_ROLLING_WEEK,
                                      
                                      #number delayed flights >15 minutes'
                                      DAY_DELAYED_TFC_15,
                                      AVG_DELAYED_TFC_15_ROLLING_WEEK
  ),
  by = join_by(DATE_2019 == FLIGHT_DATE, ARP_CODE == ARP_CODE),
  suffix = c("", "_2019")) %>% 
  #obtaining data for 2019 same date
  left_join(apt_delay_data_calc %>% select(ARP_CODE,
                                           FLIGHT_DATE,
                                           #delay minutes
                                           Y2D_DLY_YEAR,
                                           Y2D_AVG_DLY,
                                           
                                           #number of flights
                                           Y2D_TFC_YEAR,
                                           Y2D_AVG_TFC,
                                           
                                           #number of delayed flights
                                           Y2D_DELAYED_TFC_YEAR,
                                           Y2D_AVG_DELAYED_TFC,
                                           
                                           #number delayed flights >15 minutes'
                                           Y2D_DELAYED_TFC_15_YEAR,
                                           Y2D_AVG_DELAYED_TFC_15
  ),
  by = join_by(DATE_2019_SD == FLIGHT_DATE, ARP_CODE == ARP_CODE),
  suffix = c("", "_2019"))


#getting the latest date's traffic data
apt_delay_last_day <- apt_delay_data_full %>%
  filter(FLIGHT_DATE == min(data_day_date,
                            max(FLIGHT_DATE),
                            na.rm = TRUE))


#creating, selecting and renaming columns
apt_delay_for_json  <- apt_delay_last_day %>%
  mutate(
    #delay minutes
    #DAY_DLY,
    DY_DLY_DIF_PREV_YEAR_PERC = if_else(DAY_DLY_PREV_YEAR == 0, NA , DAY_DLY / DAY_DLY_PREV_YEAR - 1),
    DY_DLY_DIF_2019_PERC = if_else( DAY_DLY_2019 == 0, NA , DAY_DLY / DAY_DLY_2019 - 1),

    WK_DLY_DIF_PREV_YEAR_PERC = if_else(AVG_DLY_ROLLING_WEEK_PREV_YEAR == 0, NA , AVG_DLY_ROLLING_WEEK / AVG_DLY_ROLLING_WEEK_PREV_YEAR - 1),
    WK_DLY_DIF_2019_PERC = if_else(AVG_DLY_ROLLING_WEEK_2019 == 0 , NA, AVG_DLY_ROLLING_WEEK / AVG_DLY_ROLLING_WEEK_2019 - 1),


    Y2D_AVG_DLY_YEAR = Y2D_AVG_DLY,
    Y2D_DLY_DIF_PREV_YEAR_PERC = if_else(
      Y2D_AVG_DLY_PREV_YEAR == 0 , NA , Y2D_AVG_DLY / Y2D_AVG_DLY_PREV_YEAR - 1
    ),
    Y2D_DLY_DIF_2019_PERC = if_else(
      Y2D_AVG_DLY_2019 == 0 , NA , Y2D_AVG_DLY / Y2D_AVG_DLY_2019 - 1
    ),


    #delay per flight
    DY_DLY_FLT = if_else(DAY_TFC == 0, 0, DAY_DLY / DAY_TFC),
    DY_DLY_FLT_PY = if_else(DAY_TFC_PREV_YEAR == 0, 0, DAY_DLY_PREV_YEAR / DAY_TFC_PREV_YEAR),
    DY_DLY_FLT_2019 = if_else(DAY_TFC_2019 == 0, 0, DAY_DLY_2019 / DAY_TFC_2019),
    DY_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(DY_DLY_FLT_PY == 0, NA , DY_DLY_FLT / DY_DLY_FLT_PY - 1),
    DY_DLY_FLT_DIF_2019_PERC = if_else(DY_DLY_FLT_2019 == 0, NA , DY_DLY_FLT / DY_DLY_FLT_2019 - 1),

    WK_DLY_FLT  = if_else(AVG_TFC_ROLLING_WEEK == 0, 0, AVG_DLY_ROLLING_WEEK / AVG_TFC_ROLLING_WEEK),
    WK_DLY_FLT_PY = if_else(AVG_TFC_ROLLING_WEEK_PREV_YEAR == 0, 0, AVG_DLY_ROLLING_WEEK_PREV_YEAR / AVG_TFC_ROLLING_WEEK_PREV_YEAR),
    WK_DLY_FLT_2019 = if_else(AVG_TFC_ROLLING_WEEK_2019 == 0, 0, AVG_DLY_ROLLING_WEEK_2019 / AVG_TFC_ROLLING_WEEK_2019),
    WK_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(WK_DLY_FLT_PY == 0, NA , WK_DLY_FLT / WK_DLY_FLT_PY - 1),
    WK_DLY_FLT_DIF_2019_PERC = if_else(WK_DLY_FLT_2019 == 0, NA , WK_DLY_FLT / WK_DLY_FLT_2019 - 1),


    Y2D_DLY_FLT = if_else(Y2D_TFC_YEAR == 0, 0, Y2D_DLY_YEAR / Y2D_TFC_YEAR),
    Y2D_DLY_FLT_PY = if_else(Y2D_AVG_TFC_PREV_YEAR == 0, 0, Y2D_AVG_DLY_PREV_YEAR / Y2D_AVG_TFC_PREV_YEAR),
    Y2D_DLY_FLT_2019 = if_else(Y2D_AVG_TFC_2019 == 0, 0, Y2D_AVG_DLY_2019 / Y2D_AVG_TFC_2019),
    Y2D_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(Y2D_DLY_FLT_PY == 0, NA , Y2D_DLY_FLT / Y2D_DLY_FLT_PY -1),
    Y2D_DLY_FLT_DIF_2019_PERC = if_else(Y2D_DLY_FLT_2019 == 0, NA , Y2D_DLY_FLT / Y2D_DLY_FLT_2019 -1),


    #% of delayed flights
    DY_DELAYED_TFC_PERC = if_else(DAY_TFC == 0, 0, DAY_DELAYED_TFC / DAY_TFC),
    DY_DELAYED_TFC_PY_PERC = if_else(DAY_TFC_PREV_YEAR == 0, 0, DAY_DELAYED_TFC_PREV_YEAR / DAY_TFC_PREV_YEAR),
    DY_DELAYED_TFC_2019_PERC = if_else(DAY_TFC_2019 == 0, 0, DAY_DELAYED_TFC_2019 / DAY_TFC_2019),
    DY_DELAYED_TFC_PERC_DIF_PREV_YEAR = DY_DELAYED_TFC_PERC - DY_DELAYED_TFC_PY_PERC,
    DY_DELAYED_TFC_PERC_DIF_2019 = DY_DELAYED_TFC_PERC - DY_DELAYED_TFC_2019_PERC,


    WK_DELAYED_TFC_PERC = if_else(AVG_TFC_ROLLING_WEEK == 0, 0, AVG_DELAYED_TFC_ROLLING_WEEK / AVG_TFC_ROLLING_WEEK),
    WK_DELAYED_TFC_PY_PERC = if_else(AVG_TFC_ROLLING_WEEK_PREV_YEAR == 0, 0, AVG_DELAYED_TFC_ROLLING_WEEK_PREV_YEAR / AVG_TFC_ROLLING_WEEK_PREV_YEAR),
    WK_DELAYED_TFC_2019_PERC = if_else(AVG_TFC_ROLLING_WEEK_2019 == 0, 0, AVG_DELAYED_TFC_ROLLING_WEEK_2019 / AVG_TFC_ROLLING_WEEK_2019),
    WK_DELAYED_TFC_PERC_DIF_PREV_YEAR = WK_DELAYED_TFC_PERC - WK_DELAYED_TFC_PY_PERC,
    WK_DELAYED_TFC_PERC_DIF_2019 = WK_DELAYED_TFC_PERC - WK_DELAYED_TFC_2019_PERC,


    Y2D_DELAYED_TFC_PERC = if_else(Y2D_TFC_YEAR == 0, 0, Y2D_DELAYED_TFC_YEAR / Y2D_TFC_YEAR),
    Y2D_DELAYED_TFC_PY_PERC = if_else(Y2D_AVG_TFC_PREV_YEAR == 0, 0, Y2D_AVG_DELAYED_TFC_PREV_YEAR / Y2D_AVG_TFC_PREV_YEAR),
    Y2D_DELAYED_TFC_2019_PERC = if_else(Y2D_AVG_TFC_2019 == 0, 0, Y2D_AVG_DELAYED_TFC_2019 / Y2D_AVG_TFC_2019),
    Y2D_DELAYED_TFC_PERC_DIF_PREV_YEAR = Y2D_DELAYED_TFC_PERC - Y2D_DELAYED_TFC_PY_PERC,
    Y2D_DELAYED_TFC_PERC_DIF_2019 = Y2D_DELAYED_TFC_PERC - Y2D_DELAYED_TFC_2019_PERC,

    #% of delayed flights >15'
    DY_DELAYED_TFC_15_PERC = if_else(DAY_TFC == 0, 0, DAY_DELAYED_TFC_15 / DAY_TFC),
    DY_DELAYED_TFC_15_PY_PERC = if_else(DAY_TFC_PREV_YEAR == 0, 0, DAY_DELAYED_TFC_15_PREV_YEAR / DAY_TFC_PREV_YEAR),
    DY_DELAYED_TFC_15_2019_PERC = if_else(DAY_TFC_2019 == 0, 0, DAY_DELAYED_TFC_15_2019 / DAY_TFC_2019),
    DY_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = DY_DELAYED_TFC_15_PERC - DY_DELAYED_TFC_15_PY_PERC,
    DY_DELAYED_TFC_15_PERC_DIF_2019 = DY_DELAYED_TFC_15_PERC - DY_DELAYED_TFC_15_2019_PERC,


    WK_DELAYED_TFC_15_PERC = if_else(AVG_TFC_ROLLING_WEEK == 0, 0, AVG_DELAYED_TFC_15_ROLLING_WEEK / AVG_TFC_ROLLING_WEEK),
    WK_DELAYED_TFC_15_PY_PERC = if_else(AVG_TFC_ROLLING_WEEK_PREV_YEAR == 0, 0, AVG_DELAYED_TFC_15_ROLLING_WEEK_PREV_YEAR / AVG_TFC_ROLLING_WEEK_PREV_YEAR),
    WK_DELAYED_TFC_15_2019_PERC = if_else(AVG_TFC_ROLLING_WEEK_2019 == 0, 0, AVG_DELAYED_TFC_15_ROLLING_WEEK_2019 / AVG_TFC_ROLLING_WEEK_2019),
    WK_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = WK_DELAYED_TFC_15_PERC - WK_DELAYED_TFC_15_PY_PERC,
    WK_DELAYED_TFC_15_PERC_DIF_2019 = WK_DELAYED_TFC_15_PERC - WK_DELAYED_TFC_15_2019_PERC,


    Y2D_DELAYED_TFC_15_PERC = if_else(Y2D_TFC_YEAR == 0, 0, Y2D_DELAYED_TFC_15_YEAR / Y2D_TFC_YEAR),
    Y2D_DELAYED_TFC_15_PY_PERC = if_else(Y2D_AVG_TFC_PREV_YEAR == 0, 0, Y2D_AVG_DELAYED_TFC_15_PREV_YEAR / Y2D_AVG_TFC_PREV_YEAR),
    Y2D_DELAYED_TFC_15_2019_PERC = if_else(Y2D_AVG_TFC_2019 == 0, 0, Y2D_AVG_DELAYED_TFC_15_2019 / Y2D_AVG_TFC_2019),
    Y2D_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = Y2D_DELAYED_TFC_15_PERC - Y2D_DELAYED_TFC_15_PY_PERC,
    Y2D_DELAYED_TFC_15_PERC_DIF_2019 = Y2D_DELAYED_TFC_15_PERC - Y2D_DELAYED_TFC_15_2019_PERC

  ) %>%
  ### rank calculation
  right_join(list_airport_extended, by = c("ARP_CODE" = "apt_icao_code", "ARP_NAME" = "apt_name")) %>%
  group_by(flag_top_apt) %>%
  mutate(
    DY_DLY_RANK = if_else(flag_top_apt == "N", NA, 
                          rank(desc(DAY_DLY), ties.method = "max")),
    WK_DLY_RANK = if_else(flag_top_apt == "N", NA, 
                          rank(desc(AVG_DLY_ROLLING_WEEK), ties.method = "max")),
    Y2D_DLY_RANK = if_else(flag_top_apt == "N", NA, 
                           rank(desc(Y2D_DLY_YEAR), ties.method = "max")),

    DY_DLY_FLT_RANK = if_else(flag_top_apt == "N", NA, 
                              rank(desc(DY_DLY_FLT), ties.method = "max")),
    WK_DLY_FLT_RANK = if_else(flag_top_apt == "N", NA, 
                              rank(desc(WK_DLY_FLT), ties.method = "max")),
    Y2D_DLY_FLT_RANK = if_else(flag_top_apt == "N", NA, 
                               rank(desc(Y2D_DLY_FLT), ties.method = "max")),

    DY_DELAYED_TFC_PERC_RANK = if_else(flag_top_apt == "N", NA, 
                                       rank(desc(DY_DELAYED_TFC_PERC), ties.method = "max")),
    WK_DELAYED_TFC_PERC_RANK = if_else(flag_top_apt == "N", NA, 
                                       rank(desc(WK_DELAYED_TFC_PERC), ties.method = "max")),
    Y2D_DELAYED_TFC_PERC_RANK = if_else(flag_top_apt == "N", NA, 
                                        rank(desc(Y2D_DELAYED_TFC_PERC), ties.method = "max")),

    DY_DELAYED_TFC_15_PERC_RANK = if_else(flag_top_apt == "N", NA, 
                                          rank(desc(DY_DELAYED_TFC_15_PERC), ties.method = "max")),
    WK_DELAYED_TFC_15_PERC_RANK = if_else(flag_top_apt == "N", NA, 
                                          rank(desc(WK_DELAYED_TFC_15_PERC), ties.method = "max")),
    Y2D_DELAYED_TFC_15_PERC_RANK = if_else(flag_top_apt == "N", NA, 
                                           rank(desc(Y2D_DELAYED_TFC_15_PERC), ties.method = "max")),

    DLY_RANK_TEXT = "*Rank within top 40 airports.\nTop rank for highest."
  ) %>%
  ungroup() %>%
  select(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE,

    #delay
    DY_DLY_RANK,
    DY_DLY = DAY_DLY,
    DY_DLY_DIF_PREV_YEAR_PERC,
    DY_DLY_DIF_2019_PERC,

    WK_DLY_RANK,
    WK_DLY_AVG_ROLLING = AVG_DLY_ROLLING_WEEK,
    WK_DLY_DIF_PREV_YEAR_PERC,
    WK_DLY_DIF_2019_PERC,

    Y2D_DLY_RANK,
    Y2D_DLY_AVG = Y2D_AVG_DLY_YEAR,
    Y2D_DLY_DIF_PREV_YEAR_PERC,
    Y2D_DLY_DIF_2019_PERC,

    #delay per flight
    DY_DLY_FLT_RANK,
    DY_DLY_FLT,
    DY_DLY_FLT_DIF_PREV_YEAR_PERC,
    DY_DLY_FLT_DIF_2019_PERC,

    WK_DLY_FLT_RANK,
    WK_DLY_FLT,
    WK_DLY_FLT_DIF_PREV_YEAR_PERC,
    WK_DLY_FLT_DIF_2019_PERC,

    Y2D_DLY_FLT_RANK,
    Y2D_DLY_FLT,
    Y2D_DLY_FLT_DIF_PREV_YEAR_PERC,
    Y2D_DLY_FLT_DIF_2019_PERC,

    #% of delayed flights
    DY_DELAYED_TFC_PERC_RANK,
    DY_DELAYED_TFC_PERC,
    DY_DELAYED_TFC_PERC_DIF_PREV_YEAR,
    DY_DELAYED_TFC_PERC_DIF_2019,

    WK_DELAYED_TFC_PERC_RANK,
    WK_DELAYED_TFC_PERC,
    WK_DELAYED_TFC_PERC_DIF_PREV_YEAR,
    WK_DELAYED_TFC_PERC_DIF_2019,

    Y2D_DELAYED_TFC_PERC_RANK,
    Y2D_DELAYED_TFC_PERC,
    Y2D_DELAYED_TFC_PERC_DIF_PREV_YEAR,
    Y2D_DELAYED_TFC_PERC_DIF_2019,

    #% of delayed flights >15'
    DY_DELAYED_TFC_15_PERC_RANK,
    DY_DELAYED_TFC_15_PERC,
    DY_DELAYED_TFC_15_PERC_DIF_PREV_YEAR,
    DY_DELAYED_TFC_15_PERC_DIF_2019,

    WK_DELAYED_TFC_15_PERC_RANK,
    WK_DELAYED_TFC_15_PERC,
    WK_DELAYED_TFC_15_PERC_DIF_PREV_YEAR,
    WK_DELAYED_TFC_15_PERC_DIF_2019,

    Y2D_DELAYED_TFC_15_PERC_RANK,
    Y2D_DELAYED_TFC_15_PERC,
    Y2D_DELAYED_TFC_15_PERC_DIF_PREV_YEAR,
    Y2D_DELAYED_TFC_15_PERC_DIF_2019,

    DLY_RANK_TEXT
  ) %>%
  arrange(ARP_CODE,
          ARP_NAME) %>% 
  # iceland exception 
  mutate(across(contains("2019"), ~ replace(.x, substr(ARP_CODE, 1,2) == "BI", NA)))

#### Punctuality data ----
#querying the data in SQL
apt_punct_raw <- export_query(query_ap_punct(format(data_day_date, "%Y-%m-%d"))) %>%
  as_tibble() %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

#selecting the date for which we have the latest traffic data
last_day_punct <-  min(max(apt_punct_raw$DAY_DATE),
                       data_day_date, na.rm = TRUE)
#selecting the year for which we have the latest traffic data
last_year_punct <- as.numeric(format(last_day_punct,'%Y'))


#####  Data totals----
#preparing the data
apt_punct_data <- apt_punct_raw %>%
  group_by(ARP_CODE, ARP_NAME, DAY_DATE) %>%
  summarise(
    ARR_PUNCTUAL_FLIGHTS = sum( ARR_PUNCTUAL_FLIGHTS , na.rm = TRUE),
    ARR_SCHEDULE_FLIGHT = sum( ARR_SCHEDULE_FLIGHT , na.rm = TRUE),
    DEP_PUNCTUAL_FLIGHTS = sum( DEP_PUNCTUAL_FLIGHTS , na.rm = TRUE),
    DEP_SCHEDULE_FLIGHT = sum( DEP_SCHEDULE_FLIGHT , na.rm = TRUE),
    .groups = "drop"
  ) %>%
  group_by(ARP_CODE, ARP_NAME) %>%
  arrange( ARP_CODE, DAY_DATE ) %>%
  mutate(
    YEAR = lubridate::year(DAY_DATE),
    #ARR_PUNCTUAL_FLIGHTS = 0,  ## to hide the figures
    #DEP_PUNCTUAL_FLIGHTS = 0,  ## to hide the figures

    DAY_ARR_PUNCT = if_else(
      ARR_SCHEDULE_FLIGHT == 0 , 0 , ARR_PUNCTUAL_FLIGHTS / ARR_SCHEDULE_FLIGHT * 100
    ),
    DAY_DEP_PUNCT = if_else(
      DEP_SCHEDULE_FLIGHT == 0 , 0 , DEP_PUNCTUAL_FLIGHTS / DEP_SCHEDULE_FLIGHT * 100
    ),
    #previous year
    DAY_ARR_PUNCT_PY = lag( DAY_ARR_PUNCT , 364),
    DAY_DEP_PUNCT_PY = lag( DAY_DEP_PUNCT , 364),
    #2019
    DAY_ARR_PUNCT_2019 =if_else(
      YEAR == last_year_punct , lag( DAY_ARR_PUNCT ,  364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7), NA
    ),
    DAY_DEP_PUNCT_2019 =if_else(
      YEAR == last_year_punct , lag(DAY_DEP_PUNCT, 364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7), NA
    ),
    DAY_2019 = if_else(YEAR == last_year_punct,
                       lag(DAY_DATE,
                           364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7) , NA
    ),
    DAY_ARR_PUNCT_DIF_PY = DAY_ARR_PUNCT - DAY_ARR_PUNCT_PY,
    DAY_DEP_PUNCT_DIF_PY = DAY_DEP_PUNCT - DAY_DEP_PUNCT_PY,
    DAY_ARR_PUNCT_DIF_2019 = DAY_ARR_PUNCT - DAY_ARR_PUNCT_2019,
    DAY_DEP_PUNCT_DIF_2019 = DAY_DEP_PUNCT - DAY_DEP_PUNCT_2019
  ) %>%
  #rolling sum
  mutate(
    WEEK_ARR_PUNCT = rollsum((ARR_PUNCTUAL_FLIGHTS), 7, fill = NA, align = "right") / rollsum(ARR_SCHEDULE_FLIGHT, 7, fill = NA, align = "right") * 100,
    WEEK_DEP_PUNCT = rollsum((DEP_PUNCTUAL_FLIGHTS), 7, fill = NA, align = "right") / rollsum(DEP_SCHEDULE_FLIGHT, 7, fill = NA, align = "right") * 100
  ) %>%
  mutate(WEEK_ARR_PUNCT_PY = lag(WEEK_ARR_PUNCT, 364),
         WEEK_DEP_PUNCT_PY = lag(WEEK_DEP_PUNCT, 364),
         WEEK_ARR_PUNCT_2019 =if_else(YEAR == last_year_punct,
                                      lag(WEEK_ARR_PUNCT, 364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7),
                                      NA),
         WEEK_DEP_PUNCT_2019 =if_else(YEAR == last_year_punct,
                                      lag(WEEK_DEP_PUNCT, 364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7),
                                      NA),
         WEEK_ARR_PUNCT_DIF_PY = WEEK_ARR_PUNCT - WEEK_ARR_PUNCT_PY,
         WEEK_DEP_PUNCT_DIF_PY = WEEK_DEP_PUNCT - WEEK_DEP_PUNCT_PY,
         WEEK_ARR_PUNCT_DIF_2019 = WEEK_ARR_PUNCT - WEEK_ARR_PUNCT_2019,
         WEEK_DEP_PUNCT_DIF_2019 = WEEK_DEP_PUNCT - WEEK_DEP_PUNCT_2019
  )


#filtering the data and selecting and renaming columns
apt_punct_d_w <- apt_punct_data %>%
  filter (DAY_DATE == last_day_punct) %>%
  select(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE = DAY_DATE,
    DY_ARR_PUN = DAY_ARR_PUNCT,
    DY_DEP_PUN = DAY_DEP_PUNCT,

    DY_ARR_PUN_DIF_PREV_YEAR = DAY_ARR_PUNCT_DIF_PY,
    DY_DEP_PUN_DIF_PREV_YEAR = DAY_DEP_PUNCT_DIF_PY,
    DY_ARR_PUN_DIF_2019 = DAY_ARR_PUNCT_DIF_2019,
    DY_DEP_PUN_DIF_2019 = DAY_DEP_PUNCT_DIF_2019,

    WK_ARR_PUN = WEEK_ARR_PUNCT,
    WK_DEP_PUN = WEEK_DEP_PUNCT,
    WK_ARR_PUN_DIF_PREV_YEAR = WEEK_ARR_PUNCT_DIF_PY,
    WK_DEP_PUN_DIF_PREV_YEAR = WEEK_DEP_PUNCT_DIF_PY,
    WK_ARR_PUN_DIF_2019 = WEEK_ARR_PUNCT_DIF_2019,
    WK_DEP_PUN_DIF_2019 = WEEK_DEP_PUNCT_DIF_2019
  )



##### Year to Date----
apt_punct_y2d <- apt_punct_raw %>%
  arrange(ARP_CODE, DAY_DATE) %>%
  mutate(
    MONTH_DAY = as.numeric(format(DAY_DATE, format="%m%d"))
    # , ARR_PUNCTUAL_FLIGHTS = 0,  ## while the figures are not showable
    # DEP_PUNCTUAL_FLIGHTS = 0,  ## while the figures are not showable
  ) %>%
  filter(MONTH_DAY <= as.numeric(format(last_day_punct, format="%m%d"))) %>%
  group_by(ARP_CODE, ARP_NAME, YEAR) %>%
  summarise (
    Y2D_ARR_PUN = sum(ARR_PUNCTUAL_FLIGHTS, na.rm=TRUE) / sum(ARR_SCHEDULE_FLIGHT, na.rm=TRUE) * 100,
    Y2D_DEP_PUN = sum(DEP_PUNCTUAL_FLIGHTS, na.rm=TRUE) / sum(DEP_SCHEDULE_FLIGHT, na.rm=TRUE) * 100,
    .groups = "drop"
    ) %>%
  group_by(ARP_CODE, ARP_NAME) %>%
  mutate(Y2D_ARR_PUN_PY = lag(Y2D_ARR_PUN, 1),
         Y2D_DEP_PUN_PY = lag(Y2D_DEP_PUN, 1),
         Y2D_ARR_PUN_2019 = lag(Y2D_ARR_PUN, last_year_punct - 2019),
         Y2D_DEP_PUN_2019 = lag(Y2D_DEP_PUN, last_year_punct - 2019),
         Y2D_ARR_PUN_DIF_PREV_YEAR = Y2D_ARR_PUN - Y2D_ARR_PUN_PY,
         Y2D_DEP_PUN_DIF_PREV_YEAR = Y2D_DEP_PUN - Y2D_DEP_PUN_PY,
         Y2D_ARR_PUN_DIF_2019 = Y2D_ARR_PUN - Y2D_ARR_PUN_2019,
         Y2D_DEP_PUN_DIF_2019 = Y2D_DEP_PUN - Y2D_DEP_PUN_2019
  ) %>%
  filter(YEAR == last_year_punct) %>%
  ungroup() %>%
  select(ARP_CODE,
         ARP_NAME,
         Y2D_ARR_PUN,
         Y2D_DEP_PUN,
         Y2D_ARR_PUN_DIF_PREV_YEAR,
         Y2D_DEP_PUN_DIF_PREV_YEAR,
         Y2D_ARR_PUN_DIF_2019,
         Y2D_DEP_PUN_DIF_2019
  )


#merging the totals and the year to date data
apt_punct_for_json <- merge(apt_punct_d_w, apt_punct_y2d, by= c("ARP_CODE", "ARP_NAME")) %>%
  ### rank calculation
  right_join(list_airport_extended, by = c("ARP_CODE" = "apt_icao_code", "ARP_NAME" = "apt_name")) %>%
  group_by(flag_top_apt) %>%
  mutate(
    DY_ARR_PUN_RANK = if_else(flag_top_apt == "N", NA, min_rank(desc(DY_ARR_PUN))),
    WK_ARR_PUN_RANK = if_else(flag_top_apt == "N", NA, min_rank(desc(WK_ARR_PUN))),
    Y2D_ARR_PUN_RANK = if_else(flag_top_apt == "N", NA, min_rank(desc(Y2D_ARR_PUN))),

    DY_DEP_PUN_RANK = if_else(flag_top_apt == "N", NA, min_rank(desc(DY_DEP_PUN))),
    WK_DEP_PUN_RANK = if_else(flag_top_apt == "N", NA, min_rank(desc(WK_DEP_PUN))),
    Y2D_DEP_PUN_RANK = if_else(flag_top_apt == "N", NA, min_rank(desc(Y2D_DEP_PUN))),

    PUN_RANK_TEXT = "*Rank within top 40 airports.\nTop rank for highest."

  ) %>%
  ungroup()

#### Join strings and save  ----
apt_json_app_j <- list_airport_extended %>%
  select (
    -flag_top_apt,
    APT_LATITUDE = latitude,
    APT_LONGITUDE = longitude
  ) %>%
  arrange(apt_name)
apt_json_app_j$apt_traffic <- select(arrange(apt_traffic_for_json, ARP_NAME), -c(ARP_NAME, ARP_CODE))
apt_json_app_j$apt_delay <- select(arrange(apt_delay_for_json, ARP_NAME), -c(ARP_NAME, ARP_CODE))
apt_json_app_j$apt_punct <- select(arrange(apt_punct_for_json, ARP_NAME), -c(ARP_NAME, ARP_CODE))

apt_json_app_j <- apt_json_app_j %>%
  rename(APT_CODE = apt_icao_code)

update_day <- floor_date(lubridate::now(), unit = "days") %>%
  as_tibble() %>%
  rename(APP_UPDATE = 1)

apt_json_app_j$apt_update <- update_day

apt_json_app_j <- apt_json_app_j %>%   group_by(APT_CODE, apt_name)

apt_json_app <- apt_json_app_j %>% toJSON(., pretty = TRUE)

save_json(apt_json_app, "apt_json_app", archive_file = FALSE)
print(paste(format(now(), "%H:%M:%S"), "apt_json_app"))



# ____________________________________________________________________________________________
#
#    APT ranking tables -----
#
# ____________________________________________________________________________________________

## TRAFFIC ----
### Aircraft operator ----

#### day ----
mydataframe <- "ap_ao_data_day_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

# process data
apt_ao_data_day_int <- assign(mydataframe, df) |>
  # ensure we keep only the current code
  mutate(AO_GRP_CODE = if_else(FLAG_DAY != "CURRENT_DAY", NA, AO_GRP_CODE)) %>% 
  arrange(AO_GRP_NAME, AO_GRP_CODE) %>% 
  fill(AO_GRP_CODE, .direction = "down") %>% 
  arrange(ARP_CODE, FLAG_DAY, R_RANK) %>% 
  select(-TO_DATE) |>
  spread(key = FLAG_DAY, value = DEP_ARR) |>
  arrange(ARP_CODE, R_RANK) |>
  mutate(
    DY_RANK_DIF_PREV_WEEK = case_when(
      is.na(RANK_PREV) ~ RANK,
      .default = RANK_PREV - RANK
    ),
    DY_FLT_DIF_PREV_WEEK_PERC =   case_when(
      DAY_PREV_WEEK == 0 | is.na(DAY_PREV_WEEK) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_WEEK - 1
    ),
    DY_FLT_DIF_PREV_YEAR_PERC = case_when(
      DAY_PREV_YEAR == 0 | is.na(DAY_PREV_YEAR) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_YEAR - 1
    ),
    APT_TFC_AO_GRP_DIF = CURRENT_DAY - DAY_PREV_WEEK
  )

apt_ao_data_day <- apt_ao_data_day_int |>
  select(
    APT_CODE = ARP_CODE,
    APT_NAME = ARP_NAME,
    RANK = R_RANK,
    DY_RANK_DIF_PREV_WEEK,
    DY_AO_GRP_NAME = AO_GRP_NAME,
    DY_TO_DATE = LAST_DATA_DAY,
    DY_FLT = CURRENT_DAY,
    DY_FLT_DIF_PREV_WEEK_PERC,
    DY_FLT_DIF_PREV_YEAR_PERC
  )


#### week ----
mydataframe <- "ap_ao_data_week_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

# process data
apt_ao_data_week <- assign(mydataframe, df) |>
  mutate(
    WK_TO_DATE = max(TO_DATE),
    WK_FROM_DATE = WK_TO_DATE + days(-6)
  ) |>
  # ensure we keep only the current code
  mutate(AO_GRP_CODE = if_else(PERIOD_TYPE != "CURRENT_ROLLING_WEEK", NA, AO_GRP_CODE)) %>% 
  arrange(AO_GRP_NAME, AO_GRP_CODE) %>% 
  fill(AO_GRP_CODE, .direction = "down") %>% 
  arrange(ARP_CODE, PERIOD_TYPE, R_RANK) %>% 
  select(-FROM_DATE, -TO_DATE, -LAST_DATA_DAY) |>
  spread(key = PERIOD_TYPE, value = DEP_ARR) |>
  arrange(ARP_CODE, R_RANK) |>
  mutate(
    WK_RANK_DIF_PREV_WEEK =  case_when(
      is.na(RANK_PREV) ~ RANK,
      .default = RANK_PREV - RANK
    ),
    WK_FLT_DIF_PREV_WEEK_PERC =   case_when(
      PREV_ROLLING_WEEK == 0 | is.na(PREV_ROLLING_WEEK) ~ NA,
      .default = CURRENT_ROLLING_WEEK / PREV_ROLLING_WEEK - 1
    ),
    WK_FLT_DIF_PREV_YEAR_PERC = case_when(
      ROLLING_WEEK_PREV_YEAR == 0 | is.na(ROLLING_WEEK_PREV_YEAR) ~ NA,
      .default = CURRENT_ROLLING_WEEK / ROLLING_WEEK_PREV_YEAR - 1
    ),
    WK_FLT_AVG = CURRENT_ROLLING_WEEK/7
  ) |>
  select(
    APT_CODE = ARP_CODE,
    APT_NAME = ARP_NAME,
    RANK = R_RANK,
    WK_RANK_DIF_PREV_WEEK,
    WK_AO_GRP_NAME = AO_GRP_NAME,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_FLT_AVG,
    WK_FLT_DIF_PREV_WEEK_PERC,
    WK_FLT_DIF_PREV_YEAR_PERC
  )

#### y2d ----
mydataframe <- "ap_ao_data_y2d_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

apt_ao_y2d <- assign(mydataframe, df)

# get max year from dataset
apt_ao_y2d_max_year <-  max(apt_ao_y2d$YEAR, na.rm = TRUE)

# process data
apt_ao_data_year <- apt_ao_y2d |>
  # calculate number of days to date
  group_by(YEAR) |>
  mutate(
    Y2D_DAYS = as.numeric(max(TO_DATE, na.rm = TRUE) - min(FROM_DATE, na.rm = TRUE) +1),
    Y2D_FLT_AVG = DEP_ARR / Y2D_DAYS,
    FLAG_PERIOD = case_when(
      YEAR == 2019 ~ "YEAR2019",
      YEAR == data_day_year ~ "CURRENT",
      YEAR == data_day_year -1 ~ "PREVIOUS"
    )
    ) |>
  ungroup() |>
  select(-TO_DATE, -FROM_DATE, -Y2D_DAYS, -DEP_ARR, -YEAR, -AO_GRP_CODE) %>% 
  pivot_wider(
    # id_cols    = -c(YEAR, DEP_ARR),       
    names_from = FLAG_PERIOD,                    
    values_from= Y2D_FLT_AVG) %>% 
  arrange(ARP_CODE, R_RANK) %>% 
  mutate(
    Y2D_RANK_DIF_PREV_YEAR =  RANK_PY - RANK,
    Y2D_FLT_DIF_PREV_YEAR_PERC = ifelse(CURRENT == 0, NA,
                                        CURRENT / PREVIOUS-1),
    Y2D_FLT_DIF_2019_PERC = ifelse(CURRENT == 0, NA,
                                   CURRENT / YEAR2019-1),
    TO_DATE = data_day_date
    ) |>
  arrange(ARP_CODE, ARP_NAME, R_RANK) |>
  select(
    APT_CODE = ARP_CODE,
    APT_NAME = ARP_NAME,
    RANK = R_RANK,
    Y2D_RANK_DIF_PREV_YEAR,
    Y2D_AO_GRP_NAME = AO_GRP_NAME,
    Y2D_TO_DATE = TO_DATE,
    Y2D_FLT_AVG = CURRENT,
    Y2D_FLT_DIF_PREV_YEAR_PERC,
    Y2D_FLT_DIF_2019_PERC
  )

# apt_ao_data_year %>% group_by(APT_CODE) %>% summarise(myrows = n()) %>% filter(myrows >10)
# apt_ao_data_year %>% filter(APT_CODE == "LZIB")
# test <- apt_ao_y2d %>% filter(ARP_CODE == "LZIB")

#### main card ----
apt_ao_main_traffic <- apt_ao_data_day_int |>
  mutate(
    MAIN_TFC_AO_GRP_NAME = if_else(
      R_RANK <= 4,
      AO_GRP_NAME,
      NA
    ),
    MAIN_TFC_AO_GRP_CODE = if_else(
      R_RANK <= 4,
      AO_GRP_CODE,
      NA
    ),
    MAIN_TFC_AO_GRP_FLT = if_else(
      R_RANK <= 4,
      CURRENT_DAY,
      NA
    )
  ) |>
  select(APT_CODE = ARP_CODE, APT_NAME = ARP_NAME, RANK = R_RANK,
         MAIN_TFC_AO_GRP_NAME, MAIN_TFC_AO_GRP_CODE, MAIN_TFC_AO_GRP_FLT)

apt_ao_main_traffic_dif <- apt_ao_data_day_int |>
  arrange(ARP_CODE, desc(abs(APT_TFC_AO_GRP_DIF))) |>
  group_by(ARP_CODE) |>
  mutate(RANK_DIF_AO_TFC = row_number()) |>
  ungroup() |>
  arrange(ARP_CODE, R_RANK) |>
  mutate(
    MAIN_TFC_DIF_AO_GRP_NAME = if_else(
      RANK_DIF_AO_TFC <= 4,
      AO_GRP_NAME,
      NA
    ),
    MAIN_TFC_DIF_AO_GRP_CODE = if_else(
      RANK_DIF_AO_TFC <= 4,
      AO_GRP_CODE,
      NA
    ),
    MAIN_TFC_DIF_AO_GRP_FLT_DIF = if_else(
      RANK_DIF_AO_TFC <= 4,
      APT_TFC_AO_GRP_DIF,
      NA
    )
  ) |>
  arrange(ARP_CODE, RANK_DIF_AO_TFC) |>
  select(APT_CODE = ARP_CODE, APT_NAME = ARP_NAME, RANK = RANK_DIF_AO_TFC,
         MAIN_TFC_DIF_AO_GRP_NAME, MAIN_TFC_DIF_AO_GRP_CODE,
         MAIN_TFC_DIF_AO_GRP_FLT_DIF)


#### join tables ----
apt_ao_ranking_traffic <- apt_ao_main_traffic |>
  left_join(apt_ao_main_traffic_dif, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_ao_data_day, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_ao_data_week, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_ao_data_year, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  arrange(APT_CODE, APT_NAME, RANK) |>
  distinct(RANK, APT_CODE, APT_NAME, .keep_all = TRUE)

# convert to json and save in app data folder
apt_ao_ranking_traffic_j <- apt_ao_ranking_traffic |> toJSON(pretty = TRUE)

save_json(apt_ao_ranking_traffic_j, "apt_ao_ranking_traffic", archive_file = FALSE)
print(paste(format(now(), "%H:%M:%S"), "apt_ao_ranking_traffic"))


### Airports ----
#### day ----
mydataframe <- "ap_ap_des_data_day_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

# process data
apt_apt_data_day_int <- assign(mydataframe, df) |>
  select(-TO_DATE) |>
  spread(key = FLAG_PERIOD, value = DEP) |>
  arrange(ARP_CODE_DEP, R_RANK) |>
  mutate(
    DY_RANK_DIF_PREV_WEEK = case_when(
      is.na(RANK_PREV) ~ RANK,
      .default = RANK_PREV - RANK
    ),
    DY_FLT_DIF_PREV_WEEK_PERC =   case_when(
      DAY_PREV_WEEK == 0 | is.na(DAY_PREV_WEEK) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_WEEK - 1
    ),
    DY_FLT_DIF_PREV_YEAR_PERC = case_when(
      DAY_PREV_YEAR == 0 | is.na(DAY_PREV_YEAR) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_YEAR - 1
    ),
    APT_TFC_APT_DIF = CURRENT_DAY - DAY_PREV_WEEK
  )

apt_apt_data_day <- apt_apt_data_day_int |>
  select(
    APT_CODE = ARP_CODE_DEP,
    APT_NAME = ARP_NAME_DEP,
    RANK = R_RANK,
    DY_RANK_DIF_PREV_WEEK,
    DY_APT_NAME = ARP_NAME_ARR,
    DY_TO_DATE = LAST_DATA_DAY,
    DY_FLT = CURRENT_DAY,
    DY_FLT_DIF_PREV_WEEK_PERC,
    DY_FLT_DIF_PREV_YEAR_PERC
  )


#### week ----
mydataframe <- "ap_ap_des_data_week_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

# if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

# } else {
#   df <- read_xlsx(
#     path  = fs::path_abs(
#       str_glue(ap_base_file),
#       start = ap_base_dir),
#     sheet = "apt_apt_des_week",
#     range = cell_limits(c(1, 1), c(NA, NA))) |>
#     mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
# 
#   # save pre-processed file in archive for generation of past json files
#   write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
# }

# process data
apt_apt_data_week <- assign(mydataframe, df)  |>
  mutate(
    WK_TO_DATE = max(TO_DATE),
    WK_FROM_DATE = WK_TO_DATE + days(-6)
  ) |>
  select(-FROM_DATE, -TO_DATE, -LAST_DATA_DAY) |>
  spread(key = FLAG_PERIOD, value = DEP) |>
  arrange(ARP_CODE_DEP, R_RANK) |>
  mutate(
    WK_RANK_DIF_PREV_WEEK =  case_when(
      is.na(RANK_PREV) ~ RANK,
      .default = RANK_PREV - RANK
    ),
    WK_FLT_DIF_PREV_WEEK_PERC =   case_when(
      PREV_ROLLING_WEEK == 0 | is.na(PREV_ROLLING_WEEK) ~ NA,
      .default = (CURRENT_ROLLING_WEEK / PREV_ROLLING_WEEK - 1)
    ),
    WK_FLT_DIF_PREV_YEAR_PERC = case_when(
      ROLLING_WEEK_PREV_YEAR == 0 | is.na(ROLLING_WEEK_PREV_YEAR) ~ NA,
      .default = (CURRENT_ROLLING_WEEK / ROLLING_WEEK_PREV_YEAR - 1)
    ),
    WK_FLT_AVG = (CURRENT_ROLLING_WEEK/7)
  ) |>
  select(
    APT_CODE = ARP_CODE_DEP,
    APT_NAME = ARP_NAME_DEP,
    RANK = R_RANK,
    WK_RANK_DIF_PREV_WEEK,
    WK_APT_NAME = ARP_NAME_ARR,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_FLT_AVG,
    WK_FLT_DIF_PREV_WEEK_PERC,
    WK_FLT_DIF_PREV_YEAR_PERC
  )

#### y2d ----
mydataframe <- "ap_ap_des_data_y2d_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

# if (archive_mode) {
df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

# process data
apt_apt_y2d <- assign(mydataframe, df)

# get max year from dataset
apt_apt_y2d_max_year <-  max(apt_apt_y2d$YEAR, na.rm = TRUE)

# process data
apt_apt_data_year <- apt_apt_y2d |>
  # calculate number of days to date
  group_by(YEAR) |>
  mutate(
    Y2D_DAYS = as.numeric(max(TO_DATE, na.rm = TRUE) - min(FROM_DATE, na.rm = TRUE) +1),
    Y2D_FLT_AVG = DEP / Y2D_DAYS,
    FLAG_PERIOD = case_when(
      YEAR == 2019 ~ "YEAR2019",
      YEAR == data_day_year ~ "CURRENT",
      YEAR == data_day_year -1 ~ "PREVIOUS"
    )
  ) |>
  ungroup() |>
  select(-TO_DATE, -FROM_DATE, -Y2D_DAYS, -DEP, -YEAR, -ARP_CODE_ARR) %>% 
  pivot_wider(
    # id_cols    = -c(YEAR, DEP_ARR),       
    names_from = FLAG_PERIOD,                    
    values_from = Y2D_FLT_AVG) %>% 
  arrange(ARP_CODE_DEP, R_RANK) %>% 
  mutate(
    Y2D_RANK_DIF_PREV_YEAR =  RANK_PREV - RANK,
    Y2D_FLT_DIF_PREV_YEAR_PERC = ifelse(CURRENT == 0, NA,
                                        CURRENT / PREVIOUS-1),
    Y2D_FLT_DIF_2019_PERC = ifelse(CURRENT == 0, NA,
                                   CURRENT / YEAR2019-1),
    TO_DATE = data_day_date
  ) |>
  arrange(ARP_CODE_DEP, ARP_NAME_DEP, R_RANK) |>
  select(
    APT_CODE = ARP_CODE_DEP,
    APT_NAME = ARP_NAME_DEP,
    RANK = R_RANK,
    Y2D_RANK_DIF_PREV_YEAR,
    Y2D_APT_NAME = ARP_NAME_ARR,
    Y2D_TO_DATE = LAST_DATA_DAY,
    Y2D_FLT_AVG = CURRENT,
    Y2D_FLT_DIF_PREV_YEAR_PERC,
    Y2D_FLT_DIF_2019_PERC
  )

#### main card ----
apt_apt_main_traffic <- apt_apt_data_day_int |>
  mutate(
    MAIN_TFC_APT_NAME = if_else(
      R_RANK <= 4,
      ARP_NAME_ARR,
      NA
    ),
    MAIN_TFC_APT_CODE = if_else(
      R_RANK <= 4,
      ARP_CODE_ARR,
      NA
    ),
    MAIN_TFC_APT_FLT = if_else(
      R_RANK <= 4,
      CURRENT_DAY,
      NA
    )
  ) |>
  select(APT_CODE = ARP_CODE_DEP, APT_NAME = ARP_NAME_DEP, RANK = R_RANK,
         MAIN_TFC_APT_NAME, MAIN_TFC_APT_CODE, MAIN_TFC_APT_FLT)

apt_apt_main_traffic_dif <- apt_apt_data_day_int |>
  arrange(ARP_CODE_DEP, desc(abs(APT_TFC_APT_DIF))) |>
  group_by(ARP_CODE_DEP) |>
  mutate(RANK_DIF_APT_TFC = row_number()) |>
  ungroup() |>
  arrange(ARP_CODE_DEP, R_RANK) |>
  mutate(
    MAIN_TFC_DIF_APT_NAME = if_else(
      RANK_DIF_APT_TFC <= 4,
      ARP_NAME_ARR,
      NA
    ),
    MAIN_TFC_DIF_APT_CODE = if_else(
      RANK_DIF_APT_TFC <= 4,
      ARP_CODE_ARR,
      NA
    ),
    MAIN_TFC_DIF_APT_FLT_DIF = if_else(
      RANK_DIF_APT_TFC <= 4,
      APT_TFC_APT_DIF,
      NA
    )
  ) |>
  arrange(ARP_CODE_DEP, RANK_DIF_APT_TFC) |>
  select(APT_CODE = ARP_CODE_DEP, APT_NAME = ARP_NAME_DEP, RANK = RANK_DIF_APT_TFC,
         MAIN_TFC_DIF_APT_NAME, MAIN_TFC_DIF_APT_CODE,
         MAIN_TFC_DIF_APT_FLT_DIF)


#### join tables ----
apt_apt_ranking_traffic <- apt_apt_main_traffic |>
  left_join(apt_apt_main_traffic_dif, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_apt_data_day, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_apt_data_week, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_apt_data_year, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  arrange(APT_CODE, APT_NAME, RANK) |>
  distinct(RANK, APT_CODE, APT_NAME, .keep_all = TRUE)

# convert to json and save in app data folder
apt_apt_ranking_traffic_j <- apt_apt_ranking_traffic |> toJSON(pretty = TRUE)

save_json(apt_apt_ranking_traffic_j, "apt_apt_ranking_traffic", archive_file = FALSE)
print(paste(format(now(), "%H:%M:%S"), "apt_apt_ranking_traffic"))


### States ----
#### day ----
mydataframe <- "ap_st_des_data_day_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

# if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

# } else {
#   df <- read_xlsx(
#     path  = fs::path_abs(
#       str_glue(ap_base_file),
#       start = ap_base_dir),
#     sheet = "apt_state_des_day",
#     range = cell_limits(c(1, 1), c(NA, NA))) |>
#     mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
# 
#   # save pre-processed file in archive for generation of past json files
#   write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
# }

# process data
apt_st_data_day_int <- assign(mydataframe, df) |>
  select(-TO_DATE) |>
  spread(key = FLAG_PERIOD, value = DEP) |>
  arrange(ARP_CODE, R_RANK) |>
  mutate(
    DY_RANK_DIF_PREV_WEEK = case_when(
      is.na(RANK_PREV) ~ RANK,
      .default = RANK_PREV - RANK
    ),
    DY_FLT_DIF_PREV_WEEK_PERC =   case_when(
      DAY_PREV_WEEK == 0 | is.na(DAY_PREV_WEEK) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_WEEK - 1
    ),
    DY_FLT_DIF_PREV_YEAR_PERC = case_when(
      DAY_PREV_YEAR == 0 | is.na(DAY_PREV_YEAR) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_YEAR - 1
    ),
    APT_TFC_ST_DIF = CURRENT_DAY - DAY_PREV_WEEK
  )

apt_st_data_day <- apt_st_data_day_int |>
  select(
    APT_CODE = ARP_CODE,
    APT_NAME = ARP_NAME,
    RANK = R_RANK,
    DY_RANK_DIF_PREV_WEEK,
    DY_ST_DES_NAME = ISO_CT_NAME_ARR,
    DY_TO_DATE = LAST_DATA_DAY,
    DY_FLT = CURRENT_DAY,
    DY_FLT_DIF_PREV_WEEK_PERC,
    DY_FLT_DIF_PREV_YEAR_PERC
  )


#### week ----
mydataframe <- "ap_st_des_data_week_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

# if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

# } else {
#   df <- read_xlsx(
#     path  = fs::path_abs(
#       str_glue(ap_base_file),
#       start = ap_base_dir),
#     sheet = "apt_state_des_week",
#     range = cell_limits(c(1, 1), c(NA, NA))) |>
#     mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
# 
#   # save pre-processed file in archive for generation of past json files
#   write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
# }

# process data
apt_st_data_week <- assign(mydataframe, df) |>
  mutate(
    WK_TO_DATE = max(TO_DATE),
    WK_FROM_DATE = WK_TO_DATE + days(-6)
  ) |>
  select(-FROM_DATE, -TO_DATE, -LAST_DATA_DAY) |>
  spread(key = FLAG_PERIOD, value = DEP) |>
  arrange(ARP_CODE, R_RANK) |>
  mutate(
    WK_RANK_DIF_PREV_WEEK =  case_when(
      is.na(RANK_PREV) ~ RANK,
      .default = RANK_PREV - RANK
    ),
    WK_FLT_DIF_PREV_WEEK_PERC =   case_when(
      PREV_ROLLING_WEEK == 0 | is.na(PREV_ROLLING_WEEK) ~ NA,
      .default = round((CURRENT_ROLLING_WEEK / PREV_ROLLING_WEEK - 1), 3)
    ),
    WK_FLT_DIF_PREV_YEAR_PERC = case_when(
      ROLLING_WEEK_PREV_YEAR == 0 | is.na(ROLLING_WEEK_PREV_YEAR) ~ NA,
      .default = round((CURRENT_ROLLING_WEEK / ROLLING_WEEK_PREV_YEAR - 1), 3)
    ),
    WK_FLT_AVG = round((CURRENT_ROLLING_WEEK/7), 2)
  ) |>
  select(
    APT_CODE = ARP_CODE,
    APT_NAME = ARP_NAME,
    RANK = R_RANK,
    WK_RANK_DIF_PREV_WEEK,
    WK_ST_DES_NAME = ISO_CT_NAME_ARR,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_FLT_AVG,
    WK_FLT_DIF_PREV_WEEK_PERC,
    WK_FLT_DIF_PREV_YEAR_PERC
  )

#### y2d ----
mydataframe <- "ap_st_des_data_y2d_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

# if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

# } else {
#   df <- read_xlsx(
#     path  = fs::path_abs(
#       str_glue(ap_base_file),
#       start = ap_base_dir),
#     sheet = "apt_state_des_y2d",
#     range = cell_limits(c(1, 1), c(NA, NA))) |>
#     mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
# 
#   # save pre-processed file in archive for generation of past json files
#   write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
# }

# process data
apt_st_y2d <- assign(mydataframe, df)

# get max year from dataset
apt_st_y2d_max_year <-  max(apt_st_y2d$YEAR, na.rm = TRUE)

# process data
apt_st_data_year <- apt_st_y2d |>
  # calculate number of days to date
  group_by(YEAR) |>
  mutate(Y2D_DAYS = as.numeric(max(TO_DATE, na.rm = TRUE) - min(FROM_DATE, na.rm = TRUE) +1)) |>
  ungroup() |>
  arrange(ARP_CODE, ISO_CT_NAME_ARR, YEAR) |>
  mutate(
    Y2D_RANK_DIF_PREV_YEAR =  RANK_PREV - RANK,
    # Y2D_FLT_DIF_PREV_YEAR_PERC = ifelse(YEAR == "2024",
    #                                     round((DEP/lag(DEP)-1), 3), NA),
    # Y2D_FLT_DIF_2019_PERC = ifelse(YEAR == "2024",
    #                                round((DEP/lag(DEP, 5)-1), 3), NA)
    Y2D_FLT_AVG = DEP / Y2D_DAYS,
    Y2D_FLT_DIF_PREV_YEAR_PERC = ifelse(YEAR == apt_st_y2d_max_year,
                                        Y2D_FLT_AVG / lag(Y2D_FLT_AVG)-1, NA),
    Y2D_FLT_DIF_2019_PERC = ifelse(YEAR == apt_st_y2d_max_year,
                                   Y2D_FLT_AVG / lag(Y2D_FLT_AVG, 2)-1, NA)
  ) |>
  filter(YEAR == apt_st_y2d_max_year) |>
  arrange(ARP_CODE, ISO_CT_NAME_ARR, R_RANK) |>
  select(
    APT_CODE = ARP_CODE,
    APT_NAME = ARP_NAME,
    RANK = R_RANK,
    Y2D_RANK_DIF_PREV_YEAR,
    Y2D_ST_DES_NAME = ISO_CT_NAME_ARR,
    Y2D_TO_DATE = LAST_DATA_DAY,
    Y2D_FLT_AVG,
    Y2D_FLT_DIF_PREV_YEAR_PERC,
    Y2D_FLT_DIF_2019_PERC
  )

#### main card ----
apt_st_main_traffic <- apt_st_data_day_int |>
  mutate(
    MAIN_TFC_ST_DES_NAME = if_else(
      R_RANK <= 4,
      ISO_CT_NAME_ARR,
      NA
    ),
    MAIN_TFC_ST_DES_CODE = if_else(
      R_RANK <= 4,
      ISO_CT_CODE_ARR,
      NA
    ),
    MAIN_TFC_ST_DES_FLT = if_else(
      R_RANK <= 4,
      CURRENT_DAY,
      NA
    )
  ) |>
  select(APT_CODE = ARP_CODE, APT_NAME = ARP_NAME, RANK = R_RANK,
         MAIN_TFC_ST_DES_NAME, MAIN_TFC_ST_DES_CODE, MAIN_TFC_ST_DES_FLT)

apt_st_main_traffic_dif <- apt_st_data_day_int |>
  arrange(ARP_CODE, desc(abs(APT_TFC_ST_DIF))) |>
  group_by(ARP_CODE) |>
  mutate(RANK_DIF_ST_TFC = row_number()) |>
  ungroup() |>
  arrange(ARP_CODE, R_RANK) |>
  mutate(
    MAIN_TFC_DIF_ST_DES_NAME = if_else(
      RANK_DIF_ST_TFC <= 4,
      ISO_CT_NAME_ARR,
      NA
    ),
    MAIN_TFC_DIF_ST_DES_CODE = if_else(
      RANK_DIF_ST_TFC <= 4,
      ISO_CT_CODE_ARR,
      NA
    ),
    MAIN_TFC_DIF_ST_DES_FLT_DIF = if_else(
      RANK_DIF_ST_TFC <= 4,
      APT_TFC_ST_DIF,
      NA
    )
  ) |>
  arrange(ARP_CODE, RANK_DIF_ST_TFC) |>
  select(APT_CODE = ARP_CODE, APT_NAME = ARP_NAME, RANK = RANK_DIF_ST_TFC,
         MAIN_TFC_DIF_ST_DES_NAME, MAIN_TFC_DIF_ST_DES_CODE,
         MAIN_TFC_DIF_ST_DES_FLT_DIF)


#### join tables ----
apt_st_ranking_traffic <- apt_st_main_traffic |>
  left_join(apt_st_main_traffic_dif, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_st_data_day, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_st_data_week, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_st_data_year, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  arrange(APT_CODE, APT_NAME, RANK) |>
  distinct(RANK, APT_CODE, APT_NAME, .keep_all = TRUE)

# convert to json and save in app data folder
apt_st_ranking_traffic_j <- apt_st_ranking_traffic |> toJSON(pretty = TRUE)

save_json(apt_st_ranking_traffic_j, "apt_st_ranking_traffic", archive_file = FALSE)
print(paste(format(now(), "%H:%M:%S"), "apt_st_ranking_traffic"))



# ____________________________________________________________________________________________
#
###    APT market segments -----
#
# ____________________________________________________________________________________________


#### day ----
mydataframe <- "ap_ms_data_day_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

# if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

# } else {
#   df <- read_xlsx(
#     path  = fs::path_abs(
#       str_glue(ap_base_file),
#       start = ap_base_dir),
#     sheet = "apt_ms_day",
#     range = cell_limits(c(1, 1), c(NA, NA))) |>
#     mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
# 
#   # save pre-processed file in archive for generation of past json files
#   write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
# }

apt_ms_data_day_raw <- assign(mydataframe, df)

# Get unique combinations of all airports and MS names
apt_list <- distinct(apt_ms_data_day_raw, ARP_CODE)
ms_list <- distinct(apt_ms_data_day_raw, MARKET_SEGMENT)

# Generate full grid of APT_CODE × DY_MS_NAME
full_grid <- expand_grid(apt_list, ms_list)


# process data
apt_ms_data_day_prep <- apt_ms_data_day_raw |>
  select(-TO_DATE) |>
  spread(key = FLAG_PERIOD, value = DEP_ARR) |>
  arrange(ARP_CODE, R_RANK) |>
  mutate(
    DY_RANK_DIF_PREV_WEEK = case_when(
      is.na(RANK_PREV) ~ RANK,
      .default = RANK_PREV - RANK
    ),
    DY_FLT_DIF_PREV_WEEK_PERC =   case_when(
      DAY_PREV_WEEK == 0 | is.na(DAY_PREV_WEEK) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_WEEK - 1
    ),
    DY_FLT_DIF_PREV_YEAR_PERC = case_when(
      DAY_PREV_YEAR == 0 | is.na(DAY_PREV_YEAR) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_YEAR - 1
    )
  ) |>
  group_by(ARP_CODE) |>
  mutate(DY_MS_SHARE = ifelse(CURRENT_DAY == 0, 0,
                              CURRENT_DAY/sum(CURRENT_DAY, na.rm = TRUE))) |>
  ungroup() 


apt_ms_data_day <- full_grid |> 
  left_join(apt_ms_data_day_prep, by = c("ARP_CODE", "MARKET_SEGMENT")) |>
  group_by(ARP_CODE) |>
  arrange(ARP_CODE, desc(CURRENT_DAY)) |>
  mutate(RANK = row_number())|>
  fill(ARP_NAME, .direction = "down")|>
  ungroup() |>
  select(
    APT_CODE = ARP_CODE,
    APT_NAME = ARP_NAME,
    RANK,
    DY_RANK_DIF_PREV_WEEK,
    DY_MS_NAME = MARKET_SEGMENT,
    DY_MS_SHARE,
    DY_TO_DATE = LAST_DATA_DAY,
    DY_FLT = CURRENT_DAY,
    DY_FLT_DIF_PREV_WEEK_PERC,
    DY_FLT_DIF_PREV_YEAR_PERC
  )

#### week ----
mydataframe <- "ap_ms_data_week_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

# if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

# } else {
#   df <- read_xlsx(
#     path  = fs::path_abs(
#       str_glue(ap_base_file),
#       start = ap_base_dir),
#     sheet = "apt_ms_week",
#     range = cell_limits(c(1, 1), c(NA, NA))) |>
#     mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
# 
#   # save pre-processed file in archive for generation of past json files
#   write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
# }

# process data
apt_ms_data_week_prep <- assign(mydataframe, df) |>
  mutate(
    WK_TO_DATE = max(TO_DATE),
    WK_FROM_DATE = WK_TO_DATE + days(-6)
  ) |>
  select(-FROM_DATE, -TO_DATE, -LAST_DATA_DAY) |>
  spread(key = FLAG_PERIOD, value = DEP_ARR) |>
  arrange(ARP_CODE, R_RANK) |>
  mutate(
    WK_RANK_DIF_PREV_WEEK =  case_when(
      is.na(RANK_PREV) ~ RANK,
      .default = RANK_PREV - RANK
    ),
    WK_FLT_DIF_PREV_WEEK_PERC =   case_when(
      PREV_ROLLING_WEEK == 0 | is.na(PREV_ROLLING_WEEK) ~ NA,
      .default = round((CURRENT_ROLLING_WEEK / PREV_ROLLING_WEEK - 1), 3)
    ),
    WK_FLT_DIF_PREV_YEAR_PERC = case_when(
      ROLLING_WEEK_PREV_YEAR == 0 | is.na(ROLLING_WEEK_PREV_YEAR) ~ NA,
      .default = round((CURRENT_ROLLING_WEEK / ROLLING_WEEK_PREV_YEAR - 1), 3)
    ),
    WK_FLT_AVG = round((CURRENT_ROLLING_WEEK/7), 2)
  ) |>
  group_by(ARP_CODE) |>
  mutate(WK_MS_SHARE = ifelse(CURRENT_ROLLING_WEEK == 0, 0,
                              CURRENT_ROLLING_WEEK/sum(CURRENT_ROLLING_WEEK,
                                                       na.rm = TRUE))) |>
  ungroup() 

apt_ms_data_week <- full_grid |> 
  left_join(apt_ms_data_week_prep, by = c("ARP_CODE", "MARKET_SEGMENT")) |>
  group_by(ARP_CODE) |>
  arrange(ARP_CODE, desc(CURRENT_ROLLING_WEEK)) |>
  mutate(RANK = row_number())|>
  fill(ARP_NAME, .direction = "down")|>
  ungroup() |>
  select(
    APT_CODE = ARP_CODE,
    APT_NAME = ARP_NAME,
    RANK,
    WK_RANK_DIF_PREV_WEEK,
    WK_MS_NAME = MARKET_SEGMENT,
    WK_MS_SHARE,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_FLT_AVG,
    WK_FLT_DIF_PREV_WEEK_PERC,
    WK_FLT_DIF_PREV_YEAR_PERC
  )


#### y2d ----
mydataframe <- "ap_ms_data_y2d_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

# if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

# } else {
#   df <- read_xlsx(
#     path  = fs::path_abs(
#       str_glue(ap_base_file),
#       start = ap_base_dir),
#     sheet = "apt_ms_y2d",
#     range = cell_limits(c(1, 1), c(NA, NA))) |>
#     mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
# 
#   # save pre-processed file in archive for generation of past json files
#   write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
# }

# process data
apt_ms_y2d <- assign(mydataframe, df)

# get max year from dataset
apt_ms_y2d_max_year <-  max(apt_ms_y2d$YEAR, na.rm = TRUE)

# process data
apt_ms_data_year_prep <- apt_ms_y2d |>
  # calculate number of days to date
  group_by(YEAR) |>
  mutate(Y2D_DAYS = as.numeric(max(TO_DATE, na.rm = TRUE) - min(FROM_DATE, na.rm = TRUE) +1)) |>
  ungroup() |>
  arrange(ARP_CODE, MARKET_SEGMENT, YEAR) |>
  mutate(
    Y2D_RANK_DIF_PREV_YEAR =  RANK_PREV - RANK,
    # Y2D_FLT_DIF_PREV_YEAR_PERC = ifelse(YEAR == "2024",
    #                                     round((DEP_ARR/lag(DEP_ARR)-1), 3), NA),
    # Y2D_FLT_DIF_2019_PERC = ifelse(YEAR == "2024",
    #                                round((DEP_ARR/lag(DEP_ARR, 5)-1), 3), NA)
    Y2D_FLT_AVG = DEP_ARR / Y2D_DAYS,
    Y2D_FLT_DIF_PREV_YEAR_PERC = ifelse(YEAR == apt_ms_y2d_max_year,
                                        Y2D_FLT_AVG / lag(Y2D_FLT_AVG)-1, NA),
    Y2D_FLT_DIF_2019_PERC = ifelse(YEAR == apt_ms_y2d_max_year,
                                   Y2D_FLT_AVG / lag(Y2D_FLT_AVG, 2)-1, NA)
  ) |>
  filter(YEAR == apt_ms_y2d_max_year) |>
  group_by(ARP_CODE) |>
  mutate(Y2D_MS_SHARE = ifelse(DEP_ARR == 0, 0,
                               DEP_ARR/sum(DEP_ARR, na.rm = TRUE))) |>
  ungroup() 


apt_ms_data_year <- full_grid |> 
  left_join(apt_ms_data_year_prep, by = c("ARP_CODE", "MARKET_SEGMENT")) |>
  group_by(ARP_CODE) |>
  arrange(ARP_CODE, desc(DEP_ARR)) |>
  mutate(RANK = row_number())|>
  fill(ARP_NAME, .direction = "down")|>
  ungroup() |>
  select(
    APT_CODE = ARP_CODE,
    APT_NAME = ARP_NAME,
    RANK = R_RANK,
    Y2D_RANK_DIF_PREV_YEAR,
    Y2D_MS_NAME = MARKET_SEGMENT,
    Y2D_MS_SHARE,
    Y2D_TO_DATE = LAST_DATA_DAY,
    Y2D_FLT_AVG,
    Y2D_FLT_DIF_PREV_YEAR_PERC,
    Y2D_FLT_DIF_2019_PERC
  )


#### join tables ----
apt_ms_ranking_traffic <- apt_ms_data_day |>
  left_join(apt_ms_data_week, by = c("RANK", "APT_CODE", "APT_NAME")) |>
  left_join(apt_ms_data_year, by = c("RANK", "APT_CODE", "APT_NAME")) |>
  arrange(APT_CODE, APT_NAME, RANK) |>
  distinct(RANK, APT_CODE, APT_NAME, .keep_all = TRUE)

# convert to json and save in app data folder
apt_ms_ranking_traffic_j <- apt_ms_ranking_traffic |> toJSON(pretty = TRUE)

save_json(apt_ms_ranking_traffic_j, "apt_ms_ranking_traffic", archive_file = FALSE)
print(paste(format(now(), "%H:%M:%S"), "apt_ms_ranking_traffic"))



# ____________________________________________________________________________________________
#
#    APT Group graphs  -----
#
# ____________________________________________________________________________________________

## TRAFFIC ----
### 7-day traffic avg ----
apt_traffic_evo <- apt_traffic_data  %>%
  mutate(RWK_AVG_TFC = if_else(FLIGHT_DATE > min(data_day_date,max(LAST_DATA_DAY, na.rm = TRUE),na.rm = TRUE),
                               NA,
                               RWK_AVG_DEP_ARR)) %>%
  select(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE,

    RWK_AVG_TFC,
    RWK_AVG_TFC_PREV_YEAR = RWK_AVG_DEP_ARR_PREV_YEAR,
    RWK_AVG_TFC_2020 = RWK_AVG_DEP_ARR_2020,
    RWK_AVG_TFC_2019 = RWK_AVG_DEP_ARR_2019
  )



column_names <- c('APT_CODE', 'APT_NAME', 'FLIGHT_DATE', data_day_year, data_day_year-1, 2020, 2019)
colnames(apt_traffic_evo) <- column_names

### nest data
apt_traffic_evo_long <- apt_traffic_evo %>%
  pivot_longer(-c(APT_CODE, APT_NAME,FLIGHT_DATE), names_to = 'year', values_to = 'RWK_AVG_TFC') %>%
  group_by(APT_CODE, APT_NAME,FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")


apt_traffic_evo_j <- apt_traffic_evo_long %>% toJSON(., pretty = TRUE)

save_json(apt_traffic_evo_j, "apt_traffic_evo_chart_daily")
print(paste(format(now(), "%H:%M:%S"), "apt_traffic_evo_chart_daily"))


## PUNCTUALITY ----
### 7-day punctuality avg ----

apt_punct_evo <- apt_punct_raw %>%
  filter(DAY_DATE >= as.Date(paste0("01-01-", data_day_year-2), format = "%d-%m-%Y")) %>%
  arrange(ARP_CODE, DAY_DATE) %>%
  mutate(
    #ARR_PUNCTUAL_FLIGHTS = 0,  ## while the figures are not showable
    #DEP_PUNCTUAL_FLIGHTS = 0,  ## while the figures are not showable
    DEP_PUN_WK = rollsum(DEP_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") /
      rollsum(DEP_SCHEDULE_FLIGHT,7, fill = NA, align = "right") * 100,
    ARR_PUN_WK = rollsum(ARR_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") /
      rollsum(ARR_SCHEDULE_FLIGHT,7, fill = NA, align = "right") * 100,
    OP_FLT_WK = 100 - rollsum(MISSING_SCHED_FLIGHTS, 7, fill = NA, align = "right") /
      rollsum((MISSING_SCHED_FLIGHTS+DEP_FLIGHTS_NO_OVERFLIGHTS),7, fill = NA, align = "right")*100
  ) %>%
  filter(DAY_DATE >= as.Date(paste0("01-01-", data_day_year-1), format = "%d-%m-%Y"),
         DAY_DATE <= last_day_punct) %>%
  right_join(select(list_airport_extended, apt_icao_code, apt_name), join_by("ARP_CODE"=="apt_icao_code")) %>%
  select(
    ARP_CODE,
    ARP_NAME,
    DAY_DATE,

    DEP_PUN_WK,
    ARR_PUN_WK,
    OP_FLT_WK
  )

column_names <- c('APT_CODE',
                  'APT_NAME',
                  'FLIGHT_DATE',
                  "Departure punct.",
                  "Arrival punct.",
                  "Operated schedules"
)

colnames(apt_punct_evo) <- column_names

### nest data
apt_punct_evo_long <- apt_punct_evo %>%
  pivot_longer(-c(APT_CODE, APT_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value') %>%
  group_by(APT_CODE, APT_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")


apt_punct_evo_j <- apt_punct_evo_long %>% toJSON(., pretty = TRUE)

save_json(apt_punct_evo_j, "apt_punct_evo_chart")
print(paste(format(now(), "%H:%M:%S"), "apt_punct_evo_chart"))


## DELAY ----
### Delay category ----
apt_delay_cause_data <-  apt_delay_data_full %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
  mutate(
    TDM = DAY_DLY,
    TDM_G = coalesce(TDM_ARP_ARR_G, 0),
    TDM_CS = coalesce(TDM_ARP_ARR_CS, 0),
    TDM_IT = coalesce(TDM_ARP_ARR_IT, 0),
    TDM_WD = coalesce(TDM_ARP_ARR_WD, 0),
    TDM_NOCSGITWD = TDM - TDM_G - TDM_CS - TDM_IT - TDM_WD,
    TDM_PREV_YEAR = DAY_DLY_PREV_YEAR
  ) %>%                              # create 7day average for y2d graph
  mutate(
    RWK_TDM_G = rollsum(TDM_G, 7, fill = NA, align = "right") / 7,
    RWK_TDM_CS = rollsum(TDM_CS, 7, fill = NA, align = "right") / 7,
    RWK_TDM_IT = rollsum(TDM_IT, 7, fill = NA, align = "right") / 7,
    RWK_TDM_WD = rollsum(TDM_WD, 7, fill = NA, align = "right") / 7,
    RWK_TDM_NOCSGITWD = rollsum(TDM_NOCSGITWD, 7, fill = NA, align = "right") / 7,
    RWK_TDM_PREV_YEAR = rollsum(TDM_PREV_YEAR, 7, fill = NA, align = "right") / 7
  ) %>%
  filter(FLIGHT_DATE >= as.Date(paste0("01-01-", data_day_year), format = "%d-%m-%Y"))


#### day ----
apt_delay_cause_day <- apt_delay_cause_data %>%
  filter(FLIGHT_DATE == min(max(FLIGHT_DATE),
                            data_day_date,
                            na.rm = TRUE)
  )%>%
  mutate(
    SHARE_TDM_G = if_else(TDM == 0, 0, TDM_G / TDM),
    SHARE_TDM_CS = if_else(TDM == 0, 0, TDM_CS / TDM),
    SHARE_TDM_IT = if_else(TDM == 0, 0, TDM_IT / TDM),
    SHARE_TDM_WD = if_else(TDM == 0, 0, TDM_WD / TDM),
    SHARE_TDM_NOCSGITWD = if_else(TDM == 0, 0, TDM_NOCSGITWD / TDM)
  ) %>%
  select(ARP_CODE,
         ARP_NAME,
         FLIGHT_DATE,
         TDM_G,
         TDM_CS,
         TDM_IT,
         TDM_WD,
         TDM_NOCSGITWD,
         TDM_PREV_YEAR,
         SHARE_TDM_G,
         SHARE_TDM_CS,
         SHARE_TDM_IT,
         SHARE_TDM_WD,
         SHARE_TDM_NOCSGITWD
  ) %>% 
  arrange(ARP_CODE)

column_names <- c(
  "APT_CODE",
  "APT_NAME",
  "FLIGHT_DATE",
  "Aerodrome capacity",
  "Capacity/Staffing (ATC)",
  "Disruptions (ATC)",
  "Weather",
  "Other",
  paste0("Total delay ", data_day_year - 1),
  "share_aerodrome_capacity",
  "share_capacity_staffing_atc",
  "share_disruptions_atc",
  "share_weather",
  "share_other"
)

colnames(apt_delay_cause_day) <- column_names

### nest data
apt_delay_value_day_long <- apt_delay_cause_day %>%
  select(-c(share_aerodrome_capacity,
            share_capacity_staffing_atc,
            share_disruptions_atc,
            share_weather,
            share_other)
  ) %>%
  pivot_longer(-c(APT_CODE, APT_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

apt_delay_share_day_long <- apt_delay_cause_day %>%
  select(-c("Aerodrome capacity",
            "Capacity/Staffing (ATC)",
            "Disruptions (ATC)",
            "Weather",
            "Other",
            paste0("Total delay ", data_day_year - 1)
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(APT_CODE, APT_NAME, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

apt_delay_cause_day_long <- cbind(apt_delay_value_day_long, apt_delay_share_day_long) %>%
  select(-name) %>%
  group_by(APT_CODE, APT_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

# for consistency with v1 we use the word category in the name files... should have been cause
apt_delay_cause_evo_dy_j <- apt_delay_cause_day_long %>% toJSON(., pretty = TRUE)

save_json(apt_delay_cause_evo_dy_j, "apt_delay_category_evo_chart_dy")
print(paste(format(now(), "%H:%M:%S"), "apt_delay_category_evo_chart_dy"))


#### week ----
apt_delay_cause_wk <- apt_delay_cause_data %>%
  filter(FLIGHT_DATE >= min(max(FLIGHT_DATE), data_day_date, na.rm  = TRUE) + lubridate::days(-6),
         FLIGHT_DATE <= min(max(FLIGHT_DATE), data_day_date, na.rm  = TRUE)
  )  %>%
  group_by(ARP_CODE) %>%
  reframe(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE,
    TDM_G,
    TDM_CS,
    TDM_IT,
    TDM_WD,
    TDM_NOCSGITWD,
    TDM_PREV_YEAR,
    WK_TDM_G = sum(TDM_G, na.rm = TRUE),
    WK_TDM_CS = sum(TDM_CS, na.rm = TRUE),
    WK_TDM_IT = sum(TDM_IT, na.rm = TRUE),
    WK_TDM_WD = sum(TDM_WD, na.rm = TRUE),
    WK_TDM_NOCSGITWD = sum(TDM_NOCSGITWD, na.rm = TRUE),
    WK_TDM = sum(TDM, na.rm = TRUE)
  ) %>%
  ungroup() %>%
  mutate(
    WK_SHARE_TDM_G = if_else(WK_TDM == 0, 0, WK_TDM_G / WK_TDM),
    WK_SHARE_TDM_CS = if_else(WK_TDM == 0, 0, WK_TDM_CS / WK_TDM),
    WK_SHARE_TDM_IT = if_else(WK_TDM == 0, 0, WK_TDM_IT / WK_TDM),
    WK_SHARE_TDM_WD = if_else(WK_TDM == 0, 0, WK_TDM_WD / WK_TDM),
    WK_SHARE_TDM_NOCSGITWD = if_else(WK_TDM == 0, 0, WK_TDM_NOCSGITWD / WK_TDM)
  ) %>%
  select(ARP_CODE,
         ARP_NAME,
         FLIGHT_DATE,
         TDM_G,
         TDM_CS,
         TDM_IT,
         TDM_WD,
         TDM_NOCSGITWD,
         TDM_PREV_YEAR,
         WK_SHARE_TDM_G,
         WK_SHARE_TDM_CS,
         WK_SHARE_TDM_IT,
         WK_SHARE_TDM_WD,
         WK_SHARE_TDM_NOCSGITWD
  ) %>% 
  arrange(ARP_CODE, FLIGHT_DATE)


colnames(apt_delay_cause_wk) <- column_names


### nest data
apt_delay_value_wk_long <- apt_delay_cause_wk %>%
  select(-c(share_aerodrome_capacity,
            share_capacity_staffing_atc,
            share_disruptions_atc,
            share_weather,
            share_other)
  ) %>%
  pivot_longer(-c(APT_CODE, APT_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

apt_delay_share_wk_long <- apt_delay_cause_wk %>%
  select(-c("Aerodrome capacity",
            "Capacity/Staffing (ATC)",
            "Disruptions (ATC)",
            "Weather",
            "Other",
            paste0("Total delay ", data_day_year - 1)
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(APT_CODE, APT_NAME, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

apt_delay_cause_wk_long <- cbind(apt_delay_value_wk_long, apt_delay_share_wk_long) %>%
  select(-name) %>%
  group_by(APT_CODE, APT_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")


# for consistency with v1 we use the word category in the name files... should have been cause
apt_delay_cause_evo_wk_j <- apt_delay_cause_wk_long %>% toJSON(., pretty = TRUE)

save_json(apt_delay_cause_evo_wk_j, "apt_delay_category_evo_chart_wk")
print(paste(format(now(), "%H:%M:%S"), "apt_delay_category_evo_chart_wk"))


#### y2d ----
apt_delay_cause_y2d <- apt_delay_cause_data %>%
  filter(FLIGHT_DATE <= data_day_date) %>%
  group_by(ARP_CODE) %>%
  reframe(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE,
    RWK_TDM_G,
    RWK_TDM_CS,
    RWK_TDM_IT,
    RWK_TDM_WD,
    RWK_TDM_NOCSGITWD,
    RWK_TDM_PREV_YEAR,
    Y2D_TDM_G = sum(TDM_G),
    Y2D_TDM_CS = sum(TDM_CS),
    Y2D_TDM_IT = sum(TDM_IT),
    Y2D_TDM_WD = sum(TDM_WD),
    Y2D_TDM_NOCSGITWD = sum(TDM_NOCSGITWD),
    Y2D_TDM = sum(TDM)
  ) %>%
  ungroup() %>%
  mutate(
    Y2D_SHARE_TDM_G = if_else(Y2D_TDM == 0, 0, Y2D_TDM_G / Y2D_TDM),
    Y2D_SHARE_TDM_CS = if_else(Y2D_TDM == 0, 0, Y2D_TDM_CS / Y2D_TDM),
    Y2D_SHARE_TDM_IT = if_else(Y2D_TDM == 0, 0, Y2D_TDM_IT / Y2D_TDM),
    Y2D_SHARE_TDM_WD = if_else(Y2D_TDM == 0, 0, Y2D_TDM_WD / Y2D_TDM),
    Y2D_SHARE_TDM_NOCSGITWD = if_else(Y2D_TDM == 0, 0, Y2D_TDM_NOCSGITWD / Y2D_TDM)
  ) %>%
  select(ARP_CODE,
         ARP_NAME,
         FLIGHT_DATE,
         RWK_TDM_G,
         RWK_TDM_CS,
         RWK_TDM_IT,
         RWK_TDM_WD,
         RWK_TDM_NOCSGITWD,
         RWK_TDM_PREV_YEAR,
         Y2D_SHARE_TDM_G,
         Y2D_SHARE_TDM_CS,
         Y2D_SHARE_TDM_IT,
         Y2D_SHARE_TDM_WD,
         Y2D_SHARE_TDM_NOCSGITWD
  ) %>% 
  arrange(ARP_CODE, FLIGHT_DATE)

colnames(apt_delay_cause_y2d) <- column_names

### nest data
apt_delay_value_y2d_long <- apt_delay_cause_y2d %>%
  select(-c(share_aerodrome_capacity,
            share_capacity_staffing_atc,
            share_disruptions_atc,
            share_weather,
            share_other)
  ) %>%
  pivot_longer(-c(APT_CODE, APT_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

apt_delay_share_y2d_long <- apt_delay_cause_y2d %>%
  select(-c("Aerodrome capacity",
            "Capacity/Staffing (ATC)",
            "Disruptions (ATC)",
            "Weather",
            "Other",
            paste0("Total delay ", data_day_year - 1)
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(APT_CODE, APT_NAME, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

apt_delay_cause_y2d_long <- cbind(apt_delay_value_y2d_long, apt_delay_share_y2d_long) %>%
  select(-name) %>%
  group_by(APT_CODE, APT_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")


# for consistency with v1 we use the word category in the name files... should have been cause
apt_delay_cause_evo_y2d_j <- apt_delay_cause_y2d_long %>% toJSON(., pretty = TRUE)

save_json(apt_delay_cause_evo_y2d_j, "apt_delay_category_evo_chart_y2d")
print(paste(format(now(), "%H:%M:%S"), "apt_delay_category_evo_chart_y2d"))



## DELAY Share ----
### 7-day % of delayed flights ----
apt_delayed_flights_evo <- apt_delay_data_full  %>%
  mutate(RWK_DELAYED_TFC_PERC = AVG_DELAYED_TFC_ROLLING_WEEK/AVG_TFC_ROLLING_WEEK,
         RWK_DELAYED_TFC_PERC_PREV_YEAR =  AVG_DELAYED_TFC_ROLLING_WEEK_PREV_YEAR/ AVG_TFC_ROLLING_WEEK_PREV_YEAR,
         RWK_DELAYED_TFC_15_PERC = AVG_DELAYED_TFC_15_ROLLING_WEEK/AVG_TFC_ROLLING_WEEK,
         RWK_DELAYED_TFC_15_PERC_PREV_YEAR = AVG_DELAYED_TFC_15_ROLLING_WEEK_PREV_YEAR/AVG_TFC_ROLLING_WEEK_PREV_YEAR,
         RWK_DELAYED_TFC_PERC = case_when(
           FLIGHT_DATE > min(data_day_date,
                             max(max(FLIGHT_DATE), na.rm = TRUE),
                             na.rm = TRUE) ~ NA,
           .default = RWK_DELAYED_TFC_PERC),
         RWK_DELAYED_TFC_15_PERC = case_when(
           FLIGHT_DATE > min(data_day_date,
                             max(max(FLIGHT_DATE), na.rm = TRUE),
                             na.rm = TRUE) ~ NA,
           .default = RWK_DELAYED_TFC_15_PERC),
  ) %>%
  select(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE,
    RWK_DELAYED_TFC_PERC,
    RWK_DELAYED_TFC_PERC_PREV_YEAR,
    RWK_DELAYED_TFC_15_PERC,
    RWK_DELAYED_TFC_15_PERC_PREV_YEAR
  ) %>%
  filter(FLIGHT_DATE >= as.Date(paste0("01-01-", data_day_year), format = "%d-%m-%Y"),
         FLIGHT_DATE <= as.Date(paste0("31-12-", data_day_year), format = "%d-%m-%Y")) %>% 
  arrange(ARP_CODE, FLIGHT_DATE)

column_names <- c('APT_CODE',
                  'APT_NAME',
                  'FLIGHT_DATE',
                  paste0('% of delayed flights ', data_day_year),
                  paste0('% of delayed flights ', data_day_year -1),
                  paste0("% of delayed flights >15' ", data_day_year),
                  paste0("% of delayed flights >15' ", data_day_year -1)
)

colnames(apt_delayed_flights_evo) <- column_names

### nest data
apt_delayed_flights_evo_long <- apt_delayed_flights_evo %>%
  pivot_longer(-c(APT_CODE, APT_NAME, FLIGHT_DATE), names_to = 'year', values_to = 'daio') %>%
  group_by(APT_CODE, APT_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")



apt_delayed_flights_evo_j <- apt_delayed_flights_evo_long %>% toJSON(., pretty = TRUE)

save_json(apt_delayed_flights_evo_j, "apt_delayed_flights_evo_chart")
print(paste(format(now(), "%H:%M:%S"), "apt_delayed_flights_evo_chart"))

print(" ")