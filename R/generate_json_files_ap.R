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
source(here::here("..", "mobile-app", "R", "duckdb_functions.R"))

# Parameters ----
source(here("..", "mobile-app", "R", "params.R"))

# Queries ----
source(here("..", "mobile-app", "R", "data_queries.R"))


# Dimensions ----
if (!exists("dim_iso_country")) {
  source(here("..", "mobile-app", "R", "dimensions.R")) 
}

# archive mode for past dates
if (exists("archive_mode") == FALSE) {archive_mode <- FALSE}
if (exists("data_day_date") == FALSE) {
  data_day_date <- lubridate::today(tzone = "") +  days(-1)
}

data_day_text <- data_day_date %>% format("%Y%m%d")
data_day_year <- as.numeric(format(data_day_date,'%Y'))

print(paste("Generating ap json files", format(data_day_date, "%Y-%m-%d"), "..."))


apt_json_app <-""

# ____________________________________________________________________________________________
#
#    APT landing page -----
#
# ____________________________________________________________________________________________

#### Import data ----
mydatafile <- paste0("ap_traffic_delay_day.parquet")
stakeholder <- substr(mydatafile, 1,2)

ap_traffic_delay_data <- read_parquet(here(archive_dir_raw, stakeholder, mydatafile)) %>% 
  filter(YEAR == data_day_year) %>% 
  rename_with(~ sub("DAY_", "DY_", .x, fixed = TRUE), contains("DAY_")) %>% 
  rename_with(~ sub("RWK_", "WK_", .x, fixed = TRUE), contains("RWK_")) %>% 
  rename(ARP_CODE = STK_CODE, ARP_NAME = STK_NAME)%>%
  arrange(ARP_NAME, FLIGHT_DATE)


#getting the latest date's traffic data
ap_traffic_delay_data_last_day <- ap_traffic_delay_data %>%
  filter(FLIGHT_DATE == min(data_day_date,
                            max(DATA_DAY, na.rm = TRUE),
                            na.rm = TRUE)
  ) %>%
  arrange(ARP_NAME, FLIGHT_DATE)

#### Traffic  ----
#selecting columns and renaming
apt_traffic_for_json <- ap_traffic_delay_data_last_day %>%
  right_join(list_airport_extended_new, by = c("ARP_CODE" = "EC_AP_CODE", "ARP_NAME" = "EC_AP_NAME")) %>%
  group_by(FLAG_TOP_APT) %>%
  mutate(
    DY_TFC_RANK = if_else(FLAG_TOP_APT == "N", NA, min_rank(desc(DY_TFC))),
    WK_TFC_RANK = if_else(FLAG_TOP_APT == "N", NA, min_rank(desc(WK_AVG_TFC))),
    Y2D_TFC_RANK = if_else(FLAG_TOP_APT == "N", NA, min_rank(desc(Y2D_TFC))),
    
    TFC_RANK_TEXT = "*Rank within top 40 airports\nTop rank for highest."
    
  ) %>%
  ungroup() %>%
  select(
    ARP_NAME,
    ARP_CODE,
    FLIGHT_DATE,
    #day
    DY_TFC_RANK,
    DY_TFC,
    DY_TFC_DIF_PREV_YEAR_PERC,
    DY_TFC_DIF_2019_PERC,
    #week average
    WK_TFC_RANK,
    WK_TFC_AVG_ROLLING = WK_AVG_TFC,
    WK_TFC_DIF_PREV_YEAR_PERC,
    WK_TFC_DIF_2019_PERC,
    #year to date
    Y2D_TFC_RANK,
    Y2D_TFC,
    Y2D_TFC_AVG = Y2D_AVG_TFC,
    Y2D_TFC_DIF_PREV_YEAR_PERC,
    Y2D_TFC_DIF_2019_PERC,
    
    TFC_RANK_TEXT
  )

#### Delay  ----
apt_delay_for_json  <- ap_traffic_delay_data_last_day %>%
  ### rank calculation
  right_join(list_airport_extended_new, by = c("ARP_CODE" = "EC_AP_CODE", "ARP_NAME" = "EC_AP_NAME")) %>%
  group_by(FLAG_TOP_APT) %>%
  mutate(
    DY_DLY_RANK = if_else(FLAG_TOP_APT == "N", NA, 
                          rank(desc(DY_DLY), ties.method = "max")),
    WK_DLY_RANK = if_else(FLAG_TOP_APT == "N", NA, 
                          rank(desc(WK_AVG_DLY), ties.method = "max")),
    Y2D_DLY_RANK = if_else(FLAG_TOP_APT == "N", NA, 
                           rank(desc(Y2D_DLY), ties.method = "max")),

    DY_DLY_FLT_RANK = if_else(FLAG_TOP_APT == "N", NA, 
                              rank(desc(DY_DLY_FLT), ties.method = "max")),
    WK_DLY_FLT_RANK = if_else(FLAG_TOP_APT == "N", NA, 
                              rank(desc(WK_DLY_FLT), ties.method = "max")),
    Y2D_DLY_FLT_RANK = if_else(FLAG_TOP_APT == "N", NA, 
                               rank(desc(Y2D_DLY_FLT), ties.method = "max")),

    DY_DELAYED_TFC_PERC_RANK = if_else(FLAG_TOP_APT == "N", NA, 
                                       rank(desc(DY_DLYED_PERC), ties.method = "max")),
    WK_DELAYED_TFC_PERC_RANK = if_else(FLAG_TOP_APT == "N", NA, 
                                       rank(desc(WK_DLYED_PERC), ties.method = "max")),
    Y2D_DELAYED_TFC_PERC_RANK = if_else(FLAG_TOP_APT == "N", NA, 
                                        rank(desc(Y2D_DLYED_PERC), ties.method = "max")),

    DY_DELAYED_TFC_15_PERC_RANK = if_else(FLAG_TOP_APT == "N", NA, 
                                          rank(desc(DY_DLYED_15_PERC), ties.method = "max")),
    WK_DELAYED_TFC_15_PERC_RANK = if_else(FLAG_TOP_APT == "N", NA, 
                                          rank(desc(WK_DLYED_15_PERC), ties.method = "max")),
    Y2D_DELAYED_TFC_15_PERC_RANK = if_else(FLAG_TOP_APT == "N", NA, 
                                           rank(desc(Y2D_DLYED_15_PERC), ties.method = "max")),

    DLY_RANK_TEXT = "*Rank within top 40 airports.\nTop rank for highest."
  ) %>%  
  ungroup() %>%
  select(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE,

    #delay
    DY_DLY_RANK,
    DY_DLY,
    DY_DLY_DIF_PREV_YEAR_PERC,
    DY_DLY_DIF_2019_PERC,

    WK_DLY_RANK,
    WK_DLY_AVG_ROLLING = WK_AVG_DLY,
    WK_DLY_DIF_PREV_YEAR_PERC,
    WK_DLY_DIF_2019_PERC,

    Y2D_DLY_RANK,
    Y2D_DLY_AVG = Y2D_AVG_DLY,
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
    DY_DELAYED_TFC_PERC = DY_DLYED_PERC,
    DY_DELAYED_TFC_PERC_DIF_PREV_YEAR = DY_DLYED_PERC_DIF_PREV_YEAR,
    DY_DELAYED_TFC_PERC_DIF_2019 = DY_DLYED_PERC_DIF_2019,

    WK_DELAYED_TFC_PERC_RANK,
    WK_DELAYED_TFC_PERC = WK_DLYED_PERC,
    WK_DELAYED_TFC_PERC_DIF_PREV_YEAR = WK_DLYED_PERC_DIF_PREV_YEAR,
    WK_DELAYED_TFC_PERC_DIF_2019 = WK_DLYED_PERC_DIF_2019,

    Y2D_DELAYED_TFC_PERC_RANK,
    Y2D_DELAYED_TFC_PERC = Y2D_DLYED_PERC,
    Y2D_DELAYED_TFC_PERC_DIF_PREV_YEAR = Y2D_DLYED_PERC_DIF_PREV_YEAR,
    Y2D_DELAYED_TFC_PERC_DIF_2019 = Y2D_DLYED_PERC_DIF_2019,

    #% of delayed flights >15'
    DY_DELAYED_TFC_15_PERC_RANK,
    DY_DELAYED_TFC_15_PERC = DY_DLYED_15_PERC,
    DY_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = DY_DLYED_15_PERC_DIF_PREV_YEAR,
    DY_DELAYED_TFC_15_PERC_DIF_2019 = DY_DLYED_15_PERC_DIF_2019,

    WK_DELAYED_TFC_15_PERC_RANK,
    WK_DELAYED_TFC_15_PERC = WK_DLYED_15_PERC,
    WK_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = WK_DLYED_15_PERC_DIF_PREV_YEAR,
    WK_DELAYED_TFC_15_PERC_DIF_2019 = WK_DLYED_15_PERC_DIF_2019,
    
    Y2D_DELAYED_TFC_15_PERC_RANK,
    Y2D_DELAYED_TFC_15_PERC = Y2D_DLYED_15_PERC,
    Y2D_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = Y2D_DLYED_15_PERC_DIF_PREV_YEAR,
    Y2D_DELAYED_TFC_15_PERC_DIF_2019 = Y2D_DLYED_15_PERC_DIF_2019,
    
    DLY_RANK_TEXT
  ) %>%
  arrange(ARP_CODE,
          ARP_NAME)


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
  right_join(list_airport_extended_new, by = c("ARP_CODE" = "EC_AP_CODE", "ARP_NAME" = "EC_AP_NAME")) %>%
  group_by(FLAG_TOP_APT) %>%
  mutate(
    DY_ARR_PUN_RANK = if_else(FLAG_TOP_APT == "N", NA, min_rank(desc(DY_ARR_PUN))),
    WK_ARR_PUN_RANK = if_else(FLAG_TOP_APT == "N", NA, min_rank(desc(WK_ARR_PUN))),
    Y2D_ARR_PUN_RANK = if_else(FLAG_TOP_APT == "N", NA, min_rank(desc(Y2D_ARR_PUN))),

    DY_DEP_PUN_RANK = if_else(FLAG_TOP_APT == "N", NA, min_rank(desc(DY_DEP_PUN))),
    WK_DEP_PUN_RANK = if_else(FLAG_TOP_APT == "N", NA, min_rank(desc(WK_DEP_PUN))),
    Y2D_DEP_PUN_RANK = if_else(FLAG_TOP_APT == "N", NA, min_rank(desc(Y2D_DEP_PUN))),

    PUN_RANK_TEXT = "*Rank within top 40 airports.\nTop rank for highest."

  ) %>%
  #to have the same structure than the previous script
  rename(
    flag_top_apt = FLAG_TOP_APT,
    longitude = LONGITUDE,
    latitude = LATITUDE
  ) %>% 
  select(-ICAO2LETTER, -BK_AP_ID) %>% 
  ungroup() %>% 
  arrange(ARP_CODE)

#### Join strings and save  ----
apt_json_app_j <- list_airport_extended_new %>%
  select (
    -FLAG_TOP_APT, ICAO2LETTER, -BK_AP_ID, -ICAO2LETTER,
    APT_LATITUDE = LATITUDE,
    APT_LONGITUDE = LONGITUDE,
    apt_name = EC_AP_NAME
  ) %>%
  arrange(apt_name)
apt_json_app_j$apt_traffic <- select(arrange(apt_traffic_for_json, ARP_NAME), -c(ARP_NAME, ARP_CODE))
apt_json_app_j$apt_delay <- select(arrange(apt_delay_for_json, ARP_NAME), -c(ARP_NAME, ARP_CODE))
apt_json_app_j$apt_punct <- select(arrange(apt_punct_for_json, ARP_NAME), -c(ARP_NAME, ARP_CODE))

apt_json_app_j <- apt_json_app_j %>%
  rename(APT_CODE = EC_AP_CODE)

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
mydataframe <-  "ap_ao_agg"
stakeholder <- str_sub(mydataframe, 1, 2)

#### day ----
apt_ao_data_day_int <- create_ranking(mydataframe, "DAY", DEP_ARR) %>% 
  filter(STK_CODE %in% list_airport_new$EC_AP_CODE)

apt_ao_data_day <- apt_ao_data_day_int |>
  select(
    APT_CODE = STK_CODE,
    APT_NAME = STK_NAME,
    RANK = R_RANK,
    DY_RANK_DIF_PREV_WEEK  = RANK_DIF,
    DY_AO_GRP_NAME = NAME,
    DY_TO_DATE = TO_DATE,
    DY_FLT = CURRENT,
    DY_FLT_DIF_PREV_WEEK_PERC = DIF1_METRIC_PERC,
    DY_FLT_DIF_PREV_YEAR_PERC = DIF2_METRIC_PERC
  ) %>% 
  filter(RANK <11)

#### week ----
apt_ao_data_week_int <- create_ranking(mydataframe, "WEEK", DEP_ARR) %>% 
  filter(STK_CODE %in% list_airport_new$EC_AP_CODE)

apt_ao_data_week <- apt_ao_data_week_int |>
  select(
    APT_CODE = STK_CODE,
    APT_NAME = STK_NAME,
    RANK = R_RANK,
    WK_RANK_DIF_PREV_WEEK  = RANK_DIF,
    WK_AO_GRP_NAME = NAME,
    WK_FROM_DATE = FROM_DATE,
    WK_TO_DATE = TO_DATE,
    WK_FLT_AVG = CURRENT,
    WK_FLT_DIF_PREV_WEEK_PERC = DIF1_METRIC_PERC,
    WK_FLT_DIF_PREV_YEAR_PERC = DIF2_METRIC_PERC
  ) %>% 
  filter(RANK <11)

#### y2d ----
apt_ao_data_y2d_int <- create_ranking(mydataframe, "Y2D", DEP_ARR) %>% 
  filter(STK_CODE %in% list_airport_new$EC_AP_CODE)

apt_ao_data_y2d <- apt_ao_data_y2d_int |>
  select(
    APT_CODE = STK_CODE,
    APT_NAME = STK_NAME,
    RANK = R_RANK,
    Y2D_RANK_DIF_PREV_YEAR  = RANK_DIF,
    Y2D_AO_GRP_NAME = NAME,
    Y2D_TO_DATE = TO_DATE,
    Y2D_FLT_AVG = CURRENT,
    Y2D_FLT_DIF_PREV_YEAR_PERC = DIF1_METRIC_PERC,
    Y2D_FLT_DIF_2019_PERC = DIF2_METRIC_PERC
  )%>% 
  filter(RANK <11)

#### main card ----
apt_ao_main_traffic <- create_main_card (apt_ao_data_day_int)  %>% 
  select(APT_CODE = STK_CODE, 
         APT_NAME = STK_NAME, 
         RANK = R_RANK,
         MAIN_TFC_AO_GRP_NAME = NAME, 
         MAIN_TFC_AO_GRP_CODE = CODE, 
         MAIN_TFC_AO_GRP_FLT = CURRENT)

apt_ao_main_traffic_dif <- create_main_card_dif (apt_ao_data_day_int) %>% 
  select(APT_CODE = STK_CODE, 
         APT_NAME = STK_NAME,
         RANK = R_RANK,
         MAIN_TFC_DIF_AO_GRP_NAME = NAME,
         MAIN_TFC_DIF_AO_GRP_CODE = CODE,
         MAIN_TFC_DIF_AO_GRP_FLT_DIF = DIF1_METRIC
         )

#### join tables ----
apt_ao_ranking_traffic <- apt_ao_main_traffic |>
  left_join(apt_ao_main_traffic_dif, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_ao_data_day, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_ao_data_week, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_ao_data_y2d, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  arrange(APT_CODE, APT_NAME, RANK) |>
  distinct(RANK, APT_CODE, APT_NAME, .keep_all = TRUE)

# convert to json and save in app data folder
apt_ao_ranking_traffic_j <- apt_ao_ranking_traffic |> toJSON(pretty = TRUE)

save_json(apt_ao_ranking_traffic_j, "apt_ao_ranking_traffic", archive_file = FALSE)
print(paste(format(now(), "%H:%M:%S"), "apt_ao_ranking_traffic"))


### Airports ----
mydataframe <-  "ap_ap_des_agg"
stakeholder <- str_sub(mydataframe, 1, 2)

#### day ----
apt_apt_data_day_int <- create_ranking(mydataframe, "DAY", DEP) %>% 
  filter(STK_CODE %in% list_airport_new$EC_AP_CODE)

apt_apt_data_day <- apt_apt_data_day_int |>
  select(
    APT_CODE = STK_CODE,
    APT_NAME = STK_NAME,
    RANK = R_RANK,
    DY_RANK_DIF_PREV_WEEK  = RANK_DIF,
    DY_APT_NAME = NAME,
    DY_TO_DATE = TO_DATE,
    DY_FLT = CURRENT,
    DY_FLT_DIF_PREV_WEEK_PERC = DIF1_METRIC_PERC,
    DY_FLT_DIF_PREV_YEAR_PERC = DIF2_METRIC_PERC
  ) %>% 
  filter(RANK <11)

#### week ----
apt_apt_data_week_int <- create_ranking(mydataframe, "WEEK", DEP) %>% 
  filter(STK_CODE %in% list_airport_new$EC_AP_CODE)

apt_apt_data_week <- apt_apt_data_week_int |>
  select(
    APT_CODE = STK_CODE,
    APT_NAME = STK_NAME,
    RANK = R_RANK,
    WK_RANK_DIF_PREV_WEEK  = RANK_DIF,
    WK_APT_NAME = NAME,
    WK_FROM_DATE = FROM_DATE,
    WK_TO_DATE = TO_DATE,
    WK_FLT_AVG = CURRENT,
    WK_FLT_DIF_PREV_WEEK_PERC = DIF1_METRIC_PERC,
    WK_FLT_DIF_PREV_YEAR_PERC = DIF2_METRIC_PERC
  ) %>% 
  filter(RANK <11)

#### y2d ----
apt_apt_data_y2d_int <- create_ranking(mydataframe, "Y2D", DEP) %>% 
  filter(STK_CODE %in% list_airport_new$EC_AP_CODE)

apt_apt_data_y2d <- apt_apt_data_y2d_int |>
  select(
    APT_CODE = STK_CODE,
    APT_NAME = STK_NAME,
    RANK = R_RANK,
    Y2D_RANK_DIF_PREV_YEAR  = RANK_DIF,
    Y2D_APT_NAME = NAME,
    Y2D_TO_DATE = TO_DATE,
    Y2D_FLT_AVG = CURRENT,
    Y2D_FLT_DIF_PREV_YEAR_PERC = DIF1_METRIC_PERC,
    Y2D_FLT_DIF_2019_PERC = DIF2_METRIC_PERC
  ) %>% 
  filter(RANK <11)

#### main card ----
apt_apt_main_traffic <- create_main_card (apt_apt_data_day_int)  %>% 
  select(APT_CODE = STK_CODE, 
         APT_NAME = STK_NAME, 
         RANK = R_RANK,
         MAIN_TFC_APT_NAME = NAME, 
         MAIN_TFC_APT_CODE = CODE, 
         MAIN_TFC_APT_FLT = CURRENT)

apt_apt_main_traffic_dif <- create_main_card_dif (apt_apt_data_day_int) %>% 
  select(APT_CODE = STK_CODE, 
         APT_NAME = STK_NAME,
         RANK = R_RANK,
         MAIN_TFC_DIF_APT_NAME = NAME,
         MAIN_TFC_DIF_APT_CODE = CODE,
         MAIN_TFC_DIF_APT_FLT_DIF = DIF1_METRIC
  )

#### join tables ----
apt_apt_ranking_traffic <- apt_apt_main_traffic |>
  left_join(apt_apt_main_traffic_dif, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_apt_data_day, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_apt_data_week, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_apt_data_y2d, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  arrange(APT_CODE, APT_NAME, RANK) |>
  distinct(RANK, APT_CODE, APT_NAME, .keep_all = TRUE)

# convert to json and save in app data folder
apt_apt_ranking_traffic_j <- apt_apt_ranking_traffic |> toJSON(pretty = TRUE)

save_json(apt_apt_ranking_traffic_j, "apt_apt_ranking_traffic", archive_file = FALSE)
print(paste(format(now(), "%H:%M:%S"), "apt_apt_ranking_traffic"))

### States ----
mydataframe <- "ap_st_des_agg"
stakeholder <- str_sub(mydataframe, 1, 2)

#### day ----
apt_st_data_day_int <- create_ranking(mydataframe, "DAY", DEP) %>% 
  filter(STK_CODE %in% list_airport_new$EC_AP_CODE)

apt_st_data_day <- apt_st_data_day_int |>
  select(
    APT_CODE = STK_CODE,
    APT_NAME = STK_NAME,
    RANK = R_RANK,
    DY_RANK_DIF_PREV_WEEK  = RANK_DIF,
    DY_ST_DES_NAME = NAME,
    DY_TO_DATE = TO_DATE,
    DY_FLT = CURRENT,
    DY_FLT_DIF_PREV_WEEK_PERC = DIF1_METRIC_PERC,
    DY_FLT_DIF_PREV_YEAR_PERC = DIF2_METRIC_PERC
  )

#### week ----
apt_st_data_week_int <- create_ranking(mydataframe, "WEEK", DEP) %>% 
  filter(STK_CODE %in% list_airport_new$EC_AP_CODE)

apt_st_data_week <- apt_st_data_week_int |>
  select(
    APT_CODE = STK_CODE,
    APT_NAME = STK_NAME,
    RANK = R_RANK,
    WK_RANK_DIF_PREV_WEEK  = RANK_DIF,
    WK_ST_DES_NAME = NAME,
    WK_FROM_DATE = FROM_DATE,
    WK_TO_DATE = TO_DATE,
    WK_FLT_AVG = CURRENT,
    WK_FLT_DIF_PREV_WEEK_PERC = DIF1_METRIC_PERC,
    WK_FLT_DIF_PREV_YEAR_PERC = DIF2_METRIC_PERC
  )

#### y2d ----
apt_st_data_y2d_int <- create_ranking(mydataframe, "Y2D", DEP) %>% 
  filter(STK_CODE %in% list_airport_new$EC_AP_CODE)

apt_st_data_y2d <- apt_st_data_y2d_int |>
  select(
    APT_CODE = STK_CODE,
    APT_NAME = STK_NAME,
    RANK = R_RANK,
    Y2D_RANK_DIF_PREV_WEEK  = RANK_DIF,
    Y2D_ST_DES_NAME = NAME,
    Y2D_FROM_DATE = FROM_DATE,
    Y2D_TO_DATE = TO_DATE,
    Y2D_FLT_AVG = CURRENT,
    Y2D_FLT_DIF_PREV_YEAR_PERC = DIF1_METRIC_PERC,
    Y2D_FLT_DIF_2019_PERC = DIF2_METRIC_PERC
  )

#### main card ----
apt_st_main_traffic <- create_main_card (apt_st_data_day_int)  %>% 
  select(APT_CODE = STK_CODE, 
         APT_NAME = STK_NAME, 
         RANK = R_RANK,
         MAIN_TFC_ST_DES_NAME = NAME, 
         MAIN_TFC_ST_DES_CODE = CODE, 
         MAIN_TFC_ST_DES_FLT = CURRENT)

apt_st_main_traffic_dif <- create_main_card_dif (apt_st_data_day_int) %>% 
  select(APT_CODE = STK_CODE, 
         APT_NAME = STK_NAME,
         RANK = R_RANK,
         MAIN_TFC_DIF_ST_DES_NAME = NAME,
         MAIN_TFC_DIF_ST_DES_CODE = CODE,
         MAIN_TFC_DIF_ST_DES_FLT_DIF = DIF1_METRIC
  )

#### join tables ----
apt_st_ranking_traffic <- apt_st_main_traffic |>
  left_join(apt_st_main_traffic_dif, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_st_data_day, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_st_data_week, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_st_data_y2d, by = c("RANK", "APT_CODE", "APT_NAME"),
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

mydataframe <- "ap_ms_agg"
stakeholder <- str_sub(mydataframe, 1, 2)

ms_ap_full <- crossing(list_marktet_segment_app_new, list_airport_new$EC_AP_CODE) %>% 
  select (CODE = MS_ID, NAME = MS_NAME, STK_CODE = contains("EC_AP_CODE")) %>% arrange(STK_CODE, CODE)


#### day ----
apt_ms_data_day_int <- create_ranking(mydataframe, "DAY", DEP_ARR) %>% 
  filter(STK_CODE %in% list_airport_new$EC_AP_CODE) %>% 
  group_by(STK_CODE) |>
  mutate(SHARE = ifelse(CURRENT == 0, 0,
                              CURRENT/sum(CURRENT, na.rm = TRUE))) |>
  ungroup() 

apt_ms_data_day_full <- ms_ap_full %>% 
  left_join(select(apt_ms_data_day_int, -NAME), by = c("CODE", "STK_CODE"))  %>% 
  group_by(STK_CODE) %>% 
  arrange(STK_CODE, desc(CURRENT), NAME) %>% 
  mutate(RANK = row_number())|>
  arrange(STK_CODE, R_RANK) %>% 
  fill(c(STK_NAME, TO_DATE), .direction = "down") %>% 
  ungroup()

apt_ms_data_day <- apt_ms_data_day_full |>
  select(
    APT_CODE = STK_CODE,
    APT_NAME = STK_NAME,
    RANK,
    DY_RANK_DIF_PREV_WEEK  = RANK_DIF,
    DY_MS_NAME = NAME,
    DY_MS_SHARE = SHARE,
    DY_TO_DATE = TO_DATE,
    DY_FLT = CURRENT,
    DY_FLT_DIF_PREV_WEEK_PERC = DIF1_METRIC_PERC,
    DY_FLT_DIF_PREV_YEAR_PERC = DIF2_METRIC_PERC
  )


#### week ----
apt_ms_data_week_int <- create_ranking(mydataframe, "WEEK", DEP_ARR) %>% 
  filter(STK_CODE %in% list_airport_new$EC_AP_CODE) %>% 
  group_by(STK_CODE) |>
  mutate(SHARE = ifelse(CURRENT == 0, 0,
                              CURRENT/sum(CURRENT, na.rm = TRUE))) |>
  ungroup() 

apt_ms_data_week_full <- ms_ap_full %>% 
  left_join(select(apt_ms_data_week_int, -NAME), by = c("CODE", "STK_CODE"))  %>% 
  group_by(STK_CODE) %>% 
  arrange(STK_CODE, desc(CURRENT), NAME) %>% 
  mutate(RANK = row_number())|>
  arrange(STK_CODE, R_RANK) %>% 
  fill(c(STK_NAME, TO_DATE, FROM_DATE), .direction = "down") %>% 
  ungroup()


apt_ms_data_week <- apt_ms_data_week_full |>
  select(
    APT_CODE = STK_CODE,
    APT_NAME = STK_NAME,
    RANK,
    WK_RANK_DIF_PREV_WEEK  = RANK_DIF,
    WK_MS_NAME = NAME,
    WK_MS_SHARE = SHARE,
    WK_FROM_DATE = FROM_DATE,
    WK_TO_DATE = TO_DATE,
    WK_FLT_AVG = CURRENT,
    WK_FLT_DIF_PREV_WEEK_PERC = DIF1_METRIC_PERC,
    WK_FLT_DIF_PREV_YEAR_PERC = DIF2_METRIC_PERC
  )

#### y2d ----
apt_ms_data_y2d_int <- create_ranking(mydataframe, "Y2D", DEP_ARR) %>% 
  filter(STK_CODE %in% list_airport_new$EC_AP_CODE) %>% 
  group_by(STK_CODE) |>
  mutate(SHARE = ifelse(CURRENT == 0, 0,
                        CURRENT/sum(CURRENT, na.rm = TRUE))) |>
  ungroup() 

apt_ms_data_y2d_full <- ms_ap_full %>% 
  left_join(select(apt_ms_data_y2d_int, -NAME), by = c("CODE", "STK_CODE"))  %>% 
  group_by(STK_CODE) %>% 
  arrange(STK_CODE, desc(CURRENT), NAME) %>% 
  mutate(RANK = row_number())|>
  arrange(STK_CODE, R_RANK) %>% 
  fill(c(STK_NAME, TO_DATE, FROM_DATE), .direction = "down") %>% 
  ungroup()


apt_ms_data_y2d <- apt_ms_data_y2d_full |>
  select(
    APT_CODE = STK_CODE,
    APT_NAME = STK_NAME,
    RANK = R_RANK,
    Y2D_RANK_DIF_PREV_YEAR  = RANK_DIF,
    Y2D_MS_NAME = NAME,
    Y2D_MS_SHARE = SHARE,
    Y2D_TO_DATE = TO_DATE,
    Y2D_FLT_AVG = CURRENT,
    Y2D_FLT_DIF_PREV_YEAR_PERC = DIF1_METRIC_PERC,
    Y2D_FLT_DIF_2019_PERC = DIF2_METRIC_PERC
  )

# all.equal(apt_ms_data_y2dxx, apt_ms_data_y2d)
# 
# for(i in 1:nrow(apt_ms_data_y2dxx)) {
#   if(coalesce(apt_ms_data_y2dxx$Y2D_MS_NAME[i],"0") != coalesce((apt_ms_data_y2d$Y2D_MS_NAME[i]),"0")) {
#     print(paste(apt_ms_data_y2dxx$APT_CODE[i], apt_ms_data_y2dxx$Y2D_MS_NAME[i], apt_ms_data_y2dxx$Y2D_MS_NAME[i], apt_ms_data_y2d$Y2D_MS_NAME[i]))
#   }
# }


# 
#  
# col <- "WK_TO_DATE"
# 
# na_x  <- is.na(apt_ms_data_weekxx[[col]])
# na_t  <- is.na(apt_ms_data_week[[col]])
# 
# # 3) Locate rows where NA status differs
# idx_mismatch <- which(xor(na_x, na_t))
# 
# cat("Rows with NA status mismatch:", length(idx_mismatch), "\n")
# 
# # 4) Inspect a small sample of mismatches
# head_idx <- head(idx_mismatch, 20)
# print(head_idx)
# 
# # Optional: bind side-by-side for spot-checking (avoids full prints)
# mismatch_view <- data.frame(
#   row = idx_mismatch,
#   apt = apt_ms_data_weekxx[["APT_NAME"]][idx_mismatch],
#   date = apt_ms_data_weekxx[["WK_MS_NAME"]][idx_mismatch],
#   current_val = apt_ms_data_weekxx[[col]][idx_mismatch],
#   target_val  = apt_ms_data_week[[col]][idx_mismatch],
#   current_is_na = na_x[idx_mismatch],
# )
# print(mismatch_view)

#### join tables ----
apt_ms_ranking_traffic <- apt_ms_data_day |>
  left_join(apt_ms_data_week, by = c("RANK", "APT_CODE", "APT_NAME")) |>
  left_join(apt_ms_data_y2d, by = c("RANK", "APT_CODE", "APT_NAME")) |>
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
apt_traffic_evo <- ap_traffic_delay_data  %>%
  select(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE,

    WK_AVG_TFC,
    WK_AVG_TFC_PREV_YEAR,
    WK_AVG_TFC_2020,
    WK_AVG_TFC_2019
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
  right_join(select(list_airport_extended_new, EC_AP_CODE, EC_AP_NAME), join_by("ARP_CODE"=="EC_AP_CODE")) %>%
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
#### day ----
apt_delay_cause_day <- ap_traffic_delay_data %>%
  filter(FLIGHT_DATE == min(max(FLIGHT_DATE),
                            data_day_date,
                            na.rm = TRUE)
  )%>%
  mutate(
    SHARE_DLY_G = if_else(DY_DLY == 0, 0, DY_DLY_G / DY_DLY),
    SHARE_DLY_CS = if_else(DY_DLY == 0, 0, DY_DLY_CS / DY_DLY),
    SHARE_DLY_IT = if_else(DY_DLY == 0, 0, DY_DLY_IT / DY_DLY),
    SHARE_DLY_WD = if_else(DY_DLY == 0, 0, DY_DLY_WD / DY_DLY),
    SHARE_DLY_OTHER = if_else(DY_DLY == 0, 0, DY_DLY_OTHER / DY_DLY)
  ) %>%
  select(ARP_CODE,
         ARP_NAME,
         FLIGHT_DATE,
         DY_DLY_G,
         DY_DLY_CS,
         DY_DLY_IT,
         DY_DLY_WD,
         DY_DLY_OTHER,
         DY_DLY_PREV_YEAR,
         SHARE_DLY_G,
         SHARE_DLY_CS,
         SHARE_DLY_IT,
         SHARE_DLY_WD,
         SHARE_DLY_OTHER
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
apt_delay_cause_wk <- ap_traffic_delay_data %>%
  filter(FLIGHT_DATE >= min(max(FLIGHT_DATE), data_day_date, na.rm  = TRUE) + lubridate::days(-6),
         FLIGHT_DATE <= min(max(FLIGHT_DATE), data_day_date, na.rm  = TRUE)
  )  %>%
  group_by(ARP_CODE) %>% 
  mutate(
    WK_SHARE_DLY_G = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_G) / sum(DY_DLY)),
    WK_SHARE_DLY_CS = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_CS) / sum(DY_DLY)),
    WK_SHARE_DLY_IT = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_IT) / sum(DY_DLY)),
    WK_SHARE_DLY_WD = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_WD) / sum(DY_DLY)),
    WK_SHARE_DLY_OTHER = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_OTHER) / sum(DY_DLY))
  ) %>%
  ungroup() %>% 
  select(ARP_CODE,
         ARP_NAME,
         FLIGHT_DATE,
         DY_DLY_G,
         DY_DLY_CS,
         DY_DLY_IT,
         DY_DLY_WD,
         DY_DLY_OTHER,
         DY_DLY_PREV_YEAR,
         WK_SHARE_DLY_G,
         WK_SHARE_DLY_CS,
         WK_SHARE_DLY_IT,
         WK_SHARE_DLY_WD,
         WK_SHARE_DLY_OTHER
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
apt_delay_cause_y2d <- ap_traffic_delay_data %>%
  filter(FLIGHT_DATE <= data_day_date) %>%
  group_by(ARP_CODE) %>% 
  mutate(
    Y2D_SHARE_DLY_G = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_G) / sum(DY_DLY)),
    Y2D_SHARE_DLY_CS = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_CS) / sum(DY_DLY)),
    Y2D_SHARE_DLY_IT = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_IT) / sum(DY_DLY)),
    Y2D_SHARE_DLY_WD = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_WD) / sum(DY_DLY)),
    Y2D_SHARE_DLY_OTHER = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_OTHER) / sum(DY_DLY))
  ) %>%
  ungroup() %>% 
  select(ARP_CODE,
         ARP_NAME,
         FLIGHT_DATE,
         WK_AVG_DLY_G,
         WK_AVG_DLY_CS,
         WK_AVG_DLY_IT,
         WK_AVG_DLY_WD,
         WK_AVG_DLY_OTHER,
         WK_AVG_DLY_PREV_YEAR,
         Y2D_SHARE_DLY_G,
         Y2D_SHARE_DLY_CS,
         Y2D_SHARE_DLY_IT,
         Y2D_SHARE_DLY_WD,
         Y2D_SHARE_DLY_OTHER
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
apt_delayed_flights_evo <- ap_traffic_delay_data  %>%
  select(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE,
    RWK_DELAYED_TFC_PERC = WK_DLYED_PERC,
    RWK_DELAYED_TFC_PERC_PREV_YEAR = WK_DLYED_PERC_PREV_YEAR,
    RWK_DELAYED_TFC_15_PERC = WK_DLYED_15_PERC,
    RWK_DELAYED_TFC_15_PERC_PREV_YEAR = WK_DLYED_15_PERC_PREV_YEAR
  ) %>% 
  arrange(ARP_CODE, FLIGHT_DATE) %>% 
  # NOTE! just so we have exactly the same figures as in the old calculations
  mutate(
    across(.cols = contains("DELAYED"), ~ replace(.x, ARP_CODE == "UKBB", NA)),
    RWK_DELAYED_TFC_PERC_PREV_YEAR = if_else(ARP_CODE == "ESSB" & FLIGHT_DATE %in% seq.Date(ymd(20250727), ymd(20250803)), NA, RWK_DELAYED_TFC_PERC_PREV_YEAR),
    RWK_DELAYED_TFC_15_PERC_PREV_YEAR = if_else(ARP_CODE == "ESSB" & FLIGHT_DATE %in% seq.Date(ymd(20250727), ymd(20250803)), NA, RWK_DELAYED_TFC_15_PERC_PREV_YEAR) 
    
  )

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