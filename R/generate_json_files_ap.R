## libraries
library(data.table)
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
if (exists("mydate") == FALSE) {
  mydate <- ''
  source(here("..", "mobile-app", "R", "queries_ap.R"))
}

# airport dimension table (lists the airports and their ICAO codes)
if (exists("apt_icao") == FALSE) {
  apt_icao <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(ap_base_file),
      start = ap_base_dir),
    sheet = "lists",
    range = cell_limits(c(1, 1), c(NA, NA))) %>%
    as_tibble()
}

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

#reading the traffic sheet
apt_traffic_delay_data <- export_query(query_ap_traffic) %>%
  as_tibble() %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

#getting the latest date's traffic data
apt_traffic_delay_last_day <- apt_traffic_delay_data %>%
  filter(FLIGHT_DATE == min(data_day_date,
                            max(LAST_DATA_DAY, na.rm = TRUE),
                            na.rm = TRUE)
  ) %>%
  arrange(ARP_NAME, FLIGHT_DATE)

#selecting columns and renaming
apt_traffic_for_json <- apt_traffic_delay_last_day %>%
  select(
    ARP_NAME,
    ARP_CODE,
    FLIGHT_DATE,
    #totals
    DY_TFC = DAY_DEP_ARR, #arrival and departure traffic
    DY_TFC_DIF_PREV_YEAR_PERC = DAY_DEP_ARR_DIF_PREV_YEAR_PERC,
    DY_TFC_DIF_2019_PERC = DAY_DEP_ARR_DIF_2019_PERC,
    #week average
    WK_TFC_AVG_ROLLING = RWK_AVG_DEP_ARR,
    WK_TFC_DIF_PREV_YEAR_PERC = RWK_DEP_ARR_DIF_PREV_YEAR_PERC,
    WK_TFC_DIF_2019_PERC = RWK_DEP_ARR_DIF_2019_PERC,
    #year to date
    Y2D_TFC = Y2D_DEP_ARR_YEAR,
    Y2D_TFC_AVG = Y2D_AVG_DEP_ARR_YEAR,
    Y2D_TFC_DIF_PREV_YEAR_PERC = Y2D_DEP_ARR_DIF_PREV_YEAR_PERC,
    Y2D_TFC_DIF_2019_PERC = Y2D_DEP_ARR_DIF_2019_PERC
  )



#### Delay data ----

#arrival delay
#reading the delay cause sheet
apt_delay_data <-  export_query(query_ap_delay) %>%
  as_tibble() %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

#adding columns to the delay data
#adding rolling week average and year-to-date(Y2D)
apt_delay_data <- apt_delay_data %>%
  arrange(FLIGHT_DATE) %>%
  group_by(ARP_CODE) %>%
  mutate(#delay minutes
         DAY_DLY = TDM_ARP,
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
apt_delay_data_ <- apt_delay_data %>%
  #2019, and previous year values
  mutate(DATE_PREV_YEAR = FLIGHT_DATE-(1)*364 - round(1 / 4) * 7,
         DATE_2019 = FLIGHT_DATE-(year ( FLIGHT_DATE ) - 2019)*364 - round((year ( FLIGHT_DATE) - 2019) / 4) * 7) %>%
  arrange(ARP_CODE, FLIGHT_DATE) %>%
  #obtaining data for previous year
  left_join(apt_delay_data %>% select(ARP_CODE,
                                      FLIGHT_DATE,
                                      #delay minutes
                                      DAY_DLY,
                                      AVG_DLY_ROLLING_WEEK,
                                      Y2D_DLY_YEAR,
                                      Y2D_AVG_DLY,

                                      #number of flights
                                      DAY_TFC,
                                      AVG_TFC_ROLLING_WEEK,
                                      Y2D_TFC_YEAR,
                                      Y2D_AVG_TFC,

                                      #number of delayed flights
                                      DAY_DELAYED_TFC,
                                      AVG_DELAYED_TFC_ROLLING_WEEK,
                                      Y2D_DELAYED_TFC_YEAR,
                                      Y2D_AVG_DELAYED_TFC,

                                      #number delayed flights >15 minutes'
                                      DAY_DELAYED_TFC_15,
                                      AVG_DELAYED_TFC_15_ROLLING_WEEK,
                                      Y2D_DELAYED_TFC_15_YEAR,
                                      Y2D_AVG_DELAYED_TFC_15

                                      ),
            by = join_by(DATE_PREV_YEAR == FLIGHT_DATE, ARP_CODE == ARP_CODE),
            suffix = c("", "_PREV_YEAR"))  %>%
  #obtaining data for 2019
  left_join(apt_delay_data %>% select(ARP_CODE,
                                       FLIGHT_DATE,
                                      #delay minutes
                                      DAY_DLY,
                                      AVG_DLY_ROLLING_WEEK,
                                      Y2D_DLY_YEAR,
                                      Y2D_AVG_DLY,

                                      #number of flights
                                      DAY_TFC,
                                      AVG_TFC_ROLLING_WEEK,
                                      Y2D_TFC_YEAR,
                                      Y2D_AVG_TFC,

                                      #number of delayed flights
                                      DAY_DELAYED_TFC,
                                      AVG_DELAYED_TFC_ROLLING_WEEK,
                                      Y2D_DELAYED_TFC_YEAR,
                                      Y2D_AVG_DELAYED_TFC,

                                      #number delayed flights >15 minutes'
                                      DAY_DELAYED_TFC_15,
                                      AVG_DELAYED_TFC_15_ROLLING_WEEK,
                                      Y2D_DELAYED_TFC_15_YEAR,
                                      Y2D_AVG_DELAYED_TFC_15
                                      ),
            by = join_by(DATE_2019 == FLIGHT_DATE, ARP_CODE == ARP_CODE),
            suffix = c("", "_2019"))

#getting the latest date's traffic data
apt_delay_last_day <- apt_delay_data_ %>%
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
      Y2D_DLY_YEAR_PREV_YEAR == 0 , NA , Y2D_DLY_YEAR / Y2D_DLY_YEAR_PREV_YEAR - 1
    ),
    Y2D_DLY_DIF_2019_PERC = if_else(
      Y2D_DLY_YEAR_2019 == 0 , NA , Y2D_DLY_YEAR / Y2D_DLY_YEAR_2019 - 1
    ),


    #delay per flight
    DAY_DLY_FLT = DAY_DLY / DAY_TFC,
    DAY_DLY_FLT_PY = DAY_DLY_PREV_YEAR / DAY_TFC_PREV_YEAR,
    DAY_DLY_FLT_2019 = DAY_DLY_2019 / DAY_TFC_2019,
    DY_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(DAY_DLY_FLT_PY == 0, NA , DAY_DLY_FLT / DAY_DLY_FLT_PY - 1),
    DY_DLY_FLT_DIF_2019_PERC = if_else(DAY_DLY_FLT_2019 == 0, NA , DAY_DLY_FLT / DAY_DLY_FLT_2019 - 1),

    WK_DLY_FLT  = AVG_DLY_ROLLING_WEEK / AVG_TFC_ROLLING_WEEK,
    WK_DLY_FLT_PY = AVG_DLY_ROLLING_WEEK_PREV_YEAR / AVG_TFC_ROLLING_WEEK_PREV_YEAR,
    WK_DLY_FLT_2019 = AVG_DLY_ROLLING_WEEK_2019 / AVG_TFC_ROLLING_WEEK_2019,
    WK_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(WK_DLY_FLT_PY == 0, NA , WK_DLY_FLT / WK_DLY_FLT_PY - 1),
    WK_DLY_FLT_DIF_2019_PERC = if_else(WK_DLY_FLT_2019 == 0, NA , WK_DLY_FLT / WK_DLY_FLT_2019 - 1),


    Y2D_DLY_FLT = Y2D_DLY_YEAR / Y2D_TFC_YEAR,
    Y2D_DLY_FLT_PY = Y2D_AVG_DLY_PREV_YEAR / Y2D_AVG_TFC_PREV_YEAR,
    Y2D_DLY_FLT_2019 = Y2D_AVG_DLY_2019 / Y2D_AVG_TFC_2019,
    Y2D_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(Y2D_DLY_FLT_PY == 0, NA , Y2D_DLY_FLT / Y2D_DLY_FLT_PY -1),
    Y2D_DLY_FLT_DIF_2019_PERC = if_else(Y2D_DLY_FLT_2019 == 0, NA , Y2D_DLY_FLT / Y2D_DLY_FLT_2019 -1),


    #% of delayed flights
    DY_DELAYED_TFC_PERC = DAY_DELAYED_TFC/DAY_TFC,
    DY_DELAYED_TFC_PY = DAY_DELAYED_TFC_PREV_YEAR / DAY_TFC_PREV_YEAR,
    DY_DELAYED_TFC_2019 = DAY_DELAYED_TFC_2019 / DAY_TFC_2019,
    DY_DELAYED_TFC_PERC_DIF_PREV_YEAR = if_else(DY_DELAYED_TFC_PY == 0, NA , DY_DELAYED_TFC_PERC  / DY_DELAYED_TFC_PY - 1),
    DY_DELAYED_TFC_PERC_DIF_2019 = if_else( DY_DELAYED_TFC_2019 == 0, NA , DY_DELAYED_TFC_PERC  / DY_DELAYED_TFC_2019 - 1),


    WK_DELAYED_TFC_PERC = AVG_DELAYED_TFC_ROLLING_WEEK / AVG_TFC_ROLLING_WEEK,,
    WK_DELAYED_TFC_PY = AVG_DELAYED_TFC_ROLLING_WEEK_PREV_YEAR / AVG_TFC_ROLLING_WEEK_PREV_YEAR,
    WK_DELAYED_TFC_2019 = AVG_DELAYED_TFC_ROLLING_WEEK_2019 / AVG_TFC_ROLLING_WEEK_2019,
    WK_DELAYED_TFC_PERC_DIF_PREV_YEAR = if_else(WK_DELAYED_TFC_PY  == 0, NA , WK_DELAYED_TFC_PERC / WK_DELAYED_TFC_PY - 1),
    WK_DELAYED_TFC_PERC_DIF_2019 = if_else(WK_DELAYED_TFC_2019 == 0, NA , WK_DELAYED_TFC_PERC / WK_DELAYED_TFC_2019 - 1),


    Y2D_DELAYED_TFC_PERC = Y2D_DELAYED_TFC_YEAR / Y2D_TFC_YEAR,
    Y2D_DELAYED_TFC_PY = Y2D_AVG_DELAYED_TFC_PREV_YEAR / Y2D_AVG_TFC_PREV_YEAR,
    Y2D_DELAYED_TFC_2019 = Y2D_AVG_DELAYED_TFC_2019 / Y2D_AVG_TFC_2019,
    Y2D_DELAYED_TFC_PERC_DIF_PREV_YEAR = if_else(Y2D_DELAYED_TFC_PY == 0, NA , Y2D_DELAYED_TFC_PERC / Y2D_DELAYED_TFC_PY -1),
    Y2D_DELAYED_TFC_PERC_DIF_2019 = if_else(Y2D_DELAYED_TFC_2019 == 0, NA , Y2D_DELAYED_TFC_PERC / Y2D_DELAYED_TFC_2019 -1),

    #% of delayed flights >15'
    DY_DELAYED_TFC_15_PERC = DAY_DELAYED_TFC_15/DAY_TFC,
    DY_DELAYED_TFC_15_PY = DAY_DELAYED_TFC_15_PREV_YEAR / DAY_TFC_PREV_YEAR,
    DY_DELAYED_TFC_15_2019 = DAY_DELAYED_TFC_15_2019 / DAY_TFC_2019,
    DY_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = if_else(DY_DELAYED_TFC_15_PY == 0, NA , DY_DELAYED_TFC_15_PERC  / DY_DELAYED_TFC_15_PY - 1),
    DY_DELAYED_TFC_15_PERC_DIF_2019 = if_else(DY_DELAYED_TFC_15_2019 == 0, NA , DY_DELAYED_TFC_15_PERC  / DY_DELAYED_TFC_15_2019 - 1),


    WK_DELAYED_TFC_15_PERC = AVG_DELAYED_TFC_15_ROLLING_WEEK / AVG_TFC_ROLLING_WEEK,,
    WK_DELAYED_TFC_15_PY = AVG_DELAYED_TFC_15_ROLLING_WEEK_PREV_YEAR / AVG_TFC_ROLLING_WEEK_PREV_YEAR,
    WK_DELAYED_TFC_15_2019 = AVG_DELAYED_TFC_15_ROLLING_WEEK_2019 / AVG_TFC_ROLLING_WEEK_2019,
    WK_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = if_else(WK_DELAYED_TFC_15_PY  == 0, NA , WK_DELAYED_TFC_15_PERC / WK_DELAYED_TFC_15_PY - 1),
    WK_DELAYED_TFC_15_PERC_DIF_2019 = if_else(WK_DELAYED_TFC_15_2019 == 0, NA , WK_DELAYED_TFC_15_PERC / WK_DELAYED_TFC_15_2019 - 1),


    Y2D_DELAYED_TFC_15_PERC = Y2D_DELAYED_TFC_15_YEAR / Y2D_TFC_YEAR,
    Y2D_DELAYED_TFC_15_PY = Y2D_AVG_DELAYED_TFC_15_PREV_YEAR / Y2D_AVG_TFC_PREV_YEAR,
    Y2D_DELAYED_TFC_15_2019 = Y2D_AVG_DELAYED_TFC_15_2019 / Y2D_AVG_TFC_2019,
    Y2D_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = if_else(Y2D_DELAYED_TFC_15_PY == 0, NA , Y2D_DELAYED_TFC_15_PERC / Y2D_DELAYED_TFC_15_PY -1),
    Y2D_DELAYED_TFC_15_PERC_DIF_2019 = if_else(Y2D_DELAYED_TFC_15_2019 == 0, NA , Y2D_DELAYED_TFC_15_PERC / Y2D_DELAYED_TFC_15_2019 -1),



  ) %>%
  select(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE,

    #delay

    DY_DLY = DAY_DLY,
    DY_DLY_DIF_PREV_YEAR_PERC,
    DY_DLY_DIF_2019_PERC,

    WK_DLY_AVG_ROLLING = AVG_DLY_ROLLING_WEEK,
    WK_DLY_DIF_PREV_YEAR_PERC,
    WK_DLY_DIF_2019_PERC,

    Y2D_DLY_AVG = Y2D_AVG_DLY_YEAR,
    Y2D_DLY_DIF_PREV_YEAR_PERC,
    Y2D_DLY_DIF_2019_PERC,

    #delay per flight
    DAY_DLY_FLT,
    DY_DLY_FLT_DIF_PREV_YEAR_PERC,
    DY_DLY_FLT_DIF_2019_PERC,

    WK_DLY_FLT,
    WK_DLY_FLT_DIF_PREV_YEAR_PERC,
    WK_DLY_FLT_DIF_2019_PERC,

    Y2D_DLY_FLT,
    Y2D_DLY_FLT_DIF_PREV_YEAR_PERC,
    Y2D_DLY_FLT_DIF_2019_PERC,


    #% of delayed flights
    DY_DELAYED_TFC_PERC,
    DY_DELAYED_TFC_PERC_DIF_PREV_YEAR,
    DY_DELAYED_TFC_PERC_DIF_2019,

    WK_DELAYED_TFC_PERC,
    WK_DELAYED_TFC_PERC_DIF_PREV_YEAR,
    WK_DELAYED_TFC_PERC_DIF_2019,

    Y2D_DELAYED_TFC_PERC,
    Y2D_DELAYED_TFC_PERC_DIF_PREV_YEAR,
    Y2D_DELAYED_TFC_PERC_DIF_2019,

    #% of delayed flights >15'
    DY_DELAYED_TFC_15_PERC,
    DY_DELAYED_TFC_15_PERC_DIF_PREV_YEAR,
    DY_DELAYED_TFC_15_PERC_DIF_2019,

    WK_DELAYED_TFC_15_PERC,
    WK_DELAYED_TFC_15_PERC_DIF_PREV_YEAR,
    WK_DELAYED_TFC_15_PERC_DIF_2019,

    Y2D_DELAYED_TFC_15_PERC,
    Y2D_DELAYED_TFC_15_PERC_DIF_PREV_YEAR,
    Y2D_DELAYED_TFC_15_PERC_DIF_2019
  ) %>%
  arrange(ARP_CODE,
          ARP_NAME)





#### Punctuality data ----

#querying the data in SQL
apt_punct_raw <- export_query(query_ap_punct) %>%
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
    DEP_SCHEDULE_FLIGHT = sum( DEP_SCHEDULE_FLIGHT , na.rm = TRUE)
  ) %>%
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
  summarise (Y2D_ARR_PUN = sum(ARR_PUNCTUAL_FLIGHTS, na.rm=TRUE) / sum(ARR_SCHEDULE_FLIGHT, na.rm=TRUE) * 100,
             Y2D_DEP_PUN = sum(DEP_PUNCTUAL_FLIGHTS, na.rm=TRUE) / sum(DEP_SCHEDULE_FLIGHT, na.rm=TRUE) * 100) %>%
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
apt_punct_for_json <- merge(apt_punct_d_w, apt_punct_y2d, by= c("ARP_CODE", "ARP_NAME"))

#### Join strings and save  ----
apt_json_app_j <- apt_icao %>% arrange(apt_name)
apt_json_app_j$apt_traffic <- select(arrange(apt_traffic_for_json, ARP_NAME), -c(ARP_NAME, ARP_CODE))
apt_json_app_j$apt_delay <- select(arrange(apt_delay_for_json, ARP_NAME), -c(ARP_NAME, ARP_CODE))
apt_json_app_j$apt_punct <- select(arrange(apt_punct_for_json, ARP_NAME), -c(ARP_NAME, ARP_CODE))

#ao_json_app_j$ao_billed <- select(arrange(ao_billed_for_json, AO_GRP_NAME), -c(AO_GRP_CODE, AO_GRP_NAME))
#ao_json_app_j$ao_co2 <- select(arrange(ao_co2_for_json, AO_GRP_NAME), -c(AO_GRP_CODE, AO_GRP_NAME))

update_day <- floor_date(lubridate::now(), unit = "days") %>%
  as_tibble() %>%
  rename(APP_UPDATE = 1)

apt_json_app_j$apt_update <- update_day

apt_json_app_j <- apt_json_app_j %>%   group_by(apt_icao_code, apt_name)

apt_json_app <- apt_json_app_j %>% toJSON(., pretty = TRUE)

save_json(apt_json_app, "ap_json_app")



# last line of this section
#save_json(apt_json_app, "apt_json_app", archive_file = FALSE)



# ____________________________________________________________________________________________
#
#    APT ranking tables -----
#
# ____________________________________________________________________________________________

## TRAFFIC ----
### Aircraft operator ----
#### day ----



# last line of this seciton
#save_json(ap_ao_traffic_j, "ap_ao_traffic", archive_file = FALSE)




# ____________________________________________________________________________________________
#
#    APT Group graphs  -----
#
# ____________________________________________________________________________________________

## TRAFFIC ----
### 7-day traffic avg ----
apt_traffic_evo <- apt_traffic_delay_data  %>%
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



column_names <- c('ARP_CODE', 'ARP_NAME', 'FLIGHT_DATE', data_day_year, data_day_year-1, 2020, 2019)
colnames(apt_traffic_evo) <- column_names

### nest data
apt_traffic_evo_long <- apt_traffic_evo %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME,FLIGHT_DATE), names_to = 'year', values_to = 'RWK_AVG_TFC') %>%
  group_by(ARP_CODE, ARP_NAME,FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")


apt_traffic_evo_j <- apt_traffic_evo_long %>% toJSON(., pretty = TRUE)

save_json(apt_traffic_evo_j, "ap_traffic_evo_chart_daily")


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
  right_join(apt_icao, join_by("ARP_CODE"=="apt_icao_code")) %>%
  select(
    ARP_CODE,
    ARP_NAME,
    DAY_DATE,

    DEP_PUN_WK,
    ARR_PUN_WK,
    OP_FLT_WK
  )

column_names <- c('ARP_CODE',
                  'ARP_NAME',
                  'FLIGHT_DATE',
                  "Departure punct.",
                  "Arrival punct.",
                  "Operated schedules"
                  )

colnames(apt_punct_evo) <- column_names

### nest data
apt_punct_evo_long <- apt_punct_evo %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value') %>%
  group_by(ARP_CODE, ARP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")


apt_punct_evo_j <- apt_punct_evo_long %>% toJSON(., pretty = TRUE)

save_json(apt_punct_evo_j, "ap_punct_evo_chart")


## DELAY ----
### Delay category ----

apt_delay_cause_data <-  apt_delay_data_ %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
  mutate(
    TDM = DAY_DLY,
    TDM_G = TDM_ARP_ARR_G,
    TDM_CS = TDM_ARP_ARR_CS,
    TDM_IT = TDM_ARP_ARR_IT ,
    TDM_WD = TDM_ARP_ARR_WD,
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
  )

column_names <- c(
  "ARP_CODE",
  "ARP_NAME",
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
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

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
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

apt_delay_cause_day_long <- cbind(apt_delay_value_day_long, apt_delay_share_day_long) %>%
  select(-name) %>%
  group_by(ARP_CODE, ARP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

# for consistency with v1 we use the word category in the name files... should have been cause
apt_delay_cause_evo_dy_j <- apt_delay_cause_day_long %>% toJSON(., pretty = TRUE)

save_json(apt_delay_cause_evo_dy_j, "ap_delay_category_evo_chart_dy")





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
    WK_TDM_G = sum(TDM_G),
    WK_TDM_CS = sum(TDM_CS),
    WK_TDM_IT = sum(TDM_IT),
    WK_TDM_WD = sum(TDM_WD),
    WK_TDM_NOCSGITWD = sum(TDM_NOCSGITWD),
    WK_TDM = sum(TDM)
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
  )


colnames(apt_delay_cause_wk) <- column_names


### nest data
apt_delay_value_wk_long <- apt_delay_cause_wk %>%
  select(-c(share_aerodrome_capacity,
            share_capacity_staffing_atc,
            share_disruptions_atc,
            share_weather,
            share_other)
  ) %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

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
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

apt_delay_cause_wk_long <- cbind(apt_delay_value_wk_long, apt_delay_share_wk_long) %>%
  select(-name) %>%
  group_by(ARP_CODE, ARP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")


# for consistency with v1 we use the word category in the name files... should have been cause
apt_delay_cause_evo_wk_j <- apt_delay_cause_wk_long %>% toJSON(., pretty = TRUE)

save_json(apt_delay_cause_evo_wk_j, "ap_delay_category_evo_chart_wk")


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
  )

colnames(apt_delay_cause_y2d) <- column_names

### nest data
apt_delay_value_y2d_long <- apt_delay_cause_y2d %>%
  select(-c(share_aerodrome_capacity,
            share_capacity_staffing_atc,
            share_disruptions_atc,
            share_weather,
            share_other)
  ) %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

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
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

apt_delay_cause_y2d_long <- cbind(apt_delay_value_y2d_long, apt_delay_share_y2d_long) %>%
  select(-name) %>%
  group_by(ARP_CODE, ARP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")


# for consistency with v1 we use the word category in the name files... should have been cause
apt_delay_cause_evo_y2d_j <- apt_delay_cause_y2d_long %>% toJSON(., pretty = TRUE)

save_json(apt_delay_cause_evo_y2d_j, "ap_delay_category_evo_chart_y2d")


### Delay type ----
apt_delay_type_data <- apt_delay_data_ %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
  group_by(ARP_CODE) %>%
  arrange(ARP_CODE, FLIGHT_DATE) %>%
  mutate(
    DAY_TFC,
    DY_DLY_FLT = if_else(DAY_TFC == 0, 0, DAY_DLY/DAY_TFC),
    #DY_DLY_FLT_ERT = if_else(DAY_TFC == 0, 0, DAY_ERT_DLY/DAY_TFC),
    #DY_DLY_FLT_APT = if_else(DAY_TFC == 0, 0, DAY_ARP_DLY/DAY_TFC),
    DY_DLY_FLT_PREV_YEAR = if_else(DAY_TFC_PREV_YEAR == 0, 0, DAY_DLY_PREV_YEAR/DAY_TFC_PREV_YEAR),

    #DY_SHARE_DLY_FLT_ERT = if_else(DY_DLY_FLT == 0, 0, DY_DLY_FLT_ERT/DY_DLY_FLT),
    #DY_SHARE_DLY_FLT_APT = if_else(DY_DLY_FLT == 0, 0, DY_DLY_FLT_APT/DY_DLY_FLT),

    RWK_DLY_FLT = if_else(AVG_TFC_ROLLING_WEEK == 0, 0, AVG_DLY_ROLLING_WEEK/AVG_TFC_ROLLING_WEEK),
    #RWK_DLY_FLT_ERT = if_else(AVG_TFC_ROLLING_WEEK == 0, 0, AVG_ERT_DLY_ROLLING_WEEK/AVG_TFC_ROLLING_WEEK),
    #RWK_DLY_FLT_APT = if_else(AVG_TFC_ROLLING_WEEK == 0, 0, AVG_ARP_DLY_ROLLING_WEEK/AVG_TFC_ROLLING_WEEK),
    RWK_DLY_FLT_PREV_YEAR = if_else(AVG_TFC_ROLLING_WEEK_PREV_YEAR == 0, 0, AVG_DLY_ROLLING_WEEK_PREV_YEAR / AVG_TFC_ROLLING_WEEK_PREV_YEAR),

    Y2D_DLY_FLT = if_else(Y2D_TFC_YEAR == 0, 0, Y2D_DLY_YEAR/Y2D_TFC_YEAR),
    #Y2D_DLY_FLT_ERT = if_else(Y2D_TFC_YEAR == 0, 0, Y2D_ERT_DLY_YEAR/Y2D_TFC_YEAR),
    #Y2D_DLY_FLT_APT = if_else(Y2D_TFC_YEAR == 0, 0, Y2D_ARP_DLY_YEAR/Y2D_TFC_YEAR),
    Y2D_DLY_FLT_PREV_YEAR = if_else(Y2D_TFC_YEAR_PREV_YEAR == 0, 0, Y2D_DLY_YEAR_PREV_YEAR / Y2D_TFC_YEAR_PREV_YEAR)
  ) %>%
  ungroup()  %>%
  filter(FLIGHT_DATE >= as.Date(paste0("01-01-", data_day_year), format = "%d-%m-%Y")) %>%
  arrange(ARP_CODE, FLIGHT_DATE)

#### day ----
apt_delay_type_day <- apt_delay_type_data %>%
  filter(FLIGHT_DATE == min(max(FLIGHT_DATE),
                            data_day_date,
                            na.rm = TRUE)) %>%
  select(ARP_CODE,
         ARP_NAME,
         FLIGHT_DATE,
         DY_DLY_FLT,
         #DY_DLY_FLT_ERT,
         #DY_DLY_FLT_APT,
         DY_DLY_FLT_PREV_YEAR,
         #DY_SHARE_DLY_FLT_ERT,
         #DY_SHARE_DLY_FLT_APT
  )

column_names <- c(
  "ARP_CODE",
  "ARP_NAME",
  "FLIGHT_DATE",
  "Arrival ATFM delay/flight",
  #"En-route ATFM delay/flight",
  #"Airport ATFM delay/flight",
  paste0("Arrival ATFM delay/flight ", data_day_year - 1)
  #,
  #"share_en_route",
  #"share_airport"
)

colnames(apt_delay_type_day) <- column_names

### nest data
apt_delay_type_value_day_long <- apt_delay_type_day %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

apt_delay_type_share_day_long <- apt_delay_type_day %>%
  select(-c(paste0("Arrival ATFM delay/flight ", data_day_year - 1)
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

apt_delay_type_day_long <- cbind(apt_delay_type_value_day_long, apt_delay_type_share_day_long) %>%
  select(-name) %>%
  group_by(ARP_CODE, ARP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

apt_delay_type_evo_dy_j <- apt_delay_type_day_long %>% toJSON(., pretty = TRUE)

save_json(apt_delay_type_evo_dy_j, "ap_delay_flt_type_evo_chart_dy")


#### week ----
apt_delay_type_wk <- apt_delay_type_data %>%
  filter(FLIGHT_DATE >= min(max(FLIGHT_DATE), data_day_date, na.rm  = TRUE) + lubridate::days(-6),
         FLIGHT_DATE <= min(max(FLIGHT_DATE), data_day_date, na.rm  = TRUE)
  ) %>%
  select(ARP_CODE,
         ARP_NAME,
         FLIGHT_DATE,
         DY_DLY_FLT,
         #DY_DLY_FLT_ERT,
         #DY_DLY_FLT_APT,
         DY_DLY_FLT_PREV_YEAR,
         DAY_TFC,
         DAY_DLY
         #,
         #DAY_ERT_DLY,
         #DAY_ARP_DLY
  ) %>%
  group_by(ARP_CODE) %>%
  reframe(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE,
    DY_DLY_FLT,
    #DY_DLY_FLT_ERT,
    #DY_DLY_FLT_APT,
    DY_DLY_FLT_PREV_YEAR,

    WK_TFC = sum(DAY_TFC),
    WK_DLY = sum(DAY_DLY),
    #WK_DLY_ERT = sum(DAY_ERT_DLY),
    #WK_DLY_APT = sum(DAY_ARP_DLY),

    WK_DLY_FLT = if_else(WK_TFC == 0, 0, WK_DLY/WK_TFC)
    #,
    #WK_DLY_FLT_ERT = if_else(WK_TFC == 0, 0, WK_DLY_ERT/WK_TFC),
    #WK_DLY_FLT_APT = if_else(WK_TFC == 0, 0, WK_DLY_APT/WK_TFC),

    #WK_SHARE_DLY_FLT_ERT = if_else(WK_DLY_FLT == 0, 0, WK_DLY_FLT_ERT/WK_DLY_FLT),
    #WK_SHARE_DLY_FLT_APT = if_else(WK_DLY_FLT == 0, 0, WK_DLY_FLT_APT/WK_DLY_FLT)
  ) %>%
  select(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE,
    DY_DLY_FLT,
    #DY_DLY_FLT_ERT,
    #DY_DLY_FLT_APT,
    DY_DLY_FLT_PREV_YEAR
    #,
    #WK_SHARE_DLY_FLT_ERT,
    #WK_SHARE_DLY_FLT_APT
  )


colnames(apt_delay_type_wk) <- column_names

### nest data
apt_delay_type_value_wk_long <- apt_delay_type_wk  %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

apt_delay_type_share_wk_long <- apt_delay_type_wk %>%
  select(-c(paste0("Arrival ATFM delay/flight ", data_day_year - 1)))  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(ARP_CODE,ARP_NAME, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

apt_delay_type_wk_long <- cbind(apt_delay_type_value_wk_long, apt_delay_type_share_wk_long) %>%
  select(-name) %>%
  group_by(ARP_CODE, ARP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

apt_delay_type_evo_wk_j <- apt_delay_type_wk_long %>% toJSON(., pretty = TRUE)

save_json(apt_delay_type_evo_wk_j, "ap_delay_flt_type_evo_chart_wk")

#### y2d ----
apt_delay_type_y2d <- apt_delay_type_data %>%
  filter(FLIGHT_DATE <= data_day_date) %>%
  group_by(ARP_CODE) %>%
  reframe(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE,
    RWK_DLY_FLT,
    #RWK_DLY_FLT_ERT,
    #RWK_DLY_FLT_APT,
    RWK_DLY_FLT_PREV_YEAR,
    Y2D_TFC = sum(DAY_TFC),
    Y2D_DLY_FLT = if_else(Y2D_TFC == 0, 0, sum(DAY_DLY)/Y2D_TFC)
    #,
    #Y2D_DLY_FLT_ERT = if_else(Y2D_TFC == 0, 0, sum(DAY_ERT_DLY)/Y2D_TFC),
    #Y2D_DLY_FLT_APT = if_else(Y2D_TFC == 0, 0, sum(DAY_ARP_DLY)/Y2D_TFC),
  ) %>%
  #mutate(
  #  Y2D_SHARE_DLY_FLT_ERT = if_else(Y2D_DLY_FLT == 0, 0, Y2D_DLY_FLT_ERT / Y2D_DLY_FLT),
  #  Y2D_SHARE_DLY_FLT_APT = if_else(Y2D_DLY_FLT == 0, 0, Y2D_DLY_FLT_APT / Y2D_DLY_FLT)
  #) %>%
  select(ARP_CODE,
         ARP_NAME,
         FLIGHT_DATE,
         RWK_DLY_FLT,
         #RWK_DLY_FLT_ERT,
         #RWK_DLY_FLT_APT,
         RWK_DLY_FLT_PREV_YEAR
         #,
         #Y2D_SHARE_DLY_FLT_ERT,
         #Y2D_SHARE_DLY_FLT_APT
  )

colnames(apt_delay_type_y2d) <- column_names

### nest data
apt_delay_type_value_y2d_long <- apt_delay_type_y2d %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

apt_delay_type_share_y2d_long <- apt_delay_type_y2d %>%
  select(-c(paste0("Arrival ATFM delay/flight ", data_day_year - 1)))  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

apt_delay_type_y2d_long <- cbind(apt_delay_type_value_y2d_long, apt_delay_type_share_y2d_long) %>%
  select(-name) %>%
  group_by(ARP_CODE, ARP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

apt_delay_type_evo_y2d_j <- apt_delay_type_y2d_long %>% toJSON(., pretty = TRUE)

save_json(apt_delay_type_evo_y2d_j, "ap_delay_flt_type_evo_chart_y2d")


### DELAY Share ----
### 7-day % of delay per flight ----
apt_delay_flt_evo <- apt_delay_data_  %>%
  filter(FLIGHT_DATE <= min(data_day_date,
                            max(FLIGHT_DATE, na.rm = TRUE),
                            na.rm = TRUE)) %>%
  mutate(RWK_DLY_FLT = rollsum(DAY_DLY / DAY_TFC, 7, fill = NA, align = "right") / 7,
         RWK_DLY_FLT_PREV_YEAR =  rollsum(DAY_DLY_PREV_YEAR / DAY_TFC_PREV_YEAR, 7, fill = NA, align = "right") / 7) %>%
  select(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE,
    RWK_DLY_FLT,
    RWK_DLY_FLT_PREV_YEAR
  ) %>%
  filter(FLIGHT_DATE >= as.Date(paste0("01-01-", data_day_year), format = "%d-%m-%Y"),
         FLIGHT_DATE <= as.Date(paste0("31-12-", data_day_year), format = "%d-%m-%Y"))





column_names <- c('ARP_CODE',
                  'ARP_NAME',
                  'FLIGHT_DATE',
                  paste0('Total ATFM delay/flight ', data_day_year),
                  paste0('Total ATFM delay/flight ', data_day_year -1)
)

colnames(apt_delay_flt_evo) <- column_names

### nest data
apt_delay_flt_evo_long <- apt_delay_flt_evo %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'year', values_to = 'daio') %>%
  group_by(ARP_CODE, ARP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

###convert to json and save
apt_delay_flt_evo_j <- apt_delay_flt_evo_long %>% toJSON(., pretty = TRUE)

save_json(apt_delay_flt_evo_j, "ap_delay_per_flight_evo_chart_daily")


### 7-day % of delayed flights ----
apt_delayed_flights_evo <- apt_delay_data_  %>%
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
         FLIGHT_DATE <= as.Date(paste0("31-12-", data_day_year), format = "%d-%m-%Y"))

column_names <- c('ARP_CODE',
                  'ARP_NAME',
                  'FLIGHT_DATE',
                  paste0('% of delayed flights ', data_day_year),
                  paste0('% of delayed flights ', data_day_year -1),
                  paste0("% of delayed flights >15' ", data_day_year),
                  paste0("% of delayed flights >15' ", data_day_year -1)
)

colnames(apt_delayed_flights_evo) <- column_names

### nest data
apt_delayed_flights_evo_long <- apt_delayed_flights_evo %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'year', values_to = 'daio') %>%
  group_by(ARP_CODE, ARP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")



apt_delayed_flights_evo_j <- apt_delayed_flights_evo_long %>% toJSON(., pretty = TRUE)

save_json(apt_delayed_flights_evo_j, "ap_delayed_flights_evo_chart_daily")
