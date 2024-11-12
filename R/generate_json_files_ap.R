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
#data_day_text <- "20241104"

ap_json_app <-""

# ____________________________________________________________________________________________
#
#    APT landing page -----
#
# ____________________________________________________________________________________________



#### Traffic data ----

#reading the traffic sheet
apt_traffic_delay_data <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(ap_base_file),
    start = ap_base_dir),
  sheet = "apt_traffic",
  range = cell_limits(c(1, 1), c(NA, NA))) %>%
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
    DAY_DEP_ARR, #arrival and departure traffic
    DAY_DEP,
    DAY_ARR,
    DAY_DEP_ARR_DIF_PREV_YEAR_PERC,
    DAY_DEP_ARR_DIF_2019_PERC,
    RWK_AVG_DEP_ARR,
    RWK_AVG_DEP_ARR_PREV_YEAR,
    #DIF_WEEK_PREV_YEAR_PERC,
    RWK_DEP_ARR_DIF_2019_PERC,
    Y2D_DEP_ARR_YEAR,
    Y2D_AVG_DEP_ARR_YEAR,
    Y2D_DEP_ARR_DIF_PREV_YEAR_PERC,
    Y2D_DEP_ARR_DIF_2019_PERC
  ) %>%
  rename(
    APT_NAME = ARP_NAME,
    APT_CODE = ARP_CODE,
    DY_TFC = DAY_DEP_ARR,
    DY_TFC_DIF_PREV_YEAR_PERC = DAY_DEP_ARR_DIF_PREV_YEAR_PERC,
    DY_TFC_DIF_2019_PERC = DAY_DEP_ARR_DIF_2019_PERC,
    WK_TFC_AVG_ROLLING = RWK_AVG_DEP_ARR,
    WK_TFC_DIF_PREV_YEAR_PERC = RWK_AVG_DEP_ARR_PREV_YEAR,
    WK_TFC_DIF_2019_PERC = RWK_DEP_ARR_DIF_2019_PERC,
    Y2D_TFC = Y2D_DEP_ARR_YEAR,
    Y2D_TFC_AVG = Y2D_AVG_DEP_ARR_YEAR
  )

#### Delay data ----


#library(zoo)

#arrival delay
#reading the delay cause sheet
apt_delay_data <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(ap_base_file),
    start = ap_base_dir),
  sheet = "apt_delay_cause",
  range = cell_limits(c(1, 1), c(NA, NA))) %>%
  as_tibble() %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

#adding columns to the delay data

#adding rolling week average and year-to-date(Y2D)
apt_delay_data_ <- apt_delay_data %>% 
  arrange(FLIGHT_DATE) %>%
  group_by(ARP_CODE) %>%
  mutate(AVG_DLY_ROLLING_WEEK = rollmean( TDM_ARP , 7, align = "right", fill = NA),
         AVG_TFC_ROLLING_WEEK = rollmean( DAY_ARR , 7, align = "right", fill = NA)) %>%
  group_by( year( FLIGHT_DATE), ARP_CODE) %>%
  mutate(Y2D_DLY_YEAR = cumsum( coalesce( TDM_ARP , 0)),
         Y2D_TFC_YEAR = cumsum( coalesce( DAY_ARR , 0)),
         Y2D_AVG_DLY = cummean( coalesce( TDM_ARP , 0)),
         Y2D_AVG_TFC = cummean( coalesce( DAY_ARR , 0))) %>%
  ungroup()


#adding previous year and 2019 values
apt_delay_last_day <- apt_delay_data_ %>%
  #getting the latest date's traffic data
  filter(FLIGHT_DATE == min(data_day_date,
                            max(FLIGHT_DATE),
                            na.rm = TRUE)
  ) %>%
  #2019, and previous year values
  mutate(DATE_PREV_YEAR = FLIGHT_DATE-(1)*364 - round(1 / 4) * 7,
         DATE_2019 = FLIGHT_DATE-(year ( FLIGHT_DATE ) - 2019)*364 - round((year ( FLIGHT_DATE) - 2019) / 4) * 7) %>%
  arrange(ARP_CODE, FLIGHT_DATE) %>%
  #obtaining data for previous year
  left_join(apt_delay_data_ %>% select(ARP_CODE, 
                                       FLIGHT_DATE, 
                                       DAY_ARR, TDM_ARP, 
                                       AVG_DLY_ROLLING_WEEK, 
                                       AVG_TFC_ROLLING_WEEK, 
                                       Y2D_DLY_YEAR, Y2D_TFC_YEAR, 
                                       Y2D_AVG_DLY, 
                                       Y2D_AVG_TFC),
            by = join_by(DATE_PREV_YEAR == FLIGHT_DATE, ARP_CODE == ARP_CODE),
            suffix = c("", "_PREV_YEAR"))  %>%
  #obtaining data for 2019
  left_join(apt_delay_data_ %>% select(ARP_CODE, 
                                       FLIGHT_DATE, DAY_ARR, 
                                       TDM_ARP, 
                                       AVG_DLY_ROLLING_WEEK, 
                                       AVG_TFC_ROLLING_WEEK, 
                                       Y2D_DLY_YEAR, 
                                       Y2D_TFC_YEAR, 
                                       Y2D_AVG_DLY, 
                                       Y2D_AVG_TFC),
            by = join_by(DATE_2019 == FLIGHT_DATE, ARP_CODE == ARP_CODE),
            suffix = c("", "_2019"))


#creating, selecting and renaming columns
apt_delay_for_json  <- apt_delay_last_day %>%
  mutate(
    #day totals
    DAY_DLY = TDM_ARP,
    DAY_DLY_FLT = TDM_ARP / DAY_ARR,
    DAY_DLY_DIF_PREV_YEAR_PERC = if_else(
      TDM_ARP_PREV_YEAR == 0, NA , DAY_DLY / TDM_ARP_PREV_YEAR - 1
      ),
    DAY_DLY_DIF_2019_PERC = if_else(
      TDM_ARP_2019 == 0, NA , DAY_DLY / TDM_ARP_2019 - 1
      ),
    DAY_DLY_FLT_PY = TDM_ARP_PREV_YEAR / DAY_ARR_PREV_YEAR,
    DAY_DLY_FLT_2019 = TDM_ARP_2019 / DAY_ARR_2019,
    DAY_DLY_FLT_DIF_PY_PERC = if_else(
      DAY_DLY_FLT_PY == 0, NA , DAY_DLY_FLT / DAY_DLY_FLT_PY - 1
    ),
    DAY_DLY_FLT_DIF_2019_PERC = if_else(
      DAY_DLY_FLT_2019 == 0, NA , DAY_DLY_FLT / DAY_DLY_FLT_2019 - 1
    ),
    #week average
    WEEK_DLY_FLT = AVG_DLY_ROLLING_WEEK / AVG_TFC_ROLLING_WEEK,
    DIF_DLY_ROLLING_WEEK_PREV_YEAR_PERC = if_else(
      AVG_DLY_ROLLING_WEEK_PREV_YEAR == 0, NA , AVG_DLY_ROLLING_WEEK / AVG_DLY_ROLLING_WEEK_PREV_YEAR - 1),
    DIF_DLY_ROLLING_WEEK_2019_PERC = if_else(
      AVG_DLY_ROLLING_WEEK_2019 == 0 , NA, AVG_DLY_ROLLING_WEEK / AVG_DLY_ROLLING_WEEK_2019 - 1),
    WEEK_DLY_FLT_PY = AVG_DLY_ROLLING_WEEK_PREV_YEAR / AVG_TFC_ROLLING_WEEK_PREV_YEAR,
    WEEK_DLY_FLT_2019 = AVG_DLY_ROLLING_WEEK_2019 / AVG_TFC_ROLLING_WEEK_2019,
    WEEK_DLY_FLT_DIF_PY_PERC = if_else(
      WEEK_DLY_FLT_PY == 0, NA , WEEK_DLY_FLT / WEEK_DLY_FLT_PY - 1
    ),
    WEEK_DLY_FLT_DIF_2019_PERC = if_else(
      WEEK_DLY_FLT_2019 == 0, NA , WEEK_DLY_FLT / WEEK_DLY_FLT_2019 - 1
    ),
    #year to date
    Y2D_AVG_DLY_YEAR = Y2D_AVG_DLY,
    Y2D_DLY_FLT = Y2D_DLY_YEAR / Y2D_TFC_YEAR,
    Y2D_DLY_DIF_PREV_YEAR_PERC = if_else(
      Y2D_DLY_YEAR_PREV_YEAR == 0 , NA , Y2D_DLY_YEAR / Y2D_DLY_YEAR_PREV_YEAR - 1
      ),
    Y2D_DLY_DIF_2019_PERC = if_else(
      Y2D_DLY_YEAR_2019 == 0 , NA , Y2D_DLY_YEAR / Y2D_DLY_YEAR_2019 - 1
      ),
    Y2D_DLY_FLT_PY = Y2D_AVG_DLY_PREV_YEAR / Y2D_AVG_TFC_PREV_YEAR,
    Y2D_DLY_FLT_2019 = Y2D_AVG_DLY_2019 / Y2D_AVG_TFC_2019,
    Y2D_DLY_FLT_DIF_PY_PERC = if_else(
      Y2D_DLY_FLT_PY == 0, NA , Y2D_DLY_FLT / Y2D_DLY_FLT_PY -1
    ),
    Y2D_DLY_FLT_DIF_2019_PERC = if_else(
      Y2D_DLY_FLT_2019 == 0, NA , Y2D_DLY_FLT / Y2D_DLY_FLT_2019 -1
    )
    
  ) %>%
  select(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE,
    
    DAY_DLY,
    DAY_DLY_DIF_PREV_YEAR_PERC,
    DAY_DLY_DIF_2019_PERC,
    DAY_DLY_FLT,
    DAY_DLY_FLT_DIF_PY_PERC,
    DAY_DLY_FLT_DIF_2019_PERC,
    
    AVG_DLY_ROLLING_WEEK,
    DIF_DLY_ROLLING_WEEK_PREV_YEAR_PERC,
    DIF_DLY_ROLLING_WEEK_2019_PERC,
    WEEK_DLY_FLT,
    WEEK_DLY_FLT_DIF_PY_PERC,
    WEEK_DLY_FLT_DIF_2019_PERC,
    
    Y2D_AVG_DLY_YEAR,
    Y2D_DLY_DIF_PREV_YEAR_PERC,
    Y2D_DLY_DIF_2019_PERC,
    Y2D_DLY_FLT,
    Y2D_DLY_FLT_DIF_PY_PERC,
    Y2D_DLY_FLT_DIF_2019_PERC
  ) %>%
  rename(
    DY_DLY = DAY_DLY,
    DY_DLY_DIF_PREV_YEAR_PERC = DAY_DLY_DIF_PREV_YEAR_PERC,
    DY_DLY_DIF_2019_PERC = DAY_DLY_DIF_2019_PERC,
    DY_DLY_FLT = DAY_DLY_FLT,
    DY_DLY_FLT_DIF_PREV_YEAR_PERC = DAY_DLY_FLT_DIF_PY_PERC,
    DY_DLY_FLT_DIF_2019_PERC = DAY_DLY_FLT_DIF_2019_PERC,
    
    WK_DLY_AVG_ROLLING = AVG_DLY_ROLLING_WEEK,
    WK_DLY_DIF_PREV_YEAR_PERC = DIF_DLY_ROLLING_WEEK_PREV_YEAR_PERC,
    WK_DLY_DIF_2019_PERC = DIF_DLY_ROLLING_WEEK_2019_PERC,
    WK_DLY_FLT = WEEK_DLY_FLT,
    WK_DLY_FLT_DIF_PREV_YEAR_PERC = WEEK_DLY_FLT_DIF_PY_PERC,
    WK_DLY_FLT_DIF_2019_PERC = WEEK_DLY_FLT_DIF_2019_PERC,
    
    Y2D_DLY_AVG = Y2D_AVG_DLY_YEAR,
    Y2D_DLY_FLT_DIF_PREV_YEAR_PERC = Y2D_DLY_FLT_DIF_PY_PERC
  ) %>%
  arrange(ARP_CODE,
          ARP_NAME)

#### Punctuality data ----


query <- "WITH
  --Getting the list of airports
  AP_LIST AS ( 
  SELECT distinct ICAO_CODE as icao_code
  FROM LDW_VDM.VIEW_FAC_PUNCTUALITY_AP_DAY
  order by ICAO_CODE
  ), 
  --creating a table with the airport codes and dates since 2019
  AP_DAY AS
  (SELECT
              a.icao_code,
              t.year,
              t.month,
              t.week,
              t.week_nb_year,
              t.day_type,
              t.day_of_week_nb AS day_of_week,
              t.day_date
      FROM AP_LIST a, pru_time_references t
      WHERE
         t.day_date >= to_date('24-12-2018','DD-MM-YYYY')
         AND t.day_date < trunc(sysdate))
    --Joining the airport dates table with the aiport punctuality table    
    SELECT a.*,
    AP_DESC, 
    DEP_PUNCTUALITY_PERCENTAGE ,
    DEPARTURE_FLIGHTS , 
    AVG_DEPARTURE_SCHEDULE_DELAY ,
    ARR_PUNCTUALITY_PERCENTAGE ,
    ARRIVAL_FLIGHTS , 
    AVG_ARRIVAL_SCHEDULE_DELAY , 
    MISSING_SCHEDULES_PERCENTAGE ,
    DEP_PUNCTUAL_FLIGHTS, 
    ARR_PUNCTUAL_FLIGHTS , 
    DEP_SCHED_DELAY , 
    ARR_SCHED_DELAY ,
    MISSING_SCHED_FLIGHTS , 
    DEP_SCHEDULE_FLIGHT , 
    ARR_SCHEDULE_FLIGHT , 
    DEP_FLIGHTS_NO_OVERFLIGHTS 
      FROM AP_DAY a
      left join LDW_VDM.VIEW_FAC_PUNCTUALITY_AP_DAY b 
      on a.ICAO_CODE = b.\"ICAO_CODE\" AND a.day_date = b.\"DATE\"
"

#querying the data in SQL
ap_punct_raw <- export_query(query) %>%
  as_tibble() %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

#selecting the date for which we have the latest traffic data
last_day_punct <-  min(max(ap_punct_raw$DAY_DATE),
                       data_day_date, na.rm = TRUE)
#selecting the year for which we have the latest traffic data
last_year_punct <- as.numeric(format(last_day_punct,'%Y'))

#preparing the data
ap_punct_data <- ap_punct_raw %>%
  group_by(ICAO_CODE, DAY_DATE) %>%
  summarise(
    ARR_PUNCTUAL_FLIGHTS = sum( ARR_PUNCTUAL_FLIGHTS , na.rm = TRUE),
    ARR_SCHEDULE_FLIGHT = sum( ARR_SCHEDULE_FLIGHT , na.rm = TRUE),
    DEP_PUNCTUAL_FLIGHTS = sum( DEP_PUNCTUAL_FLIGHTS , na.rm = TRUE),
    DEP_SCHEDULE_FLIGHT = sum( DEP_SCHEDULE_FLIGHT , na.rm = TRUE)
  ) %>% 
  arrange( ICAO_CODE, DAY_DATE ) %>%
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
    DAY_ARR_PUNCT_2019 =if_else(
      YEAR == last_year_punct , lag( DAY_ARR_PUNCT ,  364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7), NA 
      ),
    #2019
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

#filtering the data and selecting aqnd renaming columns
ap_punct_d_w <- ap_punct_data %>%
  filter (DAY_DATE == last_day_punct) %>%
  select(
    ICAO_CODE,
    DAY_DATE,
    DAY_ARR_PUNCT,
    DAY_DEP_PUNCT,
    DAY_ARR_PUNCT_DIF_PY,
    DAY_DEP_PUNCT_DIF_PY,
    DAY_ARR_PUNCT_DIF_2019,
    DAY_DEP_PUNCT_DIF_2019,
    WEEK_ARR_PUNCT,
    WEEK_DEP_PUNCT,
    WEEK_ARR_PUNCT_DIF_PY,
    WEEK_DEP_PUNCT_DIF_PY,
    WEEK_ARR_PUNCT_DIF_2019,
    WEEK_DEP_PUNCT_DIF_2019
  ) %>%
  rename(
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

ap_punct_y2d <- ap_punct_raw %>%
  arrange(ICAO_CODE, DAY_DATE) %>%
  mutate(
    MONTH_DAY = as.numeric(format(DAY_DATE, format="%m%d"))
   # , ARR_PUNCTUAL_FLIGHTS = 0,  ## while the figures are not showable
   # DEP_PUNCTUAL_FLIGHTS = 0,  ## while the figures are not showable
  ) %>%
  filter(MONTH_DAY <= as.numeric(format(last_day_punct, format="%m%d"))) %>%
  group_by(ICAO_CODE, YEAR) %>%
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
  select(ICAO_CODE,
         Y2D_ARR_PUN,
         Y2D_DEP_PUN,
         Y2D_ARR_PUN_DIF_PREV_YEAR,
         Y2D_DEP_PUN_DIF_PREV_YEAR,
         Y2D_ARR_PUN_DIF_2019,
         Y2D_DEP_PUN_DIF_2019
  )

apt_punct_for_json <- merge(ap_punct_d_w, ap_punct_y2d, by= c("ICAO_CODE", "ICAO_CODE"))
#selecting only the airports in the excel list (i.e. the 40 main airports)
apt_punct_for_json <- ap_punct_for_json %>% 
  inner_join(apt_icao, by = join_by(ICAO_CODE==apt_icao_code),keep=FALSE) 

#### CO2 data ----

#### Join strings and save  ----
apt_json_app_j <- apt_icao %>% arrange(apt_name) 
apt_json_app_j$apt_traffic <- select(arrange(apt_traffic_for_json, APT_NAME), -c(APT_NAME, APT_CODE))
#ao_json_app_j$ao_delay <- select(arrange(ao_delay_for_json, AO_GRP_NAME), -c(AO_GRP_CODE, AO_GRP_NAME))
apt_json_app_j$apt_punct <- select(arrange(apt_punct_for_json, apt_name), -c(ICAO_CODE,  apt_name))

#ao_json_app_j$ao_billed <- select(arrange(ao_billed_for_json, AO_GRP_NAME), -c(AO_GRP_CODE, AO_GRP_NAME))
#ao_json_app_j$ao_co2 <- select(arrange(ao_co2_for_json, AO_GRP_NAME), -c(AO_GRP_CODE, AO_GRP_NAME))

update_day <- floor_date(lubridate::now(), unit = "days") %>%
  as_tibble() %>%
  rename(APP_UPDATE = 1)

apt_json_app_j$apt_update <- update_day

apt_json_app_j <- apt_json_app_j %>%   group_by(apt_icao_code, apt_name)

apt_json_app <- apt_json_app_j %>%
  toJSON(., pretty = TRUE)

save_json(apt_json_app, "ao_json_app")



# last line of this section
save_json(apt_json_app, "apt_json_app", archive_file = FALSE)



# ____________________________________________________________________________________________
#
#    APT ranking tables -----
#
# ____________________________________________________________________________________________

## TRAFFIC ----
### Aircraft operator ----
#### day ----



# last line of this seciton
save_json(ap_ao_traffic_j, "ap_ao_traffic", archive_file = FALSE)

