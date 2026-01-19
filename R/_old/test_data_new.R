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
source(here("..", "mobile-app", "R", "helpers.R")) # so it can be launched from the checkupdates script in grounded aircraft
source(here::here("..", "mobile-app", "R", "duckdb_functions.R"))

# queries ----
source(here("..", "mobile-app", "R", "queries_nw.R")) # so it can be launched from the checkupdates script in grounded aircraft


# parameters ----
# archive mode for past dates
# run this before the params script to set up the forecast params
if (exists("archive_mode") == FALSE) {archive_mode <- FALSE}
if (exists("data_day_date") == FALSE) {
  data_day_date <- lubridate::today(tzone = "") +  days(-1)
}
data_day_text <- data_day_date %>% format("%Y%m%d")
data_day_year <- as.numeric(format(data_day_date,'%Y'))

source(here("..", "mobile-app", "R", "params.R")) # so it can be launched from the checkupdates script in grounded aircraft

# dimensions ----
source(here::here("..", "mobile-app", "R", "dimensions.R"))


test <- function(data_day_date) {
  # data_day_date <- ymd(20251026)
  data_day_year <- year(data_day_date)
  #new dataframe ----
  mydatafile <- paste0("nw_traffic_delay_day_new.parquet")
  stakeholder <- substr(mydatafile, 1,2)
  
  nw_traffic_delay_data <- read_parquet(here(archive_dir_raw, stakeholder, mydatafile))
  
  df_new <- nw_traffic_delay_data %>%
    filter(FLIGHT_DATE <= data_day_date & YEAR == data_day_year) %>%
    select(
      FLIGHT_DATE,
      RWK_DLY_ARP_FLT,
      RWK_DLY_ARP_FLT_PREV_YEAR
    )
  
  nw_traffic_delay_last_day <- nw_traffic_delay_data %>%
    filter(FLIGHT_DATE == min(max(LAST_DATA_DAY),
                              data_day_date))
  
  y2d_delay_APT_flt <- nw_traffic_delay_last_day %>%
    select(Y2D_DLY_ARP_FLT,
           Y2D_DLY_ARP_FLT_PREV_YEAR
    )
  
  column_names <- c(
    "FLIGHT_DATE",
    paste0("En-route ATFM delay/flight ", data_day_year, " (", format(round(y2d_delay_APT_flt$Y2D_DLY_ARP_FLT,2), nsmall=2),"')"),
    paste0("En-route ATFM delay/flight ", data_day_year - 1, " (", format(round(y2d_delay_APT_flt$Y2D_DLY_ARP_FLT_PREV_YEAR,2), nsmall=2),"')")
  )
  
  colnames(df_new) <- column_names
  
  #old dataframe ----
  # traffic data
  nw_traffic_data <- export_query(query_nw_traffic_data(format(data_day_date, "%Y-%m-%d")))
  
  mydataframe <- "nw_delay_cause_day_raw"
  stakeholder <- "nw"
  
  nw_delay_cause <- read_parquet(here(archive_dir_raw, stakeholder, paste0(mydataframe, ".parquet"))) %>% as_tibble()
  
  
  # get data for last date
  nw_traffic_last_day <- nw_traffic_data %>%
    filter(FLIGHT_DATE == min(max(LAST_DATA_DAY),
                              data_day_date))
  
  nw_delay_data <- export_query(query_nw_delay_data(format(data_day_date, "%Y-%m-%d")))
  
  # calcs
  nw_delay_last_day <- nw_delay_data %>%
    mutate(FLIGHT_DATE = as.Date(FLIGHT_DATE)) %>%
    filter(FLIGHT_DATE == min(max(LAST_DATA_DAY),
                              data_day_date)) %>%
    mutate(
      DAY_DLY_FLT = DAY_DLY / nw_traffic_last_day$DAY_TFC,
      DAY_DLY_FLT_PY = DAY_DLY_PREV_YEAR / nw_traffic_last_day$DAY_TFC_PREV_YEAR,
      DAY_DLY_FLT_2019 = DAY_DLY_2019 / nw_traffic_last_day$DAY_TFC_2019,
      DAY_DLY_FLT_DIF_PY_PERC = if_else(
        DAY_DLY_FLT_PY == 0, NA, DAY_DLY_FLT / DAY_DLY_FLT_PY - 1
      ),
      DAY_DLY_FLT_DIF_2019_PERC = if_else(
        DAY_DLY_FLT_2019 == 0, NA, DAY_DLY_FLT / DAY_DLY_FLT_2019 - 1
      ),
      RWEEK_DLY_FLT = TOTAL_ROLLING_WEEK / nw_traffic_last_day$TOTAL_ROLLING_WEEK,
      RWEEK_DLY_FLT_PY = AVG_ROLLING_WEEK_PREV_YEAR / nw_traffic_last_day$AVG_ROLLING_WEEK_PREV_YEAR,
      RWEEK_DLY_FLT_2019 = AVG_ROLLING_WEEK_2019 / nw_traffic_last_day$AVG_ROLLING_WEEK_2019,
      RWEEK_DLY_FLT_DIF_PY_PERC = if_else(
        RWEEK_DLY_FLT_PY == 0, NA, RWEEK_DLY_FLT / RWEEK_DLY_FLT_PY - 1
      ),
      RWEEK_DLY_FLT_DIF_2019_PERC = if_else(
        RWEEK_DLY_FLT_2019 == 0, NA, RWEEK_DLY_FLT / RWEEK_DLY_FLT_2019 - 1
      ),
      Y2D_DLY_FLT = Y2D_DLY_YEAR / nw_traffic_last_day$Y2D_TFC_YEAR,
      Y2D_DLY_FLT_PY = Y2D_AVG_DLY_PREV_YEAR / nw_traffic_last_day$Y2D_AVG_TFC_PREV_YEAR,
      Y2D_DLY_FLT_2019 = Y2D_AVG_DLY_2019 / nw_traffic_last_day$Y2D_AVG_TFC_2019,
      Y2D_DLY_FLT_DIF_PY_PERC = if_else(
        Y2D_DLY_FLT_PY == 0, NA, Y2D_DLY_FLT / Y2D_DLY_FLT_PY - 1
      ),
      Y2D_DLY_FLT_DIF_2019_PERC = if_else(
        Y2D_DLY_FLT_2019 == 0, NA, Y2D_DLY_FLT / Y2D_DLY_FLT_2019 - 1
      )
    ) %>%
    #en-route delay
    mutate(
      DAY_DLY_ERT_SHARE = if_else(DAY_DLY == 0, 0, DAY_DLY_ERT/DAY_DLY),
      DAY_ERT_DIFF_PREV_YEAR_PERC = if_else(
        DAY_DLY_ERT_PREV_YEAR == 0, NA, DAY_DLY_ERT / DAY_DLY_ERT_PREV_YEAR - 1
      ),
      DAY_DLY_ERT_DIFF_2019_PERC = if_else(
        DAY_DLY_ERT_2019 == 0, NA, DAY_DLY_ERT / DAY_DLY_ERT_2019 - 1
      ),
      DAY_DLY_ERT_FLT = DAY_DLY_ERT / nw_traffic_last_day$DAY_TFC,
      DAY_DLY_ERT_FLT_PY = DAY_DLY_ERT_PREV_YEAR / nw_traffic_last_day$DAY_TFC_PREV_YEAR,
      DAY_DLY_ERT_FLT_2019 = DAY_DLY_ERT_2019 / nw_traffic_last_day$DAY_TFC_2019,
      DAY_DLY_ERT_FLT_DIF_PY_PERC = if_else(
        DAY_DLY_ERT_FLT_PY == 0, NA, DAY_DLY_ERT_FLT / DAY_DLY_ERT_FLT_PY - 1
      ),
      DAY_DLY_ERT_FLT_DIF_2019_PERC = if_else(
        DAY_DLY_ERT_FLT_2019 == 0, NA, DAY_DLY_ERT_FLT / DAY_DLY_ERT_FLT_2019 - 1
      ),
      ERT_DIF_WEEK_PREV_YEAR_PERC = if_else(
        AVG_ERT_ROLLING_WEEK_PREV_YEAR == 0, NA, AVG_ERT_ROLLING_WEEK/ AVG_ERT_ROLLING_WEEK_PREV_YEAR - 1
      ),
      ERT_DIF_ROLLING_WEEK_2019_PERC = if_else(
        AVG_ERT_ROLLING_WEEK_2019 == 0, NA, AVG_ERT_ROLLING_WEEK / AVG_ERT_ROLLING_WEEK_2019 - 1
      ),
      
      RWEEK_DLY_ERT_SHARE = if_else(AVG_ROLLING_WEEK == 0, 0, AVG_ERT_ROLLING_WEEK / AVG_ROLLING_WEEK),
      RWEEK_DLY_ERT_FLT = AVG_ERT_ROLLING_WEEK / nw_traffic_last_day$AVG_ROLLING_WEEK,
      RWEEK_DLY_ERT_FLT_PY = AVG_ERT_ROLLING_WEEK_PREV_YEAR / nw_traffic_last_day$AVG_ROLLING_WEEK_PREV_YEAR,
      RWEEK_DLY_ERT_FLT_2019 = AVG_ERT_ROLLING_WEEK_2019 / nw_traffic_last_day$AVG_ROLLING_WEEK_2019,
      RWEEK_DLY_ERT_FLT_DIF_PY_PERC = if_else(
        RWEEK_DLY_ERT_FLT_PY == 0, NA, RWEEK_DLY_ERT_FLT / RWEEK_DLY_ERT_FLT_PY - 1
      ),
      RWEEK_DLY_ERT_FLT_DIF_2019_PERC = if_else(
        RWEEK_DLY_ERT_FLT_2019 == 0, NA, RWEEK_DLY_ERT_FLT / RWEEK_DLY_ERT_FLT_2019 - 1
      ),
      
      Y2D_DLY_ERT_SHARE = if_else(Y2D_AVG_DLY_YEAR == 0, 0, Y2D_AVG_DLY_ERT_YEAR/Y2D_AVG_DLY_YEAR),
      Y2D_ERT_DIFF_PREV_YEAR_PERC = if_else(
        Y2D_AVG_DLY_ERT_PREV_YEAR == 0, NA, Y2D_AVG_DLY_ERT_YEAR/ Y2D_AVG_DLY_ERT_PREV_YEAR - 1
      ),
      Y2D_ERT_DIFF_2019_PERC = if_else(
        Y2D_AVG_DLY_ERT_2019 == 0, NA, Y2D_AVG_DLY_ERT_YEAR / Y2D_AVG_DLY_ERT_2019 - 1
      ),
      Y2D_DLY_ERT_FLT = Y2D_DLY_ERT_YEAR / nw_traffic_last_day$Y2D_TFC_YEAR,
      Y2D_DLY_ERT_FLT_PY = Y2D_AVG_DLY_ERT_PREV_YEAR / nw_traffic_last_day$Y2D_AVG_TFC_PREV_YEAR,
      Y2D_DLY_ERT_FLT_2019 = Y2D_AVG_DLY_ERT_2019 / nw_traffic_last_day$Y2D_AVG_TFC_2019,
      Y2D_DLY_ERT_FLT_DIF_PY_PERC = if_else(
        Y2D_DLY_ERT_FLT_PY == 0, NA, Y2D_DLY_ERT_FLT / Y2D_DLY_ERT_FLT_PY - 1
      ),
      Y2D_DLY_ERT_FLT_DIF_2019_PERC = if_else(
        Y2D_DLY_ERT_FLT_2019 == 0, NA, Y2D_DLY_ERT_FLT / Y2D_DLY_ERT_FLT_2019 - 1
      )
    ) %>%
    
    #airport delay
    mutate(
      DAY_DLY_APT_SHARE = if_else(DAY_DLY == 0, 0, DAY_DLY_APT/DAY_DLY),
      DAY_APT_DIFF_PREV_YEAR_PERC = if_else(
        DAY_DLY_APT_PREV_YEAR == 0, NA, DAY_DLY_APT / DAY_DLY_APT_PREV_YEAR - 1
      ),
      DAY_DLY_APT_DIFF_2019_PERC = if_else(
        DAY_DLY_APT_2019 == 0, NA, DAY_DLY_APT / DAY_DLY_APT_2019 - 1
      ),
      DAY_DLY_APT_FLT = DAY_DLY_APT / nw_traffic_last_day$DAY_TFC,
      DAY_DLY_APT_FLT_PY = DAY_DLY_APT_PREV_YEAR / nw_traffic_last_day$DAY_TFC_PREV_YEAR,
      DAY_DLY_APT_FLT_2019 = DAY_DLY_APT_2019 / nw_traffic_last_day$DAY_TFC_2019,
      DAY_DLY_APT_FLT_DIF_PY_PERC = if_else(
        DAY_DLY_APT_FLT_PY == 0, NA, DAY_DLY_APT_FLT / DAY_DLY_APT_FLT_PY - 1
      ),
      DAY_DLY_APT_FLT_DIF_2019_PERC = if_else(
        DAY_DLY_APT_FLT_2019 == 0, NA, DAY_DLY_APT_FLT / DAY_DLY_APT_FLT_2019 - 1
      ),
      APT_DIF_WEEK_PREV_YEAR_PERC = if_else(
        AVG_APT_ROLLING_WEEK_PREV_YEAR == 0, NA, AVG_APT_ROLLING_WEEK/ AVG_APT_ROLLING_WEEK_PREV_YEAR - 1
      ),
      APT_DIF_ROLLING_WEEK_2019_PERC = if_else(
        AVG_APT_ROLLING_WEEK_2019 == 0, NA, AVG_APT_ROLLING_WEEK / AVG_APT_ROLLING_WEEK_2019 - 1
      ),
      
      RWEEK_DLY_APT_SHARE = if_else(AVG_ROLLING_WEEK == 0, 0, AVG_APT_ROLLING_WEEK / AVG_ROLLING_WEEK),
      RWEEK_DLY_APT_FLT = AVG_APT_ROLLING_WEEK / nw_traffic_last_day$AVG_ROLLING_WEEK,
      RWEEK_DLY_APT_FLT_PY = AVG_APT_ROLLING_WEEK_PREV_YEAR / nw_traffic_last_day$AVG_ROLLING_WEEK_PREV_YEAR,
      RWEEK_DLY_APT_FLT_2019 = AVG_APT_ROLLING_WEEK_2019 / nw_traffic_last_day$AVG_ROLLING_WEEK_2019,
      RWEEK_DLY_APT_FLT_DIF_PY_PERC = if_else(
        RWEEK_DLY_APT_FLT_PY == 0, NA, RWEEK_DLY_APT_FLT / RWEEK_DLY_APT_FLT_PY - 1
      ),
      RWEEK_DLY_APT_FLT_DIF_2019_PERC = if_else(
        RWEEK_DLY_APT_FLT_2019 == 0, NA, RWEEK_DLY_APT_FLT / RWEEK_DLY_APT_FLT_2019 - 1
      ),
      
      Y2D_DLY_APT_SHARE = if_else(Y2D_AVG_DLY_YEAR == 0, 0, Y2D_AVG_DLY_APT_YEAR/Y2D_AVG_DLY_YEAR),
      Y2D_APT_DIFF_PREV_YEAR_PERC = if_else(
        Y2D_AVG_DLY_APT_PREV_YEAR == 0, NA, Y2D_AVG_DLY_APT_YEAR/ Y2D_AVG_DLY_APT_PREV_YEAR - 1
      ),
      Y2D_APT_DIFF_2019_PERC = if_else(
        Y2D_AVG_DLY_APT_2019 == 0, NA, Y2D_AVG_DLY_APT_YEAR / Y2D_AVG_DLY_APT_2019 - 1
      ),
      Y2D_DLY_APT_FLT = Y2D_DLY_APT_YEAR / nw_traffic_last_day$Y2D_TFC_YEAR,
      Y2D_DLY_APT_FLT_PY = Y2D_AVG_DLY_APT_PREV_YEAR / nw_traffic_last_day$Y2D_AVG_TFC_PREV_YEAR,
      Y2D_DLY_APT_FLT_2019 = Y2D_AVG_DLY_APT_2019 / nw_traffic_last_day$Y2D_AVG_TFC_2019,
      Y2D_DLY_APT_FLT_DIF_PY_PERC = if_else(
        Y2D_DLY_APT_FLT_PY == 0, NA, Y2D_DLY_APT_FLT / Y2D_DLY_APT_FLT_PY - 1
      ),
      Y2D_DLY_APT_FLT_DIF_2019_PERC = if_else(
        Y2D_DLY_APT_FLT_2019 == 0, NA, Y2D_DLY_APT_FLT / Y2D_DLY_APT_FLT_2019 - 1
      )
    )
  
  nw_delay_flt_APT_evo <- nw_delay_cause %>%
    filter(FLIGHT_DATE <= data_day_date) %>%
    mutate(
      ROLL_WK_AVG_FLT = rollmeanr(DAY_FLT, 7, fill = NA, align = "right"),
      ROLL_WK_AVG_DLY_FLT_APT = ROLL_WK_AVG_DLY_APT / ROLL_WK_AVG_FLT,
      ROLL_WK_AVG_DLY_FLT_APT_PREV_YEAR = lag(ROLL_WK_AVG_DLY_APT, 364) / lag(ROLL_WK_AVG_FLT, 364)
    ) %>%
    filter(FLIGHT_DATE >= paste0(data_day_year, "-01-01")) %>%
    mutate(FLIGHT_DATE = as.Date(FLIGHT_DATE)) %>%
    select(
      FLIGHT_DATE,
      ROLL_WK_AVG_DLY_FLT_APT,
      ROLL_WK_AVG_DLY_FLT_APT_PREV_YEAR
    )
  
  df <- nw_delay_flt_APT_evo %>%
    select(
      FLIGHT_DATE,
      ROLL_WK_AVG_DLY_FLT_APT,
      ROLL_WK_AVG_DLY_FLT_APT_PREV_YEAR
    )
  
  y2d_delay_APT_flt <- nw_delay_last_day %>%
    ungroup() %>%
    select(Y2D_DLY_APT_FLT,
           Y2D_DLY_APT_FLT_PY
    )
  
  column_names <- c(
    "FLIGHT_DATE",
    paste0("En-route ATFM delay/flight ", data_day_year, " (", format(round(y2d_delay_APT_flt$Y2D_DLY_APT_FLT,2), nsmall=2),"')"),
    paste0("En-route ATFM delay/flight ", data_day_year - 1, " (", format(round(y2d_delay_APT_flt$Y2D_DLY_APT_FLT_PY,2), nsmall=2),"')")
  )
  
  colnames(df) <- column_names
    
  
  print(paste(data_day_date, all.equal(df, df_new)))
  
}

mydates <- seq.Date(ymd(20251026), ymd(20251027))

walk(mydates, test)


