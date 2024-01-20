## libraries
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

# parameters
  data_folder <- here::here("data")
  # base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/'
  base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Oscar/Develop/'
  base_file <- '99a_app_state_dataset.xlsx'
  # archive_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/web_daily_json_files/app/'
  archive_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Oscar/old/'
  today <- (lubridate::now() +  days(-1)) %>% format("%Y%m%d")
  last_day <-  trunc((lubridate::now() +  days(-1)), "day")
  last_year <- as.numeric(format(last_day,'%Y'))
  st_json_app <-""
  # DB params
  usr <- Sys.getenv("PRU_DEV_USR")
  pwd <- Sys.getenv("PRU_DEV_PWD")
  dbn <- Sys.getenv("PRU_DEV_DBNAME")

# equivalence tables
  state_iso <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(base_file),
      start = base_dir),
    sheet = "lists",
    range = cell_limits(c(2, 2), c(NA, 3))) %>%
    as_tibble()

  state_crco <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(base_file),
      start = base_dir),
    sheet = "lists",
    range = cell_limits(c(2, 6), c(NA, 7))) %>%
    as_tibble()

  state_daio <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(base_file),
      start = base_dir),
    sheet = "lists",
    range = cell_limits(c(2, 10), c(NA, 11))) %>%
    as_tibble()

# functions
  export_query <- function(query) {

    # NOTE: to be set before you create your ROracle connection!
    # See http://www.oralytics.com/2015/05/r-roracle-and-oracle-date-formats_27.html
    withr::local_envvar(c("TZ" = "UTC",
                          "ORA_SDTZ" = "UTC"))
    withr::local_namespace("ROracle")
    con <- withr::local_db_connection(
      DBI::dbConnect(
        DBI::dbDriver("Oracle"),
        usr, pwd,
        dbname = dbn,
        timezone = "UTC")
    )

    data <- DBI::dbSendQuery(con, query)
    # ~2.5 min for one day
    DBI::fetch(data, n = -1) %>%
      tibble::as_tibble()
  }

# json files for landing page

  ####billing json - we do this first to avoid 'R fatal error'

  ## https://leowong.ca/blog/connect-to-microsoft-access-database-via-r/
  ## Set up driver info and database path
  DRIVERINFO <- "Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
  MDBPATH <- "G:/HQ/dgof-pru/Data/DataProcessing/Crco - Billing/CRCO_BILL.accdb"
  PATH <- paste0(DRIVERINFO, "DBQ=", MDBPATH)

  channel <- odbcDriverConnect(PATH)
  query_bill <- "SELECT *,
                  iif(
	            [Billing Zone Number] = '33', 'Bosnia and Herzegovina',
	            iif ([Billing Zone Number] = '10' OR [Billing Zone Number] = '11', 'Spain' ,
            	iif ([Billing Zone Number] = '08' OR [Billing Zone Number] = '12', 'Portugal',
	            iif ([Billing Zone Number] = '32' OR [Billing Zone Number] = '41', 'Ukraine',
	[Billing Zone Name]
)))) as corrected_cz
    FROM V_CRCO_BILL_PER_CZ
  "

  ## Load data into R dataframe
  st_billed_raw <- sqlQuery(channel,
                            query_bill,
                            stringsAsFactors = FALSE)

  ## Close and remove channel
  close(channel)
  rm(channel)

  ## process billing data
  st_billed_raw <- st_billed_raw %>%
    janitor::clean_names() %>%
    mutate(billing_period_start_date = as.Date(billing_period_start_date, format = "%d-%m-%Y"))

  last_billing_date <- max(st_billed_raw$billing_period_start_date)
  last_billing_year <- max(st_billed_raw$year)

  st_billing <- st_billed_raw %>%
    group_by(corrected_cz, year, month, billing_period_start_date) %>%
    summarise(total_billing = sum(route_charges)) %>%
    ungroup

  st_billing <- state_crco %>%
    left_join(st_billing, by = "corrected_cz", relationship = "many-to-many")

  st_billed_json <- st_billing %>%
    arrange(iso_2letter, year, billing_period_start_date) %>%
    mutate(Year = year,
           MONTH_F = format(billing_period_start_date + days(1),'%B'),
           BILL_MONTH_PY = lag(total_billing, 12),
           BILL_MONTH_2019 = lag(total_billing, (last_billing_year - 2019) * 12),
           DIF_BILL_MONTH_PY = total_billing / BILL_MONTH_PY - 1,
           DIF_BILL_MONTH_2019 = total_billing / BILL_MONTH_2019 - 1,
           BILLED = round(total_billing / 1000000, 1)
    ) %>%
    group_by(iso_2letter, Year) %>%
    mutate(
      total_billing_y2d = cumsum(total_billing)
    ) %>%
    ungroup() %>%
    mutate(
      BILL_Y2D_PY = lag(total_billing_y2d, 12),
      BILL_Y2D_2019 = lag(total_billing_y2d, (last_billing_year - 2019) * 12),
      DIF_BILL_Y2D_PY = total_billing_y2d / BILL_Y2D_PY -1,
      DIF_BILL_Y2D_2019 = total_billing_y2d / BILL_Y2D_2019 -1,
      BILLED_Y2D = round(total_billing_y2d / 1000000, 1)
    ) %>%
    filter(billing_period_start_date == last_billing_date) %>%
    select(iso_2letter,
           MONTH_F,
           BILLED,
           DIF_BILL_MONTH_PY,
           DIF_BILL_MONTH_2019,
           BILLED_Y2D,
           DIF_BILL_Y2D_PY,
           DIF_BILL_Y2D_2019
    )

    st_billed_json <- st_billed_json %>%
    toJSON() %>%
    substr(., 1, nchar(.)-1) %>%
    substr(., 2, nchar(.))

  ###############################################
  # traffic data
    st_traffic_data <-  read_xlsx(
      path  = fs::path_abs(
        str_glue(base_file),
        start = base_dir),
      sheet = "state_daio",
      range = cell_limits(c(1, 1), c(NA, NA))) %>%
      as_tibble() %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))


    st_traffic_last_day <- st_traffic_data %>%
      filter(ENTRY_DATE == last_day) %>%
      mutate(daio_zone = COUNTRY_NAME) %>%
      right_join(state_daio, by = "daio_zone", relationship = "many-to-many")

    st_traffic_json <- st_traffic_last_day %>%
      filter(ENTRY_DATE == last_day) %>%
      select(
        iso_2letter,
        ENTRY_DATE,
        TOT_TFC,
        DAY_TFC_DIFF_PREV_YEAR_PERC,
        DAY_TFC_DIFF_2019_PERC,
        AVG_ROLLING_WEEK,
        DIF_ROLLING_WEEK_PY_PERC,
        DIF_ROLLING_WEEK_2019_PERC,
        Y2D_TFC_YEAR,
        Y2D_AVG_TFC_YEAR,
        Y2D_DIFF_PREV_YEAR_PERC,
        Y2D_DIFF_2019_PERC
      )

    st_traffic_json <- st_traffic_json %>%
      toJSON(., digits = 10) %>%
      substr(., 1, nchar(.)-1) %>%
      substr(., 2, nchar(.))

