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

###############################################################################################
#                                                                                             #
#    json files for state landing page                                                        #
#                                                                                             #
###############################################################################################

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

  # traffic daio data
    st_daio_data <-  read_xlsx(
      path  = fs::path_abs(
        str_glue(base_file),
        start = base_dir),
      sheet = "state_daio",
      range = cell_limits(c(1, 1), c(NA, NA))) %>%
      as_tibble() %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))


    st_daio_last_day <- st_daio_data %>%
      filter(FLIGHT_DATE == last_day) %>%
      mutate(daio_zone = COUNTRY_NAME) %>%
      right_join(state_daio, by = "daio_zone", relationship = "many-to-many")

    st_daio_json <- st_daio_last_day %>%
      filter(FLIGHT_DATE == last_day) %>%
      select(
        iso_2letter,
        FLIGHT_DATE,
        DAY_TFC,
        DAY_DIFF_PREV_YEAR_PERC,
        DAY_TFC_DIFF_2019_PERC,
        AVG_ROLLING_WEEK,
        DIF_WEEK_PREV_YEAR_PERC,
        DIF_ROLLING_WEEK_2019_PERC,
        Y2D_TFC_YEAR,
        Y2D_AVG_TFC_YEAR,
        Y2D_DIFF_PREV_YEAR_PERC,
        Y2D_DIFF_2019_PERC
      )

    st_daio_json <- st_daio_json %>%
      toJSON(., digits = 10) %>%
      substr(., 1, nchar(.)-1) %>%
      substr(., 2, nchar(.))

###############################################################################################
#                                                                                             #
#    json files for state ranking tables                                                      #
#                                                                                             #
###############################################################################################

  # Aircraft operators traffic

    # day
    st_ao_data_day_raw <- read_xlsx(
      path  = fs::path_abs(
        str_glue(base_file),
        start = base_dir),
      sheet = "state_ao_day",
      range = cell_limits(c(1, 1), c(NA, NA))) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    st_ao_data_day_int <- st_ao_data_day_raw %>%
      mutate(TO_DATE = max(TO_DATE)) %>%
      spread(., key = FLAG_DAY, value = FLIGHT_WITHOUT_OVERFLIGHT) %>%
      arrange(COUNTRY_NAME, R_RANK) %>%
      mutate(
        DY_RANK_DIF_PREV_WEEK = case_when(
          is.na(RANK_PREV_WEEK) ~ RANK,
          .default = RANK_PREV_WEEK - RANK
        ),
        DY_DIF_PREV_WEEK_PERC =   case_when(
          DAY_PREV_WEEK == 0 | is.na(DAY_PREV_WEEK) ~ NA,
          .default = CURRENT_DAY / DAY_PREV_WEEK - 1
        ),
        DY_DIF_PREV_YEAR_PERC = case_when(
          DAY_PREV_YEAR == 0 | is.na(DAY_PREV_YEAR) ~ NA,
          .default = CURRENT_DAY / DAY_PREV_YEAR - 1
        ),
        ST_RANK = paste0(tolower(COUNTRY_NAME), R_RANK),
        ST_TFC_AO_GRP_DIF = CURRENT_DAY - DAY_PREV_WEEK
      )

    st_ao_data_day <- st_ao_data_day_int %>%
      rename(
        DY_AO_GRP_NAME = AO_GRP_NAME,
        DY_TO_DATE = TO_DATE,
        DY_FLIGHT = CURRENT_DAY
      ) %>%
      select(
        ST_RANK,
        DY_RANK_DIF_PREV_WEEK,
        DY_AO_GRP_NAME,
        DY_TO_DATE,
        DY_FLIGHT,
        DY_DIF_PREV_WEEK_PERC,
        DY_DIF_PREV_YEAR_PERC
      )

    # week
    st_ao_data_wk_raw <- read_xlsx(
      path  = fs::path_abs(
        str_glue(base_file),
        start = base_dir),
      sheet = "state_ao_week",
      range = cell_limits(c(1, 1), c(NA, NA))) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    st_ao_data_wk <- st_ao_data_wk_raw %>%
      mutate(FLIGHT_WITHOUT_OVERFLIGHT = FLIGHT_WITHOUT_OVERFLIGHT / 7) %>%
      spread(., key = FLAG_ROLLING_WEEK, value = FLIGHT_WITHOUT_OVERFLIGHT) %>%
      arrange(COUNTRY_NAME, R_RANK) %>%
      mutate(
        WK_RANK_DIF_PREV_WEEK = case_when(
          is.na(RANK_PREV_WEEK) ~ RANK,
          .default = RANK_PREV_WEEK - RANK
        ),
        WK_DIF_PREV_WEEK_PERC =   case_when(
          PREV_ROLLING_WEEK == 0 | is.na(PREV_ROLLING_WEEK) ~ NA,
          .default = CURRENT_ROLLING_WEEK / PREV_ROLLING_WEEK - 1
        ),
        WK_DIF_PREV_YEAR_PERC = case_when(
          ROLLING_WEEK_PREV_YEAR == 0 | is.na(ROLLING_WEEK_PREV_YEAR) ~ NA,
          .default = CURRENT_ROLLING_WEEK / ROLLING_WEEK_PREV_YEAR - 1
        ),
        ST_RANK = paste0(tolower(COUNTRY_NAME), R_RANK)
      ) %>%
      rename(
        WK_AO_GRP_NAME = AO_GRP_NAME,
        WK_FROM_DATE = FROM_DATE,
        WK_TO_DATE = TO_DATE,
        WK_DAILY_FLIGHT = CURRENT_ROLLING_WEEK
      ) %>%
      select(
        ST_RANK,
        WK_RANK_DIF_PREV_WEEK,
        WK_AO_GRP_NAME,
        WK_FROM_DATE,
        WK_TO_DATE,
        WK_DAILY_FLIGHT,
        WK_DIF_PREV_WEEK_PERC,
        WK_DIF_PREV_YEAR_PERC
      )

    # y2d
    st_ao_data_y2d_raw <- read_xlsx(
      path  = fs::path_abs(
        str_glue(base_file),
        start = base_dir),
      sheet = "state_ao_y2d",
      range = cell_limits(c(1, 1), c(NA, NA))) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    st_ao_data_y2d <- st_ao_data_y2d_raw %>%
      mutate(
             FROM_DATE = max(FROM_DATE),
             TO_DATE = max(TO_DATE),
             PERIOD =   case_when(
               YEAR == max(YEAR) ~ 'CURRENT_YEAR',
               YEAR == max(YEAR) - 1 ~ 'PREV_YEAR',
               .default = paste0('PERIOD_', YEAR)
               )
             ) %>%
      select(-FLIGHT_WITHOUT_OVERFLIGHT, -YEAR) %>%
      spread(., key = PERIOD, value = AVG_FLT) %>%
      arrange(COUNTRY_NAME, R_RANK) %>%
      mutate(
        Y2D_RANK_DIF_PREV_YEAR = case_when(
          is.na(RANK_PREV_YEAR) ~ RANK_CURRENT,
          .default = RANK_PREV_YEAR - RANK_CURRENT
        ),
        Y2D_DIF_PREV_YEAR_PERC =   case_when(
          PREV_YEAR == 0 | is.na(PREV_YEAR) ~ NA,
          .default = CURRENT_YEAR / PREV_YEAR - 1
        ),
        Y2D_DIF_2019_PERC  = case_when(
          PERIOD_2019 == 0 | is.na(PERIOD_2019) ~ NA,
          .default = CURRENT_YEAR / PERIOD_2019 - 1
        ),
        ST_RANK = paste0(tolower(COUNTRY_NAME), R_RANK)
      ) %>%
      rename(
        Y2D_AO_GRP_NAME = AO_GRP_NAME,
        Y2D_TO_DATE = TO_DATE,
        Y2D_DAILY_FLIGHT = CURRENT_YEAR
      ) %>%
      select(
        ST_RANK,
        Y2D_RANK_DIF_PREV_YEAR,
        Y2D_AO_GRP_NAME,
        Y2D_TO_DATE,
        Y2D_DAILY_FLIGHT,
        Y2D_DIF_PREV_YEAR_PERC,
        Y2D_DIF_2019_PERC
      )

    # main card
    st_ao_main_traffic <- st_ao_data_day_int %>%
      mutate(
        MAIN_TFC_AO_GRP_NAME = if_else(
          R_RANK <= 4,
          AO_GRP_NAME,
          NA
        ),
        MAIN_TFC_AO_GRP_FLIGHT = if_else(
          R_RANK <= 4,
          CURRENT_DAY,
          NA
        ),
        ST_RANK = paste0(tolower(COUNTRY_NAME), RANK)
        ) %>%
      select(ST_RANK, MAIN_TFC_AO_GRP_NAME, MAIN_TFC_AO_GRP_FLIGHT)

    st_ao_main_traffic_dif <- st_ao_data_day_int %>%
      arrange(COUNTRY_NAME, desc(abs(ST_TFC_AO_GRP_DIF)), R_RANK) %>%
      group_by(COUNTRY_NAME) %>%
      mutate(RANK_DIF_AO_TFC = row_number()) %>%
      ungroup() %>%
      arrange(COUNTRY_NAME, R_RANK) %>%
      mutate(
        MAIN_TFC_DIF_AO_GRP_NAME = if_else(
          RANK_DIF_AO_TFC <= 4,
          AO_GRP_NAME,
          NA
        ),
        MAIN_TFC_AO_GRP_DIF = if_else(
          RANK_DIF_AO_TFC <= 4,
          ST_TFC_AO_GRP_DIF,
          NA
        )
      ) %>%
      arrange(COUNTRY_NAME, desc(MAIN_TFC_AO_GRP_DIF)) %>%
      group_by(COUNTRY_NAME) %>%
      mutate(
        RANK_MAIN_DIF = row_number(),
        ST_RANK = paste0(tolower(COUNTRY_NAME), RANK_MAIN_DIF)
             ) %>%
      ungroup() %>%
      select(ST_RANK, MAIN_TFC_DIF_AO_GRP_NAME, MAIN_TFC_AO_GRP_DIF)

    # create list of state/rankings for left join
    state_iso_ranking <- list()
    i = 0
    for (i in 1:10) {
      i = i + 1
      state_iso_ranking <- state_iso_ranking %>%
        bind_rows(state_iso, .)
    }

    state_iso_ranking <- state_iso_ranking %>%
      arrange(state) %>%
      group_by(state) %>%
      mutate(
        RANK = row_number(),
        ST_RANK = paste0(tolower(state), RANK)
             )

    # join and reorder tables
    st_ao_data <- state_iso_ranking %>%
      left_join(st_ao_main_traffic, by = "ST_RANK") %>%
      left_join(st_ao_main_traffic_dif, by = "ST_RANK") %>%
      left_join(st_ao_data_day, by = "ST_RANK") %>%
      left_join(st_ao_data_wk, by = "ST_RANK") %>%
      left_join(st_ao_data_y2d, by = "ST_RANK") %>%
      select(-ST_RANK, -R_RANK)




