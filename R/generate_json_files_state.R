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

source(here::here("R", "helpers.R"))

# Parameters ----
  data_folder <- here::here("data", "v2")
  # base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/'
  base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Oscar/Develop/'
  base_file <- '99a_app_state_dataset.xlsx'
  nw_base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/LastVersion/'
  nw_base_file <- '99_Traffic_Landing_Page_dataset_new.xlsx'
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


# Dimension tables ----
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
    range = cell_limits(c(2, 6), c(NA, 8))) %>%
    as_tibble()

  state_daio <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(base_file),
      start = base_dir),
    sheet = "lists",
    range = cell_limits(c(2, 11), c(NA, 13))) %>%
    as_tibble()

  state_co2 <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(base_file),
      start = base_dir),
    sheet = "lists",
    range = cell_limits(c(2, 16), c(NA, 17))) %>%
    as_tibble()

  acc <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(nw_base_file),
      start = nw_base_dir),
    sheet = "ACC_names",
    range = cell_limits(c(2, 3), c(NA, NA))) %>%
    as_tibble()

  query <- "select * from PRU_AIRPORT"

  airport <- export_query(query) %>%
    as_tibble() %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
    select(ICAO_CODE, ISO_COUNTRY_CODE) %>%
    rename(iso_2letter = ISO_COUNTRY_CODE)



# ____________________________________________________________________________________________
#
#    State landing page -----
#
# ____________________________________________________________________________________________

  #### Billing data ----
  ## we do this first to avoid 'R fatal error'

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

    st_billed_for_json <- st_billing %>%
      arrange(iso_2letter, year, billing_period_start_date) %>%
      mutate(
             BILLING_DATE = (billing_period_start_date + days(1) + months(1)) + days(-1),
             Year = year,
             MONTH_TEXT = format(billing_period_start_date + days(1),'%B'),
             MM_BILLED_PY = lag(total_billing, 12),
             MM_BILLED_2019 = lag(total_billing, (last_billing_year - 2019) * 12),
             MM_BILLED_DIF_PREV_YEAR = total_billing / MM_BILLED_PY - 1,
             MM_BILLED_DIF_2019 = total_billing / MM_BILLED_2019 - 1,
             MM_BILLED = round(total_billing / 1000000, 1)
      ) %>%
      group_by(iso_2letter, Year) %>%
      mutate(
        total_billing_y2d = cumsum(total_billing)
      ) %>%
      ungroup() %>%
      mutate(
        Y2D_BILLED_PY = lag(total_billing_y2d, 12),
        Y2D_BILLED_2019 = lag(total_billing_y2d, (last_billing_year - 2019) * 12),
        Y2D_BILLED_DIF_PREV_YEAR = total_billing_y2d / Y2D_BILLED_PY -1,
        Y2D_BILLED_DIF_2019 = total_billing_y2d / Y2D_BILLED_2019 -1,
        Y2D_BILLED = round(total_billing_y2d / 1000000, 1)
      ) %>%
      filter(billing_period_start_date == last_billing_date) %>%
      select(iso_2letter,
             BILLING_DATE,
             MONTH_TEXT,
             MM_BILLED,
             MM_BILLED_DIF_PREV_YEAR,
             MM_BILLED_DIF_2019,
             Y2D_BILLED,
             Y2D_BILLED_DIF_PREV_YEAR,
             Y2D_BILLED_DIF_2019
      ) %>%
      right_join(state_iso, by ="iso_2letter") %>%
      select(-state) %>%
      arrange(iso_2letter)

  #### Traffic DAIO data ----
    st_daio_data <-  read_xlsx(
      path  = fs::path_abs(
        str_glue(base_file),
        start = base_dir),
      sheet = "state_daio",
      range = cell_limits(c(1, 1), c(NA, NA))) %>%
      as_tibble() %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    st_daio_data_zone <- st_daio_data %>%
      mutate(daio_zone_lc = tolower(COUNTRY_NAME)) %>%
      right_join(state_daio, by = "daio_zone_lc", relationship = "many-to-many") %>%
      arrange(iso_2letter, daio_zone_lc, FLIGHT_DATE)

    st_daio_last_day <- st_daio_data_zone %>%
      filter(FLIGHT_DATE == last_day)

    st_daio_for_json <- st_daio_last_day %>%
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
      ) %>%
      rename(
        DY_DAIO = DAY_TFC,
        DY_DAIO_DIF_PREV_YEAR_PERC = DAY_DIFF_PREV_YEAR_PERC,
        DY_DAIO_DIF_2019_PERC = DAY_TFC_DIFF_2019_PERC,
        WK_DAIO_AVG_ROLLING = AVG_ROLLING_WEEK,
        WK_DAIO_DIF_PREV_YEAR_PERC = DIF_WEEK_PREV_YEAR_PERC,
        WK_DAIO_DIF_2019_PERC = DIF_ROLLING_WEEK_2019_PERC,
        Y2D_DAIO = Y2D_TFC_YEAR,
        Y2D_DAIO_AVG = Y2D_AVG_TFC_YEAR,
        Y2D_DAIO_DIF_PREV_YEAR_PERC = Y2D_DIFF_PREV_YEAR_PERC,
        Y2D_DAIO_DIF_2019_PERC = Y2D_DIFF_2019_PERC
      ) %>%
      right_join(state_iso, by ="iso_2letter") %>%
      select(-state) %>%
      arrange(iso_2letter)

  #### Traffic DAI data ----
    st_dai_data <-  read_xlsx(
      path  = fs::path_abs(
        str_glue(base_file),
        start = base_dir),
      sheet = "state_dai",
      range = cell_limits(c(1, 1), c(NA, NA))) %>%
      as_tibble() %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    st_dai_data_zone <- st_dai_data %>%
      mutate(daio_zone_lc = tolower(COUNTRY_NAME)) %>%
      right_join(state_daio, by = "daio_zone_lc", relationship = "many-to-many") %>%
      arrange(iso_2letter, daio_zone_lc, FLIGHT_DATE)

    st_dai_last_day <- st_dai_data_zone %>%
      filter(FLIGHT_DATE == last_day)

    st_dai_for_json <- st_dai_last_day %>%
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
      ) %>%
      rename(
        DY_DAI = DAY_TFC,
        DY_DAI_DIF_PREV_YEAR_PERC = DAY_DIFF_PREV_YEAR_PERC,
        DY_DAI_DIF_2019_PERC = DAY_TFC_DIFF_2019_PERC,
        WK_DAI_AVG_ROLLING = AVG_ROLLING_WEEK,
        WK_DAI_DIF_PREV_YEAR_PERC = DIF_WEEK_PREV_YEAR_PERC,
        WK_DAI_DIF_2019_PERC = DIF_ROLLING_WEEK_2019_PERC,
        Y2D_DAI = Y2D_TFC_YEAR,
        Y2D_DAI_AVG = Y2D_AVG_TFC_YEAR,
        Y2D_DAI_DIF_PREV_YEAR_PERC = Y2D_DIFF_PREV_YEAR_PERC,
        Y2D_DAI_DIF_2019_PERC = Y2D_DIFF_2019_PERC
      ) %>%
      right_join(state_iso, by ="iso_2letter") %>%
      select(-state) %>%
      arrange(iso_2letter)

  #### Traffic overflight data ----
    st_dai_data_zone_p <- st_dai_data_zone %>%
      mutate(flight_type = 'dai') %>%
      mutate_all(~replace(., is.na(.), 0)) %>%
      select(FLIGHT_DATE,
             iso_2letter,
             daio_zone,
             flight_type,
             DAY_TFC,
             DAY_TFC_PREV_WEEK,
             DAY_TFC_PREV_YEAR,
             DAY_TFC_2019,

             AVG_ROLLING_WEEK,
             AVG_ROLLING_PREV_WEEK,
             AVG_ROLLING_WEEK_PREV_YEAR,
             AVG_ROLLING_WEEK_2020,
             AVG_ROLLING_WEEK_2019,

             Y2D_TFC_YEAR,
             Y2D_TFC_PREV_YEAR,
             Y2D_TFC_2019,
             Y2D_AVG_TFC_YEAR,
             Y2D_AVG_TFC_PREV_YEAR,
             Y2D_AVG_TFC_2019
      ) %>%
      mutate(across(-c(FLIGHT_DATE, flight_type, iso_2letter, daio_zone), ~ .* -1 ))


    st_daio_data_zone_p <- st_daio_data_zone %>%
      mutate(flight_type = 'daio') %>%
      mutate_all(~replace(., is.na(.), 0)) %>%
      select(FLIGHT_DATE,
             iso_2letter,
             daio_zone,
             flight_type,
             DAY_TFC,
             DAY_TFC_PREV_WEEK,
             DAY_TFC_PREV_YEAR,
             DAY_TFC_2019,

             AVG_ROLLING_WEEK,
             AVG_ROLLING_PREV_WEEK,
             AVG_ROLLING_WEEK_PREV_YEAR,
             AVG_ROLLING_WEEK_2020,
             AVG_ROLLING_WEEK_2019,

             Y2D_TFC_YEAR,
             Y2D_TFC_PREV_YEAR,
             Y2D_TFC_2019,
             Y2D_AVG_TFC_YEAR,
             Y2D_AVG_TFC_PREV_YEAR,
             Y2D_AVG_TFC_2019
      )

    st_overflight_data_zone <- rbind(st_daio_data_zone_p, st_dai_data_zone_p) %>%
      group_by(iso_2letter, daio_zone, FLIGHT_DATE) %>%
      summarise(DAY_TFC = sum(DAY_TFC),
                DAY_TFC_PREV_WEEK = sum(DAY_TFC_PREV_WEEK),
                DAY_TFC_PREV_YEAR = sum(DAY_TFC_PREV_YEAR),
                DAY_TFC_2019 = sum(DAY_TFC_2019),

                AVG_ROLLING_WEEK = sum(AVG_ROLLING_WEEK),
                AVG_ROLLING_PREV_WEEK = sum(AVG_ROLLING_PREV_WEEK),
                AVG_ROLLING_WEEK_PREV_YEAR = sum(AVG_ROLLING_WEEK_PREV_YEAR),
                AVG_ROLLING_WEEK_2020 = sum(AVG_ROLLING_WEEK_2020),
                AVG_ROLLING_WEEK_2019 = sum(AVG_ROLLING_WEEK_2019),

                Y2D_TFC_YEAR = sum(Y2D_TFC_YEAR),
                Y2D_TFC_PREV_YEAR = sum(Y2D_TFC_PREV_YEAR),
                Y2D_TFC_2019 = sum(Y2D_TFC_2019),
                Y2D_AVG_TFC_YEAR = sum(Y2D_AVG_TFC_YEAR),
                Y2D_AVG_TFC_PREV_YEAR = sum(Y2D_AVG_TFC_PREV_YEAR),
                Y2D_AVG_TFC_2019 = sum(Y2D_AVG_TFC_2019)
      ) %>%
      mutate(
        DAY_TFC_DIFF_PREV_WEEK = DAY_TFC - DAY_TFC_PREV_WEEK,
        DAY_TFC_DIFF_PREV_YEAR = DAY_TFC - DAY_TFC_PREV_YEAR,
        DAY_TFC_DIFF_2019 = DAY_TFC - DAY_TFC_2019,
        DAY_TFC_PREV_WEEK_PERC = if_else(DAY_TFC_PREV_WEEK != 0, DAY_TFC/DAY_TFC_PREV_WEEK - 1, 0),
        DAY_DIFF_PREV_YEAR_PERC	= if_else(DAY_TFC_PREV_YEAR != 0, DAY_TFC/DAY_TFC_PREV_YEAR - 1, 0),
        DAY_TFC_DIFF_2019_PERC = if_else(DAY_TFC_2019 != 0, DAY_TFC/DAY_TFC_2019 - 1, 0),

        DIF_WEEK_PREV_YEAR_PERC = if_else(AVG_ROLLING_WEEK_PREV_YEAR != 0, AVG_ROLLING_WEEK/AVG_ROLLING_WEEK_PREV_YEAR - 1, 0),
        DIF_ROLLING_WEEK_2019_PERC = if_else(AVG_ROLLING_WEEK_2019 != 0, AVG_ROLLING_WEEK/AVG_ROLLING_WEEK_2019 - 1, 0),

        Y2D_DIFF_PREV_YEAR_PERC	= if_else(Y2D_AVG_TFC_PREV_YEAR != 0, Y2D_AVG_TFC_YEAR/Y2D_AVG_TFC_PREV_YEAR - 1, 0),
        Y2D_DIFF_2019_PERC = if_else(Y2D_AVG_TFC_2019 != 0, Y2D_AVG_TFC_YEAR/Y2D_AVG_TFC_2019 - 1, 0)
      ) %>% ungroup()

    st_overflight_last_day <- st_overflight_data_zone %>%
      filter(FLIGHT_DATE == last_day)

    mycolnames <- colnames(st_overflight_last_day) %>%
      str_replace_all('TFC', 'OVF')

    st_overflight_for_json <- st_overflight_last_day %>%
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
      ) %>%
      rename(
        DY_OVF = DAY_TFC,
        DY_OVF_DIF_PREV_YEAR_PERC = DAY_DIFF_PREV_YEAR_PERC,
        DY_OVF_DIF_2019_PERC = DAY_TFC_DIFF_2019_PERC,
        WK_OVF_AVG_ROLLING = AVG_ROLLING_WEEK,
        WK_OVF_DIF_PREV_YEAR_PERC = DIF_WEEK_PREV_YEAR_PERC,
        WK_OVF_DIF_2019_PERC = DIF_ROLLING_WEEK_2019_PERC,
        Y2D_OVF = Y2D_TFC_YEAR,
        Y2D_OVF_AVG = Y2D_AVG_TFC_YEAR,
        Y2D_OVF_DIF_PREV_YEAR_PERC = Y2D_DIFF_PREV_YEAR_PERC,
        Y2D_OVF_DIF_2019_PERC = Y2D_DIFF_2019_PERC
      ) %>%
      right_join(state_iso, by ="iso_2letter") %>%
      select(-state) %>%
      arrange(iso_2letter)

  #### Delay data ----
    st_delay_data <-  read_xlsx(
      path  = fs::path_abs(
        str_glue(base_file),
        start = base_dir),
      sheet = "state_delay",
      range = cell_limits(c(1, 1), c(NA, NA))) %>%
      as_tibble() %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    st_delay_last_day <- st_delay_data %>%
      filter(FLIGHT_DATE == max(LAST_DATA_DAY)) %>%
      mutate(daio_zone_lc = tolower(COUNTRY_NAME)) %>%
      right_join(state_daio, by = "daio_zone_lc", relationship = "many-to-many")

    st_delay_for_json  <- st_delay_last_day %>%
      mutate(
        DAY_DLY_FLT = DAY_DLY / DAY_TFC,
        DAY_DLY_FLT_PY = DAY_DLY_PREV_YEAR / DAY_TFC_PREV_YEAR,
        DAY_DLY_FLT_2019 = DAY_DLY_2019 / DAY_TFC_2019,
        DAY_DLY_FLT_DIF_PY_PERC = if_else(
          DAY_DLY_FLT_PY == 0, NA , DAY_DLY_FLT / DAY_DLY_FLT_PY -1
        ),
        DAY_DLY_FLT_DIF_2019_PERC = if_else(
          DAY_DLY_FLT_2019 == 0, NA , DAY_DLY_FLT / DAY_DLY_FLT_2019 -1
        ),

        WEEK_DLY_FLT = AVG_DLY_ROLLING_WEEK / AVG_TFC_ROLLING_WEEK,
        WEEK_DLY_FLT_PY = AVG_DLY_ROLLING_WEEK_PREV_YEAR / AVG_TFC_ROLLING_WEEK_PREV_YEAR,
        WEEK_DLY_FLT_2019 = AVG_DLY_ROLLING_WEEK_2019 / AVG_TFC_ROLLING_WEEK_2019,
        WEEK_DLY_FLT_DIF_PY_PERC = if_else(
          WEEK_DLY_FLT_PY == 0, NA , WEEK_DLY_FLT / WEEK_DLY_FLT_PY -1
        ),
        WEEK_DLY_FLT_DIF_2019_PERC = if_else(
          WEEK_DLY_FLT_2019 == 0, NA , WEEK_DLY_FLT / WEEK_DLY_FLT_2019 -1
        ),

        Y2D_DLY_FLT = Y2D_DLY_YEAR / Y2D_TFC_YEAR,
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
        iso_2letter,
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
      right_join(state_iso, by ="iso_2letter") %>%
      select(-state) %>%
      arrange(iso_2letter)

  #### Punctuality data ----
    query <- "
      WITH

      LIST_COUNTRY AS (
      SELECT distinct ISO_CT_CODE as iso_2letter
      FROM LDW_VDM.VIEW_FAC_PUNCTUALITY_CT_DAY
      group by ISO_CT_CODE
      order by ISO_CT_CODE
      )

      , CTRY_DAY AS (
      SELECT
              a.iso_2letter,
              t.year,
              t.month,
              t.week,
              t.week_nb_year,
              t.day_type,
              t.day_of_week_nb AS day_of_week,
              t.day_date
      FROM LIST_COUNTRY a, pru_time_references t
      WHERE
         t.day_date >= to_date('24-12-2018','DD-MM-YYYY')
         AND t.day_date < trunc(sysdate)
      )

      SELECT a.*, b.*

      FROM CTRY_DAY a
      left join LDW_VDM.VIEW_FAC_PUNCTUALITY_CT_DAY b on a.ISO_2LETTER = b.ISO_CT_CODE and a.day_date = b.\"DATE\"
   "

    st_punct_raw <- export_query(query) %>%
      as_tibble() %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    last_day_punct <-  max(st_punct_raw$DAY_DATE)
    last_year_punct <- as.numeric(format(last_day_punct,'%Y'))

    st_punct_data <- st_punct_raw %>%
      arrange(ISO_2LETTER, DAY_DATE) %>%
      mutate(
             DAY_ARR_PUNCT = case_when(
               ARR_SCHEDULE_FLIGHT == 0 ~ NA,
               .default = ARR_PUNCTUAL_FLIGHTS/ARR_SCHEDULE_FLIGHT * 100
               ),
             DAY_DEP_PUNCT = case_when(
               DEP_SCHEDULE_FLIGHT == 0 ~ NA,
               .default = DEP_PUNCTUAL_FLIGHTS/DEP_SCHEDULE_FLIGHT * 100
             ),
             DAY_ARR_PUNCT_PY = lag(DAY_ARR_PUNCT, 364),
             DAY_DEP_PUNCT_PY = lag(DAY_DEP_PUNCT, 364),
             DAY_ARR_PUNCT_2019 =if_else(YEAR == last_year_punct,
                                   lag(DAY_ARR_PUNCT,
                                       364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7),
                                   NA),
             DAY_DEP_PUNCT_2019 =if_else(YEAR == last_year_punct,
                                   lag(DAY_DEP_PUNCT,
                                       364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7),
                                   NA),
             DAY_2019 = if_else(YEAR == last_year_punct,
                                lag(DAY_DATE,
                                    364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7)
                                , NA),
             DAY_ARR_PUNCT_DIF_PY = DAY_ARR_PUNCT - DAY_ARR_PUNCT_PY,
             DAY_DEP_PUNCT_DIF_PY = DAY_DEP_PUNCT - DAY_DEP_PUNCT_PY,
             DAY_ARR_PUNCT_DIF_2019 = DAY_ARR_PUNCT - DAY_ARR_PUNCT_2019,
             DAY_DEP_PUNCT_DIF_2019 = DAY_DEP_PUNCT - DAY_DEP_PUNCT_2019
      ) %>%
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

    st_punct_d_w <- st_punct_data %>%
      filter (DAY_DATE == last_day_punct) %>%
      select(
        ISO_2LETTER,
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

    st_punct_y2d <- st_punct_raw %>%
      arrange(ISO_2LETTER, DAY_DATE) %>%
      mutate(MONTH_DAY = as.numeric(format(DAY_DATE, format="%m%d"))) %>%
      filter(MONTH_DAY <= as.numeric(format(last_day_punct, format="%m%d"))) %>%
      group_by(ISO_2LETTER, YEAR) %>%
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
      select(ISO_2LETTER,
             Y2D_ARR_PUN,
             Y2D_DEP_PUN,
             Y2D_ARR_PUN_DIF_PREV_YEAR,
             Y2D_DEP_PUN_DIF_PREV_YEAR,
             Y2D_ARR_PUN_DIF_2019,
             Y2D_DEP_PUN_DIF_2019
      )

    st_punct_for_json <- merge(st_punct_d_w, st_punct_y2d, by="ISO_2LETTER") %>%
      rename(iso_2letter = ISO_2LETTER) %>%
      right_join(state_iso, by = "iso_2letter") %>%
      select (-state) %>%
      arrange(iso_2letter)

  #### CO2 data ----
    query <- str_glue("
    SELECT *
      FROM TABLE (emma_pub.api_aiu_stats.MM_AIU_STATE_DEP ())
      where year >= 2019 and STATE_NAME not in ('LIECHTENSTEIN')
    ORDER BY 2, 3, 4
   ")

    st_co2_data_raw <- export_query(query) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    st_co2_data_filtered <- st_co2_data_raw %>%
      mutate(co2_state = STATE_NAME) %>%
      right_join(state_co2, by = "co2_state") %>%
      select(-c(STATE_NAME, STATE_CODE, co2_state, CREA_DATE) )


    st_co2_data <- st_co2_data_filtered %>%
      select(iso_2letter,
             FLIGHT_MONTH,
             CO2_QTY_TONNES,
             LY_CO2_QTY_TONNES,
             TF,
             LY_TF,
             YEAR,
             MONTH) %>%
      group_by(iso_2letter, FLIGHT_MONTH, YEAR, MONTH) %>%
      summarise (MM_DEP = sum(TF, na.rm=TRUE) / 10^6,
                 MM_DEP_PY = sum(LY_TF, na.rm=TRUE) / 10^6,
                 MM_CO2 = sum(CO2_QTY_TONNES, na.rm=TRUE) / 10^6,
                 MM_CO2_PY = sum(LY_CO2_QTY_TONNES, na.rm=TRUE) / 10^6
      ) %>%
      ungroup() %>%
      mutate(
        CO2_DATE = FLIGHT_MONTH,
        MM_CO2_DEP = MM_CO2 / MM_DEP,
        MM_CO2_DEP_PY = MM_CO2_PY / MM_DEP_PY
      ) %>%
      arrange(iso_2letter, FLIGHT_MONTH) %>%
      mutate(FLIGHT_MONTH = ceiling_date(as_date(FLIGHT_MONTH), unit = 'month')-1)

    st_co2_last_date <- max(st_co2_data$FLIGHT_MONTH, na.rm=TRUE)
    st_co2_last_month <- format(st_co2_last_date,'%B')
    st_co2_last_month_num <- as.numeric(format(st_co2_last_date,'%m'))
    st_co2_last_year <- max(st_co2_data$YEAR, na.rm=TRUE)

    #check last month number of flights
    check_flights <- st_co2_data %>%
      filter (YEAR == st_co2_last_year) %>% filter(MONTH == st_co2_last_month_num) %>%
      summarise (TTF = sum(MM_DEP, na.rm=TRUE)) %>%
      select(TTF) %>% pull()

    if (check_flights < 1000) {
      st_co2_data <- st_co2_data %>% filter (FLIGHT_MONTH < st_co2_last_date)
      st_co2_last_date <- max(st_co2_data$FLIGHT_MONTH, na.rm=TRUE)
    }

    st_co2_for_json <- st_co2_data %>%
      arrange(iso_2letter, FLIGHT_MONTH) %>%
      mutate(
        MONTH_TEXT = format(FLIGHT_MONTH,'%B'),
        MM_CO2_2019 = lag(MM_CO2, (as.numeric(st_co2_last_year) - 2019) * 12),
        MM_DEP_2019 = lag(MM_DEP, (as.numeric(st_co2_last_year) - 2019) * 12),
        MM_CO2_DEP_2019 = lag(MM_CO2_DEP, (as.numeric(st_co2_last_year) - 2019) * 12)
      ) %>%
      mutate(
        MM_CO2_DIF_PREV_YEAR = MM_CO2 / MM_CO2_PY - 1,
        MM_DEP_DIF_PREV_YEAR = MM_DEP / MM_DEP_PY - 1,
        MM_CO2_DEP_DIF_PREV_YEAR = MM_CO2_DEP / MM_CO2_DEP_PY - 1,
        MM_CO2_DIF_2019 = MM_CO2 / MM_CO2_2019 - 1,
        MM_DEP_DIF_2019 = MM_DEP / MM_DEP_2019 - 1,
        MM_CO2_DEP_DIF_2019 = MM_CO2_DEP / MM_CO2_DEP_2019 - 1
      ) %>%
      group_by(iso_2letter, YEAR) %>%
      mutate(
        Y2D_CO2 = cumsum(MM_CO2),
        Y2D_DEP = cumsum(MM_DEP),
        Y2D_CO2_DEP = cumsum(MM_CO2) / cumsum(MM_DEP)
      ) %>%
      ungroup() %>%
      mutate(
        Y2D_CO2_PY = lag(Y2D_CO2, 12),
        Y2D_DEP_PY = lag(Y2D_DEP, 12),
        Y2D_CO2_DEP_PY = lag(Y2D_CO2_DEP, 12),
        Y2D_CO2_2019 = lag(Y2D_CO2, (as.numeric(st_co2_last_year) - 2019) * 12),
        Y2D_DEP_2019 = lag(Y2D_DEP, (as.numeric(st_co2_last_year) - 2019) * 12),
        Y2D_CO2_DEP_2019 = lag(Y2D_CO2_DEP, (as.numeric(st_co2_last_year) - 2019) * 12)
      ) %>%
      mutate(
        Y2D_CO2_DIF_PREV_YEAR = Y2D_CO2 / Y2D_CO2_PY - 1,
        Y2D_DEP_DIF_PREV_YEAR = Y2D_DEP / Y2D_DEP_PY - 1,
        Y2D_CO2_DEP_DIF_PREV_YEAR = Y2D_CO2_DEP / Y2D_CO2_DEP_PY - 1,
        Y2D_CO2_DIF_2019 = Y2D_CO2 / Y2D_CO2_2019 - 1,
        Y2D_DEP_DIF_2019 = Y2D_DEP / Y2D_DEP_2019 - 1,
        Y2D_CO2_DEP_DIF_2019 = Y2D_CO2_DEP / Y2D_CO2_DEP_2019 - 1
      ) %>%
      select(
        iso_2letter,
        FLIGHT_MONTH,
        MONTH_TEXT,
        MM_CO2,
        MM_CO2_DIF_PREV_YEAR,
        MM_CO2_DIF_2019,
        MM_CO2_DEP,
        MM_CO2_DEP_DIF_PREV_YEAR,
        MM_CO2_DEP_DIF_2019,
        Y2D_CO2,
        Y2D_CO2_DIF_PREV_YEAR,
        Y2D_CO2_DIF_2019,
        Y2D_CO2_DEP,
        Y2D_CO2_DEP_DIF_PREV_YEAR,
        Y2D_CO2_DEP_DIF_2019
      ) %>%
      filter(FLIGHT_MONTH == st_co2_last_date) %>%
      right_join(state_iso, by = "iso_2letter") %>%
      select(-state) %>%
      arrange(iso_2letter)


  #### Join strings and save  ----
    ### https://www.lexjansen.com/pharmasug-cn/2021/SR/Pharmasug-China-2021-SR031.pdf

    st_json_app_j <- state_iso %>% select(iso_2letter, state) %>% arrange(iso_2letter)
    st_json_app_j <- st_json_app_j %>% cbind(select(st_daio_for_json, -c(iso_2letter))) %>%
      group_by(iso_2letter, state) %>%
      nest_legacy(.key = "st_daio")

    st_json_app_j <- st_json_app_j %>% cbind(select(st_dai_for_json, -c(iso_2letter))) %>%
      group_by(iso_2letter, state, st_daio) %>%
      nest_legacy(.key = "st_dai")

    st_json_app_j <- st_json_app_j %>% cbind(select(st_overflight_for_json, -c(iso_2letter))) %>%
      group_by(iso_2letter, state, st_daio, st_dai) %>%
      nest_legacy(.key = "st_ovf")

    st_json_app_j <- st_json_app_j %>% cbind(select(st_delay_for_json, -c(iso_2letter))) %>%
      group_by(iso_2letter, state, st_daio, st_dai, st_ovf) %>%
      nest_legacy(.key = "st_delay")

    st_json_app_j <- st_json_app_j %>% cbind(select(st_punct_for_json, -c(iso_2letter))) %>%
      group_by(iso_2letter, state, st_daio, st_dai, st_ovf, st_delay) %>%
      nest_legacy(.key = "st_punct")

    st_json_app_j <- st_json_app_j %>% cbind(select(st_billed_for_json, -c(iso_2letter))) %>%
      group_by(iso_2letter, state, st_daio, st_dai, st_ovf, st_delay, st_punct) %>%
      nest_legacy(.key = "st_billed")

    st_json_app_j <- st_json_app_j %>% cbind(select(st_co2_for_json, -c(iso_2letter))) %>%
      group_by(iso_2letter, state, st_daio, st_dai, st_ovf, st_delay, st_punct, st_billed) %>%
      nest_legacy(.key = "st_co2")

    st_json_app <- st_json_app_j %>%
      toJSON(., pretty = TRUE)


    # st_json_app_j <- state_iso %>% select(iso_2letter, state) %>% arrange(iso_2letter)
    # st_json_app_j$st_daio <- st_daio_for_json
    # st_json_app_j$st_dai <- st_dai_for_json
    # st_json_app_j$st_ofv <- st_overflight_for_json
    # st_json_app_j$st_delay <- st_delay_for_json
    # st_json_app_j$st_punct <- st_punct_for_json
    # st_json_app_j$st_billed <- st_billed_for_json
    # st_json_app_j$st_co2 <- st_co2_for_json
    #
    # st_json_app <- st_json_app_j %>%
    #   toJSON(., pretty = TRUE) %>%
    #   substr(., 1, nchar(.)-1) %>%
    #   substr(., 2, nchar(.))

    write(st_json_app, here(data_folder,"st_json_app.json"))
    write(st_json_app, paste0(archive_dir, "st_json_app.json"))
    write(st_json_app, paste0(archive_dir, today, "_st_json_app.json"))


# ____________________________________________________________________________________________
#
#    State ranking tables  -----
#
# ____________________________________________________________________________________________

  ## TRAFFIC ----
  ### Aircraft operators ----
    #### day ----
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
        DY_FLT_DIF_PREV_WEEK_PERC =   case_when(
          DAY_PREV_WEEK == 0 | is.na(DAY_PREV_WEEK) ~ NA,
          .default = CURRENT_DAY / DAY_PREV_WEEK - 1
        ),
        DY_FLT_DIF_PREV_YEAR_PERC = case_when(
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
        DY_FLT = CURRENT_DAY
      ) %>%
      select(
        ST_RANK,
        DY_RANK_DIF_PREV_WEEK,
        DY_AO_GRP_NAME,
        DY_TO_DATE,
        DY_FLT,
        DY_FLT_DIF_PREV_WEEK_PERC,
        DY_FLT_DIF_PREV_YEAR_PERC
      )

    #### week ----
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
        WK_FLT_DIF_PREV_WEEK_PERC =   case_when(
          PREV_ROLLING_WEEK == 0 | is.na(PREV_ROLLING_WEEK) ~ NA,
          .default = CURRENT_ROLLING_WEEK / PREV_ROLLING_WEEK - 1
        ),
        WK_FLT_DIF_PREV_YEAR_PERC = case_when(
          ROLLING_WEEK_PREV_YEAR == 0 | is.na(ROLLING_WEEK_PREV_YEAR) ~ NA,
          .default = CURRENT_ROLLING_WEEK / ROLLING_WEEK_PREV_YEAR - 1
        ),
        ST_RANK = paste0(tolower(COUNTRY_NAME), R_RANK)
      ) %>%
      rename(
        WK_AO_GRP_NAME = AO_GRP_NAME,
        WK_FROM_DATE = FROM_DATE,
        WK_TO_DATE = TO_DATE,
        WK_FLT_AVG = CURRENT_ROLLING_WEEK
      ) %>%
      select(
        ST_RANK,
        WK_RANK_DIF_PREV_WEEK,
        WK_AO_GRP_NAME,
        WK_FROM_DATE,
        WK_TO_DATE,
        WK_FLT_AVG,
        WK_FLT_DIF_PREV_WEEK_PERC,
        WK_FLT_DIF_PREV_YEAR_PERC
      )

    #### y2d ----
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
        Y2D_FLT_DIF_PREV_YEAR_PERC =   case_when(
          PREV_YEAR == 0 | is.na(PREV_YEAR) ~ NA,
          .default = CURRENT_YEAR / PREV_YEAR - 1
        ),
        Y2D_FLT_DIF_2019_PERC  = case_when(
          PERIOD_2019 == 0 | is.na(PERIOD_2019) ~ NA,
          .default = CURRENT_YEAR / PERIOD_2019 - 1
        ),
        ST_RANK = paste0(tolower(COUNTRY_NAME), R_RANK)
      ) %>%
      rename(
        Y2D_AO_GRP_NAME = AO_GRP_NAME,
        Y2D_TO_DATE = TO_DATE,
        Y2D_FLT_AVG = CURRENT_YEAR
      ) %>%
      select(
        ST_RANK,
        Y2D_RANK_DIF_PREV_YEAR,
        Y2D_AO_GRP_NAME,
        Y2D_TO_DATE,
        Y2D_FLT_AVG,
        Y2D_FLT_DIF_PREV_YEAR_PERC,
        Y2D_FLT_DIF_2019_PERC
      )

    #### main card ----
    st_ao_main_traffic <- st_ao_data_day_int %>%
      mutate(
        MAIN_TFC_AO_GRP_NAME = if_else(
          R_RANK <= 4,
          AO_GRP_NAME,
          NA
        ),
        MAIN_TFC_AO_GRP_FLT = if_else(
          R_RANK <= 4,
          CURRENT_DAY,
          NA
        ),
        ST_RANK = paste0(tolower(COUNTRY_NAME), R_RANK)
        ) %>%
      select(ST_RANK, MAIN_TFC_AO_GRP_NAME, MAIN_TFC_AO_GRP_FLT)

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
        MAIN_TFC_DIF_AO_GRP_FLT_DIF = if_else(
          RANK_DIF_AO_TFC <= 4,
          ST_TFC_AO_GRP_DIF,
          NA
        )
      ) %>%
      arrange(COUNTRY_NAME, desc(MAIN_TFC_DIF_AO_GRP_FLT_DIF)) %>%
      group_by(COUNTRY_NAME) %>%
      mutate(
        RANK_MAIN_DIF = row_number(),
        ST_RANK = paste0(tolower(COUNTRY_NAME), RANK_MAIN_DIF)
             ) %>%
      ungroup() %>%
      select(ST_RANK, MAIN_TFC_DIF_AO_GRP_NAME, MAIN_TFC_DIF_AO_GRP_FLT_DIF)

    #### join tables ----
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
      ungroup() %>%
      select(-ST_RANK, -state) %>%
      arrange (iso_2letter, RANK)

    # covert to json and save in app data folder and archive
    st_ao_data_j <- st_ao_data %>% toJSON(., pretty = TRUE)
    #xxx write(st_ao_data_j, here(data_folder,"ao_ranking_traffic.json"))
    write(st_ao_data_j, paste0(archive_dir, today, "_st_ao_ranking_traffic.json"))
    write(st_ao_data_j, paste0(archive_dir, "st_ao_ranking_traffic.json"))

  ### Airports ----
    #### day ----
    st_apt_data_day_raw <- read_xlsx(
      path  = fs::path_abs(
        str_glue(base_file),
        start = base_dir),
      sheet = "state_apt_day",
      range = cell_limits(c(1, 1), c(NA, NA))) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    st_apt_data_day_int <- st_apt_data_day_raw %>%
      mutate(TO_DATE = max(TO_DATE)) %>%
      spread(., key = FLAG_DAY, value = DEP_ARR) %>%
      arrange(COUNTRY_NAME, R_RANK) %>%
      mutate(
        DY_RANK_DIF_PREV_WEEK = case_when(
          is.na(RANK_PREV_WEEK) ~ RANK,
          .default = RANK_PREV_WEEK - RANK
        ),
        DY_FLT_DIF_PREV_WEEK_PERC =   case_when(
          DAY_PREV_WEEK == 0 | is.na(DAY_PREV_WEEK) ~ NA,
          .default = CURRENT_DAY / DAY_PREV_WEEK - 1
        ),
        DY_FLT_DIF_PREV_YEAR_PERC = case_when(
          DAY_PREV_YEAR == 0 | is.na(DAY_PREV_YEAR) ~ NA,
          .default = CURRENT_DAY / DAY_PREV_YEAR - 1
        ),
        ST_RANK = paste0(tolower(COUNTRY_NAME), R_RANK),
        ST_TFC_APT_DIF = CURRENT_DAY - DAY_PREV_WEEK
      )

    st_apt_data_day <- st_apt_data_day_int %>%
      rename(
        DY_APT_NAME = AIRPORT_NAME,
        DY_TO_DATE = TO_DATE,
        DY_FLT = CURRENT_DAY
      ) %>%
      select(
        ST_RANK,
        DY_RANK_DIF_PREV_WEEK,
        DY_APT_NAME,
        DY_TO_DATE,
        DY_FLT,
        DY_FLT_DIF_PREV_WEEK_PERC,
        DY_FLT_DIF_PREV_YEAR_PERC
      )

    #### week ----
    st_apt_data_wk_raw <- read_xlsx(
      path  = fs::path_abs(
        str_glue(base_file),
        start = base_dir),
      sheet = "state_apt_week",
      range = cell_limits(c(1, 1), c(NA, NA))) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    st_apt_data_wk <- st_apt_data_wk_raw %>%
      mutate(DEP_ARR = DEP_ARR / 7) %>%
      spread(., key = FLAG_ROLLING_WEEK, value = DEP_ARR) %>%
      arrange(COUNTRY_NAME, R_RANK) %>%
      mutate(
        WK_RANK_DIF_PREV_WEEK = case_when(
          is.na(RANK_PREV_WEEK) ~ RANK,
          .default = RANK_PREV_WEEK - RANK
        ),
        WK_FLT_DIF_PREV_WEEK_PERC =   case_when(
          PREV_ROLLING_WEEK == 0 | is.na(PREV_ROLLING_WEEK) ~ NA,
          .default = CURRENT_ROLLING_WEEK / PREV_ROLLING_WEEK - 1
        ),
        WK_FLT_DIF_PREV_YEAR_PERC = case_when(
          ROLLING_WEEK_PREV_YEAR == 0 | is.na(ROLLING_WEEK_PREV_YEAR) ~ NA,
          .default = CURRENT_ROLLING_WEEK / ROLLING_WEEK_PREV_YEAR - 1
        ),
        ST_RANK = paste0(tolower(COUNTRY_NAME), R_RANK)
      ) %>%
      rename(
        WK_APT_NAME = AIRPORT_NAME,
        WK_FROM_DATE = FROM_DATE,
        WK_TO_DATE = TO_DATE,
        WK_FLT_AVG = CURRENT_ROLLING_WEEK
      ) %>%
      select(
        ST_RANK,
        WK_RANK_DIF_PREV_WEEK,
        WK_APT_NAME,
        WK_FROM_DATE,
        WK_TO_DATE,
        WK_FLT_AVG,
        WK_FLT_DIF_PREV_WEEK_PERC,
        WK_FLT_DIF_PREV_YEAR_PERC
      )

    #### y2d ----
    st_apt_data_y2d_raw <- read_xlsx(
      path  = fs::path_abs(
        str_glue(base_file),
        start = base_dir),
      sheet = "state_apt_y2d",
      range = cell_limits(c(1, 1), c(NA, NA))) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    st_apt_data_y2d <- st_apt_data_y2d_raw %>%
      mutate(
        FROM_DATE = max(FROM_DATE),
        TO_DATE = max(TO_DATE),
        PERIOD =   case_when(
          YEAR == max(YEAR) ~ 'CURRENT_YEAR',
          YEAR == max(YEAR) - 1 ~ 'PREV_YEAR',
          .default = paste0('PERIOD_', YEAR)
        )
      ) %>%
      select(-DEP_ARR, -YEAR) %>%
      spread(., key = PERIOD, value = AVG_DEP_ARR) %>%
      arrange(COUNTRY_NAME, R_RANK) %>%
      mutate(
        Y2D_RANK_DIF_PREV_YEAR = case_when(
          is.na(RANK_PREV_YEAR) ~ RANK_CURRENT,
          .default = RANK_PREV_YEAR - RANK_CURRENT
        ),
        Y2D_FLT_DIF_PREV_YEAR_PERC =   case_when(
          PREV_YEAR == 0 | is.na(PREV_YEAR) ~ NA,
          .default = CURRENT_YEAR / PREV_YEAR - 1
        ),
        Y2D_FLT_DIF_2019_PERC  = case_when(
          PERIOD_2019 == 0 | is.na(PERIOD_2019) ~ NA,
          .default = CURRENT_YEAR / PERIOD_2019 - 1
        ),
        ST_RANK = paste0(tolower(COUNTRY_NAME), R_RANK)
      ) %>%
      rename(
        Y2D_APT_NAME = AIRPORT_NAME,
        Y2D_TO_DATE = TO_DATE,
        Y2D_FLT_AVG = CURRENT_YEAR
      ) %>%
      select(
        ST_RANK,
        Y2D_RANK_DIF_PREV_YEAR,
        Y2D_APT_NAME,
        Y2D_TO_DATE,
        Y2D_FLT_AVG,
        Y2D_FLT_DIF_PREV_YEAR_PERC,
        Y2D_FLT_DIF_2019_PERC
      )

    #### main card ----
    st_apt_main_traffic <- st_apt_data_day_int %>%
      mutate(
        MAIN_TFC_APT_NAME = if_else(
          R_RANK <= 4,
          AIRPORT_NAME,
          NA
        ),
        MAIN_TFC_APT_FLT = if_else(
          R_RANK <= 4,
          CURRENT_DAY,
          NA
        ),
        ST_RANK = paste0(tolower(COUNTRY_NAME), R_RANK)
      ) %>%
      select(ST_RANK, MAIN_TFC_APT_NAME, MAIN_TFC_APT_FLT)

    st_apt_main_traffic_dif <- st_apt_data_day_int %>%
      arrange(COUNTRY_NAME, desc(abs(ST_TFC_APT_DIF)), R_RANK) %>%
      group_by(COUNTRY_NAME) %>%
      mutate(RANK_DIF_APT_TFC = row_number()) %>%
      ungroup() %>%
      arrange(COUNTRY_NAME, R_RANK) %>%
      mutate(
        MAIN_TFC_DIF_APT_NAME = if_else(
          RANK_DIF_APT_TFC <= 4,
          AIRPORT_NAME,
          NA
        ),
        MAIN_TFC_DIF_APT_FLT_DIF = if_else(
          RANK_DIF_APT_TFC <= 4,
          ST_TFC_APT_DIF,
          NA
        )
      ) %>%
      arrange(COUNTRY_NAME, desc(MAIN_TFC_DIF_APT_FLT_DIF)) %>%
      group_by(COUNTRY_NAME) %>%
      mutate(
        RANK_MAIN_DIF = row_number(),
        ST_RANK = paste0(tolower(COUNTRY_NAME), RANK_MAIN_DIF)
      ) %>%
      ungroup() %>%
      select(ST_RANK, MAIN_TFC_DIF_APT_NAME, MAIN_TFC_DIF_APT_FLT_DIF)

    #### join tables ----
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
    st_apt_data <- state_iso_ranking %>%
      left_join(st_apt_main_traffic, by = "ST_RANK") %>%
      left_join(st_apt_main_traffic_dif, by = "ST_RANK") %>%
      left_join(st_apt_data_day, by = "ST_RANK") %>%
      left_join(st_apt_data_wk, by = "ST_RANK") %>%
      left_join(st_apt_data_y2d, by = "ST_RANK") %>%
      select(-ST_RANK)

    # covert to json and save in app data folder and archive
    st_apt_data_j <- st_apt_data %>% toJSON(., pretty = TRUE)
    #xxx write(st_ao_data_j, here(data_folder,"ao_ranking_traffic.json"))
    write(st_apt_data_j, paste0(archive_dir, today, "_st_apt_ranking_traffic.json"))
    write(st_apt_data_j, paste0(archive_dir, "st_apt_ranking_traffic.json"))

  ### State pair ----
    #### day ----
    st_st_data_day_raw <- read_xlsx(
      path  = fs::path_abs(
        str_glue(base_file),
        start = base_dir),
      sheet = "state_st_day",
      range = cell_limits(c(1, 1), c(NA, NA))) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    st_st_data_day_int <- st_st_data_day_raw %>%
      mutate(TO_DATE = max(TO_DATE)) %>%
      spread(., key = FLAG_DAY, value = TOT_MVT) %>%
      arrange(COUNTRY_NAME, R_RANK) %>%
      mutate(
        DY_RANK_DIF_PREV_WEEK = case_when(
          is.na(RANK_PREV_WEEK) ~ RANK,
          .default = RANK_PREV_WEEK - RANK
        ),
        DY_FLT_DIF_PREV_WEEK_PERC =   case_when(
          DAY_PREV_WEEK == 0 | is.na(DAY_PREV_WEEK) ~ NA,
          .default = CURRENT_DAY / DAY_PREV_WEEK - 1
        ),
        DY_FLT_DIF_PREV_YEAR_PERC = case_when(
          DAY_PREV_YEAR == 0 | is.na(DAY_PREV_YEAR) ~ NA,
          .default = CURRENT_DAY / DAY_PREV_YEAR - 1
        ),
        ST_RANK = paste0(tolower(COUNTRY_NAME), R_RANK),
        ST_TFC_CTRY_DIF = CURRENT_DAY - DAY_PREV_WEEK
      )

    st_st_data_day <- st_st_data_day_int %>%
      rename(
        DY_COUNTRY_NAME = FROM_TO_COUNTRY_NAME,
        DY_TO_DATE = TO_DATE,
        DY_FLT = CURRENT_DAY
      ) %>%
      select(
        ST_RANK,
        DY_RANK_DIF_PREV_WEEK,
        DY_COUNTRY_NAME,
        DY_TO_DATE,
        DY_FLT,
        DY_FLT_DIF_PREV_WEEK_PERC,
        DY_FLT_DIF_PREV_YEAR_PERC
      )

    #### week ----
    st_st_data_wk_raw <- read_xlsx(
      path  = fs::path_abs(
        str_glue(base_file),
        start = base_dir),
      sheet = "state_st_week",
      range = cell_limits(c(1, 1), c(NA, NA))) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    st_st_data_wk <- st_st_data_wk_raw %>%
      mutate(TOT_MVT = TOT_MVT / 7) %>%
      spread(., key = FLAG_ROLLING_WEEK, value = TOT_MVT) %>%
      arrange(COUNTRY_NAME, R_RANK) %>%
      mutate(
        WK_RANK_DIF_PREV_WEEK = case_when(
          is.na(RANK_PREV_WEEK) ~ RANK,
          .default = RANK_PREV_WEEK - RANK
        ),
        WK_FLT_DIF_PREV_WEEK_PERC =   case_when(
          PREV_ROLLING_WEEK == 0 | is.na(PREV_ROLLING_WEEK) ~ NA,
          .default = CURRENT_ROLLING_WEEK / PREV_ROLLING_WEEK - 1
        ),
        WK_FLT_DIF_PREV_YEAR_PERC = case_when(
          ROLLING_WEEK_PREV_YEAR == 0 | is.na(ROLLING_WEEK_PREV_YEAR) ~ NA,
          .default = CURRENT_ROLLING_WEEK / ROLLING_WEEK_PREV_YEAR - 1
        ),
        ST_RANK = paste0(tolower(COUNTRY_NAME), R_RANK)
      ) %>%
      rename(
        WK_COUNTRY_NAME = FROM_TO_COUNTRY_NAME,
        WK_FROM_DATE = FROM_DATE,
        WK_TO_DATE = TO_DATE,
        WK_FLT_AVG = CURRENT_ROLLING_WEEK
      ) %>%
      select(
        ST_RANK,
        WK_RANK_DIF_PREV_WEEK,
        WK_COUNTRY_NAME,
        WK_FROM_DATE,
        WK_TO_DATE,
        WK_FLT_AVG,
        WK_FLT_DIF_PREV_WEEK_PERC,
        WK_FLT_DIF_PREV_YEAR_PERC
      )

    #### y2d ----
    st_st_data_y2d_raw <- read_xlsx(
      path  = fs::path_abs(
        str_glue(base_file),
        start = base_dir),
      sheet = "state_st_y2d",
      range = cell_limits(c(1, 1), c(NA, NA))) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    st_st_data_y2d <- st_st_data_y2d_raw %>%
      mutate(
        FROM_DATE = max(FROM_DATE),
        TO_DATE = max(TO_DATE),
        PERIOD =   case_when(
          YEAR == max(YEAR) ~ 'CURRENT_YEAR',
          YEAR == max(YEAR) - 1 ~ 'PREV_YEAR',
          .default = paste0('PERIOD_', YEAR)
        )
      ) %>%
      select(-TOT_MVT, -YEAR) %>%
      spread(., key = PERIOD, value = AVG_MVT) %>%
      arrange(COUNTRY_NAME, R_RANK) %>%
      mutate(
        Y2D_RANK_DIF_PREV_YEAR = case_when(
          is.na(RANK_PREV_YEAR) ~ RANK_CURRENT,
          .default = RANK_PREV_YEAR - RANK_CURRENT
        ),
        Y2D_FLT_DIF_PREV_YEAR_PERC =   case_when(
          PREV_YEAR == 0 | is.na(PREV_YEAR) ~ NA,
          .default = CURRENT_YEAR / PREV_YEAR - 1
        ),
        Y2D_FLT_DIF_2019_PERC  = case_when(
          PERIOD_2019 == 0 | is.na(PERIOD_2019) ~ NA,
          .default = CURRENT_YEAR / PERIOD_2019 - 1
        ),
        ST_RANK = paste0(tolower(COUNTRY_NAME), R_RANK)
      ) %>%
      rename(
        Y2D_COUNTRY_NAME = FROM_TO_COUNTRY_NAME,
        Y2D_TO_DATE = TO_DATE,
        Y2D_FLT_AVG = CURRENT_YEAR
      ) %>%
      select(
        ST_RANK,
        Y2D_RANK_DIF_PREV_YEAR,
        Y2D_COUNTRY_NAME,
        Y2D_TO_DATE,
        Y2D_FLT_AVG,
        Y2D_FLT_DIF_PREV_YEAR_PERC,
        Y2D_FLT_DIF_2019_PERC
      )

    #### main card ----
    st_st_main_traffic <- st_st_data_day_int %>%
      mutate(
        MAIN_TFC_CTRY_NAME = if_else(
          R_RANK <= 4,
          FROM_TO_COUNTRY_NAME,
          NA
        ),
        MAIN_TFC_CTRY_FLT = if_else(
          R_RANK <= 4,
          CURRENT_DAY,
          NA
        ),
        ST_RANK = paste0(tolower(COUNTRY_NAME), R_RANK)
      ) %>%
      select(ST_RANK, MAIN_TFC_CTRY_NAME, MAIN_TFC_CTRY_FLT)

    st_st_main_traffic_dif <- st_st_data_day_int %>%
      arrange(COUNTRY_NAME, desc(abs(ST_TFC_CTRY_DIF)), R_RANK) %>%
      group_by(COUNTRY_NAME) %>%
      mutate(RANK_DIF_CTRY_TFC = row_number()) %>%
      ungroup() %>%
      arrange(COUNTRY_NAME, R_RANK) %>%
      mutate(
        MAIN_TFC_DIF_CTRY_NAME = if_else(
          RANK_DIF_CTRY_TFC <= 4,
          FROM_TO_COUNTRY_NAME,
          NA
        ),
        MAIN_TFC_DIF_CTRY_FLT_DIF = if_else(
          RANK_DIF_CTRY_TFC <= 4,
          ST_TFC_CTRY_DIF,
          NA
        )
      ) %>%
      arrange(COUNTRY_NAME, desc(MAIN_TFC_DIF_CTRY_FLT_DIF)) %>%
      group_by(COUNTRY_NAME) %>%
      mutate(
        RANK_MAIN_DIF = row_number(),
        ST_RANK = paste0(tolower(COUNTRY_NAME), RANK_MAIN_DIF)
      ) %>%
      ungroup() %>%
      select(ST_RANK, MAIN_TFC_DIF_CTRY_NAME, MAIN_TFC_DIF_CTRY_FLT_DIF)

    #### join tables ----
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
    st_st_data <- state_iso_ranking %>%
      left_join(st_st_main_traffic, by = "ST_RANK") %>%
      left_join(st_st_main_traffic_dif, by = "ST_RANK") %>%
      left_join(st_st_data_day, by = "ST_RANK") %>%
      left_join(st_st_data_wk, by = "ST_RANK") %>%
      left_join(st_st_data_y2d, by = "ST_RANK") %>%
      select(-ST_RANK)

    # covert to json and save in app data folder and archive
    st_st_data_j <- st_st_data %>% toJSON(., pretty = TRUE)
    #xxx write(st_ao_data_j, here(data_folder,"ao_ranking_traffic.json"))
    write(st_st_data_j, paste0(archive_dir, today, "_st_st_ranking_traffic.json"))
    write(st_st_data_j, paste0(archive_dir, "st_st_ranking_traffic.json"))

  ## DELAY ----
  ### ACC  ----
    #### day ----
    acc_delay_day_raw <-  read_xlsx(
      path  = fs::path_abs(
        str_glue(nw_base_file),
        start = nw_base_dir),
      sheet = "ACC_DAY_DELAY",
      range = cell_limits(c(5, 1), c(NA, 20))) %>%
      as_tibble() %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    acc_delay_day <- acc_delay_day_raw %>%
      arrange(desc(DLY_ER), NAME) %>%
      mutate(
        ICAO_code = UNIT_CODE,
        DY_ACC_NAME = NAME,
        DY_ACC_ER_DLY = DLY_ER,
        DY_ACC_ER_DLY_FLT = DLY_ER / FLIGHT,
        DY_TO_DATE = ENTRY_DATE) %>%
      right_join(acc, by = "ICAO_code") %>%
      left_join(state_iso, by = "iso_2letter") %>%
      group_by(iso_2letter) %>%
      arrange(iso_2letter, desc(DY_ACC_ER_DLY), DY_ACC_NAME) %>%
      mutate (
        DY_RANK = row_number(),
        ST_RANK = paste0(tolower(state), DY_RANK),
      ) %>%
      ungroup() %>%
      select(
        ST_RANK,
        DY_RANK,
        DY_ACC_NAME,
        DY_TO_DATE,
        DY_ACC_ER_DLY,
        DY_ACC_ER_DLY_FLT
        )

    #### week ----
    acc_delay_week_raw <-  read_xlsx(
      path  = fs::path_abs(
        str_glue(nw_base_file),
        start = nw_base_dir),
      sheet = "ACC_WEEK_DELAY",
      range = cell_limits(c(5, 1), c(NA, 16))) %>%
      as_tibble() %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    acc_delay_week <- acc_delay_week_raw %>%
      arrange(desc(DAILY_DLY_ER), NAME) %>%
      mutate(
        ICAO_code = UNIT_CODE,
        WK_ACC_NAME = NAME,
        WK_ACC_ER_DLY = DAILY_DLY_ER,
        WK_ACC_ER_DLY_FLT = DAILY_DLY_ER / DAILY_FLIGHT,
        WK_FROM_DATE = MIN_ENTRY_DATE,
        WK_TO_DATE = MAX_ENTRY_DATE
        ) %>%
      right_join(acc, by = "ICAO_code") %>%
      left_join(state_iso, by = "iso_2letter") %>%
      group_by(iso_2letter) %>%
      arrange(iso_2letter, desc(WK_ACC_ER_DLY), WK_ACC_NAME) %>%
      mutate (
        WK_RANK = row_number(),
        ST_RANK = paste0(tolower(state), WK_RANK),
      ) %>%
      ungroup() %>%
      select(
        ST_RANK,
        WK_RANK,
        WK_ACC_NAME,
        WK_FROM_DATE,
        WK_TO_DATE,
        WK_ACC_ER_DLY,
        WK_ACC_ER_DLY_FLT
      )

    #### y2d ----
    acc_delay_y2d_raw <-  read_xlsx(
      path  = fs::path_abs(
        str_glue(nw_base_file),
        start = nw_base_dir),
      sheet = "ACC_Y2D_DELAY",
      range = cell_limits(c(7, 1), c(NA, 13))) %>%
      as_tibble() %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
      rename(Y2D_FROM_DATE = 13)

    acc_delay_y2d <- acc_delay_y2d_raw %>%
      arrange(desc(Y2D_AVG_DLY), NAME) %>%
      mutate(
        ICAO_code = UNIT_CODE,
        Y2D_ACC_NAME = NAME,
        Y2D_ACC_ER_DLY = Y2D_AVG_DLY,
        Y2D_ACC_ER_DLY_FLT = Y2D_AVG_DLY / Y2D_AVG_FLIGHT,
        Y2D_TO_DATE = ENTRY_DATE
      ) %>%
      right_join(acc, by = "ICAO_code") %>%
      left_join(state_iso, by = "iso_2letter") %>%
      group_by(iso_2letter) %>%
      arrange(iso_2letter, desc(Y2D_ACC_ER_DLY), Y2D_ACC_NAME) %>%
      mutate (
        Y2D_RANK = row_number(),
        ST_RANK = paste0(tolower(state), Y2D_RANK),
      ) %>%
      ungroup() %>%
      select(
        ST_RANK,
        Y2D_RANK,
        Y2D_ACC_NAME,
        Y2D_FROM_DATE,
        Y2D_TO_DATE,
        Y2D_ACC_ER_DLY,
        Y2D_ACC_ER_DLY_FLT
      )

    # no main card

    #### join tables ----
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
    st_acc_delay <- state_iso_ranking %>%
      left_join(acc_delay_day, by = "ST_RANK") %>%
      left_join(acc_delay_week, by = "ST_RANK") %>%
      left_join(acc_delay_y2d, by = "ST_RANK") %>%
      select(-ST_RANK)

    # covert to json and save in app data folder and archive
    st_acc_delay_j <- st_acc_delay %>% toJSON(., pretty = TRUE)
    #xxx write(st_acc_delay_j, here(data_folder,"st_acc_ranking_delay.json"))
    write(st_acc_delay_j, paste0(archive_dir, today, "_st_acc_ranking_delay.json"))
    write(st_acc_delay_j, paste0(archive_dir, "st_acc_ranking_delay.json"))

  ### Airport ----
    # raw data
    st_apt_delay_raw <-  read_xlsx(
      path  = fs::path_abs(
        str_glue(nw_base_file),
        start = nw_base_dir),
      sheet = "APT_DELAY",
      range = cell_limits(c(5, 2), c(NA, 50))) %>%
      as_tibble() %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
      rename(
        ICAO_CODE = ARP_CODE,
        APT_NAME = ARP_NAME
        ) %>%
      left_join(airport, by = "ICAO_CODE") %>%
      left_join(state_iso, by = "iso_2letter")

    #### day ----
    st_apt_delay_day <- st_apt_delay_raw %>%
      arrange(desc(DLY_ARR),APT_NAME) %>%
      mutate(
        DY_APT_NAME = APT_NAME,
        DY_APT_ARR_DLY = DLY_ARR,
        DY_APT_ARR_DLY_FLT = ifelse(FLT_ARR == 0, 0, round(DLY_ARR / FLT_ARR, 2)),
        DY_TO_DATE = FLIGHT_DATE) %>%
      group_by(iso_2letter) %>%
      arrange(iso_2letter, desc(DY_APT_ARR_DLY), DY_APT_NAME) %>%
      mutate (
        DY_RANK = row_number(),
        ST_RANK = paste0(tolower(state), DY_RANK),
      ) %>%
      ungroup() %>%
      select(
        ST_RANK,
        DY_RANK,
        DY_APT_NAME,
        DY_TO_DATE,
        DY_APT_ARR_DLY,
        DY_APT_ARR_DLY_FLT
      )

    #### week ----
    st_apt_delay_week <- st_apt_delay_raw %>%
      arrange(desc(ROLL_WEEK_DLY_ARR),APT_NAME) %>%
      mutate(
        WK_APT_NAME = APT_NAME,
        WK_APT_ARR_DLY = ROLL_WEEK_DLY_ARR,
        WK_APT_ARR_DLY_FLT = ifelse(ROLL_WEEK_ARR == 0, 0, round(ROLL_WEEK_DLY_ARR / ROLL_WEEK_ARR,2)),
        WK_FROM_DATE =  FLIGHT_DATE +  days(-6),
        WK_TO_DATE = FLIGHT_DATE
        ) %>%
      group_by(iso_2letter) %>%
      arrange(iso_2letter, desc(WK_APT_ARR_DLY), WK_APT_NAME) %>%
      mutate (
        WK_RANK = row_number(),
        ST_RANK = paste0(tolower(state), WK_RANK),
      ) %>%
      ungroup() %>%
      select(
        ST_RANK,
        WK_RANK,
        WK_APT_NAME,
        WK_FROM_DATE,
        WK_TO_DATE,
        WK_APT_ARR_DLY,
        WK_APT_ARR_DLY_FLT
      )

    #### y2d ----
    st_apt_delay_y2d <- st_apt_delay_raw %>%
      arrange(desc(Y2D_AVG_DLY_ARR),APT_NAME) %>%
      mutate(
        Y2D_APT_NAME = APT_NAME,
        Y2D_APT_ARR_DLY = Y2D_AVG_DLY_ARR,
        Y2D_APT_ARR_DLY_FLT = ifelse(Y2D_AVG_ARR == 0, 0, round(Y2D_AVG_DLY_ARR / Y2D_AVG_ARR, 2)),
        Y2D_TO_DATE = FLIGHT_DATE
      ) %>%
      group_by(iso_2letter) %>%
      arrange(iso_2letter, desc(Y2D_APT_ARR_DLY), Y2D_APT_NAME) %>%
      mutate (
        Y2D_RANK = row_number(),
        ST_RANK = paste0(tolower(state), Y2D_RANK),
      ) %>%
      ungroup() %>%
      select(
        ST_RANK,
        Y2D_RANK,
        Y2D_APT_NAME,
        Y2D_TO_DATE,
        Y2D_APT_ARR_DLY,
        Y2D_APT_ARR_DLY_FLT
      )

    # no main card

    #### join tables ----
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
    st_apt_delay <- state_iso_ranking %>%
      left_join(st_apt_delay_day, by = "ST_RANK") %>%
      left_join(st_apt_delay_week, by = "ST_RANK") %>%
      left_join(st_apt_delay_y2d, by = "ST_RANK") %>%
      select(-ST_RANK)

    # covert to json and save in app data folder and archive
    st_apt_delay_j <- st_apt_delay %>% toJSON(., pretty = TRUE)
    #xxx write(st_apt_delay_j, here(data_folder,"st_apt_ranking_delay.json"))
    write(st_apt_delay_j, paste0(archive_dir, today, "_st_apt_ranking_delay.json"))
    write(st_apt_delay_j, paste0(archive_dir, "st_apt_ranking_delay.json"))

  ## PUNTCUALITY ----
  ### Airport ----
    # raw data
    query <- "
     WITH
        DIM_AIRPORT as (
          SELECT
            a.code as apt_code, a.id as apt_id, a.dashboard_name as apt_name,
            a.ISO_COUNTRY_CODE
          FROM prudev.pru_airport a
        )

      , LIST_AIRPORT as (
            select distinct
                a.ICAO_CODE as apt_code,
                b.apt_name,
                b.iso_country_code
            from LDW_VDM.VIEW_FAC_PUNCTUALITY_AP_DAY a
            left join DIM_AIRPORT b on a.icao_code = b.apt_code
            order by 1

        ),

        LIST_STATE as (
          SELECT DISTINCT
            AIU_ISO_COUNTRY_NAME as EC_ISO_CT_NAME,
            AIU_ISO_COUNTRY_CODE AS EC_ISO_CT_CODE
          FROM prudev.pru_country_iso
          WHERE till > TRUNC(SYSDATE)-1
        ),

        APT_DAY AS (
          SELECT
                  a.apt_code,
                  a.apt_name,
                  a.ISO_COUNTRY_CODE,
                  t.year,
                  t.month,
                  t.week,
                  t.week_nb_year,
                  t.day_type,
                  t.day_of_week_nb AS day_of_week,
                  t.day_date
          FROM LIST_AIRPORT a, pru_time_references t
          WHERE
             t.day_date >= to_date('24-12-2018','DD-MM-YYYY')
             AND t.day_date < trunc(sysdate)
          )

          SELECT
            a.* , b.*, c.EC_ISO_CT_NAME
          FROM APT_DAY a
          left join LDW_VDM.VIEW_FAC_PUNCTUALITY_AP_DAY b on a.day_date = b.\"DATE\" and a.apt_code = b.icao_code
          left join LIST_STATE c on a.ISO_COUNTRY_CODE = c.EC_ISO_CT_CODE
          where a.apt_code<>'LTBA'
          order by a.apt_CODE, b.\"DATE\"
   "

    st_apt_punct_raw <- export_query(query)

    last_punctuality_day <-  max(st_apt_punct_raw$DAY_DATE)

    # calc
    st_apt_punct_calc <- st_apt_punct_raw %>%
      # select(DAY_DATE, APT_NAME, DAY_ARR_PUNCT, RANK)
      group_by(APT_NAME) %>%
      arrange(DAY_DATE) %>%
      mutate(
        DY_ARR_PUNCT = ARR_PUNCTUALITY_PERCENTAGE / 100,
        DY_ARR_PUNCT_DIF_PREV_WEEK = (DY_ARR_PUNCT - lag(DY_ARR_PUNCT, 7)),
        DY_ARR_PUNCT_DIF_PREV_YEAR = (DY_ARR_PUNCT - lag(DY_ARR_PUNCT, 364)),
        WK_ARR_PUNCT = rollsum(ARR_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") / rollsum(ARR_SCHEDULE_FLIGHT,7, fill = NA, align = "right"),
        iso_2letter = ISO_COUNTRY_CODE,
        state = EC_ISO_CT_NAME
      )  %>%
      ungroup()

    #### day ----
    st_apt_punct_dy <- st_apt_punct_calc %>%
      group_by(iso_2letter, DAY_DATE) %>%
      arrange(iso_2letter, desc(DY_ARR_PUNCT), APT_NAME) %>%
      mutate(RANK = row_number()) %>%
      ungroup() %>%
      group_by(APT_NAME) %>%
      arrange(DAY_DATE) %>%
      mutate(
             DY_RANK_DIF_PREV_WEEK = lag(RANK, 7) - RANK,
             DY_APT_NAME = APT_NAME,
             DY_TO_DATE = round_date(DAY_DATE, "day"),
             ST_RANK = paste0(tolower(state), RANK)
      ) %>%
      ungroup() %>%
      filter(DAY_DATE == last_punctuality_day) %>%
      group_by(iso_2letter) %>%
      arrange(iso_2letter, desc(DY_ARR_PUNCT), DY_APT_NAME) %>%
      ungroup() %>%
      select(
        ST_RANK,
        DY_RANK_DIF_PREV_WEEK,
        DY_APT_NAME,
        DY_TO_DATE,
        DY_ARR_PUNCT,
        DY_ARR_PUNCT_DIF_PREV_WEEK,
        DY_ARR_PUNCT_DIF_PREV_YEAR
      )

    #### week ----
    st_apt_punct_wk <- st_apt_punct_calc %>%
      group_by(iso_2letter, DAY_DATE) %>%
      arrange(iso_2letter, desc(WK_ARR_PUNCT), APT_NAME) %>%
      mutate(RANK = row_number()) %>%
      ungroup() %>%
      group_by(APT_NAME) %>%
      arrange(DAY_DATE) %>%
      mutate(
        WK_RANK_DIF_PREV_WEEK = lag(RANK, 7) - RANK,
        WK_APT_NAME = APT_NAME,
        WK_TO_DATE = round_date(DAY_DATE, "day"),
        WK_FROM_DATE = round_date(DAY_DATE, "day") + days(-7),
        WK_ARR_PUNCT_DIF_PREV_WEEK = (WK_ARR_PUNCT - lag(WK_ARR_PUNCT, 7)),
        WK_ARR_PUNCT_DIF_PREV_YEAR = (WK_ARR_PUNCT - lag(WK_ARR_PUNCT, 364)),
        ST_RANK = paste0(tolower(state), RANK)
      ) %>%
      ungroup() %>%
      filter(DAY_DATE == last_punctuality_day) %>%
      group_by(iso_2letter) %>%
      arrange(iso_2letter, desc(WK_ARR_PUNCT), WK_APT_NAME) %>%
      ungroup() %>%
      select(
        ST_RANK,
        WK_RANK_DIF_PREV_WEEK,
        WK_APT_NAME,
        WK_FROM_DATE,
        WK_TO_DATE,
        WK_ARR_PUNCT,
        WK_ARR_PUNCT_DIF_PREV_WEEK,
        WK_ARR_PUNCT_DIF_PREV_YEAR
      )

    #### y2d ----
    st_apt_punct_y2d <- st_apt_punct_calc %>%
      mutate(MONTH_DAY = as.numeric(format(DAY_DATE, format = "%m%d"))) %>%
      filter(MONTH_DAY <= as.numeric(format(last_punctuality_day, format = "%m%d"))) %>%
      mutate(YEAR = as.numeric(format(DAY_DATE, format="%Y"))) %>%
      group_by(state, APT_NAME, ICAO_CODE, YEAR) %>%
      summarise (Y2D_ARR_PUNCT = sum(ARR_PUNCTUAL_FLIGHTS, na.rm=TRUE) / sum(ARR_SCHEDULE_FLIGHT, na.rm=TRUE)
      ) %>%
      ungroup() %>%
      group_by(state, YEAR) %>%
      arrange(desc(Y2D_ARR_PUNCT), APT_NAME) %>%
      mutate(RANK = row_number(),
             Y2D_RANK = RANK) %>%
      ungroup() %>%
      group_by(APT_NAME) %>%
      arrange(YEAR) %>%
      mutate(
        Y2D_RANK_DIF_PREV_YEAR = lag(RANK, 1) - RANK,
        Y2D_ARR_PUNCT_DIF_PREV_YEAR = (Y2D_ARR_PUNCT - lag(Y2D_ARR_PUNCT, 1)),
        Y2D_ARR_PUNCT_DIF_2019 = (Y2D_ARR_PUNCT - lag(Y2D_ARR_PUNCT, max(YEAR) - 2019)),
        ST_RANK = paste0(tolower(state), RANK)
      )  %>%
      ungroup() %>%
      filter(YEAR == max(YEAR), RANK < 11) %>%
      mutate(Y2D_APT_NAME = APT_NAME) %>%
      select(
        ST_RANK,
        Y2D_RANK_DIF_PREV_YEAR,
        Y2D_APT_NAME,
        Y2D_ARR_PUNCT,
        Y2D_ARR_PUNCT_DIF_PREV_YEAR,
        Y2D_ARR_PUNCT_DIF_2019
      )

    # no main card

    #### join tables ----
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
    st_apt_punctuality <- state_iso_ranking %>%
      left_join(st_apt_punct_dy, by = "ST_RANK") %>%
      left_join(st_apt_punct_wk, by = "ST_RANK") %>%
      left_join(st_apt_punct_y2d, by = "ST_RANK") %>%
      select(-ST_RANK)

    # covert to json and save in app data folder and archive
    st_apt_punctuality_j <- st_apt_punctuality %>% toJSON(., pretty = TRUE)
    #xxx write(st_apt_punctuality_j, here(data_folder,"st_apt_ranking_punctuality.json"))
    write(st_apt_punctuality_j, paste0(archive_dir, today, "_st_apt_ranking_punctuality.json"))
    write(st_apt_punctuality_j, paste0(archive_dir, "st_apt_ranking_punctuality.json"))


# ____________________________________________________________________________________________
#
#    State graphs  -----
#
# ____________________________________________________________________________________________

  ## TRAFFIC ----
    ### 7-day DAIO avg ----
    st_daio_evo_app <- st_daio_data_zone  %>%
      select(
        iso_2letter,
        daio_zone,
        FLIGHT_DATE,
        AVG_ROLLING_WEEK,
        AVG_ROLLING_WEEK_PREV_YEAR,
        AVG_ROLLING_WEEK_2020,
        AVG_ROLLING_WEEK_2019
        )

    column_names <- c('iso_2letter', 'daio_zone', 'FLIGHT_DATE', last_year, last_year-1, 2020, 2019)
    colnames(st_daio_evo_app) <- column_names

    ### nest data
    st_daio_evo_app_long <- st_daio_evo_app %>%
      pivot_longer(-c(iso_2letter, daio_zone, FLIGHT_DATE), names_to = 'year', values_to = 'daio') %>%
      group_by(iso_2letter, daio_zone, FLIGHT_DATE) %>%
      nest_legacy(.key = "statistics")


    st_daio_evo_app_j <- st_daio_evo_app_long %>% toJSON(., pretty = TRUE)
    # write(nw_traffic_evo_app_j, here(data_folder,"nw_traffic_evo_chart_daily.json"))
    write(st_daio_evo_app_j, paste0(archive_dir, today, "_st_daio_evo_chart_daily.json"))
    write(st_daio_evo_app_j, paste0(archive_dir, "st_daio_evo_chart_daily.json"))

    ### 7-day DAI avg ----
    st_dai_evo_app <- st_dai_data_zone %>%
      select(
        iso_2letter,
        daio_zone,
        FLIGHT_DATE,
        AVG_ROLLING_WEEK,
        AVG_ROLLING_WEEK_PREV_YEAR,
        AVG_ROLLING_WEEK_2020,
        AVG_ROLLING_WEEK_2019
      )

    column_names <- c('iso_2letter', 'daio_zone', 'FLIGHT_DATE', last_year, last_year-1, 2020, 2019)
    colnames(st_dai_evo_app) <- column_names

    ### nest data
    st_dai_evo_app_long <- st_dai_evo_app %>%
      pivot_longer(-c(iso_2letter, daio_zone, FLIGHT_DATE), names_to = 'year', values_to = 'dai') %>%
      group_by(iso_2letter, daio_zone, FLIGHT_DATE) %>%
      nest_legacy(.key = "statistics")

    st_dai_evo_app_j <- st_dai_evo_app_long %>% toJSON(., pretty = TRUE)
    # write(nw_traffic_evo_app_j, here(data_folder,"nw_traffic_evo_chart_daily.json"))
    write(st_dai_evo_app_j, paste0(archive_dir, today, "_st_dai_evo_chart_daily.json"))
    write(st_dai_evo_app_j, paste0(archive_dir, "st_dai_evo_chart_daily.json"))

    ### 7-day OVF avg ----
    st_ovf_evo_app <- st_overflight_data_zone %>%
      select(
        iso_2letter,
        daio_zone,
        FLIGHT_DATE,
        AVG_ROLLING_WEEK,
        AVG_ROLLING_WEEK_PREV_YEAR,
        AVG_ROLLING_WEEK_2020,
        AVG_ROLLING_WEEK_2019
      )

    column_names <- c('iso_2letter', 'daio_zone', 'FLIGHT_DATE', last_year, last_year-1, 2020, 2019)
    colnames(st_ovf_evo_app) <- column_names

    ### nest data
    st_ovf_evo_app_long <- st_ovf_evo_app %>%
      pivot_longer(-c(iso_2letter, daio_zone, FLIGHT_DATE), names_to = 'year', values_to = 'ovf') %>%
      group_by(iso_2letter, daio_zone, FLIGHT_DATE) %>%
      nest_legacy(.key = "statistics")

    st_ovf_evo_app_j <- st_ovf_evo_app_long %>% toJSON(., pretty = TRUE)
    # write(nw_traffic_evo_app_j, here(data_folder,"nw_traffic_evo_chart_daily.json"))
    write(st_ovf_evo_app_j, paste0(archive_dir, today, "_st_ovf_evo_chart_daily.json"))
    write(st_ovf_evo_app_j, paste0(archive_dir, "st_ovf_evo_chart_daily.json"))

  ## PUNCTUALITY ----
    ### 7-day punctuality avg ----
    st_punct_evo_app <- st_punct_raw %>%
      filter(DAY_DATE >= as.Date(paste0("01-01-", last_year-2), format = "%d-%m-%Y")) %>%
      arrange(ISO_2LETTER, DAY_DATE) %>%
      mutate(
        DEP_PUN_WK = rollsum(DEP_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") /
               rollsum(DEP_SCHEDULE_FLIGHT,7, fill = NA, align = "right") * 100,
        ARR_PUN_WK = rollsum(ARR_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") /
               rollsum(ARR_SCHEDULE_FLIGHT,7, fill = NA, align = "right") * 100,
        OP_FLT_WK = 100 - rollsum(MISSING_SCHED_FLIGHTS, 7, fill = NA, align = "right") /
               rollsum((MISSING_SCHED_FLIGHTS+DEP_FLIGHTS_NO_OVERFLIGHTS),7, fill = NA, align = "right")*100
             ) %>%
      filter(DATE >= as.Date(paste0("01-01-", last_year-1), format = "%d-%m-%Y")) %>%
      mutate(iso_2letter = ISO_2LETTER) %>%
      right_join(state_iso, by ="iso_2letter") %>%
      select(
        iso_2letter,
        state,
        DAY_DATE,
        DEP_PUN_WK,
        ARR_PUN_WK,
        OP_FLT_WK
      )

    column_names <- c('iso_2letter',
                      'state',
                      'FLIGHT_DATE',
                      "Departure punct.",
                      "Arrival punct.",
                      "Operated schedules")

    colnames(st_punct_evo_app) <- column_names

    ### nest data
    st_punct_evo_app_long <- st_punct_evo_app %>%
      pivot_longer(-c(iso_2letter, state, FLIGHT_DATE), names_to = 'metric', values_to = 'value') %>%
      group_by(iso_2letter, state, FLIGHT_DATE) %>%
      nest_legacy(.key = "statistics")


    st_punct_evo_app_j <- st_punct_evo_app_long %>% toJSON(., pretty = TRUE)
    # write(nw_punct_evo_app_j, here(data_folder,"nw_punct_evo_chart.json"))
    write(st_punct_evo_app_j, paste0(archive_dir, today, "_st_punct_evo_chart.json"))
    write(st_punct_evo_app_j, paste0(archive_dir, "st_punct_evo_chart.json"))

  ## DELAY ----
    ### Delay cause ----
    st_delay_data <-  read_xlsx(
      path  = fs::path_abs(
        str_glue(base_file),
        start = base_dir),
      sheet = "state_delay",
      range = cell_limits(c(1, 1), c(NA, NA))) %>%
      as_tibble() %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    st_delay_last_day <- st_delay_data %>%
      filter(FLIGHT_DATE == max(LAST_DATA_DAY)) %>%
      mutate(daio_zone_lc = tolower(COUNTRY_NAME)) %>%
      right_join(state_daio, by = "daio_zone_lc", relationship = "many-to-many")

    st_delay_cause_data <-  read_xlsx(
      path  = fs::path_abs(
        str_glue(base_file),
        start = base_dir),
      sheet = "state_delay_cause",
      range = cell_limits(c(1, 1), c(NA, NA))) %>%
      as_tibble() %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
      filter(YEAR >= last_year) %>%
      mutate(daio_zone_lc = tolower(COUNTRY_NAME)) %>%
      right_join(state_daio, by = "daio_zone_lc", relationship = "many-to-many")


  ## BILLING ----
    st_billing_evo <- st_billing %>%
      arrange(iso_2letter, year, month) %>%
      mutate(
        total_billing = total_billing/10^6,
        total_billing_py = lag(total_billing, 12),
        total_billing_dif_mm_perc = total_billing / total_billing_py -1
      ) %>%
      group_by(iso_2letter, corrected_cz, year) %>%
      mutate(
        total_billing_y2d = cumsum(total_billing)
      ) %>%
      ungroup() %>%
      mutate(
        total_billing_y2d_py = lag(total_billing_y2d, 12),
        total_billing_dif_y2d_perc = total_billing_y2d / total_billing_y2d_py -1
      ) %>%
      filter(year == last_billing_year) %>%
      select(
        iso_2letter,
        cz_proper,
        month,
        total_billing,
        total_billing_py,
        total_billing_dif_mm_perc,
        total_billing_dif_y2d_perc
        ) %>%
      mutate(
        month = month.name[month],
        min_right_axis = -0.2,
        max_right_axis = 1.3
      )

    column_names <- c(
      'iso_2letter',
      'charging_zone',
      "month",
      last_billing_year,
      last_billing_year - 1,
      paste0("Monthly variation vs ", last_billing_year - 1),
      paste0 ("Year-to-date variation vs ", last_billing_year - 1),
      "min_right_axis",
      "max_right_axis"
    )

    colnames(st_billing_evo) <- column_names

    ### nest data
    st_billing_evo_long <- st_billing_evo %>%
      pivot_longer(-c(iso_2letter, charging_zone, month), names_to = 'metric', values_to = 'value') %>%
      group_by(iso_2letter, charging_zone, month) %>%
      nest_legacy(.key = "statistics")


    st_billing_evo_j <- st_billing_evo_long %>% toJSON(., pretty = TRUE)
    # write(nw_billing_evo_j, here(data_folder,"nw_billing_evo_chart.json"))
    write(st_billing_evo_j, paste0(archive_dir, today, "_st_billing_evo.json"))
    write(st_billing_evo_j, paste0(archive_dir, "st_billing_evo.json"))

  ## CO2 ----
    st_co2_data_filtered <- st_co2_data_raw %>%
      mutate(co2_state = STATE_NAME) %>%
      right_join(state_co2, by = "co2_state") %>%
      left_join(state_iso, by = "iso_2letter") %>%
      select(-c(STATE_NAME, STATE_CODE, co2_state, CREA_DATE) )

    st_co2_evo <- st_co2_data_filtered %>%
      group_by(iso_2letter, state, FLIGHT_MONTH)%>%
      summarise(TTF = sum(TF), TCO2 = sum(CO2_QTY_TONNES)) %>%
      mutate(
        YEAR = as.numeric(format(FLIGHT_MONTH,'%Y')),
        MONTH = as.numeric(format(FLIGHT_MONTH,'%m'))
      )%>%
      arrange(iso_2letter, FLIGHT_MONTH) %>%
      mutate(
        DEP_IDX = TTF / first(TTF) * 100,
        CO2_IDX = TCO2 / first(TCO2) * 100,
        FLIGHT_MONTH = ceiling_date(as_date(FLIGHT_MONTH), unit = 'month') - 1
      ) %>%
      select(
        iso_2letter, state,
        FLIGHT_MONTH,
        CO2_IDX,
        DEP_IDX
      )

    column_names <- c(
      "iso_2letter",
      "state",
      "month",
      "CO2 index",
      "Departures index"
    )

    colnames(st_co2_evo) <- column_names

    ### nest data
    st_co2_evo_long <- st_co2_evo %>%
      pivot_longer(-c(iso_2letter, state, month), names_to = 'metric', values_to = 'value') %>%
      group_by(iso_2letter, state, month) %>%
      nest_legacy(.key = "statistics")

    st_co2_evo_j <- st_co2_evo_long %>% toJSON(., pretty = TRUE)
    # write(st_co2_evo_j, here(data_folder,"nw_co2_evo_chart.json"))
    write(st_co2_evo_j, paste0(archive_dir, "st_co2_evo.json"))
    write(st_co2_evo_j, paste0(archive_dir, today, "_st_co2_evo.json"))

