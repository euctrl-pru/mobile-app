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
    range = cell_limits(c(2, 10), c(NA, 12))) %>%
    as_tibble()

  state_co2 <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(base_file),
      start = base_dir),
    sheet = "lists",
    range = cell_limits(c(2, 15), c(NA, 16))) %>%
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
      mutate(daio_zone_lc = tolower(COUNTRY_NAME)) %>%
      right_join(state_daio, by = "daio_zone_lc", relationship = "many-to-many")

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


    ###############################################
    # traffic dai data
    st_dai_data <-  read_xlsx(
      path  = fs::path_abs(
        str_glue(base_file),
        start = base_dir),
      sheet = "state_dai",
      range = cell_limits(c(1, 1), c(NA, NA))) %>%
      as_tibble() %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))


    st_dai_last_day <- st_dai_data %>%
      filter(FLIGHT_DATE == last_day) %>%
      mutate(daio_zone_lc = tolower(COUNTRY_NAME)) %>%
      right_join(state_daio, by = "daio_zone_lc", relationship = "many-to-many")

    st_dai_json <- st_dai_last_day %>%
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

    st_dai_json <- st_dai_json %>%
      toJSON(., digits = 10) %>%
      substr(., 1, nchar(.)-1) %>%
      substr(., 2, nchar(.))


    ###############################################
    # punctuality data
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

    st_punct_d_w <- st_punct_raw %>%
      arrange(ISO_2LETTER, DAY_DATE) %>%
      mutate(
             ARR_PUNCTUALITY_PERCENTAGE = case_when(
               ARR_SCHEDULE_FLIGHT == 0 ~ NA,
               .default = ARR_PUNCTUAL_FLIGHTS/ARR_SCHEDULE_FLIGHT * 100
               ),
             DEP_PUNCTUALITY_PERCENTAGE = case_when(
               DEP_SCHEDULE_FLIGHT == 0 ~ NA,
               .default = DEP_PUNCTUAL_FLIGHTS/DEP_SCHEDULE_FLIGHT * 100
             ),
             ARR_PUN_PREV_YEAR = lag(ARR_PUNCTUALITY_PERCENTAGE, 364),
             DEP_PUN_PREV_YEAR = lag(DEP_PUNCTUALITY_PERCENTAGE, 364),
             ARR_PUN_2019 =if_else(YEAR == last_year_punct,
                                   lag(ARR_PUNCTUALITY_PERCENTAGE,
                                       364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7),
                                   NA),
             DEP_PUN_2019 =if_else(YEAR == last_year_punct,
                                   lag(DEP_PUNCTUALITY_PERCENTAGE,
                                       364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7),
                                   NA),
             DAY_2019 = if_else(YEAR == last_year_punct,
                                lag(DAY_DATE,
                                    364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7)
                                , NA),
             DAY_ARR_PUN_DIF_PY_PERC = ARR_PUNCTUALITY_PERCENTAGE - ARR_PUN_PREV_YEAR,
             DAY_DEP_PUN_DIF_PY_PERC = DEP_PUNCTUALITY_PERCENTAGE - DEP_PUN_PREV_YEAR,
             DAY_ARR_PUN_DIF_2019_PERC = ARR_PUNCTUALITY_PERCENTAGE - ARR_PUN_2019,
             DAY_DEP_PUN_DIF_2019_PERC = DEP_PUNCTUALITY_PERCENTAGE - DEP_PUN_2019
      ) %>%
      mutate(
        ARR_PUN_WK = rollsum((ARR_PUNCTUAL_FLIGHTS), 7, fill = NA, align = "right") / rollsum(ARR_SCHEDULE_FLIGHT, 7, fill = NA, align = "right") * 100,
        DEP_PUN_WK = rollsum((DEP_PUNCTUAL_FLIGHTS), 7, fill = NA, align = "right") / rollsum(DEP_SCHEDULE_FLIGHT, 7, fill = NA, align = "right") * 100
      ) %>%
      mutate(ARR_PUN_WK_PREV_YEAR = lag(ARR_PUN_WK, 364),
             DEP_PUN_WK_PREV_YEAR = lag(DEP_PUN_WK, 364),
             ARR_PUN_WK_2019 =if_else(YEAR == last_year_punct,
                                      lag(ARR_PUN_WK, 364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7),
                                      NA),
             DEP_PUN_WK_2019 =if_else(YEAR == last_year_punct,
                                      lag(DEP_PUN_WK, 364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7),
                                      NA),
             WK_ARR_PUN_DIF_PY_PERC = ARR_PUN_WK - ARR_PUN_WK_PREV_YEAR,
             WK_DEP_PUN_DIF_PY_PERC = DEP_PUN_WK - DEP_PUN_WK_PREV_YEAR,
             WK_ARR_PUN_DIF_2019_PERC = ARR_PUN_WK - ARR_PUN_WK_2019,
             WK_DEP_PUN_DIF_2019_PERC = DEP_PUN_WK - DEP_PUN_WK_2019
      ) %>%
      filter (DAY_DATE == last_day_punct) %>%
      select(
        ISO_2LETTER,
        ARR_PUNCTUALITY_PERCENTAGE,
        DEP_PUNCTUALITY_PERCENTAGE,
        DAY_ARR_PUN_DIF_PY_PERC,
        DAY_DEP_PUN_DIF_PY_PERC,
        DAY_ARR_PUN_DIF_2019_PERC,
        DAY_DEP_PUN_DIF_2019_PERC,
        ARR_PUN_WK,
        DEP_PUN_WK,
        WK_ARR_PUN_DIF_PY_PERC,
        WK_DEP_PUN_DIF_PY_PERC,
        WK_ARR_PUN_DIF_2019_PERC,
        WK_DEP_PUN_DIF_2019_PERC
      )

    st_punct_y2d <- st_punct_raw %>%
      arrange(ISO_2LETTER, DAY_DATE) %>%
      mutate(MONTH_DAY = as.numeric(format(DAY_DATE, format="%m%d"))) %>%
      filter(MONTH_DAY <= as.numeric(format(last_day_punct, format="%m%d"))) %>%
      group_by(ISO_2LETTER, YEAR) %>%
      summarise (ARR_PUN_Y2D = sum(ARR_PUNCTUAL_FLIGHTS, na.rm=TRUE) / sum(ARR_SCHEDULE_FLIGHT, na.rm=TRUE) * 100,
                 DEP_PUN_Y2D = sum(DEP_PUNCTUAL_FLIGHTS, na.rm=TRUE) / sum(DEP_SCHEDULE_FLIGHT, na.rm=TRUE) * 100) %>%
      mutate(Y2D_ARR_PUN_PREV_YEAR = lag(ARR_PUN_Y2D, 1),
             Y2D_DEP_PUN_PREV_YEAR = lag(DEP_PUN_Y2D, 1),
             Y2D_ARR_PUN_2019 = lag(ARR_PUN_Y2D, last_year_punct - 2019),
             Y2D_DEP_PUN_2019 = lag(DEP_PUN_Y2D, last_year_punct - 2019),
             Y2D_ARR_PUN_DIF_PY_PERC = ARR_PUN_Y2D - Y2D_ARR_PUN_PREV_YEAR,
             Y2D_DEP_PUN_DIF_PY_PERC = DEP_PUN_Y2D - Y2D_DEP_PUN_PREV_YEAR,
             Y2D_ARR_PUN_DIF_2019_PERC = ARR_PUN_Y2D - Y2D_ARR_PUN_2019,
             Y2D_DEP_PUN_DIF_2019_PERC = DEP_PUN_Y2D - Y2D_DEP_PUN_2019
      ) %>%
      filter(YEAR == last_year_punct) %>%
      ungroup() %>%
      select(ISO_2LETTER,
             ARR_PUN_Y2D,
             DEP_PUN_Y2D,
             Y2D_ARR_PUN_DIF_PY_PERC,
             Y2D_DEP_PUN_DIF_PY_PERC,
             Y2D_ARR_PUN_DIF_2019_PERC,
             Y2D_DEP_PUN_DIF_2019_PERC
      )

    st_punct_json <- merge(st_punct_d_w, st_punct_y2d, by="ISO_2LETTER") %>%
      rename(iso_2letter = ISO_2LETTER) %>%
      right_join(state_iso, by = "iso_2letter") %>%
      relocate(state, .after= iso_2letter) %>%
      toJSON() %>%
      substr(., 1, nchar(.)-1) %>%
      substr(., 2, nchar(.))

    ###############################################
    # CO2 data
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
      summarise (MM_TTF = sum(TF, na.rm=TRUE),
                 MM_TTF_PREV_YEAR = sum(LY_TF, na.rm=TRUE),
                 MM_CO2 = sum(CO2_QTY_TONNES, na.rm=TRUE),
                 MM_CO2_PREV_YEAR = sum(LY_CO2_QTY_TONNES, na.rm=TRUE)
      ) %>%
      ungroup() %>%
      mutate(
        MM_CO2_DEP = MM_CO2 / MM_TTF,
        MM_CO2_DEP_PREV_YEAR = MM_CO2_PREV_YEAR / MM_TTF_PREV_YEAR
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
      summarise (TTF = sum(MM_TTF, na.rm=TRUE)) %>%
      select(TTF) %>% pull()

    if (check_flights < 1000) {
      st_co2_data <- st_co2_data %>% filter (FLIGHT_MONTH < st_co2_last_date)
      st_co2_last_date <- max(st_co2_data$FLIGHT_MONTH, na.rm=TRUE)
    }

    co2_for_json <- st_co2_data %>%
      arrange(iso_2letter, FLIGHT_MONTH) %>%
      mutate(
        MONTH_TEXT = format(FLIGHT_MONTH,'%B'),
        MM_CO2_2019 = lag(MM_CO2, (as.numeric(st_co2_last_year) - 2019) * 12),
        MM_TTF_2019 = lag(MM_TTF, (as.numeric(st_co2_last_year) - 2019) * 12),
        MM_CO2_DEP_2019 = lag(MM_CO2_DEP, (as.numeric(st_co2_last_year) - 2019) * 12)
      ) %>%
      mutate(
        DIF_CO2_MONTH_PREV_YEAR = MM_CO2 / MM_CO2_PREV_YEAR - 1,
        DIF_TTF_MONTH_PREV_YEAR = MM_TTF / MM_TTF_PREV_YEAR - 1,
        DIF_CO2_DEP_MONTH_PREV_YEAR = MM_CO2_DEP / MM_CO2_DEP_PREV_YEAR - 1,
        DIF_CO2_MONTH_2019 = MM_CO2 / MM_CO2_2019 - 1,
        DIF_TTF_MONTH_2019 = MM_TTF / MM_TTF_2019 - 1,
        DIF_CO2_DEP_MONTH_2019 = MM_CO2_DEP / MM_CO2_DEP_2019 - 1
      ) %>%
      group_by(iso_2letter, YEAR) %>%
      mutate(
        YTD_CO2 = cumsum(MM_CO2),
        YTD_TTF = cumsum(MM_TTF),
        YTD_CO2_DEP = cumsum(MM_CO2) / cumsum(MM_TTF)
      ) %>%
      ungroup() %>%
      mutate(
        YTD_CO2_PREV_YEAR = lag(YTD_CO2, 12),
        YTD_TTF_PREV_YEAR = lag(YTD_TTF, 12),
        YTD_CO2_DEP_PREV_YEAR = lag(YTD_CO2_DEP, 12),
        YTD_CO2_2019 = lag(YTD_CO2, (as.numeric(co2_last_year) - 2019) * 12),
        YTD_CO2_DEP_2019 = lag(YTD_CO2_DEP, (as.numeric(co2_last_year) - 2019) * 12),
        YTD_TTF_2019 = lag(YTD_TTF, (as.numeric(co2_last_year) - 2019) * 12)
      ) %>%
      mutate(
        YTD_DIF_CO2_PREV_YEAR = YTD_CO2 / YTD_CO2_PREV_YEAR - 1,
        YTD_DIF_TTF_PREV_YEAR = YTD_TTF / YTD_TTF_PREV_YEAR - 1,
        YTD_DIF_CO2_DEP_PREV_YEAR = YTD_CO2_DEP / YTD_CO2_DEP_PREV_YEAR - 1,
        YTD_DIF_CO2_2019 = YTD_CO2 / YTD_CO2_2019 - 1,
        YTD_DIF_CO2_DEP_2019 = YTD_CO2_DEP / YTD_CO2_DEP_2019 - 1,
        YTD_DIF_TTF_2019 = YTD_TTF / YTD_TTF_2019 - 1
      ) %>%
      select(
        iso_2letter,
        FLIGHT_MONTH,
        MONTH_TEXT,
        MM_CO2,
        DIF_CO2_MONTH_PREV_YEAR,
        DIF_CO2_MONTH_2019,
        MM_CO2_DEP,
        DIF_CO2_DEP_MONTH_PREV_YEAR,
        DIF_CO2_DEP_MONTH_2019,
        YTD_CO2,
        YTD_DIF_CO2_PREV_YEAR,
        YTD_DIF_CO2_2019,
        YTD_CO2_DEP,
        YTD_DIF_CO2_DEP_PREV_YEAR,
        YTD_DIF_CO2_DEP_2019
      ) %>%
      filter(FLIGHT_MONTH == st_co2_last_date)

    st_co2_json <- co2_for_json %>%
      right_join(state_iso, by = "iso_2letter") %>%
      toJSON() %>%
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
      select(-ST_RANK)

    # covert to json and save in app data folder and archive
    st_ao_data_j <- st_ao_data %>% toJSON()
    # write(st_ao_data_j, here(data_folder,"ao_ranking_traffic.json"))
    write(st_ao_data_j, paste0(archive_dir, today, "_st_ao_ranking_traffic.json"))


