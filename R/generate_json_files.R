# libraries  ----
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

# source(here::here("R", "helpers.R"))
source(here("..", "mobile-app", "R", "helpers.R")) # so it can be launched from the checkupdates script in grounded aircraft

# parameters ----
data_folder <- here::here("data")
base_dir <- "//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/"
base_file <- "99_Traffic_Landing_Page_dataset_new_{today}.xlsx"
archive_dir <- "//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/web_daily_json_files/app/"
today <- (lubridate::now() + days(-1)) %>% format("%Y%m%d")
last_day <- (lubridate::now() + days(-1))
last_year <- as.numeric(format(last_day, "%Y"))
nw_json_app <- ""
# DB params
usr <- Sys.getenv("PRU_DEV_USR")
pwd <- Sys.getenv("PRU_DEV_PWD")
dbn <- Sys.getenv("PRU_DEV_DBNAME")

# json for main page ----

  ## Network billed ----
    #### billing json - we do this first to avoid 'R fatal error'

    # dir_billing <- "G:/HQ/dgof-pru/Data/DataProcessing/Covid19/Oscar/Billing"
    #
    # nw_billed_data_raw <-  read_xlsx(
    #   path  = fs::path_abs(
    #     str_glue("Billing_tables.xlsx"),
    #     start = dir_billing),
    #   sheet = "network",
    #   range = cell_limits(c(5, 2), c(NA, NA))) %>%
    #   as_tibble()%>%
    #   mutate(DATE = as.Date(Billing_period_start_date, format = "%d-%m-%Y"))

    # https://leowong.ca/blog/connect-to-microsoft-access-database-via-r/

    # Set up driver info and database path
    DRIVERINFO <- "Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
    MDBPATH <- "G:/HQ/dgof-pru/Data/DataProcessing/Crco - Billing/CRCO_BILL.accdb"
    PATH <- paste0(DRIVERINFO, "DBQ=", MDBPATH)

    channel <- odbcDriverConnect(PATH)
    query_bill <- "SELECT * FROM V_CRCO_BILL_PER_CZ"

    # Load data into R dataframe
    nw_billed_raw <- sqlQuery(channel,
      query_bill,
      stringsAsFactors = FALSE
    )

    # Close and remove channel
    close(channel)
    rm(channel)

    # format dates and extract date parameters
    nw_billed_raw <- nw_billed_raw %>%
      janitor::clean_names() %>%
      mutate(billing_period_start_date = as.Date(billing_period_start_date, format = "%d-%m-%Y"))

    last_billing_date <- max(nw_billed_raw$billing_period_start_date)
    last_billing_year <- max(nw_billed_raw$year)

    # calculate network total
    nw_billing <- nw_billed_raw %>%
      group_by(year, month, billing_period_start_date) %>%
      summarise(total_billing = sum(route_charges)) %>%
      ungroup()

    # calcs + format
    nw_billed_for_json <- nw_billing %>%
      arrange(year, billing_period_start_date) %>%
      mutate(
        BILLING_DATE = (billing_period_start_date + days(1) + months(1)) + days(-1),
        Year = year,
        MONTH_F = format(billing_period_start_date + days(1), "%B"),
        BILL_MONTH_PY = lag(total_billing, 12),
        BILL_MONTH_2019 = lag(total_billing, (last_billing_year - 2019) * 12),
        DIF_BILL_MONTH_PY = total_billing / BILL_MONTH_PY - 1,
        DIF_BILL_MONTH_2019 = total_billing / BILL_MONTH_2019 - 1,
        BILLED = round(total_billing / 1000000, 0)
      ) %>%
      group_by(Year) %>%
      mutate(
        total_billing_y2d = cumsum(total_billing)
      ) %>%
      ungroup() %>%
      mutate(
        BILL_Y2D_PY = lag(total_billing_y2d, 12),
        BILL_Y2D_2019 = lag(total_billing_y2d, (last_billing_year - 2019) * 12),
        DIF_BILL_Y2D_PY = total_billing_y2d / BILL_Y2D_PY - 1,
        DIF_BILL_Y2D_2019 = total_billing_y2d / BILL_Y2D_2019 - 1,
        BILLED_Y2D = round(total_billing_y2d / 1000000, 0)
      ) %>%
      filter(billing_period_start_date == last_billing_date) %>%
      select(
        BILLING_DATE,
        MONTH_F,
        BILLED,
        DIF_BILL_MONTH_PY,
        DIF_BILL_MONTH_2019,
        BILLED_Y2D,
        DIF_BILL_Y2D_PY,
        DIF_BILL_Y2D_2019
      )

    # rename some fields for app v2
    nw_billed_for_json_v2 <- nw_billed_for_json %>%
      rename(MONTH_TEXT = MONTH_F,
             MM_BILLED = BILLED,
             MM_BILLED_DIF_PREV_YEAR = DIF_BILL_MONTH_PY,
             MM_BILLED_DIF_2019 = DIF_BILL_MONTH_2019,
             Y2D_BILLED = BILLED_Y2D,
             Y2D_BILLED_DIF_PREV_YEAR = DIF_BILL_Y2D_PY,
             Y2D_BILLED_DIF_2019 = DIF_BILL_Y2D_2019)

    # app v1 json
    nw_billed_json <- nw_billed_for_json %>%
      toJSON(., pretty = TRUE) %>%
      substr(., 1, nchar(.) - 1) %>%
      substr(., 2, nchar(.))

    # app v2 json
    nw_billed_json_v2 <- nw_billed_for_json_v2 %>%
      toJSON(., pretty = TRUE) %>%
      substr(., 1, nchar(.) - 1) %>%
      substr(., 2, nchar(.))


  ## Network traffic ----
    # traffic data
    nw_traffic_data <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "NM_Daily_Traffic_All",
      range = cell_limits(c(2, 1), c(NA, 39))
    ) %>%
      as_tibble()


    # get data for last date
    nw_traffic_last_day <- nw_traffic_data %>%
      filter(FLIGHT_DATE == max(LAST_DATA_DAY))

    # select relevant fields
    nw_traffic_for_json <- nw_traffic_last_day %>%
      select(
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

    # rename some fields for app v2
    nw_traffic_for_json_v2 <- nw_traffic_for_json %>%
      rename(
        DY_TFC = DAY_TFC,
        DY_TFC_DIF_PREV_YEAR_PERC = DAY_DIFF_PREV_YEAR_PERC,
        DY_TFC_DIF_2019_PERC = DAY_TFC_DIFF_2019_PERC,
        WK_TFC_AVG_ROLLING = AVG_ROLLING_WEEK,
        WK_TFC_DIF_PREV_YEAR_PERC = DIF_WEEK_PREV_YEAR_PERC,
        WK_TFC_DIF_2019_PERC = DIF_ROLLING_WEEK_2019_PERC,
        Y2D_TFC = Y2D_TFC_YEAR,
        Y2D_TFC_AVG = Y2D_AVG_TFC_YEAR,
        Y2D_TFC_DIF_PREV_YEAR_PERC = Y2D_DIFF_PREV_YEAR_PERC,
        Y2D_TFC_DIF_2019_PERC = Y2D_DIFF_2019_PERC
        )

    # app v1 json
    nw_traffic_json <- nw_traffic_for_json %>%
      toJSON(., pretty = TRUE, digits = 10) %>%
      substr(., 1, nchar(.) - 1) %>%
      substr(., 2, nchar(.))

    # app v2 json
    nw_traffic_json_v2 <- nw_traffic_for_json_v2 %>%
      toJSON(., pretty = TRUE, digits = 10) %>%
      substr(., 1, nchar(.) - 1) %>%
      substr(., 2, nchar(.))

  ## Network delay ----

    # delay data
    nw_delay_data <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "NM_Daily_Delay_All",
      range = cell_limits(c(2, 1), c(NA, 39))
    ) %>%
      as_tibble()

    # calcs
    nw_delay_for_json <- nw_delay_data %>%
      mutate(FLIGHT_DATE = as.Date(FLIGHT_DATE)) %>%
      filter(FLIGHT_DATE == max(LAST_DATA_DAY)) %>%
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
      select(
        FLIGHT_DATE,
        DAY_DLY,
        DAY_DIFF_PREV_YEAR_PERC,
        DAY_DLY_DIFF_2019_PERC,
        DAY_DLY_FLT,
        DAY_DLY_FLT_DIF_PY_PERC,
        DAY_DLY_FLT_DIF_2019_PERC,
        AVG_ROLLING_WEEK,
        DIF_WEEK_PREV_YEAR_PERC,
        DIF_ROLLING_WEEK_2019_PERC,
        RWEEK_DLY_FLT,
        RWEEK_DLY_FLT_DIF_PY_PERC,
        RWEEK_DLY_FLT_DIF_2019_PERC,
        Y2D_AVG_DLY_YEAR,
        Y2D_DIFF_PREV_YEAR_PERC,
        Y2D_DIFF_2019_PERC,
        Y2D_DLY_FLT,
        Y2D_DLY_FLT_DIF_PY_PERC,
        Y2D_DLY_FLT_DIF_2019_PERC
      )

    # rename some fields for app v2
    nw_delay_for_json_v2 <- nw_delay_for_json %>%
      rename(
        DY_DLY = DAY_DLY,
        DY_DLY_DIF_PREV_YEAR_PERC = DAY_DIFF_PREV_YEAR_PERC,
        DY_DLY_DIF_2019_PERC = DAY_DLY_DIFF_2019_PERC,
        DY_DLY_FLT = DAY_DLY_FLT,
        DY_DLY_FLT_DIF_PREV_YEAR_PERC = DAY_DLY_FLT_DIF_PY_PERC,
        DY_DLY_FLT_DIF_2019_PERC = DAY_DLY_FLT_DIF_2019_PERC,
        WK_DLY_AVG_ROLLING = AVG_ROLLING_WEEK,
        WK_DLY_DIF_PREV_YEAR_PERC = DIF_WEEK_PREV_YEAR_PERC,
        WK_DLY_DIF_2019_PERC = DIF_ROLLING_WEEK_2019_PERC,
        WK_DLY_FLT = RWEEK_DLY_FLT,
        WK_DLY_FLT_DIF_PREV_YEAR_PERC = RWEEK_DLY_FLT_DIF_PY_PERC,
        WK_DLY_FLT_DIF_2019_PERC = RWEEK_DLY_FLT_DIF_2019_PERC,
        Y2D_DLY_AVG = Y2D_AVG_DLY_YEAR,
        Y2D_DLY_DIF_PREV_YEAR_PERC = Y2D_DIFF_PREV_YEAR_PERC,
        Y2D_DLY_DIF_2019_PERC = Y2D_DIFF_2019_PERC,
        Y2D_DLY_FLT_DIF_PREV_YEAR_PERC = Y2D_DLY_FLT_DIF_PY_PERC,
        )

    # app V1 json
    nw_delay_json <- nw_delay_for_json %>%
      toJSON(., pretty = TRUE) %>%
      substr(., 1, nchar(.) - 1) %>%
      substr(., 2, nchar(.))

    # app V2 json
    nw_delay_json_v2 <- nw_delay_for_json_v2 %>%
      toJSON(., pretty = TRUE) %>%
      substr(., 1, nchar(.) - 1) %>%
      substr(., 2, nchar(.))

  ##------ Network punctuality ----

    # punctuality data
      ### select * from LDW_VDM.VIEW_FAC_PUNCTUALITY_NW_DAY

    nw_punct_data_raw <- read_xlsx(
      path = fs::path_abs(
        str_glue("98_PUNCTUALITY_{today}.xlsx"),
        start = base_dir
      ),
      sheet = "NETWORK",
      range = cell_limits(c(1, 1), c(NA, NA))
      ) %>%
      as_tibble() %>%
      mutate(DATE = as.Date(DATE, format = "%d-%m-%Y"))

    # pull out date parameters
    last_day_punct <- max(nw_punct_data_raw$DATE)
    last_year_punct <- as.numeric(format(last_day_punct, "%Y"))

    # day/week calculations
    nw_punct_data_d_w <- nw_punct_data_raw %>%
      arrange(DATE) %>%
      mutate(YEAR_FLIGHT = as.numeric(format(DATE, "%Y"))) %>%
      mutate(
        ARR_PUN_PREV_YEAR = lag(ARR_PUNCTUALITY_PERCENTAGE, 364),
        DEP_PUN_PREV_YEAR = lag(DEP_PUNCTUALITY_PERCENTAGE, 364),
        ARR_PUN_2019 = if_else(
          YEAR_FLIGHT == last_year_punct,
          lag(
            ARR_PUNCTUALITY_PERCENTAGE,
            364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7
            ),
          1
          ),
        DEP_PUN_2019 = if_else(
          YEAR_FLIGHT == last_year_punct,
          lag(
            DEP_PUNCTUALITY_PERCENTAGE,
            364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7
            ),
          1
          ),
        DAY_2019 = if_else(
          YEAR_FLIGHT == last_year_punct,
          lag(
            DATE,
            364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7
            ),
          last_day_punct
          ),
        DAY_ARR_PUN_DIF_PY_PERC = ARR_PUNCTUALITY_PERCENTAGE - ARR_PUN_PREV_YEAR,
        DAY_DEP_PUN_DIF_PY_PERC = DEP_PUNCTUALITY_PERCENTAGE - DEP_PUN_PREV_YEAR,
        DAY_ARR_PUN_DIF_2019_PERC = ARR_PUNCTUALITY_PERCENTAGE - ARR_PUN_2019,
        DAY_DEP_PUN_DIF_2019_PERC = DEP_PUNCTUALITY_PERCENTAGE - DEP_PUN_2019
        ) %>%
      mutate(
        ARR_PUN_WK = rollsum(ARR_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") /
          rollsum(ARR_SCHEDULE_FLIGHT, 7, fill = NA, align = "right") * 100,
        DEP_PUN_WK = rollsum(DEP_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") /
          rollsum(DEP_SCHEDULE_FLIGHT, 7, fill = NA, align = "right") * 100
        ) %>%
      mutate(
        ARR_PUN_WK_PREV_YEAR = lag(ARR_PUN_WK, 364),
        DEP_PUN_WK_PREV_YEAR = lag(DEP_PUN_WK, 364),
        ARR_PUN_WK_2019 = if_else(
          YEAR_FLIGHT == last_year_punct,
          lag(ARR_PUN_WK, 364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7),
          1
          ),
        DEP_PUN_WK_2019 = if_else(
          YEAR_FLIGHT == last_year_punct,
          lag(DEP_PUN_WK, 364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7),
          1
          ),
        WK_ARR_PUN_DIF_PY_PERC = ARR_PUN_WK - ARR_PUN_WK_PREV_YEAR,
        WK_DEP_PUN_DIF_PY_PERC = DEP_PUN_WK - DEP_PUN_WK_PREV_YEAR,
        WK_ARR_PUN_DIF_2019_PERC = ARR_PUN_WK - ARR_PUN_WK_2019,
        WK_DEP_PUN_DIF_2019_PERC = DEP_PUN_WK - DEP_PUN_WK_2019
        ) %>%
      filter(DATE == last_day_punct) %>%
      mutate(FLIGHT_DATE = DATE) %>%
      select(
        FLIGHT_DATE,
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
      ) %>%
      mutate(INDEX = 1)

    # y2d calculations
    nw_punct_data_y2d <- nw_punct_data_raw %>%
      arrange(DATE) %>%
      mutate(YEAR_FLIGHT = as.numeric(format(DATE, "%Y"))) %>%
      mutate(MONTH_DAY = as.numeric(format(DATE, format = "%m%d"))) %>%
      filter(MONTH_DAY <= as.numeric(format(last_day_punct, format = "%m%d"))) %>%
      mutate(YEAR = as.numeric(format(DATE, format = "%Y"))) %>%
      group_by(YEAR) %>%
      summarise(
        ARR_PUN_Y2D = sum(ARR_PUNCTUAL_FLIGHTS, na.rm = TRUE) / sum(ARR_SCHEDULE_FLIGHT, na.rm = TRUE) * 100,
        DEP_PUN_Y2D = sum(DEP_PUNCTUAL_FLIGHTS, na.rm = TRUE) / sum(DEP_SCHEDULE_FLIGHT, na.rm = TRUE) * 100
        ) %>%
      mutate(
        Y2D_ARR_PUN_PREV_YEAR = lag(ARR_PUN_Y2D, 1),
        Y2D_DEP_PUN_PREV_YEAR = lag(DEP_PUN_Y2D, 1),
        Y2D_ARR_PUN_2019 = lag(ARR_PUN_Y2D, last_year_punct - 2019),
        Y2D_DEP_PUN_2019 = lag(DEP_PUN_Y2D, last_year_punct - 2019),
        Y2D_ARR_PUN_DIF_PY_PERC = ARR_PUN_Y2D - Y2D_ARR_PUN_PREV_YEAR,
        Y2D_DEP_PUN_DIF_PY_PERC = DEP_PUN_Y2D - Y2D_DEP_PUN_PREV_YEAR,
        Y2D_ARR_PUN_DIF_2019_PERC = ARR_PUN_Y2D - Y2D_ARR_PUN_2019,
        Y2D_DEP_PUN_DIF_2019_PERC = DEP_PUN_Y2D - Y2D_DEP_PUN_2019
        ) %>%
      filter(YEAR == as.numeric(format(last_day_punct, format = "%Y"))) %>%
      select(
        ARR_PUN_Y2D,
        DEP_PUN_Y2D,
        Y2D_ARR_PUN_DIF_PY_PERC,
        Y2D_DEP_PUN_DIF_PY_PERC,
        Y2D_ARR_PUN_DIF_2019_PERC,
        Y2D_DEP_PUN_DIF_2019_PERC
        ) %>%
      mutate(INDEX = 1)

    # merge day/week and y2d tables
    nw_punct_for_json <- merge(nw_punct_data_d_w, nw_punct_data_y2d, by = "INDEX") %>%
      select(-INDEX)

    # rename some fields for app v2
    nw_punct_for_json_v2 <- nw_punct_for_json %>%
      rename(
        DY_ARR_PUN = ARR_PUNCTUALITY_PERCENTAGE,
        DY_DEP_PUN = DEP_PUNCTUALITY_PERCENTAGE,
        DY_ARR_PUN_DIF_PREV_YEAR = DAY_ARR_PUN_DIF_PY_PERC,
        DY_DEP_PUN_DIF_PREV_YEAR = DAY_DEP_PUN_DIF_PY_PERC,
        DY_ARR_PUN_DIF_2019 = DAY_ARR_PUN_DIF_2019_PERC,
        DY_DEP_PUN_DIF_2019 = DAY_DEP_PUN_DIF_2019_PERC,
        WK_ARR_PUN = ARR_PUN_WK,
        WK_DEP_PUN = DEP_PUN_WK,
        WK_ARR_PUN_DIF_PREV_YEAR = WK_ARR_PUN_DIF_PY_PERC,
        WK_DEP_PUN_DIF_PREV_YEAR = WK_DEP_PUN_DIF_PY_PERC,
        WK_ARR_PUN_DIF_2019 = WK_ARR_PUN_DIF_2019_PERC,
        WK_DEP_PUN_DIF_2019 = WK_DEP_PUN_DIF_2019_PERC,
        Y2D_ARR_PUN = ARR_PUN_Y2D,
        Y2D_DEP_PUN = DEP_PUN_Y2D,
        Y2D_ARR_PUN_DIF_PREV_YEAR = Y2D_ARR_PUN_DIF_PY_PERC,
        Y2D_DEP_PUN_DIF_PREV_YEAR = Y2D_DEP_PUN_DIF_PY_PERC,
        Y2D_ARR_PUN_DIF_2019 = Y2D_ARR_PUN_DIF_2019_PERC,
        Y2D_DEP_PUN_DIF_2019 = Y2D_DEP_PUN_DIF_2019_PERC
        )

    # app v1 json
    nw_punct_json <- nw_punct_for_json %>%
      toJSON(., pretty = TRUE) %>%
      substr(., 1, nchar(.) - 1) %>%
      substr(., 2, nchar(.))

    # app v2 json
    nw_punct_json_v2 <- nw_punct_for_json_v2 %>%
      toJSON(., pretty = TRUE) %>%
      substr(., 1, nchar(.) - 1) %>%
      substr(., 2, nchar(.))


  ##------ Network CO2 emissions ----

    # CO2 data
    query <- str_glue("
        SELECT *
          FROM TABLE (emma_pub.api_aiu_stats.MM_AIU_STATE_DEP ())
          where year >= 2019 and STATE_NAME not in ('LIECHTENSTEIN')
        ORDER BY 2, 3, 4
       ")

    co2_data_raw <- export_query(query) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
      # filter(YEAR <= 2023) ## NOTE: temporary line while the app upgrade is implemented

    # calcs + format
    co2_data_evo_nw <- co2_data_raw %>%
      select(
        FLIGHT_MONTH,
        CO2_QTY_TONNES,
        TF,
        YEAR,
        MONTH
        ) %>%
      group_by(FLIGHT_MONTH) %>%
      summarise(MM_TTF = sum(TF) / 1000000, MM_CO2 = sum(CO2_QTY_TONNES) / 1000000) %>%
      mutate(
        YEAR = as.numeric(format(FLIGHT_MONTH, "%Y")),
        MONTH = as.numeric(format(FLIGHT_MONTH, "%m")),
        MM_CO2_DEP = MM_CO2 / MM_TTF
        ) %>%
      arrange(FLIGHT_MONTH) %>%
      mutate(FLIGHT_MONTH = ceiling_date(as_date(FLIGHT_MONTH), unit = "month") - 1)

    # pull out date parameters
    co2_last_date <- max(co2_data_evo_nw$FLIGHT_MONTH, na.rm = TRUE)
    co2_last_month <- format(co2_last_date, "%B")
    co2_last_month_num <- as.numeric(format(co2_last_date, "%m"))
    co2_last_year <- max(co2_data_evo_nw$YEAR)

    # check last month number of flights
    check_flights <- co2_data_evo_nw %>%
      filter(YEAR == max(YEAR)) %>%
      filter(MONTH == max(MONTH)) %>%
      select(MM_TTF) %>%
      pull() * 1000000

    # if last month has less than 1000 flights, take the previous
    if (check_flights < 1000) {
      co2_data_raw <- co2_data_raw %>% filter(FLIGHT_MONTH < max(FLIGHT_MONTH))
      co2_data_evo_nw <- co2_data_evo_nw %>% filter(FLIGHT_MONTH < max(FLIGHT_MONTH))
      co2_last_date <- max(co2_data_evo_nw$FLIGHT_MONTH, na.rm = TRUE)
    }

    # calcs
    co2_for_json <- co2_data_evo_nw %>%
      mutate(
        MONTH_TEXT = format(FLIGHT_MONTH, "%B"),
        MM_CO2_PREV_YEAR = lag(MM_CO2, 12),
        MM_TTF_PREV_YEAR = lag(MM_TTF, 12),
        MM_CO2_2019 = lag(MM_CO2, (as.numeric(co2_last_year) - 2019) * 12),
        MM_TTF_2019 = lag(MM_TTF, (as.numeric(co2_last_year) - 2019) * 12),
        MM_CO2_DEP_PREV_YEAR = lag(MM_CO2_DEP, 12),
        MM_CO2_DEP_2019 = lag(MM_CO2_DEP, (as.numeric(co2_last_year) - 2019) * 12)
        ) %>%
      mutate(
        DIF_CO2_MONTH_PREV_YEAR = MM_CO2 / MM_CO2_PREV_YEAR - 1,
        DIF_TTF_MONTH_PREV_YEAR = MM_TTF / MM_TTF_PREV_YEAR - 1,
        DIF_CO2_DEP_MONTH_PREV_YEAR = MM_CO2_DEP / MM_CO2_DEP_PREV_YEAR - 1,
        DIF_CO2_MONTH_2019 = MM_CO2 / MM_CO2_2019 - 1,
        DIF_TTF_MONTH_2019 = MM_TTF / MM_TTF_2019 - 1,
        DIF_CO2_DEP_MONTH_2019 = MM_CO2_DEP / MM_CO2_DEP_2019 - 1
        ) %>%
      group_by(YEAR) %>%
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
      filter(FLIGHT_MONTH == co2_last_date)

    # rename some fields for app v2
    co2_for_json_v2 <- co2_for_json %>%
      rename(
        MM_CO2_DIF_PREV_YEAR = DIF_CO2_MONTH_PREV_YEAR,
        MM_CO2_DIF_2019 = DIF_CO2_MONTH_2019,
        MM_CO2_DEP_DIF_PREV_YEAR = DIF_CO2_DEP_MONTH_PREV_YEAR,
        MM_CO2_DEP_DIF_2019 = DIF_CO2_DEP_MONTH_2019
        , Y2D_CO2 = YTD_CO2
        , Y2D_CO2_DIF_PREV_YEAR = YTD_DIF_CO2_PREV_YEAR
        , Y2D_CO2_DIF_2019 = YTD_DIF_CO2_2019
        , Y2D_CO2_DEP = YTD_CO2_DEP
        , Y2D_CO2_DEP_DIF_PREV_YEAR = YTD_DIF_CO2_DEP_PREV_YEAR
        , Y2D_CO2_DEP_DIF_2019 = YTD_DIF_CO2_DEP_2019
        )

    # app v1 json
    nw_co2_json <- co2_for_json %>%
      toJSON(., pretty = TRUE) %>%
      substr(., 1, nchar(.) - 1) %>%
      substr(., 2, nchar(.))

    # app v2 json
    nw_co2_json_v2 <- co2_for_json_v2 %>%
      toJSON(., pretty = TRUE) %>%
      substr(., 1, nchar(.) - 1) %>%
      substr(., 2, nchar(.))

  ##------ update date ----

    # add date to json

    update_day <- floor_date(lubridate::now(), unit = "days") %>%
      as_tibble() %>%
      rename(APP_UPDATE = 1)

    update_day_json <- update_day %>%
      toJSON(., pretty = TRUE) %>%
      substr(., 1, nchar(.) - 1) %>%
      substr(., 2, nchar(.))

  ## join data strings and save ----
    # app v1 json
    nw_json_app <- paste0(
      "{",
      '"nw_traffic":', nw_traffic_json,
      ', "nw_delay":', nw_delay_json,
      ', "nw_punct":', nw_punct_json,
      ', "nw_co2":', nw_co2_json,
      ', "nw_billed":', nw_billed_json,
      ', "app_update":', update_day_json,
      "}"
    )

    write(nw_json_app, here(data_folder, "nw_json_app.json"))
    write(nw_json_app, paste0(archive_dir, today, "_nw_json_app.json"))

    # app v2 json
    nw_json_app_v2 <- paste0(
      "{",
      '"nw_traffic":', nw_traffic_json_v2,
      ', "nw_delay":', nw_delay_json_v2,
      ', "nw_punct":', nw_punct_json_v2,
      ', "nw_co2":', nw_co2_json_v2,
      ', "nw_billed":', nw_billed_json_v2,
      ', "app_update":', update_day_json,
      "}"
    )

    write(nw_json_app_v2, here(data_folder, "v2", "nw_json_app.json"))
    write(nw_json_app, paste0(archive_dir, today, "_nw_json_app_v2.json"))

# jsons for graphs -------
  ## traffic -----

    ### 7-day average daily ----
    nw_traffic_evo_app <- nw_traffic_data %>%
      select(
        FLIGHT_DATE, AVG_ROLLING_WEEK, AVG_ROLLING_WEEK_PREV_YEAR,
        AVG_ROLLING_WEEK_2020, AVG_ROLLING_WEEK_2019
      )

    column_names <- c("FLIGHT_DATE", last_year, last_year - 1, 2020, 2019)
    colnames(nw_traffic_evo_app) <- column_names

    # write.csv(nw_traffic_evo_app,
    #           file = here(data_folder,"nw_traffic_evo_app.csv"),
    #           row.names = FALSE)

    nw_traffic_evo_app_j <- nw_traffic_evo_app %>% toJSON(., pretty = TRUE)
    write(nw_traffic_evo_app_j, here(data_folder, "nw_traffic_evo_chart_daily.json"))
    write(nw_traffic_evo_app_j, here(data_folder, "v2", "nw_traffic_evo_chart_daily.json"))
    write(nw_traffic_evo_app_j, paste0(archive_dir, today, "_nw_traffic_evo_chart_daily.json"))

    ### monthly ----
    base_dir <- "//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/"
    base_file <- "99_Traffic_Landing_Page_dataset_new_{today}.xlsx"

    nw_traffic_month_data <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "NM_Monthly_traffic",
      range = cell_limits(c(5, 10), c(NA, NA))
    ) %>%
      as_tibble()

    nw_traffic_month_data_j <- nw_traffic_month_data %>% toJSON(., pretty = TRUE)
    write(nw_traffic_month_data_j, here(data_folder, "nw_traffic_evo_chart_monthly.json"))
    write(nw_traffic_month_data_j, here(data_folder,"v2", "nw_traffic_evo_chart_monthly.json"))
    write(nw_traffic_month_data_j, paste0(archive_dir, today, "_nw_traffic_evo_chart_monthly.json"))


  ## delay ----
  nw_delay_raw <- read_xlsx(
    path = fs::path_abs(
      str_glue(base_file),
      start = base_dir
    ),
    sheet = "NM_Delay_for_graph",
    range = cell_limits(c(2, 1), c(NA, 34))
  ) %>%
    as_tibble()

    ### delay per cause v1 ----
    nw_delay_evo <- nw_delay_raw %>%
      mutate(
        ROLL_WK_AVG_DLY_PREV_YEAR = lag(ROLL_WK_AVG_DLY, 364),
        DAY_DLY_PREV_YEAR = lag(DAY_DLY, 364)
      ) %>%
      filter(FLIGHT_DATE >= paste0(last_year, "-01-01")) %>%
      # mutate(FLIGHT_YEAR = as.character(format(FLIGHT_DATE,'%Y')))%>%
      mutate(FLIGHT_DATE = as.Date(FLIGHT_DATE))

    nw_delay_evo_app <- nw_delay_evo %>%
      select(
        FLIGHT_DATE,
        ROLL_WK_AVG_DLY_CAP_STAF,
        ROLL_WK_AVG_DLY_DISR,
        ROLL_WK_AVG_DLY_WTH,
        ROLL_WK_AVG_DLY_OTH,
        ROLL_WK_AVG_DLY_PREV_YEAR
      )

    column_names <- c(
      "FLIGHT_DATE",
      "Capacity/Staffing",
      "Disruptions (ATC)",
      "Weather",
      "Other",
      paste0("Total delay ", last_year - 1)
    )

    colnames(nw_delay_evo_app) <- column_names

    nw_delay_evo_app_j <- nw_delay_evo_app %>% toJSON(., pretty = TRUE)
    write(nw_delay_evo_app_j, here(data_folder, "nw_delay_category_evo_chart.json"))
    write(nw_delay_evo_app_j, here(data_folder, "v2", "nw_delay_category_evo_chart.json"))
    write(nw_delay_evo_app_j, paste0(archive_dir, today, "_nw_delay_category_evo_chart.json"))

    ### delay per cause v2 ----

    #### day ----
    nw_delay_cause_day <- nw_delay_evo %>%
      filter(FLIGHT_DATE == max(FLIGHT_DATE)) %>%
      mutate(
        SHARE_TDM_G = if_else(DAY_DLY == 0, 0, DAY_DLY_APT_CAP / DAY_DLY),
        SHARE_TDM_CS = if_else(DAY_DLY == 0, 0, DAY_DLY_CAP_STAF_NOG / DAY_DLY),
        SHARE_TDM_IT = if_else(DAY_DLY == 0, 0, DAY_DLY_DISR / DAY_DLY),
        SHARE_TDM_WD = if_else(DAY_DLY == 0, 0, DAY_DLY_WTH / DAY_DLY),
        SHARE_TDM_NOCSGITWD = if_else(DAY_DLY == 0, 0, DAY_DLY_OTH / DAY_DLY)
      ) %>%
      select(FLIGHT_DATE,
             DAY_DLY_APT_CAP,
             DAY_DLY_CAP_STAF_NOG,
             DAY_DLY_DISR,
             DAY_DLY_WTH,
             DAY_DLY_OTH,
             DAY_DLY_PREV_YEAR,
             SHARE_TDM_G,
             SHARE_TDM_CS,
             SHARE_TDM_IT,
             SHARE_TDM_WD,
             SHARE_TDM_NOCSGITWD
      )

    column_names <- c(
      "FLIGHT_DATE",
      "Aerodrome capacity",
      "Capacity/Staffing (ATC)",
      "Disruptions (ATC)",
      "Weather",
      "Other",
      paste0("Total delay ", last_year - 1),
      "share_aerodrome_capacity",
      "share_capacity_staffing_atc",
      "share_disruptions_atc",
      "share_weather",
      "share_other"
    )

    colnames(nw_delay_cause_day) <- column_names

    ### nest data
    nw_delay_value_day_long <- nw_delay_cause_day %>%
      select(-c(share_aerodrome_capacity,
                share_capacity_staffing_atc,
                share_disruptions_atc,
                share_weather,
                share_other)
      ) %>%
      pivot_longer(-c(FLIGHT_DATE), names_to = 'metric', values_to = 'value')

    nw_delay_share_day_long <- nw_delay_cause_day %>%
      select(-c("Aerodrome capacity",
                "Capacity/Staffing (ATC)",
                "Disruptions (ATC)",
                "Weather",
                "Other",
                paste0("Total delay ", last_year - 1)
                )
             )  %>%
      mutate(share_delay_prev_year = NA) %>%
      pivot_longer(-c(FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
      select(name, share)

    nw_delay_cause_day_long <- cbind(nw_delay_value_day_long, nw_delay_share_day_long) %>%
      select(-name) %>%
      group_by(FLIGHT_DATE) %>%
      nest_legacy(.key = "statistics")

    nw_delay_cause_evo_dy_j <- nw_delay_cause_day_long %>% toJSON(., pretty = TRUE)
    write(nw_delay_cause_evo_dy_j, here(data_folder, 'v2', "nw_delay_category_evo_chart_dy.json"))
    write(nw_delay_cause_evo_dy_j, paste0(archive_dir, today, "_nw_delay_category_chart_evo_dy.json"))
    write(nw_delay_cause_evo_dy_j, paste0(archive_dir, "nw_delay_category_evo_chart_dy.json"))

    #### week ----
    nw_delay_cause_wk <- nw_delay_evo %>%
      filter(FLIGHT_DATE >= max(FLIGHT_DATE) + lubridate::days(-6)) %>%
      mutate(
        SHARE_TDM_G = if_else(sum(DAY_DLY) == 0, 0, sum(DAY_DLY_APT_CAP) / sum(DAY_DLY)),
        SHARE_TDM_CS = if_else(sum(DAY_DLY) == 0, 0, sum(DAY_DLY_CAP_STAF_NOG) / sum(DAY_DLY)),
        SHARE_TDM_IT = if_else(sum(DAY_DLY) == 0, 0, sum(DAY_DLY_DISR) / sum(DAY_DLY)),
        SHARE_TDM_WD = if_else(sum(DAY_DLY) == 0, 0, sum(DAY_DLY_WTH) / sum(DAY_DLY)),
        SHARE_TDM_NOCSGITWD = if_else(sum(DAY_DLY) == 0, 0, sum(DAY_DLY_OTH) / sum(DAY_DLY))
      ) %>%
      select(FLIGHT_DATE,
             DAY_DLY_APT_CAP,
             DAY_DLY_CAP_STAF_NOG,
             DAY_DLY_DISR,
             DAY_DLY_WTH,
             DAY_DLY_OTH,
             DAY_DLY_PREV_YEAR,
             SHARE_TDM_G,
             SHARE_TDM_CS,
             SHARE_TDM_IT,
             SHARE_TDM_WD,
             SHARE_TDM_NOCSGITWD
      )

    colnames(nw_delay_cause_wk) <- column_names

    ### nest data
    nw_delay_value_wk_long <- nw_delay_cause_wk %>%
      select(-c(share_aerodrome_capacity,
                share_capacity_staffing_atc,
                share_disruptions_atc,
                share_weather,
                share_other)
      ) %>%
      pivot_longer(-c(FLIGHT_DATE), names_to = 'metric', values_to = 'value')

    nw_delay_share_wk_long <- nw_delay_cause_wk %>%
      select(-c("Aerodrome capacity",
                "Capacity/Staffing (ATC)",
                "Disruptions (ATC)",
                "Weather",
                "Other",
                paste0("Total delay ", last_year - 1)
                )
             )  %>%
      mutate(share_delay_prev_year = NA) %>%
      pivot_longer(-c(FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
      select(name, share)

    nw_delay_cause_wk_long <- cbind(nw_delay_value_wk_long, nw_delay_share_wk_long) %>%
      select(-name) %>%
      group_by(FLIGHT_DATE) %>%
      nest_legacy(.key = "statistics")

    nw_delay_cause_evo_wk_j <- nw_delay_cause_wk_long %>% toJSON(., pretty = TRUE)
    write(nw_delay_cause_evo_wk_j, here(data_folder, 'v2', "nw_delay_category_evo_chart_wk.json"))
    write(nw_delay_cause_evo_wk_j, paste0(archive_dir, today, "_nw_delay_category_chart_evo_wk.json"))
    write(nw_delay_cause_evo_wk_j, paste0(archive_dir, "nw_delay_category_evo_chart_wk.json"))

    #### y2d ----
    nw_delay_cause_y2d <- nw_delay_evo %>%
      mutate(
        SHARE_TDM_G = if_else(sum(DAY_DLY) == 0, 0, sum(DAY_DLY_APT_CAP) / sum(DAY_DLY)),
        SHARE_TDM_CS = if_else(sum(DAY_DLY) == 0, 0, sum(DAY_DLY_CAP_STAF_NOG) / sum(DAY_DLY)),
        SHARE_TDM_IT = if_else(sum(DAY_DLY) == 0, 0, sum(DAY_DLY_DISR) / sum(DAY_DLY)),
        SHARE_TDM_WD = if_else(sum(DAY_DLY) == 0, 0, sum(DAY_DLY_WTH) / sum(DAY_DLY)),
        SHARE_TDM_NOCSGITWD = if_else(sum(DAY_DLY) == 0, 0, sum(DAY_DLY_OTH) / sum(DAY_DLY))
      ) %>%
      select(FLIGHT_DATE,
             ROLL_WK_AVG_DLY_APT_CAP,
             ROLL_WK_AVG_DLY_CAP_STAF_NOG,
             ROLL_WK_AVG_DLY_DISR,
             ROLL_WK_AVG_DLY_WTH,
             ROLL_WK_AVG_DLY_OTH,
             ROLL_WK_AVG_DLY_PREV_YEAR,
             SHARE_TDM_G,
             SHARE_TDM_CS,
             SHARE_TDM_IT,
             SHARE_TDM_WD,
             SHARE_TDM_NOCSGITWD
      )

    colnames(nw_delay_cause_y2d) <- column_names

    ### nest data
    nw_delay_value_y2d_long <- nw_delay_cause_y2d %>%
      select(-c(share_aerodrome_capacity,
                share_capacity_staffing_atc,
                share_disruptions_atc,
                share_weather,
                share_other)
      ) %>%
      pivot_longer(-c(FLIGHT_DATE), names_to = 'metric', values_to = 'value')

    nw_delay_share_y2d_long <- nw_delay_cause_y2d %>%
      select(-c("Aerodrome capacity",
                "Capacity/Staffing (ATC)",
                "Disruptions (ATC)",
                "Weather",
                "Other",
                paste0("Total delay ", last_year - 1)
      )
      )  %>%
      mutate(share_delay_prev_year = NA) %>%
      pivot_longer(-c(FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
      select(name, share)

    nw_delay_cause_y2d_long <- cbind(nw_delay_value_y2d_long, nw_delay_share_y2d_long) %>%
      select(-name) %>%
      group_by(FLIGHT_DATE) %>%
      nest_legacy(.key = "statistics")

    nw_delay_cause_evo_y2d_j <- nw_delay_cause_y2d_long %>% toJSON(., pretty = TRUE)
    write(nw_delay_cause_evo_y2d_j, here(data_folder, 'v2', "nw_delay_category_evo_chart_y2d.json"))
    write(nw_delay_cause_evo_y2d_j, paste0(archive_dir, today, "_nw_delay_category_chart_evo_y2d.json"))
    write(nw_delay_cause_evo_y2d_j, paste0(archive_dir, "nw_delay_category_evo_chart_y2d.json"))


  ### delay per flight per type ----
  nw_delay_flt_evo_app <- nw_delay_raw %>%
    mutate(
      ROLL_WK_AVG_FLT = rollmeanr(DAY_FLT, 7, fill = NA, align = "right"),
      ROLL_WK_AVG_DLY_FLT_ERT = ROLL_WK_AVG_DLY_ERT / ROLL_WK_AVG_FLT,
      ROLL_WK_AVG_DLY_FLT_APT = ROLL_WK_AVG_DLY_APT / ROLL_WK_AVG_FLT,
      ROLL_WK_AVG_DLY_FLT_PREV_YEAR = lag(ROLL_WK_AVG_DLY, 364) / lag(ROLL_WK_AVG_FLT, 364)
    ) %>%
    filter(FLIGHT_DATE >= paste0(last_year, "-01-01")) %>%
    mutate(FLIGHT_DATE = as.Date(FLIGHT_DATE)) %>%
    select(
      FLIGHT_DATE,
      ROLL_WK_AVG_DLY_FLT_ERT,
      ROLL_WK_AVG_DLY_FLT_APT,
      ROLL_WK_AVG_DLY_FLT_PREV_YEAR
    )

  column_names <- c(
    "FLIGHT_DATE",
    "En-route ATFM delay/flight",
    "Airport ATFM delay/flight",
    paste0("Total ATFM delay/flight ", last_year - 1)
  )
  colnames(nw_delay_flt_evo_app) <- column_names

  nw_delay_flt_evo_app_j <- nw_delay_flt_evo_app %>% toJSON(., pretty = TRUE)
  write(nw_delay_flt_evo_app_j, here(data_folder, "nw_delay_flt_type_evo_chart.json"))
  write(nw_delay_flt_evo_app_j, here(data_folder, "v2", "nw_delay_flt_type_evo_chart.json"))
  write(nw_delay_flt_evo_app_j, paste0(archive_dir, today, "_nw_delay_flt_type_evo_chart.json"))

  ## punctuality ----
  nw_punct_evo_app <- nw_punct_data_raw %>%
    filter(DATE >= as.Date(paste0("01-01-", last_year - 2), format = "%d-%m-%Y")) %>%
    arrange(DATE) %>%
    mutate(DEP_PUN = DEP_PUNCTUALITY_PERCENTAGE, ARR_PUN = ARR_PUNCTUALITY_PERCENTAGE, OPERATED = 100 - MISSING_SCHEDULES_PERCENTAGE) %>%
    mutate(
      DEP_PUN_WK = rollsum(DEP_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") / rollsum(DEP_SCHEDULE_FLIGHT, 7, fill = NA, align = "right") * 100,
      ARR_PUN_WK = rollsum(ARR_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") / rollsum(ARR_SCHEDULE_FLIGHT, 7, fill = NA, align = "right") * 100,
      OP_FLT_WK = 100 - rollsum(MISSING_SCHED_FLIGHTS, 7, fill = NA, align = "right") / rollsum((MISSING_SCHED_FLIGHTS + DEP_FLIGHTS_NO_OVERFLIGHTS), 7, fill = NA, align = "right") * 100
    ) %>%
    select(DATE, DEP_PUN_WK, ARR_PUN_WK, OP_FLT_WK) %>%
    filter(DATE >= as.Date(paste0("01-01-", last_year - 1), format = "%d-%m-%Y"))


  column_names <- c(
    "FLIGHT_DATE",
    "Departure punct.",
    "Arrival punct.",
    "Operated schedules"
  )
  colnames(nw_punct_evo_app) <- column_names

  nw_punct_evo_app_j <- nw_punct_evo_app %>% toJSON(., pretty = TRUE)
  write(nw_punct_evo_app_j, here(data_folder, "nw_punct_evo_chart.json"))
  write(nw_punct_evo_app_j, here(data_folder, "v2", "nw_punct_evo_chart.json"))
  write(nw_punct_evo_app_j, paste0(archive_dir, today, "_nw_punct_evo_chart.json"))


  ## billing ----
  nw_billing_evo <- nw_billing %>%
    arrange(year, month) %>%
    mutate(
      total_billing = total_billing / 10^6,
      total_billing_py = lag(total_billing, 12),
      total_billing_dif_mm_perc = total_billing / total_billing_py - 1
    ) %>%
    group_by(year) %>%
    mutate(
      total_billing_y2d = cumsum(total_billing)
    ) %>%
    ungroup() %>%
    mutate(
      total_billing_y2d_py = lag(total_billing_y2d, 12),
      total_billing_dif_y2d_perc = total_billing_y2d / total_billing_y2d_py - 1
    ) %>%
    filter(year == last_billing_year) %>%
    select(
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
    "Month",
    last_billing_year,
    last_billing_year - 1,
    paste0("Monthly variation vs ", last_billing_year - 1),
    paste0("Year-to-date variation vs ", last_billing_year - 1),
    "min_right_axis",
    "max_right_axis"
  )

  colnames(nw_billing_evo) <- column_names

  nw_billing_evo_j <- nw_billing_evo %>% toJSON(., pretty = TRUE)
  write(nw_billing_evo_j, here(data_folder, "nw_billing_evo_chart.json"))
  write(nw_billing_evo_j, here(data_folder, "v2", "nw_billing_evo_chart.json"))
  write(nw_billing_evo_j, paste0(archive_dir, today, "_nw_billing_evo_chart.json"))

  ## co2 emissions ----
  ### app v1
  nw_co2_evo <- co2_data_raw %>%
    select(
      FLIGHT_MONTH,
      CO2_QTY_TONNES,
      TF,
      YEAR,
      MONTH
    ) %>%
    group_by(FLIGHT_MONTH) %>%
    summarise(TTF = sum(TF), TCO2 = sum(CO2_QTY_TONNES)) %>%
    mutate(
      YEAR = as.numeric(format(FLIGHT_MONTH, "%Y")),
      MONTH = as.numeric(format(FLIGHT_MONTH, "%m"))
    ) %>%
    arrange(FLIGHT_MONTH) %>%
    mutate(
      DEP_IDX = TTF / first(TTF) * 100,
      CO2_IDX = TCO2 / first(TCO2) * 100,
      FLIGHT_MONTH = ceiling_date(as_date(FLIGHT_MONTH), unit = "month") - 1
    ) %>%
    select(
      FLIGHT_MONTH,
      CO2_IDX,
      DEP_IDX
    )

  column_names <- c(
    "Month",
    "CO2 index",
    "Departures index"
  )

  colnames(nw_co2_evo) <- column_names
  write(nw_co2_evo_j, here(data_folder, "nw_co2_evo_chart.json"))
  # write(nw_co2_evo_j, paste0(archive_dir, today, "_nw_co2_evo_chart.json"))

  nw_co2_evo_j <- nw_co2_evo %>% toJSON(., pretty = TRUE)

  ### app v2
  nw_co2_evo_v2 <- nw_co2_evo %>%
    mutate(iso_2letter = 'XX', state = 'Network') %>%
    relocate(iso_2letter:state, .before = 'Month')

  ### nest data
  nw_co2_evo_v2_long <- nw_co2_evo_v2 %>%
    pivot_longer(-c(iso_2letter, state, Month), names_to = 'metric', values_to = 'value') %>%
    group_by(iso_2letter, state, Month) %>%
    nest_legacy(.key = "statistics")

  nw_co2_evo_v2_j <- nw_co2_evo_v2_long %>% toJSON(., pretty = TRUE)
  write(nw_co2_evo_v2_j, here(data_folder, "v2", "nw_co2_evo_chart.json"))
  write(nw_co2_evo_v2_j, paste0(archive_dir, today, "_nw_co2_evo_chart.json"))


# jsons for ranking tables ----

  ## Aircraft operators traffic ----

    ### day
    ao_data_dy <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "AO_DAY",
      range = cell_limits(c(3, 2), c(NA, 10))
    ) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
      filter(WK_R_RANK_BY_DAY <= 10) %>%
      select(-DY_FLIGHT_DIFF_2019_PERC, -DY_R_RANK_BY_DAY)

    ### week
    ao_data_wk <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "AO_WEEK",
      range = cell_limits(c(3, 2), c(NA, 9))
    ) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
      filter(WK_R_RANK_BY_DAY <= 10) %>%
      select(-WK_FLIGHT_DIFF_2019_PERC)

    ### y2d
    ao_data_y2d <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "TOP40_AO_ALL",
      range = cell_limits(c(5, 2), c(NA, 8))
    ) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
      filter(WK_R_RANK_BY_DAY <= 10)

    ### main card
    ao_main_traffic <- ao_data_dy %>%
      mutate(
        MAIN_TFC_AO_GRP_NAME = if_else(
          WK_R_RANK_BY_DAY <= 4,
          DY_AO_GRP_NAME,
          NA
        ),
        MAIN_TFC_AO_GRP_FLIGHT = if_else(
          WK_R_RANK_BY_DAY <= 4,
          DY_FLIGHT,
          NA
        )
      ) %>%
      select(WK_R_RANK_BY_DAY, MAIN_TFC_AO_GRP_NAME, MAIN_TFC_AO_GRP_FLIGHT)

    ao_main_traffic_dif <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "AO_DAY_MAIN",
      range = cell_limits(c(3, 2), c(NA, 4))
      )

    ### merge and reorder tables
    ao_data <- merge(x = ao_data_wk, y = ao_data_dy, by = "WK_R_RANK_BY_DAY")
    ao_data <- merge(x = ao_data, y = ao_data_y2d, by = "WK_R_RANK_BY_DAY")
    ao_data <- merge(x = ao_data, y = ao_main_traffic, by = "WK_R_RANK_BY_DAY")
    ao_data <- merge(x = ao_data, y = ao_main_traffic_dif, by = "WK_R_RANK_BY_DAY")

    ao_data <- ao_data %>%
      mutate(WK_MIN_ENTRY_DATE = WK_MAX_ENTRY_DATE - 6)

    ao_data <- ao_data %>%
      relocate(c(
        RANK = WK_R_RANK_BY_DAY,
        MAIN_TFC_AO_GRP_NAME,
        MAIN_TFC_AO_GRP_FLIGHT,
        MAIN_TFC_DIF_AO_GRP_NAME,
        MAIN_TFC_AO_GRP_DIF,
        DY_RANK_DIF_PREV_WEEK = DY_RANK_DIFF_7DAY,
        DY_AO_GRP_NAME,
        DY_TO_DATE = DY_ENTRY_DATE,
        DY_FLIGHT,
        DY_DIF_PREV_WEEK_PERC = DY_FLIGHT_DIFF_7DAY_PERC,
        DY_DIF_PREV_YEAR_PERC = DY_FLIGHT_DIFF_PERC,
        WK_RANK_DIF_PREV_WEEK = WK_RANK_DIFF_7DAY,
        WK_AO_GRP_NAME,
        WK_FROM_DATE = WK_MIN_ENTRY_DATE,
        WK_TO_DATE = WK_MAX_ENTRY_DATE,
        # WK_FLIGHT = WK_DAILY_FLIGHT,
        WK_DAILY_FLIGHT,
        WK_DIF_PREV_WEEK_PERC = WK_FLIGHT_DIFF_7DAY_PERC,
        WK_DIF_PREV_YEAR_PERC = WK_FLIGHT_DIFF_PERC,
        Y2D_RANK_DIF_PREV_YEAR,
        Y2D_AO_GRP_NAME,
        Y2D_TO_DATE = last_data_day,
        # Y2D_FLIGHT = "1_Y2D_CURRENT_YEAR",
        Y2D_DAILY_FLIGHT = "1_Y2D_CURRENT_YEAR",
        Y2D_DIF_PREV_YEAR_PERC = Dif_prev_year,
        Y2D_DIF_2019_PERC = Dif_2019
      ))

    ### covert to json and save in app data folder and archive
    ao_data_j <- ao_data %>% toJSON(., pretty = TRUE)
    write(ao_data_j, here(data_folder, "ao_ranking_traffic.json"))
    write(ao_data_j, here(data_folder, "v2", "ao_ranking_traffic.json"))
    write(ao_data_j, paste0(archive_dir, today, "_ao_ranking_traffic.json"))

  ## Airport traffic ----

    ### day
    apt_data_dy <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "APT_DAY",
      range = cell_limits(c(3, 2), c(NA, 10))
    ) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
      filter(DY_R_RANK_BY_DAY <= 10) %>%
      select(-DY_AIRPORT_CODE, -DY_DEP_ARR_2019_PERC)

    ### week
    apt_data_wk <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "APT_WEEK",
      range = cell_limits(c(3, 2), c(NA, 10))
    ) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
      filter(DY_R_RANK_BY_DAY <= 10) %>%
      select(-WK_AIRPORT_CODE, -WK_2019_DIFF_PERC)

    ### y2d
    apt_data_y2d <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "TOP40_APT_ALL",
      range = cell_limits(c(5, 2), c(NA, 11))
    ) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
      filter(DY_R_RANK_BY_DAY <= 10) %>%
      select(-Y2D_ARP_CODE, -Y2D_DEP_ARR_2019, -Y2D_DEP_ARR_PREV_YEAR)

    ### main card
    apt_main_traffic <- apt_data_dy %>%
      mutate(
        MAIN_TFC_AIRPORT_NAME = if_else(
          DY_R_RANK_BY_DAY <= 4,
          DY_AIRPORT_NAME,
          NA
        ),
        MAIN_TFC_AIRPORT_DEP_ARR = if_else(
          DY_R_RANK_BY_DAY <= 4,
          DY_DEP_ARR,
          NA
        )
      ) %>%
      select(DY_R_RANK_BY_DAY, MAIN_TFC_AIRPORT_NAME, MAIN_TFC_AIRPORT_DEP_ARR)

    apt_main_traffic_dif <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "APT_DAY_MAIN",
      range = cell_limits(c(3, 2), c(NA, 4))
    )

    ### merge and reorder tables
    apt_data <- merge(x = apt_data_dy, y = apt_data_wk, by = "DY_R_RANK_BY_DAY")
    apt_data <- merge(x = apt_data, y = apt_data_y2d, by = "DY_R_RANK_BY_DAY")
    apt_data <- merge(x = apt_data, y = apt_main_traffic, by = "DY_R_RANK_BY_DAY")
    apt_data <- merge(x = apt_data, y = apt_main_traffic_dif, by = "DY_R_RANK_BY_DAY")

    apt_data <- apt_data %>%
      mutate(WK_TO_DATE = CURRENT_WEEK_FIRST_DAY + 6) %>%
      relocate(c(
        RANK = DY_R_RANK_BY_DAY,
        MAIN_TFC_AIRPORT_NAME,
        MAIN_TFC_AIRPORT_DEP_ARR,
        MAIN_TFC_DIF_AIRPORT_NAME,
        MAIN_TFC_AIRPORT_DIF,
        DY_RANK_DIF_PREV_WEEK,
        DY_AIRPORT_NAME,
        DY_TO_DATE = DY_ENTRY_DATE,
        DY_DEP_ARR,
        DY_DIF_PREV_WEEK_PERC = DY_DEP_ARR_7DAY_PERC,
        DY_DIF_PREV_YEAR_PERC = DY_DEP_ARR_PREV_YEAR_PERC,
        WK_RANK_DIF_PREV_WEEK,
        WK_AIRPORT_NAME,
        WK_FROM_DATE = CURRENT_WEEK_FIRST_DAY,
        WK_TO_DATE,
        WK_DAILY_DEP_ARR,
        WK_DIF_PREV_WEEK_PERC = WK_PREV_WEEK_DIFF_PERC,
        WK_DIF_PREV_YEAR_PERC = WK_PREV_YEAR_DIFF_PERC,
        Y2D_RANK_DIF_PREV_YEAR,
        Y2D_ARP_NAME,
        Y2D_TO_DATE,
        Y2D_DEP_ARR = Y2D_DEP_ARR_CURRENT_YEAR,
        Y2D_DIF_PREV_YEAR_PERC,
        Y2D_DIF_2019_PERC
      ))

    ### covert to json and save in app data folder and archive
    apt_data_j <- apt_data %>% toJSON(., pretty = TRUE)
    write(apt_data_j, here(data_folder, "apt_ranking_traffic.json"))
    write(apt_data_j, here(data_folder, "v2", "apt_ranking_traffic.json"))
    write(apt_data_j, paste0(archive_dir, today, "_apt_ranking_traffic.json"))

  ## Country traffic DAI ----

    ### day
    ct_dai_data_dy <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "CTRY_DAI_DAY",
      range = cell_limits(c(3, 2), c(NA, 8))
    ) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
      filter(DY_R_RANK_BY_DAY <= 10)

    ### week
    ct_dai_data_wk <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "CTRY_DAI_WK",
      range = cell_limits(c(3, 2), c(NA, 9))
    ) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
      filter(DY_R_RANK_BY_DAY <= 10)

    ### y2d
    ct_dai_data_y2d <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "CTRY_DAI_Y2D",
      range = cell_limits(c(3, 2), c(NA, 8))
    ) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
      filter(DY_R_RANK_BY_DAY <= 10)

    ### main card
    ct_main_traffic <- ct_dai_data_dy %>%
      mutate(
        MAIN_TFC_CTRY_NAME = if_else(
          DY_R_RANK_BY_DAY <= 4,
          DY_COUNTRY_NAME,
          NA
        ),
        MAIN_TFC_CTRY_DAI = if_else(
          DY_R_RANK_BY_DAY <= 4,
          DY_CTRY_DAI,
          NA
        )
      ) %>%
      select(DY_R_RANK_BY_DAY, MAIN_TFC_CTRY_NAME, MAIN_TFC_CTRY_DAI)

    ct_main_traffic_dif <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "CTRY_DAI_MAIN",
      range = cell_limits(c(3, 2), c(NA, 4))
    )

    ### merge and reorder tables
    ct_dai_data <- merge(x = ct_dai_data_dy, y = ct_dai_data_wk, by = "DY_R_RANK_BY_DAY")
    ct_dai_data <- merge(x = ct_dai_data, y = ct_dai_data_y2d, by = "DY_R_RANK_BY_DAY")
    ct_dai_data <- merge(x = ct_dai_data, y = ct_main_traffic, by = "DY_R_RANK_BY_DAY")
    ct_dai_data <- merge(x = ct_dai_data, y = ct_main_traffic_dif, by = "DY_R_RANK_BY_DAY")

    ct_dai_data <- ct_dai_data %>%
      relocate(c(
        RANK = DY_R_RANK_BY_DAY,
        MAIN_TFC_CTRY_NAME,
        MAIN_TFC_CTRY_DAI,
        MAIN_TFC_DIF_CTRY_NAME,
        MAIN_TFC_CTRY_DIF,
        DY_RANK_DIF_PREV_WEEK,
        DY_COUNTRY_NAME,
        DY_TO_DATE = DY_ENTRY_DATE,
        DY_CTRY_DAI,
        DY_DIF_PREV_WEEK_PERC = DY_CTRY_DAI_7DAY_PERC,
        DY_DIF_PREV_YEAR_PERC = DY_CTRY_DAI_PREV_YEAR_PERC,
        WK_RANK_DIF_PREV_WEEK,
        WK_COUNTRY_NAME,
        WK_FROM_DATE,
        WK_TO_DATE,
        WK_CTRY_DAI,
        WK_DIF_PREV_WEEK_PERC = WK_CTRY_DAI_7DAY_PERC,
        WK_DIF_PREV_YEAR_PERC = WK_CTRY_DAI_PREV_YEAR_PERC,
        Y2D_RANK_DIF_PREV_YEAR,
        Y2D_COUNTRY_NAME,
        Y2D_TO_DATE,
        Y2D_CTRY_DAI,
        Y2D_CTRY_DAI_PREV_YEAR_PERC,
        Y2D_RANK_DIF_PREV_YEAR
      ))

    ### covert to json and save in app data folder and archive
    ct_dai_data_j <- ct_dai_data %>% toJSON(., pretty = TRUE)
    write(ct_dai_data_j, here(data_folder, "ctry_ranking_traffic_DAI.json"))
    write(ct_dai_data_j, here(data_folder, "v2", "ctry_ranking_traffic_DAI.json"))
    write(ct_dai_data_j, paste0(archive_dir, today, "_ctry_ranking_traffic_DAI.json"))

  ## Airport delay -----

    ### raw data
    apt_rank_data_raw <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "APT_DELAY",
      range = cell_limits(c(5, 2), c(NA, 50))
    ) %>%
      as_tibble()

    ### day data
    apt_rank_data_day <- apt_rank_data_raw %>%
      arrange(desc(DLY_ARR), ARP_NAME) %>%
      mutate(
        R_RANK_DLY_DAY = row_number(),
        ARP_NAME_DAY = ARP_NAME,
        DLY_PER_FLT = ifelse(FLT_ARR == 0, 0, round(DLY_ARR / FLT_ARR, 2))
      ) %>%
      select(R_RANK_DLY_DAY, ARP_NAME_DAY, FLIGHT_DATE, DLY_ARR, DLY_PER_FLT) %>%
      as.data.frame() %>%
      filter(R_RANK_DLY_DAY <= 10)

    ### week
    apt_rank_data_week <- apt_rank_data_raw %>%
      arrange(desc(ROLL_WEEK_DLY_ARR), ARP_NAME) %>%
      mutate(
        R_RANK_DLY_DAY = row_number(),
        WK_RANK = R_RANK_DLY_DAY,
        ARP_NAME_WK = ARP_NAME,
        DLY_PER_FLT_WEEK = ifelse(ROLL_WEEK_ARR == 0, 0, round(ROLL_WEEK_DLY_ARR / ROLL_WEEK_ARR, 2)),
        WK_FROM_DATE = FLIGHT_DATE + days(-6),
        WK_TO_DATE = FLIGHT_DATE
      ) %>%
      select(
        R_RANK_DLY_DAY, WK_RANK, ARP_NAME_WK, WK_FROM_DATE, WK_TO_DATE,
        ROLL_WEEK_DLY_ARR, DLY_PER_FLT_WEEK
      ) %>%
      as.data.frame() %>%
      filter(R_RANK_DLY_DAY <= 10)

    ### y2d
    apt_rank_data_y2d <- apt_rank_data_raw %>%
      arrange(desc(Y2D_AVG_DLY_ARR), ARP_NAME) %>%
      mutate(
        R_RANK_DLY_DAY = row_number(),
        Y2D_RANK = R_RANK_DLY_DAY,
        ARP_NAME_Y2D = ARP_NAME,
        DLY_PER_FLT_Y2D = ifelse(Y2D_AVG_FLT == 0,
          0,
          round(Y2D_AVG_DLY / Y2D_AVG_ARR, 2)
        ),
        Y2D_TO_DATE = FLIGHT_DATE
      ) %>%
      select(R_RANK_DLY_DAY, Y2D_RANK, ARP_NAME_Y2D, Y2D_TO_DATE, Y2D_AVG_DLY_ARR, DLY_PER_FLT_Y2D) %>%
      as.data.frame()

    ### main card
    apt_main_delay <- apt_rank_data_day %>%
      mutate(
        MAIN_DLY_APT_NAME = if_else(
          R_RANK_DLY_DAY <= 4,
          ARP_NAME_DAY,
          NA
        ),
        MAIN_DLY_APT_DLY = if_else(
          R_RANK_DLY_DAY <= 4,
          DLY_ARR,
          NA
        )
      ) %>%
      select(R_RANK_DLY_DAY, MAIN_DLY_APT_NAME, MAIN_DLY_APT_DLY)

    apt_main_delay_flt <- apt_rank_data_day %>%
      arrange(desc(DLY_PER_FLT), ARP_NAME_DAY) %>%
      mutate(
        R_RANK_DLY_DAY = row_number(),
        MAIN_DLY_FLT_APT_NAME = if_else(
          R_RANK_DLY_DAY <= 4,
          ARP_NAME_DAY,
          NA
        ),
        MAIN_DLY_FLT_APT_DLY_FLT = if_else(
          R_RANK_DLY_DAY <= 4,
          DLY_PER_FLT,
          NA
        )
      ) %>%
      select(R_RANK_DLY_DAY, MAIN_DLY_FLT_APT_NAME, MAIN_DLY_FLT_APT_DLY_FLT)

    ### merge and reorder tables
    apt_rank_data <- merge(x = apt_rank_data_day, y = apt_rank_data_week, by = "R_RANK_DLY_DAY")
    apt_rank_data <- merge(x = apt_rank_data, y = apt_rank_data_y2d, by = "R_RANK_DLY_DAY")
    apt_rank_data <- merge(x = apt_rank_data, y = apt_main_delay, by = "R_RANK_DLY_DAY")
    apt_rank_data <- merge(x = apt_rank_data, y = apt_main_delay_flt, by = "R_RANK_DLY_DAY")

    apt_rank_data <- apt_rank_data %>%
      relocate(c(
        RANK = R_RANK_DLY_DAY,
        MAIN_DLY_APT_NAME,
        MAIN_DLY_APT_DLY,
        MAIN_DLY_FLT_APT_NAME,
        MAIN_DLY_FLT_APT_DLY_FLT,
        DY_RANK = R_RANK_DLY_DAY,
        DY_AIRPORT_NAME = ARP_NAME_DAY,
        DY_TO_DATE = FLIGHT_DATE,
        DY_AIRPORT_DLY = DLY_ARR,
        DY_AIRPORT_DLY_PER_FLT = DLY_PER_FLT,
        WK_RANK,
        WK_AIRPORT_NAME = ARP_NAME_WK,
        WK_FROM_DATE,
        WK_TO_DATE,
        WK_AIRPORT_DLY = ROLL_WEEK_DLY_ARR,
        WK_AIRPORT_DLY_PER_FLT = DLY_PER_FLT_WEEK,
        Y2D_RANK,
        Y2D_AIRPORT_NAME = ARP_NAME_Y2D,
        Y2D_TO_DATE,
        Y2D_AIRPORT_DLY = Y2D_AVG_DLY_ARR,
        Y2D_AIRPORT_DLY_PER_FLT = DLY_PER_FLT_Y2D
      ))

    ### covert to json and save in app data folder and archive
    apt_rank_data_j <- apt_rank_data %>% toJSON(., pretty = TRUE)
    write(apt_rank_data_j, here(data_folder, "apt_ranking_delay.json"))
    write(apt_rank_data_j, here(data_folder, "v2", "apt_ranking_delay.json"))
    write(apt_rank_data_j, paste0(archive_dir, today, "_apt_ranking_delay.json"))


  ## ACC delay ----

    ### day data
    acc_rank_data_day_raw <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "ACC_DAY_DELAY",
      range = cell_limits(c(5, 1), c(NA, 20))
    ) %>%
      as_tibble() %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
      arrange(desc(DLY_ER), NAME) %>%
      mutate(
        DY_RANK = row_number(),
        DY_ACC_NAME = NAME,
        DY_ACC_DLY = DLY_ER,
        DY_ACC_DLY_PER_FLT = DLY_ER / FLIGHT,
        DY_TO_DATE = ENTRY_DATE
      )

    acc_rank_data_day <- acc_rank_data_day_raw %>%
      select(DY_RANK, R_RANK_DLY_DAY, DY_ACC_NAME, DY_TO_DATE, DY_ACC_DLY, DY_ACC_DLY_PER_FLT) %>%
      filter(DY_RANK <= 10)

    ### week

    acc_rank_data_week_raw <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "ACC_WEEK_DELAY",
      range = cell_limits(c(5, 1), c(NA, 16))
    ) %>%
      as_tibble() %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    acc_rank_data_week <- acc_rank_data_week_raw %>%
      arrange(desc(DAILY_DLY_ER), NAME) %>%
      mutate(
        DY_RANK = row_number(),
        WK_RANK = row_number(),
        WK_ACC_NAME = NAME,
        WK_ACC_DLY = DAILY_DLY_ER,
        WK_ACC_DLY_PER_FLT = DAILY_DLY_ER / DAILY_FLIGHT,
        WK_FROM_DATE = MIN_ENTRY_DATE,
        WK_TO_DATE = MAX_ENTRY_DATE
      ) %>%
      select(DY_RANK, WK_RANK, WK_ACC_NAME, WK_FROM_DATE, WK_TO_DATE, WK_ACC_DLY, WK_ACC_DLY_PER_FLT) %>%
      filter(DY_RANK <= 10)

    ### y2d

    acc_rank_data_y2d_raw <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "ACC_Y2D_DELAY",
      range = cell_limits(c(7, 1), c(NA, 12))
    ) %>%
      as_tibble() %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    acc_rank_data_y2d <- acc_rank_data_y2d_raw %>%
      arrange(desc(Y2D_AVG_DLY_ER), NAME) %>%
      mutate(
        DY_RANK = row_number(),
        Y2D_RANK = row_number(),
        Y2D_ACC_NAME = NAME,
        Y2D_ACC_DLY = Y2D_AVG_DLY_ER,
        Y2D_ACC_DLY_PER_FLT = Y2D_AVG_DLY_ER / Y2D_AVG_FLIGHT,
        Y2D_TO_DATE = ENTRY_DATE
      ) %>%
      select(DY_RANK, Y2D_RANK, Y2D_ACC_NAME, Y2D_TO_DATE, Y2D_ACC_DLY, Y2D_ACC_DLY_PER_FLT) %>%
      filter(DY_RANK <= 10)

    ### main card
    acc_main_delay <- acc_rank_data_day %>%
      mutate(
        MAIN_DLY_ACC_NAME = if_else(
          DY_RANK <= 4,
          DY_ACC_NAME,
          NA
        ),
        MAIN_DLY_ACC_DLY = if_else(
          DY_RANK <= 4,
          DY_ACC_DLY,
          NA
        )
      ) %>%
      select(DY_RANK, MAIN_DLY_ACC_NAME, MAIN_DLY_ACC_DLY)

    acc_main_delay_flt <- acc_rank_data_day_raw %>%
      arrange(desc(DY_ACC_DLY_PER_FLT), NAME) %>%
      mutate(
        DY_RANK = row_number(),
        MAIN_DLY_FLT_ACC_NAME = if_else(
          DY_RANK <= 4,
          NAME,
          NA
        ),
        MAIN_DLY_FLT_ACC_DLY_FLT = if_else(
          DY_RANK <= 4,
          DY_ACC_DLY_PER_FLT,
          NA
        )
      ) %>%
      select(DY_RANK, MAIN_DLY_FLT_ACC_NAME, MAIN_DLY_FLT_ACC_DLY_FLT) %>%
      filter(DY_RANK <= 10)

    ### merge and reorder tables
    acc_rank_data <- merge(x = acc_rank_data_day, y = acc_rank_data_week, by = "DY_RANK")
    acc_rank_data <- merge(x = acc_rank_data, y = acc_rank_data_y2d, by = "DY_RANK")
    acc_rank_data <- merge(x = acc_rank_data, y = acc_main_delay, by = "DY_RANK")
    acc_rank_data <- merge(x = acc_rank_data, y = acc_main_delay_flt, by = "DY_RANK")

    acc_rank_data <- acc_rank_data %>%
      relocate(c(
        RANK = DY_RANK,
        MAIN_DLY_ACC_NAME,
        MAIN_DLY_ACC_DLY,
        MAIN_DLY_FLT_ACC_NAME,
        MAIN_DLY_FLT_ACC_DLY_FLT,
        DY_RANK = DY_RANK,
        DY_ACC_NAME,
        DY_TO_DATE,
        DY_ACC_DLY,
        DY_ACC_DLY_PER_FLT,
        WK_RANK,
        WK_ACC_NAME,
        WK_FROM_DATE,
        WK_TO_DATE,
        WK_ACC_DLY,
        WK_ACC_DLY_PER_FLT,
        Y2D_RANK,
        Y2D_ACC_NAME,
        Y2D_TO_DATE,
        Y2D_ACC_DLY,
        Y2D_ACC_DLY_PER_FLT
      ))

    ### covert to json and save in app data folder and archive
    acc_rank_data_j <- acc_rank_data %>% toJSON(., pretty = TRUE)
    write(acc_rank_data_j, here(data_folder, "acc_ranking_delay.json"))
    write(acc_rank_data_j, here(data_folder, "v2", "acc_ranking_delay.json"))
    write(acc_rank_data_j, paste0(archive_dir, today, "_acc_ranking_delay.json"))


  ## Country delay ----

    ### day data
    ct_rank_data_day_raw <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "CTRY_DLY_DAY",
      range = cell_limits(c(5, 2), c(NA, 5))
    ) %>%
      as_tibble() %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))


    ct_rank_data_day <- ct_rank_data_day_raw %>%
      arrange(desc(DY_CTRY_DLY), DY_CTRY_DLY_NAME) %>%
      mutate(
        RANK = row_number(),
        DY_RANK = RANK
      ) %>%
      filter(DY_RANK <= 10)

    ### week

    ct_rank_data_week_raw <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "CTRY_DLY_WK",
      range = cell_limits(c(3, 2), c(NA, 6))
    ) %>%
      as_tibble() %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    ct_rank_data_week <- ct_rank_data_week_raw %>%
      arrange(desc(WK_CTRY_DLY), WK_CTRY_DLY_NAME) %>%
      mutate(
        DY_RANK = row_number(),
        WK_RANK = DY_RANK
      ) %>%
      filter(DY_RANK <= 10)

    ### y2d

    ct_rank_data_y2d_raw <- read_xlsx(
      path = fs::path_abs(
        str_glue(base_file),
        start = base_dir
      ),
      sheet = "CTRY_DLY_Y2D",
      range = cell_limits(c(3, 2), c(NA, 6))
    ) %>%
      as_tibble() %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    ct_rank_data_y2d <- ct_rank_data_y2d_raw %>%
      arrange(desc(Y2D_CTRY_DLY), Y2D_CTRY_DLY_NAME) %>%
      mutate(
        DY_RANK = row_number(),
        Y2D_RANK = DY_RANK
      ) %>%
      filter(DY_RANK <= 10)

    ### main card
    ct_main_delay <- ct_rank_data_day %>%
      mutate(
        MAIN_DLY_CTRY_NAME = if_else(
          DY_RANK <= 4,
          DY_CTRY_DLY_NAME,
          NA
        ),
        MAIN_DLY_CTRY_DLY = if_else(
          DY_RANK <= 4,
          DY_CTRY_DLY,
          NA
        )
      ) %>%
      select(DY_RANK, MAIN_DLY_CTRY_NAME, MAIN_DLY_CTRY_DLY) %>%
      filter(DY_RANK <= 10)

    ct_main_delay_flt <- ct_rank_data_day_raw %>%
      arrange(desc(DY_CTRY_DLY_PER_FLT), DY_CTRY_DLY_NAME) %>%
      mutate(
        DY_RANK = row_number(),
        MAIN_DLY_FLT_CTRY_NAME = if_else(
          DY_RANK <= 4,
          DY_CTRY_DLY_NAME,
          NA
        ),
        MAIN_DLY_FLT_CTRY_DLY_FLT = if_else(
          DY_RANK <= 4,
          DY_CTRY_DLY_PER_FLT,
          NA
        )
      ) %>%
      select(DY_RANK, MAIN_DLY_FLT_CTRY_NAME, MAIN_DLY_FLT_CTRY_DLY_FLT) %>%
      filter(DY_RANK <= 10)

    ### merge and reorder tables
    ct_rank_data <- merge(x = ct_rank_data_day, y = ct_rank_data_week, by = "DY_RANK")
    ct_rank_data <- merge(x = ct_rank_data, y = ct_rank_data_y2d, by = "DY_RANK")
    ct_rank_data <- merge(x = ct_rank_data, y = ct_main_delay, by = "DY_RANK")
    ct_rank_data <- merge(x = ct_rank_data, y = ct_main_delay_flt, by = "DY_RANK")

    ct_rank_data <- ct_rank_data %>%
      relocate(c(
        RANK,
        MAIN_DLY_CTRY_NAME,
        MAIN_DLY_CTRY_DLY,
        MAIN_DLY_FLT_CTRY_NAME,
        MAIN_DLY_FLT_CTRY_DLY_FLT,
        DY_RANK,
        DY_CTRY_DLY_NAME,
        DY_TO_DATE,
        DY_CTRY_DLY,
        DY_CTRY_DLY_PER_FLT,
        WK_RANK,
        WK_CTRY_DLY_NAME,
        WK_FROM_DATE,
        WK_TO_DATE,
        WK_CTRY_DLY,
        WK_CTRY_DLY_PER_FLT,
        Y2D_RANK,
        Y2D_CTRY_DLY_NAME,
        Y2D_TO_DATE,
        Y2D_CTRY_DLY,
        Y2D_CTRY_DLY_PER_FLT
      ))

    ### covert to json and save in app data folder and archive
    ct_rank_data_j <- ct_rank_data %>% toJSON(., pretty = TRUE)
    write(ct_rank_data_j, here(data_folder, "ctry_ranking_delay.json"))
    write(ct_rank_data_j, here(data_folder, "v2", "ctry_ranking_delay.json"))
    write(ct_rank_data_j, paste0(archive_dir, today, "_ctry_ranking_delay.json"))


  ## Airport punctuality ----
    ### we need data from 2019 so I'm using the source view instead of the excel file

    query <- "
         WITH
            DIM_AIRPORT as (
              SELECT
                a.code as arp_code, a.id as arp_id, a.dashboard_name as arp_name,
                a.ISO_COUNTRY_CODE
              FROM prudev.pru_airport a
            )

          , LIST_AIRPORT as (
                select distinct
                    a.ICAO_CODE as arp_code,
                    b.arp_name,
                    b.iso_country_code
                from LDW_VDM.VIEW_FAC_PUNCTUALITY_AP_DAY a
                left join DIM_AIRPORT b on a.icao_code = b.arp_code
                order by 1

            ),

            LIST_STATE as (
              SELECT
                AIU_ISO_COUNTRY_NAME as EC_ISO_CT_NAME,
                AIU_ISO_COUNTRY_CODE AS EC_ISO_CT_CODE
              FROM prudev.pru_country_iso
              WHERE till > TRUNC(SYSDATE)-1
            ),

            APT_DAY AS (
              SELECT
                      a.arp_code,
                      a.arp_name,
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
                a.* , b.*
              FROM APT_DAY a
              left join LDW_VDM.VIEW_FAC_PUNCTUALITY_AP_DAY b on a.day_date = b.\"DATE\" and a.arp_code = b.icao_code
              where a.arp_code not in ('LTBA', 'UKBB')
              order by a.ARP_CODE, b.\"DATE\"
       "

    apt_punct_raw <- export_query(query)

    last_punctuality_day <- max(apt_punct_raw$DAY_DATE)

    ### calc
    apt_punct_calc <- apt_punct_raw %>%
      group_by(DAY_DATE) %>%
      arrange(desc(ARR_PUNCTUALITY_PERCENTAGE), ARP_NAME) %>%
      mutate(RANK = row_number()) %>%
      ungroup() %>%
      # select(DAY_DATE, ARP_NAME, ARR_PUNCTUALITY_PERCENTAGE, RANK)
      group_by(ARP_NAME) %>%
      arrange(DAY_DATE) %>%
      mutate(
        DY_RANK_DIF_PREV_WEEK = lag(RANK, 7) - RANK,
        DY_PUNCT_DIF_PREV_WEEK_PERC = (ARR_PUNCTUALITY_PERCENTAGE - lag(ARR_PUNCTUALITY_PERCENTAGE, 7)) / 100,
        DY_PUNCT_DIF_PREV_YEAR_PERC = (ARR_PUNCTUALITY_PERCENTAGE - lag(ARR_PUNCTUALITY_PERCENTAGE, 364)) / 100,
        WK_APT_ARR_PUNCT = rollsum(ARR_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") / rollsum(ARR_SCHEDULE_FLIGHT, 7, fill = NA, align = "right")
      ) %>%
      ungroup()

    ### day
    apt_punct_dy <- apt_punct_calc %>%
      filter(DAY_DATE == last_punctuality_day, RANK < 11) %>%
      mutate(
        DY_APT_NAME = ARP_NAME,
        DY_APT_ARR_PUNCT = ARR_PUNCTUALITY_PERCENTAGE / 100,
        DY_TO_DATE = round_date(DAY_DATE, "day")
      ) %>%
      select(
        RANK,
        DY_RANK_DIF_PREV_WEEK,
        DY_APT_NAME,
        DY_TO_DATE,
        DY_APT_ARR_PUNCT,
        DY_PUNCT_DIF_PREV_WEEK_PERC,
        DY_PUNCT_DIF_PREV_YEAR_PERC
      )

    ### week
    apt_punct_wk <- apt_punct_calc %>%
      group_by(DAY_DATE) %>%
      arrange(desc(WK_APT_ARR_PUNCT), ARP_NAME) %>%
      mutate(
        RANK = row_number(),
        WK_RANK = RANK
      ) %>%
      ungroup() %>%
      group_by(ARP_NAME) %>%
      arrange(DAY_DATE) %>%
      mutate(
        WK_RANK_DIF_PREV_WEEK = lag(RANK, 7) - RANK,
        WK_PUNCT_DIF_PREV_WEEK_PERC = (WK_APT_ARR_PUNCT - lag(WK_APT_ARR_PUNCT, 7)),
        WK_PUNCT_DIF_PREV_YEAR_PERC = (WK_APT_ARR_PUNCT - lag(WK_APT_ARR_PUNCT, 364))
      ) %>%
      ungroup() %>%
      filter(DAY_DATE == last_punctuality_day, RANK < 11) %>%
      mutate(
        WK_APT_NAME = ARP_NAME,
        WK_FROM_DATE = round_date(DAY_DATE, "day") + lubridate::days(-6),
        WK_TO_DATE = round_date(DAY_DATE, "day")
      ) %>%
      select(
        RANK,
        WK_RANK_DIF_PREV_WEEK,
        WK_APT_NAME,
        WK_FROM_DATE,
        WK_TO_DATE,
        WK_APT_ARR_PUNCT,
        WK_PUNCT_DIF_PREV_WEEK_PERC,
        WK_PUNCT_DIF_PREV_YEAR_PERC
      )

    ### y2d
    apt_punct_y2d <- apt_punct_calc %>%
      mutate(MONTH_DAY = as.numeric(format(DAY_DATE, format = "%m%d"))) %>%
      filter(MONTH_DAY <= as.numeric(format(last_punctuality_day, format = "%m%d"))) %>%
      mutate(YEAR = as.numeric(format(DAY_DATE, format = "%Y"))) %>%
      group_by(ARP_NAME, ICAO_CODE, YEAR) %>%
      summarise(Y2D_APT_ARR_PUNCT = sum(ARR_PUNCTUAL_FLIGHTS, na.rm = TRUE) / sum(ARR_SCHEDULE_FLIGHT, na.rm = TRUE)) %>%
      ungroup() %>%
      group_by(YEAR) %>%
      arrange(desc(Y2D_APT_ARR_PUNCT), ARP_NAME) %>%
      mutate(
        RANK = row_number(),
        Y2D_RANK = RANK
      ) %>%
      ungroup() %>%
      group_by(ARP_NAME) %>%
      arrange(YEAR) %>%
      mutate(
        Y2D_RANK_DIF_PREV_YEAR = lag(RANK, 1) - RANK,
        Y2D_PUNCT_DIF_PREV_YEAR_PERC = (Y2D_APT_ARR_PUNCT - lag(Y2D_APT_ARR_PUNCT, 1)),
        Y2D_PUNCT_DIF_2019_PERC = (Y2D_APT_ARR_PUNCT - lag(Y2D_APT_ARR_PUNCT, max(YEAR) - 2019))
      ) %>%
      ungroup() %>%
      filter(YEAR == max(YEAR), RANK < 11) %>%
      mutate(Y2D_APT_NAME = ARP_NAME) %>%
      select(
        RANK,
        Y2D_RANK_DIF_PREV_YEAR,
        Y2D_APT_NAME,
        Y2D_APT_ARR_PUNCT,
        Y2D_PUNCT_DIF_PREV_YEAR_PERC,
        Y2D_PUNCT_DIF_2019_PERC
      )

    ### main card
    apt_main_punct_top <- apt_punct_dy %>%
      mutate(
        MAIN_PUNCT_APT_NAME = if_else(
          RANK <= 4,
          DY_APT_NAME,
          NA
        ),
        MAIN_PUNCT_APT_ARR_PUNCT = if_else(
          RANK <= 4,
          DY_APT_ARR_PUNCT,
          NA
        )
      ) %>%
      select(RANK, MAIN_PUNCT_APT_NAME, MAIN_PUNCT_APT_ARR_PUNCT)

    apt_main_punct_bottom <- apt_punct_calc %>%
      filter(DAY_DATE == last_punctuality_day) %>%
      mutate(
        RANK = max(RANK) + 1 - RANK,
        DY_APT_NAME = ARP_NAME,
        DY_APT_ARR_PUNCT = ARR_PUNCTUALITY_PERCENTAGE / 100
      ) %>%
      mutate(
        MAIN_PUNCT_APT_NAME_BOTTOM = if_else(
          RANK <= 4,
          DY_APT_NAME,
          NA
        ),
        MAIN_PUNCT_APT_ARR_PUNCT_BOTTOM = if_else(
          RANK <= 4,
          DY_APT_ARR_PUNCT,
          NA
        )
      ) %>%
      filter(RANK < 11) %>%
      arrange(RANK) %>%
      select(RANK, MAIN_PUNCT_APT_NAME_BOTTOM, MAIN_PUNCT_APT_ARR_PUNCT_BOTTOM)


    ### merge and reorder tables
    apt_punct_data <- merge(x = apt_punct_dy, y = apt_punct_wk, by = "RANK")
    apt_punct_data <- merge(x = apt_punct_data, y = apt_punct_y2d, by = "RANK")
    apt_punct_data <- merge(x = apt_punct_data, y = apt_main_punct_top, by = "RANK")
    apt_punct_data <- merge(x = apt_punct_data, y = apt_main_punct_bottom, by = "RANK")

    apt_punct_data <- apt_punct_data %>%
      mutate(Y2D_TO_DATE = DY_TO_DATE) %>%
      relocate(c(
        RANK,
        MAIN_PUNCT_APT_NAME,
        MAIN_PUNCT_APT_ARR_PUNCT,
        MAIN_PUNCT_APT_NAME_BOTTOM,
        MAIN_PUNCT_APT_ARR_PUNCT_BOTTOM,
        DY_RANK_DIF_PREV_WEEK,
        DY_APT_NAME,
        DY_TO_DATE,
        DY_APT_ARR_PUNCT,
        DY_PUNCT_DIF_PREV_WEEK_PERC,
        DY_PUNCT_DIF_PREV_YEAR_PERC,
        WK_RANK_DIF_PREV_WEEK,
        WK_APT_NAME,
        WK_FROM_DATE,
        WK_TO_DATE,
        WK_APT_ARR_PUNCT,
        WK_PUNCT_DIF_PREV_WEEK_PERC,
        WK_PUNCT_DIF_PREV_YEAR_PERC,
        Y2D_RANK_DIF_PREV_YEAR,
        Y2D_APT_NAME,
        Y2D_TO_DATE,
        Y2D_APT_ARR_PUNCT,
        Y2D_PUNCT_DIF_PREV_YEAR_PERC,
        Y2D_PUNCT_DIF_2019_PERC
      ))

    ### covert to json and save in app data folder and archive
    apt_punct_data_j <- apt_punct_data %>% toJSON(., pretty = TRUE)
    write(apt_punct_data_j, here(data_folder, "apt_ranking_punctuality.json"))
    write(apt_punct_data_j, here(data_folder, "v2", "apt_ranking_punctuality.json"))
    write(apt_punct_data_j, paste0(archive_dir, today, "_apt_ranking_punctuality.json"))

  ## Country punctuality ----
  ##### NOte: the time series for each country is not full. At some point it needs to be fixed either here or in the initial query so the lag functions yield the right result

    ### we need data from 2019 so I'm using the source view instead of the excel file

    query <- "
    WITH
      LIST_STATE as (
        SELECT
          EC_ISO_CT_NAME, EC_ISO_CT_CODE
        FROM SWH_FCT.DIM_ISO_COUNTRY
        WHERE VALID_TO > TRUNC(SYSDATE)-1
      )

      SELECT
        a.*,
        b.EC_ISO_CT_NAME
      FROM LDW_VDM.VIEW_FAC_PUNCTUALITY_CT_DAY a
      LEFT JOIN LIST_STATE b on a.ISO_CT_CODE = b.EC_ISO_CT_CODE
      ORDER BY b.EC_ISO_CT_NAME
       "

    ct_punct_raw <- export_query(query) %>%
      filter(
        ISO_CT_CODE != "GI",
        ISO_CT_CODE != "FO",
        ISO_CT_CODE != "SJ",
        ISO_CT_CODE != "MC",
        ISO_CT_CODE != "PM",
        ISO_CT_CODE != "UA"
      ) %>%
      mutate_at("EC_ISO_CT_NAME", ~ if_else(. == "Turkiye", "Türkiye", .))

    last_punctuality_day <- max(ct_punct_raw$DATE)

    ### calc
    ct_punct_calc <- ct_punct_raw %>%
      group_by(DATE) %>%
      arrange(desc(ARR_PUNCTUALITY_PERCENTAGE), EC_ISO_CT_NAME) %>%
      mutate(RANK = row_number()) %>%
      ungroup() %>%
      group_by(EC_ISO_CT_NAME) %>%
      arrange(DATE) %>%
      mutate(
        DY_RANK_DIF_PREV_WEEK = lag(RANK, 7) - RANK,
        DY_PUNCT_DIF_PREV_WEEK_PERC = (ARR_PUNCTUALITY_PERCENTAGE - lag(ARR_PUNCTUALITY_PERCENTAGE, 7)) / 100,
        DY_PUNCT_DIF_PREV_YEAR_PERC = (ARR_PUNCTUALITY_PERCENTAGE - lag(ARR_PUNCTUALITY_PERCENTAGE, 364)) / 100,
        WK_CTRY_ARR_PUNCT = rollsum(ARR_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") / rollsum(ARR_SCHEDULE_FLIGHT, 7, fill = NA, align = "right")
      ) %>%
      ungroup()

    ### day
    ct_punct_dy <- ct_punct_calc %>%
      filter(DATE == last_punctuality_day, RANK < 11) %>%
      mutate(
        DY_CTRY_NAME = EC_ISO_CT_NAME,
        DY_CTRY_ARR_PUNCT = ARR_PUNCTUALITY_PERCENTAGE / 100,
        DY_TO_DATE = round_date(DATE, "day")
      ) %>%
      select(
        RANK,
        DY_TO_DATE,
        DY_RANK_DIF_PREV_WEEK,
        DY_CTRY_NAME,
        DY_CTRY_ARR_PUNCT,
        DY_PUNCT_DIF_PREV_WEEK_PERC,
        DY_PUNCT_DIF_PREV_YEAR_PERC
      )

    ### week
    ct_punct_wk <- ct_punct_calc %>%
      group_by(DATE) %>%
      arrange(desc(WK_CTRY_ARR_PUNCT), EC_ISO_CT_NAME) %>%
      mutate(
        RANK = row_number(),
        WK_RANK = RANK
      ) %>%
      ungroup() %>%
      group_by(EC_ISO_CT_NAME) %>%
      arrange(DATE) %>%
      mutate(
        WK_RANK_DIF_PREV_WEEK = lag(RANK, 7) - RANK,
        WK_PUNCT_DIF_PREV_WEEK_PERC = (WK_CTRY_ARR_PUNCT - lag(WK_CTRY_ARR_PUNCT, 7)),
        WK_PUNCT_DIF_PREV_YEAR_PERC = (WK_CTRY_ARR_PUNCT - lag(WK_CTRY_ARR_PUNCT, 364))
      ) %>%
      ungroup() %>%
      filter(DATE == last_punctuality_day, RANK < 11) %>%
      mutate(
        WK_CTRY_NAME = EC_ISO_CT_NAME,
        WK_FROM_DATE = round_date(DATE, "day") + lubridate::days(-6),
        WK_TO_DATE = round_date(DATE, "day")
      ) %>%
      select(
        RANK,
        WK_FROM_DATE,
        WK_TO_DATE,
        WK_RANK_DIF_PREV_WEEK,
        WK_CTRY_NAME,
        WK_CTRY_ARR_PUNCT,
        WK_PUNCT_DIF_PREV_WEEK_PERC,
        WK_PUNCT_DIF_PREV_YEAR_PERC
      )

    ### y2d
    ct_punct_y2d <- ct_punct_calc %>%
      mutate(MONTH_DAY = as.numeric(format(DATE, format = "%m%d"))) %>%
      filter(MONTH_DAY <= as.numeric(format(last_punctuality_day, format = "%m%d"))) %>%
      mutate(YEAR = as.numeric(format(DATE, format = "%Y"))) %>%
      group_by(EC_ISO_CT_NAME, YEAR) %>%
      summarise(Y2D_CTRY_ARR_PUNCT = sum(ARR_PUNCTUAL_FLIGHTS, na.rm = TRUE) / sum(ARR_SCHEDULE_FLIGHT, na.rm = TRUE)) %>%
      ungroup() %>%
      group_by(YEAR) %>%
      arrange(desc(Y2D_CTRY_ARR_PUNCT), EC_ISO_CT_NAME) %>%
      mutate(
        RANK = row_number(),
        Y2D_RANK = RANK
      ) %>%
      ungroup() %>%
      group_by(EC_ISO_CT_NAME) %>%
      arrange(YEAR) %>%
      mutate(
        Y2D_RANK_DIF_PREV_YEAR = lag(RANK, 1) - RANK,
        Y2D_PUNCT_DIF_PREV_YEAR_PERC = (Y2D_CTRY_ARR_PUNCT - lag(Y2D_CTRY_ARR_PUNCT, 1)),
        Y2D_PUNCT_DIF_2019_PERC = (Y2D_CTRY_ARR_PUNCT - lag(Y2D_CTRY_ARR_PUNCT, max(YEAR) - 2019))
      ) %>%
      ungroup() %>%
      filter(YEAR == max(YEAR), RANK < 11) %>%
      mutate(Y2D_CTRY_NAME = EC_ISO_CT_NAME) %>%
      select(
        RANK,
        Y2D_RANK_DIF_PREV_YEAR,
        Y2D_CTRY_NAME,
        Y2D_CTRY_ARR_PUNCT,
        Y2D_PUNCT_DIF_PREV_YEAR_PERC,
        Y2D_PUNCT_DIF_2019_PERC
      )

    ### main card
    ct_main_punct_top <- ct_punct_dy %>%
      mutate(
        MAIN_PUNCT_CTRY_NAME = if_else(
          RANK <= 4,
          DY_CTRY_NAME,
          NA
        ),
        MAIN_PUNCT_CTRY_ARR_PUNCT = if_else(
          RANK <= 4,
          DY_CTRY_ARR_PUNCT,
          NA
        )
      ) %>%
      select(RANK, MAIN_PUNCT_CTRY_NAME, MAIN_PUNCT_CTRY_ARR_PUNCT)

    ct_main_punct_bottom <- ct_punct_calc %>%
      filter(DATE == last_punctuality_day) %>%
      mutate(
        RANK = max(RANK) + 1 - RANK,
        DY_CTRY_NAME = EC_ISO_CT_NAME,
        DY_CTRY_ARR_PUNCT = ARR_PUNCTUALITY_PERCENTAGE / 100
      ) %>%
      mutate(
        MAIN_PUNCT_CTRY_NAME_BOTTOM = if_else(
          RANK <= 4,
          DY_CTRY_NAME,
          NA
        ),
        MAIN_PUNCT_CTRY_ARR_PUNCT_BOTTOM = if_else(
          RANK <= 4,
          DY_CTRY_ARR_PUNCT,
          NA
        )
      ) %>%
      filter(RANK < 11) %>%
      arrange(RANK) %>%
      select(RANK, MAIN_PUNCT_CTRY_NAME_BOTTOM, MAIN_PUNCT_CTRY_ARR_PUNCT_BOTTOM)


    ### merge and reorder tables
    ct_punct_data <- merge(x = ct_punct_dy, y = ct_punct_wk, by = "RANK")
    ct_punct_data <- merge(x = ct_punct_data, y = ct_punct_y2d, by = "RANK")
    ct_punct_data <- merge(x = ct_punct_data, y = ct_main_punct_top, by = "RANK")
    ct_punct_data <- merge(x = ct_punct_data, y = ct_main_punct_bottom, by = "RANK")

    ct_punct_data <- ct_punct_data %>%
      mutate(Y2D_TO_DATE = DY_TO_DATE) %>%
      relocate(c(
        RANK,
        MAIN_PUNCT_CTRY_NAME,
        MAIN_PUNCT_CTRY_ARR_PUNCT,
        MAIN_PUNCT_CTRY_NAME_BOTTOM,
        MAIN_PUNCT_CTRY_ARR_PUNCT_BOTTOM,
        DY_RANK_DIF_PREV_WEEK,
        DY_CTRY_NAME,
        DY_TO_DATE,
        DY_CTRY_ARR_PUNCT,
        DY_PUNCT_DIF_PREV_WEEK_PERC,
        DY_PUNCT_DIF_PREV_YEAR_PERC,
        WK_RANK_DIF_PREV_WEEK,
        WK_CTRY_NAME,
        WK_FROM_DATE,
        WK_TO_DATE,
        WK_CTRY_ARR_PUNCT,
        WK_PUNCT_DIF_PREV_WEEK_PERC,
        WK_PUNCT_DIF_PREV_YEAR_PERC,
        Y2D_RANK_DIF_PREV_YEAR,
        Y2D_CTRY_NAME,
        Y2D_TO_DATE,
        Y2D_CTRY_ARR_PUNCT,
        Y2D_PUNCT_DIF_PREV_YEAR_PERC,
        Y2D_PUNCT_DIF_2019_PERC
      ))

    ### covert to json and save in app data folder and archive
    ct_punct_data_j <- ct_punct_data %>% toJSON(., pretty = TRUE)
    write(ct_punct_data_j, here(data_folder, "ctry_ranking_punctuality.json"))
    write(ct_punct_data_j, here(data_folder, "v2", "ctry_ranking_punctuality.json"))
    write(ct_punct_data_j, paste0(archive_dir, today, "_ctry_ranking_punctuality.json"))
