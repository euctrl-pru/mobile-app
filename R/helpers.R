library(withr)
library(DBI)
library(tibble)
library(dplyr)
library(odbc)
library(purrr)
library(magrittr)
library(janitor)
library(lubridate)

library(readxl)
library(fs)
library(stringr)

library(zoo)



export_query <- function(query) {
  # NOTE: to be set before you create your ROracle connection!
  # See http://www.oralytics.com/2015/05/r-roracle-and-oracle-date-formats_27.html
  withr::local_envvar(c(
    "TZ" = "UTC",
    "ORA_SDTZ" = "UTC"
  ))
  withr::local_namespace("ROracle")
  con <- withr::local_db_connection(
    DBI::dbConnect(
      DBI::dbDriver("Oracle"),
      usr, pwd,
      dbname = dbn,
      timezone = "UTC"
    )
  )

  data <- DBI::dbSendQuery(con, query)
  # ~2.5 min for one day
  DBI::fetch(data, n = -1) |>
    tibble::as_tibble()
}

# get values for the day before `tdy`
network_traffic_full_latest <- function(today = lubridate::today()) {
  yesterday <- today |> magrittr::subtract(days(1))
  base_dir <- "//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/"
  base_file <- str_glue(
    "99_Traffic_Landing_Page_dataset_new_{yyyymmdd}.xlsx",
    yyyymmdd = yesterday |> format("%Y%m%d")
  )

  last_year <- yesterday |> lubridate::year()

  nw_traffic_data <- read_xlsx(
    path  = fs::path_abs(base_file, start = base_dir),
    sheet = "NM_Daily_Traffic_All",
    range = cell_limits(c(2, 1), c(NA, 39))
  ) |>
    dplyr::mutate(across(starts_with("FLIGHT_DATE"), lubridate::as_date)) |>
    as_tibble()

  nw_traffic_last_day <- nw_traffic_data |>
    filter(FLIGHT_DATE == yesterday)

  nw_traffic_latest <- nw_traffic_last_day |>
    as.list() |>
    purrr::list_transpose() |>
    magrittr::extract2(1)

  nw_traffic_latest
}
network_traffic_latest <- function(today = lubridate::today()) {
  network_traffic_full_latest(today) |>
    magrittr::extract(
      c("FLIGHT_DATE",
        "DAY_TFC",
        "DAY_DIFF_PREV_YEAR_PERC",
        "DAY_TFC_DIFF_2019_PERC",
        "AVG_ROLLING_WEEK",
        "DIF_WEEK_PREV_YEAR_PERC",
        "DIF_ROLLING_WEEK_2019_PERC",
        "Y2D_TFC_YEAR",
        "Y2D_AVG_TFC_YEAR",
        "Y2D_DIFF_PREV_YEAR_PERC",
        "Y2D_DIFF_2019_PERC")
    )
}

network_delay_latest <- function(today = lubridate::today()) {
  nw_traffic_last_day <- network_traffic_latest(today)

  yesterday <- today |> magrittr::subtract(days(1))
  base_dir <- "//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/"
  base_file <- str_glue(
    "99_Traffic_Landing_Page_dataset_new_{yyyymmdd}.xlsx",
    yyyymmdd = yesterday |> format("%Y%m%d")
  )

  last_year <- yesterday |> lubridate::year()


  nw_delay_data <- read_xlsx(
    path  = fs::path_abs(base_file, start = base_dir),
    sheet = "NM_Daily_Delay_All",
    range = cell_limits(c(2, 1), c(NA, 39))
  ) |>
    dplyr::mutate(across(starts_with("FLIGHT_DATE"), lubridate::as_date)) |>
    as_tibble()

  nw_delay_latest <- nw_delay_data |>
    filter(FLIGHT_DATE == yesterday) |>
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
    ) |>
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
    ) |>
    as.list() |>
    purrr::list_transpose() |>
    magrittr::extract2(1)

  nw_delay_latest
}

network_punctuality_latest <- function(today = lubridate::today()) {
  yesterday <- today |> magrittr::subtract(days(1))
  base_dir <- "//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/"
  base_file <- str_glue(
    "98_PUNCTUALITY_{yyyymmdd}.xlsx",
    yyyymmdd = yesterday |> format("%Y%m%d")
  )

  last_day <- yesterday
  last_year <- yesterday |> lubridate::year()


  nw_punct_data_raw <- read_xlsx(
    path  = fs::path_abs(base_file, start = base_dir),
    sheet = "NETWORK",
    range = cell_limits(c(1, 1), c(NA, NA))
  ) |>
    dplyr::mutate(across(starts_with("DATE"), lubridate::as_date)) |>
    as_tibble()

  nw_punct_data_d_w <- nw_punct_data_raw |>
    arrange(DATE) |>
    mutate(YEAR_FLIGHT = as.numeric(format(DATE, "%Y"))) |>
    mutate(
      ARR_PUN_PREV_YEAR = lag(ARR_PUNCTUALITY_PERCENTAGE, 364),
      DEP_PUN_PREV_YEAR = lag(DEP_PUNCTUALITY_PERCENTAGE, 364),
      ARR_PUN_2019 = if_else(YEAR_FLIGHT == last_year,
        lag(
          ARR_PUNCTUALITY_PERCENTAGE,
          364 * (last_year - 2019) + floor((last_year - 2019) / 4) * 7
        ),
        1
      ),
      DEP_PUN_2019 = if_else(YEAR_FLIGHT == last_year,
        lag(
          DEP_PUNCTUALITY_PERCENTAGE,
          364 * (last_year - 2019) + floor((last_year - 2019) / 4) * 7
        ),
        1
      ),
      DAY_2019 = if_else(YEAR_FLIGHT == last_year,
        lag(
          DATE,
          364 * (last_year - 2019) + floor((last_year - 2019) / 4) * 7
        ),
        last_day
      ),
      DAY_ARR_PUN_DIF_PY_PERC = ARR_PUNCTUALITY_PERCENTAGE - ARR_PUN_PREV_YEAR,
      DAY_DEP_PUN_DIF_PY_PERC = DEP_PUNCTUALITY_PERCENTAGE - DEP_PUN_PREV_YEAR,
      DAY_ARR_PUN_DIF_2019_PERC = ARR_PUNCTUALITY_PERCENTAGE - ARR_PUN_2019,
      DAY_DEP_PUN_DIF_2019_PERC = DEP_PUNCTUALITY_PERCENTAGE - DEP_PUN_2019
    ) |>
    mutate(
      ARR_PUN_WK = rollsum((ARR_PUNCTUAL_FLIGHTS), 7, fill = NA, align = "right") / rollsum(ARR_SCHEDULE_FLIGHT, 7, fill = NA, align = "right") * 100,
      DEP_PUN_WK = rollsum((DEP_PUNCTUAL_FLIGHTS), 7, fill = NA, align = "right") / rollsum(DEP_SCHEDULE_FLIGHT, 7, fill = NA, align = "right") * 100
    ) |>
    mutate(
      ARR_PUN_WK_PREV_YEAR = lag(ARR_PUN_WK, 364),
      DEP_PUN_WK_PREV_YEAR = lag(DEP_PUN_WK, 364),
      ARR_PUN_WK_2019 = if_else(YEAR_FLIGHT == last_year,
        lag(ARR_PUN_WK, 364 * (last_year - 2019) + floor((last_year - 2019) / 4) * 7),
        1
      ),
      DEP_PUN_WK_2019 = if_else(YEAR_FLIGHT == last_year,
        lag(DEP_PUN_WK, 364 * (last_year - 2019) + floor((last_year - 2019) / 4) * 7),
        1
      ),
      WK_ARR_PUN_DIF_PY_PERC = ARR_PUN_WK - ARR_PUN_WK_PREV_YEAR,
      WK_DEP_PUN_DIF_PY_PERC = DEP_PUN_WK - DEP_PUN_WK_PREV_YEAR,
      WK_ARR_PUN_DIF_2019_PERC = ARR_PUN_WK - ARR_PUN_WK_2019,
      WK_DEP_PUN_DIF_2019_PERC = DEP_PUN_WK - DEP_PUN_WK_2019
    ) |>
    filter(DATE == last_day) |>
    mutate(FLIGHT_DATE = DATE) |>
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
    ) |>
    mutate(INDEX = 1)

  nw_punct_data_y2d <- nw_punct_data_raw |>
    arrange(DATE) |>
    mutate(YEAR_FLIGHT = as.numeric(format(DATE, "%Y"))) |>
    mutate(MONTH_DAY = as.numeric(format(DATE, format = "%m%d"))) |>
    filter(MONTH_DAY <= as.numeric(format(last_day, format = "%m%d"))) |>
    mutate(YEAR = as.numeric(format(DATE, format = "%Y"))) |>
    group_by(YEAR) |>
    summarise(
      ARR_PUN_Y2D = sum(ARR_PUNCTUAL_FLIGHTS, na.rm = TRUE) / sum(ARR_SCHEDULE_FLIGHT, na.rm = TRUE) * 100,
      DEP_PUN_Y2D = sum(DEP_PUNCTUAL_FLIGHTS, na.rm = TRUE) / sum(DEP_SCHEDULE_FLIGHT, na.rm = TRUE) * 100
    ) |>
    mutate(
      Y2D_ARR_PUN_PREV_YEAR = lag(ARR_PUN_Y2D, 1),
      Y2D_DEP_PUN_PREV_YEAR = lag(DEP_PUN_Y2D, 1),
      Y2D_ARR_PUN_2019 = lag(ARR_PUN_Y2D, last_year - 2019),
      Y2D_DEP_PUN_2019 = lag(DEP_PUN_Y2D, last_year - 2019),
      Y2D_ARR_PUN_DIF_PY_PERC = ARR_PUN_Y2D - Y2D_ARR_PUN_PREV_YEAR,
      Y2D_DEP_PUN_DIF_PY_PERC = DEP_PUN_Y2D - Y2D_DEP_PUN_PREV_YEAR,
      Y2D_ARR_PUN_DIF_2019_PERC = ARR_PUN_Y2D - Y2D_ARR_PUN_2019,
      Y2D_DEP_PUN_DIF_2019_PERC = DEP_PUN_Y2D - Y2D_DEP_PUN_2019
    ) |>
    filter(YEAR == as.numeric(format(last_day, format = "%Y"))) |>
    select(
      ARR_PUN_Y2D,
      DEP_PUN_Y2D,
      Y2D_ARR_PUN_DIF_PY_PERC,
      Y2D_DEP_PUN_DIF_PY_PERC,
      Y2D_ARR_PUN_DIF_2019_PERC,
      Y2D_DEP_PUN_DIF_2019_PERC
    ) |>
    mutate(INDEX = 1)


  nw_punct_json <- merge(nw_punct_data_d_w, nw_punct_data_y2d, by = "INDEX") |>
    select(-INDEX) |>
    as.list() |>
    purrr::list_transpose() |>
    magrittr::extract2(1)

  nw_punct_json
}

network_billed_latest <- function() {
  ## https://leowong.ca/blog/connect-to-microsoft-access-database-via-r/
  ## Set up driver info and database path
  driver_string <- "Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
  db_filename <- "G:/HQ/dgof-pru/Data/DataProcessing/Crco - Billing/CRCO_BILL.accdb"
  dbq_string <- paste0("DBQ=", db_filename)

  db_connect_string <- paste0(driver_string, dbq_string)

  # make sure that the file exists before attempting to connect
  if (!file.exists(db_filename)) {
    stop("DB file does not exist at ", db_filename)
  }

  nw_billed_raw <- withr::with_db_connection(
    list(con = DBI::dbConnect(odbc::odbc(), .connection_string = db_connect_string)),
    tbl(con, "V_CRCO_BILL_PER_CZ") |>
      collect() |>
      janitor::clean_names() |>
      mutate(across(.cols = c("billing_period_start_date", "last_update"), lubridate::as_date))
  )

  last_billing_date <- max(nw_billed_raw$billing_period_start_date)
  last_billing_year <- max(nw_billed_raw$year)

  nw_billing <- nw_billed_raw |>
    group_by(year, month, billing_period_start_date) |>
    summarise(total_billing = sum(route_charges)) |>
    ungroup()


  nw_billed_latest <- nw_billing |>
    arrange(year, billing_period_start_date) |>
    mutate(
      BILLING_DATE = (billing_period_start_date + days(1) + months(1)) + days(-1),
      Year = year,
      MONTH_F = format(billing_period_start_date + days(1), "%B"),
      BILL_MONTH_PY = lag(total_billing, 12),
      BILL_MONTH_2019 = lag(total_billing, (last_billing_year - 2019) * 12),
      DIF_BILL_MONTH_PY = total_billing / BILL_MONTH_PY - 1,
      DIF_BILL_MONTH_2019 = total_billing / BILL_MONTH_2019 - 1,
      BILLED = round(total_billing / 1000000, 0)
    ) |>
    group_by(Year) |>
    mutate(
      total_billing_y2d = cumsum(total_billing)
    ) |>
    ungroup() |>
    mutate(
      BILL_Y2D_PY = lag(total_billing_y2d, 12),
      BILL_Y2D_2019 = lag(total_billing_y2d, (last_billing_year - 2019) * 12),
      DIF_BILL_Y2D_PY = total_billing_y2d / BILL_Y2D_PY - 1,
      DIF_BILL_Y2D_2019 = total_billing_y2d / BILL_Y2D_2019 - 1,
      BILLED_Y2D = round(total_billing_y2d / 1000000, 0)
    ) |>
    filter(billing_period_start_date == last_billing_date) |>
    select(
      BILLING_DATE,
      MONTH_F,
      BILLED,
      DIF_BILL_MONTH_PY,
      DIF_BILL_MONTH_2019,
      BILLED_Y2D,
      DIF_BILL_Y2D_PY,
      DIF_BILL_Y2D_2019
    ) |>
    as.list() |>
    purrr::list_transpose() |>
    magrittr::extract2(1)

  nw_billed_latest
}
