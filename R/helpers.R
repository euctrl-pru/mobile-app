
library(here)
library(withr)
library(DBI)
library(tibble)
library(dplyr)
# library(odbc)
library(purrr)
library(magrittr)
library(janitor)
library(lubridate)

library(readxl)
library(fs)
library(stringr)

library(zoo)
library(DBI)
library(ROracle)
library(RODBC)

library(eurocontrol)
library(tidyverse)
library(jsonlite)


date_sql_string <- function(date_string) {
  paste0("TO_DATE('", date_string ,"', 'yyyy-mm-dd') + 1")
}

export_query <- function(query, schema = "PRU_DEV") {
  withr::local_envvar(c(
    "TZ" = "UTC",
    "ORA_SDTZ" = "UTC",
    "NLS_LANG" = ".AL32UTF8"
  ))

  con <- withr::local_db_connection(
    eurocontrol::db_connection(schema = schema)
  )

  dplyr::tbl(con, dplyr::sql(query)) |>
    collect()

  # data <- DBI::dbSendQuery(con, query)
  # # ~2.5 min for one day
  # DBI::fetch(data, n = -1) |>
  #   tibble::as_tibble()
}


# save json file
save_json <- function(df, filename, mydate = data_day_date, archive_file = TRUE) {
  data_day_text_dash <- mydate %>% format("%Y-%m-%d")

  # nw_status_test <- list("prod", "dev")

  # df <-st_ao_data_j
  # filename <- "st_ao_ranking_traffic"

  # save in local data folder
  # stakeholder_prefix <- stringr::str_sub(filename, 1,
  #                                        regexpr("_", substr(filename, 1, nchar(filename)))-1)

  stakeholder_prefix <- stringr::str_sub(filename, 1, 2)

  target_dir <- get(paste0(stakeholder_prefix, "_","local_data_folder"))
  write(df, here(target_dir, paste0(filename,".json")))

  # save in archive
  if (archive_file){
    # check if date folder already exists
    archive_dir_date <- here(archive_dir, data_day_text_dash)
    if (!dir.exists(archive_dir_date)) {
      dir.create(archive_dir_date)
    }

    write(df, here(archive_dir_date, paste0(filename,".json")))
  }
}

# get values for the day before `tdy`
network_traffic_full_latest <- function(today = lubridate::today()) {
  yesterday <- today |> magrittr::subtract(days(1))
  base_dir <- "//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/"
  base_file <- str_glue(
    "099_Traffic_Landing_Page_dataset_new_{yyyymmdd}.xlsx",
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
        "DAY_TFC_PREV_WEEK_PERC",
        "DAY_DIFF_PREV_YEAR_PERC",
        "DAY_TFC_DIFF_2019_PERC",
        "AVG_ROLLING_WEEK",
        "DIF_PREV_WEEK_PERC",
        "DIF_WEEK_PREV_YEAR_PERC",
        "DIF_ROLLING_WEEK_2019_PERC",
        "Y2D_TFC_YEAR",
        "Y2D_AVG_TFC_YEAR",
        "Y2D_DIFF_PREV_YEAR_PERC",
        "Y2D_DIFF_2019_PERC")
    )
}

network_delay_latest <- function(today = lubridate::today()) {
  nw_traffic_last_day <- network_traffic_full_latest(today)

  yesterday <- today |> magrittr::subtract(days(1))
  base_dir <- "//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/"
  base_file <- str_glue(
    "099_Traffic_Landing_Page_dataset_new_{yyyymmdd}.xlsx",
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

#function to be adapted to avoid dependency with 098
# network_punctuality_latest <- function(today = lubridate::today()) {
#   yesterday <- today |> magrittr::subtract(days(1))
#   base_dir <- "//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/"
#   base_file <- str_glue(
#     "098_PUNCTUALITY_{yyyymmdd}.xlsx",
#     yyyymmdd = yesterday |> format("%Y%m%d")
#   )
#
#   last_day <- yesterday
#   last_year <- yesterday |> lubridate::year()
#
#
#   nw_punct_data_raw <- read_xlsx(
#     path  = fs::path_abs(base_file, start = base_dir),
#     sheet = "NETWORK",
#     range = cell_limits(c(1, 1), c(NA, NA))
#   ) |>
#     dplyr::mutate(across(starts_with("DATE"), lubridate::as_date)) |>
#     as_tibble()
#
#   nw_punct_data_d_w <- nw_punct_data_raw |>
#     arrange(DATE) |>
#     mutate(YEAR_FLIGHT = as.numeric(format(DATE, "%Y"))) |>
#     mutate(
#       ARR_PUN_PREV_YEAR = lag(ARR_PUNCTUALITY_PERCENTAGE, 364),
#       DEP_PUN_PREV_YEAR = lag(DEP_PUNCTUALITY_PERCENTAGE, 364),
#       ARR_PUN_2019 = if_else(YEAR_FLIGHT == last_year,
#                              lag(
#                                ARR_PUNCTUALITY_PERCENTAGE,
#                                364 * (last_year - 2019) + floor((last_year - 2019) / 4) * 7
#                              ),
#                              1
#       ),
#       DEP_PUN_2019 = if_else(YEAR_FLIGHT == last_year,
#                              lag(
#                                DEP_PUNCTUALITY_PERCENTAGE,
#                                364 * (last_year - 2019) + floor((last_year - 2019) / 4) * 7
#                              ),
#                              1
#       ),
#       DAY_2019 = if_else(YEAR_FLIGHT == last_year,
#                          lag(
#                            DATE,
#                            364 * (last_year - 2019) + floor((last_year - 2019) / 4) * 7
#                          ),
#                          last_day
#       ),
#       DAY_ARR_PUN_DIF_PY_PERC = ARR_PUNCTUALITY_PERCENTAGE - ARR_PUN_PREV_YEAR,
#       DAY_DEP_PUN_DIF_PY_PERC = DEP_PUNCTUALITY_PERCENTAGE - DEP_PUN_PREV_YEAR,
#       DAY_ARR_PUN_DIF_2019_PERC = ARR_PUNCTUALITY_PERCENTAGE - ARR_PUN_2019,
#       DAY_DEP_PUN_DIF_2019_PERC = DEP_PUNCTUALITY_PERCENTAGE - DEP_PUN_2019
#     ) |>
#     mutate(
#       ARR_PUN_WK = rollsum((ARR_PUNCTUAL_FLIGHTS), 7, fill = NA, align = "right") / rollsum(ARR_SCHEDULE_FLIGHT, 7, fill = NA, align = "right") * 100,
#       DEP_PUN_WK = rollsum((DEP_PUNCTUAL_FLIGHTS), 7, fill = NA, align = "right") / rollsum(DEP_SCHEDULE_FLIGHT, 7, fill = NA, align = "right") * 100
#     ) |>
#     mutate(
#       ARR_PUN_WK_PREV_YEAR = lag(ARR_PUN_WK, 364),
#       DEP_PUN_WK_PREV_YEAR = lag(DEP_PUN_WK, 364),
#       ARR_PUN_WK_2019 = if_else(YEAR_FLIGHT == last_year,
#                                 lag(ARR_PUN_WK, 364 * (last_year - 2019) + floor((last_year - 2019) / 4) * 7),
#                                 1
#       ),
#       DEP_PUN_WK_2019 = if_else(YEAR_FLIGHT == last_year,
#                                 lag(DEP_PUN_WK, 364 * (last_year - 2019) + floor((last_year - 2019) / 4) * 7),
#                                 1
#       ),
#       WK_ARR_PUN_DIF_PY_PERC = ARR_PUN_WK - ARR_PUN_WK_PREV_YEAR,
#       WK_DEP_PUN_DIF_PY_PERC = DEP_PUN_WK - DEP_PUN_WK_PREV_YEAR,
#       WK_ARR_PUN_DIF_2019_PERC = ARR_PUN_WK - ARR_PUN_WK_2019,
#       WK_DEP_PUN_DIF_2019_PERC = DEP_PUN_WK - DEP_PUN_WK_2019
#     ) |>
#     filter(DATE == last_day) |>
#     mutate(FLIGHT_DATE = DATE) |>
#     select(
#       FLIGHT_DATE,
#       ARR_PUNCTUALITY_PERCENTAGE,
#       DEP_PUNCTUALITY_PERCENTAGE,
#       DAY_ARR_PUN_DIF_PY_PERC,
#       DAY_DEP_PUN_DIF_PY_PERC,
#       DAY_ARR_PUN_DIF_2019_PERC,
#       DAY_DEP_PUN_DIF_2019_PERC,
#       ARR_PUN_WK,
#       DEP_PUN_WK,
#       WK_ARR_PUN_DIF_PY_PERC,
#       WK_DEP_PUN_DIF_PY_PERC,
#       WK_ARR_PUN_DIF_2019_PERC,
#       WK_DEP_PUN_DIF_2019_PERC
#     ) |>
#     mutate(INDEX = 1)
#
#   nw_punct_data_y2d <- nw_punct_data_raw |>
#     arrange(DATE) |>
#     mutate(YEAR_FLIGHT = as.numeric(format(DATE, "%Y"))) |>
#     mutate(MONTH_DAY = as.numeric(format(DATE, format = "%m%d"))) |>
#     filter(MONTH_DAY <= as.numeric(format(last_day, format = "%m%d"))) |>
#     mutate(YEAR = as.numeric(format(DATE, format = "%Y"))) |>
#     group_by(YEAR) |>
#     summarise(
#       ARR_PUN_Y2D = sum(ARR_PUNCTUAL_FLIGHTS, na.rm = TRUE) / sum(ARR_SCHEDULE_FLIGHT, na.rm = TRUE) * 100,
#       DEP_PUN_Y2D = sum(DEP_PUNCTUAL_FLIGHTS, na.rm = TRUE) / sum(DEP_SCHEDULE_FLIGHT, na.rm = TRUE) * 100
#     ) |>
#     mutate(
#       Y2D_ARR_PUN_PREV_YEAR = lag(ARR_PUN_Y2D, 1),
#       Y2D_DEP_PUN_PREV_YEAR = lag(DEP_PUN_Y2D, 1),
#       Y2D_ARR_PUN_2019 = lag(ARR_PUN_Y2D, last_year - 2019),
#       Y2D_DEP_PUN_2019 = lag(DEP_PUN_Y2D, last_year - 2019),
#       Y2D_ARR_PUN_DIF_PY_PERC = ARR_PUN_Y2D - Y2D_ARR_PUN_PREV_YEAR,
#       Y2D_DEP_PUN_DIF_PY_PERC = DEP_PUN_Y2D - Y2D_DEP_PUN_PREV_YEAR,
#       Y2D_ARR_PUN_DIF_2019_PERC = ARR_PUN_Y2D - Y2D_ARR_PUN_2019,
#       Y2D_DEP_PUN_DIF_2019_PERC = DEP_PUN_Y2D - Y2D_DEP_PUN_2019
#     ) |>
#     filter(YEAR == as.numeric(format(last_day, format = "%Y"))) |>
#     select(
#       ARR_PUN_Y2D,
#       DEP_PUN_Y2D,
#       Y2D_ARR_PUN_DIF_PY_PERC,
#       Y2D_DEP_PUN_DIF_PY_PERC,
#       Y2D_ARR_PUN_DIF_2019_PERC,
#       Y2D_DEP_PUN_DIF_2019_PERC
#     ) |>
#     mutate(INDEX = 1)
#
#
#   nw_punct_json <- merge(nw_punct_data_d_w, nw_punct_data_y2d, by = "INDEX") |>
#     select(-INDEX) |>
#     as.list() |>
#     purrr::list_transpose() |>
#     magrittr::extract2(1)
#
#   nw_punct_json
# }

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


# network emissions for the month of `today` (tipycally 2 months before now)
network_emissions_latest <- function(today = lubridate::today(tzone = "UTC")) {
  first_today <- floor_date(today |> as_date(tzone = "UTC"), unit = "month")
  query <- str_glue("
    SELECT
      FLIGHT_MONTH,
      CO2_QTY_TONNES,
      TF,
      YEAR,
      MONTH
    FROM TABLE (emma_pub.api_aiu_stats.MM_AIU_STATE_DEP ())
    WHERE
      YEAR >= 2019 and STATE_NAME not in ('LIECHTENSTEIN')
      AND FLIGHT_MONTH <= TO_DATE('{first_today}', 'YYYY-MM-DD')
    ORDER BY YEAR, MONTH, STATE_NAME
   ")

  co2_data_raw <- export_query(query) |>
    mutate(FLIGHT_MONTH = as_date(FLIGHT_MONTH, tz = "UTC"))

  co2_data_evo_nw <- co2_data_raw |>
    group_by(FLIGHT_MONTH) |>
    summarise(MM_TTF = sum(TF) / 1000000, MM_CO2 = sum(CO2_QTY_TONNES) / 1000000) |>
    mutate(
      YEAR = as.numeric(format(FLIGHT_MONTH, "%Y")),
      MONTH = as.numeric(format(FLIGHT_MONTH, "%m")),
      MM_CO2_DEP = MM_CO2 / MM_TTF
    ) |>
    arrange(FLIGHT_MONTH) |>
    mutate(FLIGHT_MONTH = ceiling_date(as_date(FLIGHT_MONTH), unit = "month") - 1)

  last_of_month <- ceiling_date(today, unit = "month") - days(1)
  year_of_month  <- month(last_of_month)

  co2_last_date <- max(co2_data_evo_nw$FLIGHT_MONTH, na.rm = TRUE)
  co2_last_month <- format(co2_last_date, "%B")
  co2_last_month_num <- as.numeric(format(co2_last_date, "%m"))
  co2_last_year <- max(co2_data_evo_nw$YEAR)

  # check last month number of flights
  check_flights <- co2_data_evo_nw |>
    filter(YEAR == max(YEAR)) |>
    filter(MONTH == max(MONTH)) |>
    select(MM_TTF) |>
    pull() * 1000000

  if (check_flights < 1000) {
    co2_data_raw <- co2_data_raw |> filter(FLIGHT_MONTH < max(FLIGHT_MONTH))
    co2_data_evo_nw <- co2_data_evo_nw |> filter(FLIGHT_MONTH < max(FLIGHT_MONTH))
    co2_last_date <- max(co2_data_evo_nw$FLIGHT_MONTH, na.rm = TRUE)
  }

  co2_latest <- co2_data_evo_nw |>
    mutate(
      MONTH_TEXT = format(FLIGHT_MONTH, "%B"),
      MM_CO2_PREV_YEAR = lag(MM_CO2, 12),
      MM_TTF_PREV_YEAR = lag(MM_TTF, 12),
      MM_CO2_2019 = lag(MM_CO2, (as.numeric(co2_last_year) - 2019) * 12),
      MM_TTF_2019 = lag(MM_TTF, (as.numeric(co2_last_year) - 2019) * 12),
      MM_CO2_DEP_PREV_YEAR = lag(MM_CO2_DEP, 12),
      MM_CO2_DEP_2019 = lag(MM_CO2_DEP, (as.numeric(co2_last_year) - 2019) * 12)
    ) |>
    mutate(
      DIF_CO2_MONTH_PREV_YEAR = MM_CO2 / MM_CO2_PREV_YEAR - 1,
      DIF_TTF_MONTH_PREV_YEAR = MM_TTF / MM_TTF_PREV_YEAR - 1,
      DIF_CO2_DEP_MONTH_PREV_YEAR = MM_CO2_DEP / MM_CO2_DEP_PREV_YEAR - 1,
      DIF_CO2_MONTH_2019 = MM_CO2 / MM_CO2_2019 - 1,
      DIF_TTF_MONTH_2019 = MM_TTF / MM_TTF_2019 - 1,
      DIF_CO2_DEP_MONTH_2019 = MM_CO2_DEP / MM_CO2_DEP_2019 - 1
    ) |>
    group_by(YEAR) |>
    mutate(
      YTD_CO2 = cumsum(MM_CO2),
      YTD_TTF = cumsum(MM_TTF),
      YTD_CO2_DEP = cumsum(MM_CO2) / cumsum(MM_TTF)
    ) |>
    ungroup() |>
    mutate(
      YTD_CO2_PREV_YEAR = lag(YTD_CO2, 12),
      YTD_TTF_PREV_YEAR = lag(YTD_TTF, 12),
      YTD_CO2_DEP_PREV_YEAR = lag(YTD_CO2_DEP, 12),
      YTD_CO2_2019 = lag(YTD_CO2, (as.numeric(co2_last_year) - 2019) * 12),
      YTD_CO2_DEP_2019 = lag(YTD_CO2_DEP, (as.numeric(co2_last_year) - 2019) * 12),
      YTD_TTF_2019 = lag(YTD_TTF, (as.numeric(co2_last_year) - 2019) * 12)
    ) |>
    mutate(
      YTD_DIF_CO2_PREV_YEAR = YTD_CO2 / YTD_CO2_PREV_YEAR - 1,
      YTD_DIF_TTF_PREV_YEAR = YTD_TTF / YTD_TTF_PREV_YEAR - 1,
      YTD_DIF_CO2_DEP_PREV_YEAR = YTD_CO2_DEP / YTD_CO2_DEP_PREV_YEAR - 1,
      YTD_DIF_CO2_2019 = YTD_CO2 / YTD_CO2_2019 - 1,
      YTD_DIF_CO2_DEP_2019 = YTD_CO2_DEP / YTD_CO2_DEP_2019 - 1,
      YTD_DIF_TTF_2019 = YTD_TTF / YTD_TTF_2019 - 1
    ) |>
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
    ) |>
    filter(FLIGHT_MONTH == co2_last_date) |>
    as.list() |>
    purrr::list_transpose() |>
    magrittr::extract2(1)

  co2_latest
}

init_collection <- function(wef, til, app, collection, extractor, token) {
  for (d in seq(from = lubridate::as_date(wef), to = lubridate::as_date(til), by = "1 day")) {
    record <- extractor(as_date(d))
    ph_create_record(
      app = app,
      api = "/api/collections",
      collection = collection,
      token = token,
      body = record)
  }

}


tasks_status_latest <- function() {
  list(
    product = "Portal Traffic Landing Page (traffic)",
    status = "true",
    last_day_data = lubridate::today(tzone = "UTC"),
    source_type = "excel",
    base_dir = "//ihx-vdm05/LIVE_var_www_Economics$/Download/",
    file_name = "Network_Traffic.xlsx",
    sheet = "Data",
    task_name = "copy_traffic_landing_page_to_gsheet.R",
    repo_folder = "grounded_aircraft/R",
    update_frequency = "daily",
    start_time = "08:30"
  )
}

########## added by Oscar, discuss with Enrico how to merge both billing functions
get_billing_data <- function() {

  ## https://leowong.ca/blog/connect-to-microsoft-access-database-via-r/
  ## Set up driver info and database path
  DRIVERINFO <- "Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
  MDBPATH <- "G:/HQ/dgof-pru/Data/DataProcessing/Crco - Billing/CRCO_BILL.accdb"
  PATH <- paste0(DRIVERINFO, "DBQ=", MDBPATH)

  channel <- odbcDriverConnect(PATH)
  query_bill <- "SELECT *,
                  iif(
	            [Billing Zone Number] = '33', 'Bosnia and Herzegovina',
            	iif ([Billing Zone Number] = '08' OR [Billing Zone Number] = '12', 'Portugal',
	            iif ([Billing Zone Number] = '32' OR [Billing Zone Number] = '41', 'Ukraine',
	[Billing Zone Name]
))) as corrected_cz
    FROM V_CRCO_BILL_PER_CZ
  "
  # iif ([Billing Zone Number] = '10' OR [Billing Zone Number] = '11', 'Spain' ,



  ## Load data into R dataframe
  billed_raw <- sqlQuery(channel,
                         query_bill,
                         stringsAsFactors = FALSE)

  ## Close and remove channel
  close(channel)
  rm(channel)

  nw_billed_per_cz <- billed_raw %>%
    janitor::clean_names() %>%
    mutate(billing_period_start_date = as.Date(billing_period_start_date, format = "%d-%m-%Y"))

  return(nw_billed_per_cz)
}

get_co2_data <- function() {
  query <- str_glue("
        SELECT *
          FROM TABLE (emma_pub.api_aiu_stats.MM_AIU_STATE_DEP ())
          where year >= 2019 and STATE_NAME not in ('LIECHTENSTEIN')
        ORDER BY 2, 3, 4
       ")

  check_co2 <- try({
    co2_data_raw <- export_query(query) %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  })

  # # Check if an error occurred
  # if (inherits(check_co2, "try-error")) {
  #   co2_data_raw <- read_xlsx(
  #     path = fs::path_abs(
  #       str_glue("CO2_backup.xlsx"),
  #       start = '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Project/DDP/AIU app/data_archive'
  #     ),
  #     sheet = "All Data vs Y(-1)",
  #     range = cell_limits(c(3, 1), c(NA, NA))
  #   ) %>%
  #     as_tibble() %>%
  #     mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
  #     filter(YEAR>=2019, STATE_NAME != 'LIECHTENSTEIN') %>%
  #     arrange(2, 3, 4)
  #
  # } else {
  #   co2_data_raw <- export_query(query) %>%
  #     mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # }

  co2_data_raw <- export_query(query) %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  return(co2_data_raw)
}

get_ao_billing_data <- function() {

  # ## https://leowong.ca/blog/connect-to-microsoft-access-database-via-r/
  # ## Set up driver info and database path
  # DRIVERINFO <- "Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
  # MDBPATH <- "G:/HQ/dgof-pru/Data/DataProcessing/Crco - Billing/CRCO_BILL.accdb"
  # PATH <- paste0(DRIVERINFO, "DBQ=", MDBPATH)
  #
  # channel <- odbcDriverConnect(PATH)
  #
  # query_bill <- paste0("SELECT AO_GRP_LAST_NAME, Billing_period_start_date, FLAG_YTD,
  #   [Billing Zone Name], [Route Charges], Year, Month
  #   FROM V_CRCO_BILL_PER_AO_CZ_GRP_ACTUAL
  #   where (Year = ", last_year, " or Year = ", last_year-1," or Year = ", 2019,") and FLAG_YTD = 'Y'
  # ")
  #
  # ## Load data into R dataframe
  # billed_ao_raw <- sqlQuery(channel,
  #                        query_bill,
  #                        stringsAsFactors = FALSE)
  #
  # ## Close and remove channel
  # close(channel)
  # rm(channel)

  billed_ao_raw <-  read_xlsx(
    path  = fs::path_abs(
      str_glue("billing per cz_ao_app.xlsx"),
      start = "G:/HQ/dgof-pru/Data/DataProcessing/Covid19/Oscar/Develop"),
    sheet = "cz",
    range = cell_limits(c(4, 1), c(NA, NA))) %>%
    as_tibble() %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x))) |>
    clean_names()

  billed_ao_raw <- billed_ao_raw |>
    mutate(month = as.numeric(month)) |>
    rename(route_charges = total)
  return(billed_ao_raw)
}

get_punct_data_spain <- function() {

  temp_data_archive <-'//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Project/DDP/AIU app/data_archive/'
  punct_data_spain_prev <- read_csv(paste0(temp_data_archive, 'punct_data_spain.csv'), show_col_types = FALSE)

  max_date_prev <- max(punct_data_spain_prev$DAY_DATE, na.rm = TRUE)

  query1 <- paste0("
select
      a.*,
      trunc(a.ACTUAL_DEP_TIME - 3 / 24) as dep_date,
      trunc(a.ACTUAL_ARR_TIME - 3 / 24) as arr_date,
      case when substr(ADES, 1,2) = 'GC' then 'IC'
            when substr(ADES, 1,2) = 'LE' then 'ES'
            when substr(ADES, 1,2) = 'GE' then 'ES'
        end iso_ct_code_des,
     case when substr(ADEP, 1,2) = 'GC' then 'IC'
            when substr(ADEP, 1,2) = 'LE' then 'ES'
            when substr(ADEP, 1,2) = 'GE' then 'ES'
        end iso_ct_code_dep

from LDW_VDM.V_DELAY_TRACKER_ARCHIVE_TURN a
where (substr(ADEP, 1,2) in ('GC', 'GE', 'LE') or substr(ADES, 1,2) in ('GC', 'GE', 'LE'))
   and (
        trunc(a.ACTUAL_DEP_TIME - 3 / 24) >= TO_DATE('", max_date_prev ,"', 'yyyy-mm-dd') -7
        or
        trunc(a.ACTUAL_ARR_TIME - 3 / 24) >= TO_DATE('", max_date_prev ,"', 'yyyy-mm-dd') -7
        )
"
  )

  punct_data_raw_raw <- export_query(query1)

  punct_data_raw_calc <- punct_data_raw_raw |>
    # mutate(across(.cols = where(is.instant), ~ as.POSIXct(.x, format="%Y-%m-%d %H:%M:%S")))  |>
    mutate(
      ARR_DATE = as.Date(ARR_DATE),
      ARR_PUNCTUAL_FLIGHTS = case_when(
        is.na(SLOT_TIME_ADES) == FALSE & ACTUAL_ARR_TIME < SLOT_TIME_ADES + lubridate::minutes(16) ~ 1,
        .default = 0
      ),
      ARR_SCHEDULE_FLIGHT = case_when (
        is.na(SLOT_TIME_ADES) == FALSE ~ 1,
        .default =0
      ),
      DEP_DATE = as.Date(DEP_DATE),
      DEP_PUNCTUAL_FLIGHTS = case_when(
        is.na(SLOT_TIME_ADEP) == FALSE & ACTUAL_DEP_TIME < (SLOT_TIME_ADEP + lubridate::minutes(16)) ~ 1,
        .default = 0
      ),
      DEP_SCHEDULE_FLIGHT = case_when (
        is.na(SLOT_TIME_ADEP) == FALSE ~ 1,
        .default =0
      )
    )


  punct_data_arr <- punct_data_raw_calc |>
    group_by(ISO_CT_CODE_DES, ARR_DATE) |>
    summarise(ARR_PUNCTUAL_FLIGHTS = sum(ARR_PUNCTUAL_FLIGHTS, na.rm = TRUE),
              ARR_SCHEDULE_FLIGHT = sum(ARR_SCHEDULE_FLIGHT, na.rm = TRUE),
              ARR_FLIGHTS = n())

  punct_data_dep <- punct_data_raw_calc |>
    group_by(ISO_CT_CODE_DEP, DEP_DATE) |>
    summarise(DEP_PUNCTUAL_FLIGHTS = sum(DEP_PUNCTUAL_FLIGHTS, na.rm = TRUE),
              DEP_SCHEDULE_FLIGHT = sum(ARR_SCHEDULE_FLIGHT, na.rm = TRUE),
              DEP_FLIGHTS = n())

  start_date <- min(min(punct_data_arr$ARR_DATE, na.rm = TRUE), min(punct_data_dep$DEP_DATE, na.rm = TRUE)) +days(2)
  end_date <- lubridate::today() + days(-1)

  date_seq <- seq(from = start_date, to = end_date, by = "day")
  my_codes <- c('IC', 'ES')
  repeated_dates <- rep(date_seq, times = length(my_codes))
  repeated_values <- rep(my_codes, each = length(date_seq))
  country_day <- data.frame(DAY_DATE = repeated_dates, ISO_2LETTER = repeated_values) |>
    arrange(ISO_2LETTER, DAY_DATE)

  punct_data_spain_joined <- country_day |>
    left_join(punct_data_arr, by = c("DAY_DATE" = "ARR_DATE", "ISO_2LETTER" = "ISO_CT_CODE_DES")) |>
    left_join(punct_data_dep, by = c("DAY_DATE" = "DEP_DATE", "ISO_2LETTER" = "ISO_CT_CODE_DEP"))


  punct_data_spain_raw <- punct_data_spain_prev %>%
    filter(DAY_DATE < start_date) %>%
    rbind(punct_data_spain_joined) %>%
    arrange(DAY_DATE, ISO_2LETTER)

  punct_data_spain_raw %>% write_csv(paste0(temp_data_archive, 'punct_data_spain.csv'))
  punct_data_spain_raw %>% write_csv(paste0('G:/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/app/csv/',
                                            format(end_date, "%Y%m%d"), '_punct_data_spain.csv'))

  # punct_data_spain_raw_prev %>% write_csv(paste0(temp_data_archive, 'punct_data_spain_prev.csv'))

  return(punct_data_spain_raw)
}

get_punct_data_state <- function() {
  query <- "
  WITH

DIM_STATE as (
  SELECT distinct
    AIU_ISO_COUNTRY_NAME as EC_ISO_CT_NAME,
    AIU_ISO_COUNTRY_CODE AS EC_ISO_CT_CODE
  FROM prudev.pru_country_iso
  WHERE till > TRUNC(SYSDATE)-1
),

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
  ),

  COUNTRY_DAY_DATA as (
  SELECT a.*, b.*

  FROM CTRY_DAY a
  left join LDW_VDM.VIEW_FAC_PUNCTUALITY_CT_DAY b on a.ISO_2LETTER = b.ISO_CT_CODE and a.day_date = b.\"DATE\"
  )

  SELECT
      a.*,
      c.EC_ISO_CT_NAME
  FROM COUNTRY_DAY_DATA a
  left join DIM_STATE c on a.ISO_2LETTER = c.EC_ISO_CT_CODE

"


  st_punct_raw <- export_query(query) %>%
    as_tibble() %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  return(st_punct_raw)
}

get_punct_data_apt <- function() {
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
          SELECT distinct
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
            a.* , b.*, c.EC_ISO_CT_NAME
          FROM APT_DAY a
          left join LDW_VDM.VIEW_FAC_PUNCTUALITY_AP_DAY b on a.day_date = b.\"DATE\" and a.arp_code = b.icao_code
          left join LIST_STATE c on a.ISO_COUNTRY_CODE = c.EC_ISO_CT_CODE
          where a.arp_code not in ('LTBA', 'UKBB')
          order by a.ARP_CODE, b.\"DATE\"
   "

  apt_punct_raw <- export_query(query) %>%
    as_tibble() %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

   return(apt_punct_raw)
}
