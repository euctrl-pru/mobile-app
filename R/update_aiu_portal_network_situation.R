# # update network situation of AIU Portal

library(eurocontrol)
library(dplyr)
library(lubridate)
library(stringr)
library(pockethostr)
library(yyjsonr)

# in the Task manager set "Start in" to this repo root
source(here::here("R", "helpers.R"))

# TODO: use ph_list_records()

weeks_back <- 5

# TODO
retrieve_from_excell <- function(wef = today(tzone = "UTC") - dweeks(weeks_back)) {
  today <- (lubridate::now() +  days(-1)) |> format("%Y%m%d")

  base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/'
  base_file <- '099_Traffic_Landing_Page_dataset_new_{today}.xlsx'

  # import traffic data
  nw_traffic_data <- read_xlsx(
    path  = fs::path_abs(str_glue(base_file), start = base_dir),
    sheet = "NM_Daily_Traffic_All",
    range = cell_limits(c(2, 1), c(NA, 39))) |>
    mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  # process data for traffic update on the portal landing page
  nw_traffic_data_main_page <- nw_traffic_data |>
    filter(DAY_TFC > 0) |>
    filter(FLIGHT_DATE >= wef) |>
    select(
      date = FLIGHT_DATE,
      day_traffic =  DAY_TFC,
      dif_day_prev_week = DAY_TFC_PREV_WEEK_PERC,
      dif_day_prev_year = DAY_DIFF_PREV_YEAR_PERC,
      dif_day_2019 = DAY_TFC_DIFF_2019_PERC,
      avg_week_traffic = AVG_ROLLING_WEEK,
      dif_week_prev_week = DIF_PREV_WEEK_PERC,
      dif_week_prev_year = DIF_WEEK_PREV_YEAR_PERC,
      dif_week_2019 = DIF_ROLLING_WEEK_2019_PERC,
      y2d_flights_total = Y2D_TFC_YEAR,
      y2d_flights_daily_average = Y2D_AVG_TFC_YEAR,
      y2d_diff_previous_year_percentage = Y2D_DIFF_PREV_YEAR_PERC,
      y2d_diff_2019_year_percentage = Y2D_DIFF_2019_PERC
      )



  # import delay data
  nw_delay_data <- read_xlsx(
    path  = fs::path_abs(str_glue(base_file), start = base_dir),
    sheet = "NM_Daily_Delay_All",
    range = cell_limits(c(2, 1), c(NA, 39))) |>
    mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  # process data for traffic update on the portal landing page
  nw_delay_data_main_page <- nw_delay_data |>
    filter(FLIGHT_DATE >= wef) |>
    select(
      date = FLIGHT_DATE,
      day_delay = DAY_DLY,
      dif_day_delay_prev_week_perc = DAY_DLY_PREV_WEEK_PERC,
      dif_day_delay_prev_year_perc = DAY_DIFF_PREV_YEAR_PERC,
      dif_day_delay_2019_perc = DAY_DLY_DIFF_2019_PERC,
      avg_week_delay = AVG_ROLLING_WEEK,
      dif_week_delay_prev_week_perc = DIF_PREV_WEEK_PERC,
      dif_week_delay_prev_year_perc = DIF_WEEK_PREV_YEAR_PERC,
      dif_week_delay_2019_perc = DIF_ROLLING_WEEK_2019_PERC,
      y2d_delay_total = Y2D_DLY_YEAR,
      y2d_delay_daily_average = Y2D_AVG_DLY_YEAR,
      dif_y2d_delay_prev_year_perc = Y2D_DIFF_PREV_YEAR_PERC,
      dif_y2d_delay_2019_perc = Y2D_DIFF_2019_PERC
    )

  cols <- c(
    "avg_week_delay",
    "avg_week_traffic",
    # "collectionId",
    # "collectionName",
    # "created",
    "date",
    "day_delay",
    "day_traffic",
    "dif_day_2019",
    "dif_day_delay_2019_perc",
    "dif_day_delay_prev_week_perc",
    "dif_day_delay_prev_year_perc",
    "dif_day_prev_week",
    "dif_day_prev_year",
    "dif_week_2019",
    "dif_week_delay_2019_perc",
    "dif_week_delay_prev_week_perc",
    "dif_week_delay_prev_year_perc",
    "dif_week_prev_week",
    "dif_week_prev_year",
    "dif_y2d_delay_2019_perc",
    "dif_y2d_delay_prev_year_perc",
    # "id",
    # "updated",
    "y2d_delay_daily_average",
    "y2d_delay_total",
    "y2d_diff_2019_year_percentage",
    "y2d_diff_previous_year_percentage",
    "y2d_flights_daily_average",
    "y2d_flights_total",
    NULL
  )
  nw_traffic_data_main_page |>
    left_join(nw_delay_data_main_page, by = "date") |>
    select(all_of(cols))
}

retrieve_from_api <- function(wef = today(tzone = "UTC") - dweeks(weeks_back)) {
  base_url <- "https://aiu-portal.pockethost.io/api/collections/"
  url <- str_c(base_url,
               "network_situation/records",
               stringr::str_glue("?perPage=200&filter=(date>'{wef}')")) |>
    url()

  cols <- c(
    "avg_week_delay",
    "avg_week_traffic",
    "collectionId",
    "collectionName",
    "created",
    "date",
    "day_delay",
    "day_traffic",
    "dif_day_2019",
    "dif_day_delay_2019_perc",
    "dif_day_delay_prev_week_perc",
    "dif_day_delay_prev_year_perc",
    "dif_day_prev_week",
    "dif_day_prev_year",
    "dif_week_2019",
    "dif_week_delay_2019_perc",
    "dif_week_delay_prev_week_perc",
    "dif_week_delay_prev_year_perc",
    "dif_week_prev_week",
    "dif_week_prev_year",
    "dif_y2d_delay_2019_perc",
    "dif_y2d_delay_prev_year_perc",
    "id",
    "updated",
    "y2d_delay_daily_average",
    "y2d_delay_total",
    "y2d_diff_2019_year_percentage",
    "y2d_diff_previous_year_percentage",
    "y2d_flights_daily_average",
    "y2d_flights_total",
    NULL
  )

  aa <- yyjsonr::read_json_conn(url) |>
    magrittr::use_series("items") |>
    dplyr::as_tibble() |>
    dplyr::mutate(
      date    = lubridate::as_date(date),
      created = lubridate::as_date(created),
      updated = lubridate::as_date(updated)) |>
    select(all_of(cols))

  aa
}

# check if there are missing data points in the last 3 weeks
wef <- today(tzone = "UTC") - dweeks(weeks_back)
# 1. perform a query on last 3 weeks
aa <- retrieve_from_api(wef)
# 2. run DB query for last 3 weeks
bb <- retrieve_from_excell(wef)
# |>
#   # simulate new value
#   add_row(date = as_date("2024-05-21"), flights = 11)

# 3. compare and retain missing data points
api <- aa |>
  select(date, day_traffic, day_delay)
db <- bb |>
  select(date, day_traffic, day_delay)

# dates that aren't yet in the API
dd_missing <- dplyr::setdiff(db |> select(date), api |> select(date)) |>
  dplyr::pull(date)

# dates that are different in the API -> candidates for update
dd_update <- dplyr::setdiff(
  db |> dplyr::filter(!date %in% dd_missing),
  api) |>
  dplyr::pull(date)

# push missing/changed data points
# 1. authenticate
username <- Sys.getenv("PH_AIU_PORTAL_USR")
password <- Sys.getenv("PH_AIU_PORTAL_PWD")

app_main <- "aiu-portal"
collection <- "network_situation"

adm_main <- ph_authenticate_admin_username_password(
  app_main,
  "/api/admins/auth-with-password",
  username,
  password)


# 2. upload new data points
bb |>
  dplyr::filter(date %in% dd_missing) |>
  purrr::pwalk(.f = function(
    avg_week_delay,
    avg_week_traffic,
    date,
    day_delay,
    day_traffic,
    dif_day_2019,
    dif_day_delay_2019_perc,
    dif_day_delay_prev_week_perc,
    dif_day_delay_prev_year_perc,
    dif_day_prev_week,
    dif_day_prev_year,
    dif_week_2019,
    dif_week_delay_2019_perc,
    dif_week_delay_prev_week_perc,
    dif_week_delay_prev_year_perc,
    dif_week_prev_week,
    dif_week_prev_year,
    dif_y2d_delay_2019_perc,
    dif_y2d_delay_prev_year_perc,
    y2d_delay_daily_average,
    y2d_delay_total,
    y2d_diff_2019_year_percentage,
    y2d_diff_previous_year_percentage,
    y2d_flights_daily_average,
    y2d_flights_total
  ) {
    body <- list(
      avg_week_delay = avg_week_delay,
      avg_week_traffic = avg_week_traffic,
      date = date,
      day_delay = day_delay,
      day_traffic = day_traffic,
      dif_day_2019 = dif_day_2019,
      dif_day_delay_2019_perc = dif_day_delay_2019_perc,
      dif_day_delay_prev_week_perc = dif_day_delay_prev_week_perc,
      dif_day_delay_prev_year_perc = dif_day_delay_prev_year_perc,
      dif_day_prev_week = dif_day_prev_week,
      dif_day_prev_year = dif_day_prev_year,
      dif_week_2019 = dif_week_2019,
      dif_week_delay_2019_perc = dif_week_delay_2019_perc,
      dif_week_delay_prev_week_perc = dif_week_delay_prev_week_perc,
      dif_week_delay_prev_year_perc = dif_week_delay_prev_year_perc,
      dif_week_prev_week = dif_week_prev_week,
      dif_week_prev_year = dif_week_prev_year,
      dif_y2d_delay_2019_perc = dif_y2d_delay_2019_perc,
      dif_y2d_delay_prev_year_perc = dif_y2d_delay_prev_year_perc,
      y2d_delay_daily_average = y2d_delay_daily_average,
      y2d_delay_total = y2d_delay_total,
      y2d_diff_2019_year_percentage = y2d_diff_2019_year_percentage,
      y2d_diff_previous_year_percentage = y2d_diff_previous_year_percentage,
      y2d_flights_daily_average = y2d_flights_daily_average,
      y2d_flights_total = y2d_flights_total
    ) |>
      purrr::list_transpose() |>
      magrittr::extract2(1)

    ph_create_record(
      app = app_main,
      api = "/api/collections",
      collection = collection,
      token = adm_main$token,
      body = body)
  })


# 3. upload updated data points
bb |>
  dplyr::filter(date %in% dd_update) |>
  dplyr::left_join(aa, by = c("date" = "date"), suffix = c("", ".old")) |>
  dplyr::select(-ends_with(".old")) |>
  dplyr::select(
    avg_week_delay,
    avg_week_traffic,
    date,
    day_delay,
    day_traffic,
    dif_day_2019,
    dif_day_delay_2019_perc,
    dif_day_delay_prev_week_perc,
    dif_day_delay_prev_year_perc,
    dif_day_prev_week,
    dif_day_prev_year,
    dif_week_2019,
    dif_week_delay_2019_perc,
    dif_week_delay_prev_week_perc,
    dif_week_delay_prev_year_perc,
    dif_week_prev_week,
    dif_week_prev_year,
    dif_y2d_delay_2019_perc,
    dif_y2d_delay_prev_year_perc,
    y2d_delay_daily_average,
    y2d_delay_total,
    y2d_diff_2019_year_percentage,
    y2d_diff_previous_year_percentage,
    y2d_flights_daily_average,
    y2d_flights_total,
    id
  ) |>
  purrr::pwalk(.f = function(
    avg_week_delay,
    avg_week_traffic,
    date,
    day_delay,
    day_traffic,
    dif_day_2019,
    dif_day_delay_2019_perc,
    dif_day_delay_prev_week_perc,
    dif_day_delay_prev_year_perc,
    dif_day_prev_week,
    dif_day_prev_year,
    dif_week_2019,
    dif_week_delay_2019_perc,
    dif_week_delay_prev_week_perc,
    dif_week_delay_prev_year_perc,
    dif_week_prev_week,
    dif_week_prev_year,
    dif_y2d_delay_2019_perc,
    dif_y2d_delay_prev_year_perc,
    y2d_delay_daily_average,
    y2d_delay_total,
    y2d_diff_2019_year_percentage,
    y2d_diff_previous_year_percentage,
    y2d_flights_daily_average,
    y2d_flights_total,
    id
  ) {
    body <- list(
      avg_week_delay = avg_week_delay,
      avg_week_traffic = avg_week_traffic,
      date = date,
      day_delay = day_delay,
      day_traffic = day_traffic,
      dif_day_2019 = dif_day_2019,
      dif_day_delay_2019_perc = dif_day_delay_2019_perc,
      dif_day_delay_prev_week_perc = dif_day_delay_prev_week_perc,
      dif_day_delay_prev_year_perc = dif_day_delay_prev_year_perc,
      dif_day_prev_week = dif_day_prev_week,
      dif_day_prev_year = dif_day_prev_year,
      dif_week_2019 = dif_week_2019,
      dif_week_delay_2019_perc = dif_week_delay_2019_perc,
      dif_week_delay_prev_week_perc = dif_week_delay_prev_week_perc,
      dif_week_delay_prev_year_perc = dif_week_delay_prev_year_perc,
      dif_week_prev_week = dif_week_prev_week,
      dif_week_prev_year = dif_week_prev_year,
      dif_y2d_delay_2019_perc = dif_y2d_delay_2019_perc,
      dif_y2d_delay_prev_year_perc = dif_y2d_delay_prev_year_perc,
      y2d_delay_daily_average = y2d_delay_daily_average,
      y2d_delay_total = y2d_delay_total,
      y2d_diff_2019_year_percentage = y2d_diff_2019_year_percentage,
      y2d_diff_previous_year_percentage = y2d_diff_previous_year_percentage,
      y2d_flights_daily_average = y2d_flights_daily_average,
      y2d_flights_total = y2d_flights_total
    ) |>
      purrr::list_transpose() |>
      magrittr::extract2(1)
    ph_update_record(
      id = id,
      app = app_main,
      api = "/api/collections",
      collection = collection,
      token = adm_main$token,
      body = body)
  })
