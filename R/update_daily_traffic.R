# update network traffic data

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

retrieve_from_db <- function(wef = today(tzone = "UTC") - dweeks(weeks_back)) {
  from_date <- wef |> lubridate::as_date()
  query <- str_glue("
    SELECT
      A_FIRST_ENTRY_TIME_DATE FLIGHT_DATE ,
      SUM(NVL(A.ALL_TRAFFIC, 0)) DAY_TFC
    FROM
      V_AIU_AGG_GLOBAL_DAILY_COUNTS A
    WHERE
      A.A_FIRST_ENTRY_TIME_DATE  >= TO_DATE('{from_date}', 'YYYY-MM-DD')
      AND A.A_FIRST_ENTRY_TIME_DATE  < TRUNC (SYSDATE)
    GROUP BY
      A.A_FIRST_ENTRY_TIME_DATE
  ")

  dd <- export_query(query) |>
    dplyr::mutate(FLIGHT_DATE = as_date(FLIGHT_DATE, tz = "UTC")) |>
    arrange(FLIGHT_DATE) |>
    rename(date = FLIGHT_DATE, flights = DAY_TFC)

  dd
}
retrieve_from_api <- function(wef) {
  base_url <- "https://aiu-portal.pockethost.io/api/collections/"
  url <- str_c(base_url,
               "network_traffic/records",
               stringr::str_glue("?perPage=200&filter=(date>'{wef}')")) |>
    url()
  aa <- yyjsonr::read_json_conn(url) |>
    magrittr::use_series("items") |>
    dplyr::as_tibble() |>
    dplyr::mutate(date = lubridate::as_date(date)) |>
    dplyr::select(id, date, flights)

  aa
}

# check if there are missing data points in the last 3 weeks
wef <- today(tzone = "UTC") - dweeks(weeks_back)
# 1. perform a query on last 3 weeks
aa <- retrieve_from_api(wef)
# 2. run DB query for last 3 weeks
bb <- retrieve_from_db(wef)
# |>
#   # simulate new value
#   add_row(date = as_date("2024-05-21"), flights = 11)

# 3. compare and retain missing data points
api <- aa |>
  select(date, flights)
db <- bb |>
  select(date, flights)

# dates that aren't yet in the API
dd_missing <- dplyr::setdiff(db |> select(date), api |> select(date)) |>
  dplyr::pull(date)

# dates that are different in the API -> candidates fro update
dd_update <- dplyr::setdiff(
  db |> dplyr::filter(!date %in% dd_missing),
  api) |>
  dplyr::pull(date)

# push missing/changed data points
# 1. authenticate
username <- Sys.getenv("PH_AIU_PORTAL_USR")
password <- Sys.getenv("PH_AIU_PORTAL_PWD")

app_main <- "aiu-portal"
collection <- "network_traffic"

adm_main <- ph_authenticate_admin_username_password(
  app_main,
  "/api/admins/auth-with-password",
  username,
  password)


# 2. upload new data points
bb |>
  dplyr::filter(date %in% dd_missing) |>
  purrr::pwalk(.f = function(date, flights) {
    body <- list(date    = date,
                 flights = flights) |>
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
  dplyr::select(id, date, flights) |>
  purrr::pwalk(.f = function(id, date, flights) {
    body <- list(date    = date,
                 flights = flights) |>
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
