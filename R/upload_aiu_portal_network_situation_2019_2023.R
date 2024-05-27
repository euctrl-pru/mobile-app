# network situation for AIU Portal: archived data 2019 - 2023

library(readxl)
library(fs)
library(lubridate)
library(stringr)
library(dplyr)
# pak::pkg_install("euctrl-pru/pockethostr")
library(pockethostr)


today <- (lubridate::now() +  days(-1)) |> format("%Y%m%d")
base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/'
base_file <- str_glue('99_Traffic_Landing_Page_dataset_new_{today}.xlsx')


nw_traffic_data <- read_xlsx(
  path  = fs::path_abs(base_file, start = base_dir),
  sheet = "NM_Daily_Traffic_All",
  range = cell_limits(c(2, 1), c(NA, 39))) |>
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

dd_2024 <- nw_traffic_data |>
  select("FLIGHT_DATE", "DAY_TFC") |>
  filter(year(FLIGHT_DATE) == 2024, DAY_TFC > 0)

dd_2020 <- nw_traffic_data |>
  select("FLIGHT_DATE" = "FLIGHT_DATE_2020", "DAY_TFC" = "DAY_TFC_2020") |>
  filter(year(FLIGHT_DATE) == 2020)

dd_2019 <- nw_traffic_data |>
  select("FLIGHT_DATE" = "FLIGHT_DATE_2019", "DAY_TFC" = "DAY_TFC_2019") |>
  filter(year(FLIGHT_DATE) == 2019)

dd_2023 <- nw_traffic_data |>
  select("FLIGHT_DATE" = "FLIGHT_DATE_PREV_YEAR", "DAY_TFC" = "DAY_TFC_PREV_YEAR") |>
  filter(year(FLIGHT_DATE) == 2023)


# for traffic update on the portal landing page
nw_traffic_data_main_page <- nw_traffic_data |>
  filter(DAY_TFC > 0) |>
  filter(FLIGHT_DATE == max(FLIGHT_DATE))

body <- nw_traffic_data_main_page |>
  select(
    date = FLIGHT_DATE,
    y2d_flights_total = Y2D_TFC_YEAR,
    y2d_flights_daily_average = Y2D_AVG_TFC_YEAR,
    y2d_diff_previous_year_percentage = Y2D_DIFF_PREV_YEAR_PERC,
    y2d_diff_2019_year_percentage = Y2D_DIFF_2019_PERC) |>
  as.list() |>
  purrr::list_transpose() |>
  magrittr::extract2(1)



username <- Sys.getenv("PH_AIU_PORTAL_USR")
password <- Sys.getenv("PH_AIU_PORTAL_PWD")

app_main <- "aiu-portal"
app_test <- "aiu-portal-test"
collection <- "network_situation"


# authenticate over test app
adm_test <- ph_authenticate_admin_username_password(
  app_test,
  "/api/admins/auth-with-password",
  username,
  password)

# authenticate over main app
adm_main <- ph_authenticate_admin_username_password(
  app_main,
  "/api/admins/auth-with-password",
  username,
  password)



# TODO: cope with
# * update of existing entry
# * need to fill holes, i.e. missing days
# * errors

ph_create_record(
  app = app_test,
  api = "/api/collections",
  collection = collection,
  token = adm_test$token,
  body = body)

ph_create_record(
  app = app_main,
  api = "/api/collections",
  collection = collection,
  token = adm_main$token,
  body = body)
