# insert latest entries in
library(readxl)
library(fs)
library(lubridate)
library(stringr)
library(dplyr)
# pak::pkg_install("euctrl-pru/pockethostr")
library(pockethostr)

username <- Sys.getenv("PH_AIU_PORTAL_USR")
password <- Sys.getenv("PH_AIU_PORTAL_PWD")

app_main <- "eurocontrol-data"
app_test <- "eurocontrol-data-test"
collection <- "nw_traffic"

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


today <- (lubridate::now() +  days(-1)) |> format("%Y%m%d")
base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/'
base_file <- '99_Traffic_Landing_Page_dataset_new_{today}.xlsx'

last_day <-  (lubridate::now() +  days(-1))
last_year <- as.numeric(format(last_day,'%Y'))

nw_traffic_data <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(base_file),
    start = base_dir),
  sheet = "NM_Daily_Traffic_All",
  range = cell_limits(c(2, 1), c(NA, 39))) |>
  as_tibble()

nw_traffic_last_day <- nw_traffic_data %>%
  # filter(FLIGHT_DATE == max(LAST_DATA_DAY))
  filter(FLIGHT_DATE == as_datetime("2024-01-28"))

nw_traffic_latest <- nw_traffic_last_day %>%
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
  ) |>
  as.list() |>
  purrr::list_transpose() |>
  magrittr::extract2(1)


# TODO: cope with
# * update of existing entry
# * need to fill holes, i.e. missing days
# * errors

ph_create_record(
  app = app_test,
  api = "/api/collections",
  collection = collection,
  token = adm_test$token,
  body = nw_traffic_latest)

ph_create_record(
  app = app_main,
  api = "/api/collections",
  collection = collection,
  token = adm_main$token,
  body = nw_traffic_latest)
