# parameters ----
# this will set in which folder data is saved locally in the repo
nw_status <- "prod"
st_status <- "prod"
ao_status <- "prod"
ap_status <- "prod"
sp_status <- "prod"

nw_local_data_folder_prod <- here::here("..", "mobile-app", "data", "prod", "nw")
st_local_data_folder_prod <- here::here("..", "mobile-app", "data", "prod", "st")
ao_local_data_folder_prod <- here::here("..", "mobile-app", "data", "prod", "ao")
ap_local_data_folder_prod <- here::here("..", "mobile-app", "data", "prod", "ap")
sp_local_data_folder_prod <- here::here("..", "mobile-app", "data", "prod", "sp")

nw_local_data_folder_dev <- here::here("..", "mobile-app", "data", "dev", "nw")
st_local_data_folder_dev <- here::here("..", "mobile-app", "data", "dev", "st")
ao_local_data_folder_dev <- here::here("..", "mobile-app", "data", "dev", "ao")
ap_local_data_folder_dev <- here::here("..", "mobile-app", "data", "dev", "ap")
sp_local_data_folder_dev <- here::here("..", "mobile-app", "data", "dev", "sp")

nw_local_data_folder <- here::here("..", "mobile-app", "data", nw_status, "nw")
st_local_data_folder <- here::here("..", "mobile-app", "data", st_status, "st")
ao_local_data_folder <- here::here("..", "mobile-app", "data", ao_status, "ao")
ap_local_data_folder <- here::here("..", "mobile-app", "data", ap_status, "ap")
sp_local_data_folder <- here::here("..", "mobile-app", "data", sp_status, "sp")

archive_dir <- "//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/app/json/"
archive_dir_raw <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Project/DDP/AIU app/data_archive'
archive_dir_raw_backup <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/app/csv'

# DB params
usr <- Sys.getenv("PRU_DEV_USR")
pwd <- Sys.getenv("PRU_DEV_PWD")
dbn <- Sys.getenv("PRU_DEV_DBNAME")

# STATFOR forecast params
# list of forecast names and Ids
forecast_list <- data.frame(
  id = c(
    3716,
    3731,
    3910,
    3950,
    4041,
    NULL
  ),
  name = c(
    "October 2023 Forecast",
    "February 2024 Forecast",
    "October 2024 Forecast",
    "February 2025 Forecast",
    "October 2025 Forecast",
    NULL),
  publication_date = c(
    "2023-10-18",
    "2024-02-26",
    "2024-10-15",
    "2025-02-28",
    "2025-10-17",
    NULL
  ),
  name_date = c(
    "2023-10-01",
    "2024-02-01",
    "2024-10-01",
    "2025-02-01",
    "2025-10-01",
    NULL
  )
) 

if (!exists("data_day_date")) {
  data_day_date <- lubridate::today(tzone = "") +  days(-1)
  # data_day_date <- ymd("2025-03-01")
}

# Filter and select the latest valid forecast
valid_rows <- forecast_list[as.Date(forecast_list$publication_date) < data_day_date + days(2), ]
latest_row <- valid_rows[which.max(as.Date(valid_rows$publication_date)), ]

forecast_id <- latest_row$id
forecast_name_value <-  latest_row$name
forecast_name_date <-  latest_row$name_date

forecast_file_name <- tolower(
  paste0(
    stringr::str_replace_all(forecast_name_value," ", "_"),
    ".csv")
  )

forecast_min_year_graph <- 2019

