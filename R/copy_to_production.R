library(here)
library("R.utils")
library(sendmailR)
library(dplyr)
library(stringr)
library(readxl)
library(tidyverse)
library(lubridate)
library(purrr)

## params
source(here("..", "mobile-app", "R", "params.R"))

destination_dir <- '//ihx-vdm05/LIVE_var_www_performance$/briefing'

# set the archive_mode to FALSE to run the scripts for day-1.
# set the archive_mode to TRUE to run the scripts
# for the sequence of dates set below.

archive_mode <- FALSE

if (archive_mode) {
  wef <- "2025-02-21"  #included in output
  til <- "2025-02-21"  #included in output
  data_day_date <- seq(ymd(wef), ymd(til), by = "day")
} else {
  data_day_date <- lubridate::today(tzone = "") +  days(-1)
}

# check data status ----

files_to_read <- c(
  here(nw_base_dir, nw_base_file),
  here(st_base_dir, st_base_file),
  here(ao_base_dir, ao_base_file),
  here(ap_base_dir, ap_base_file)
)

check_status <- function(check_file) {
  checK_last_date <- read_xlsx(check_file,
           sheet = "checks",
           range = cell_limits(c(1, 4), c(2, 4))) %>% pull()
  return(checK_last_date == data_day_date )
}

data_status <- tryCatch({
  prod(map_lgl(files_to_read, check_status)) == 1
}, error = function(e) {
  FALSE
})

# clean folder function
clean_folder <- function(folder_address) {
  files <- list.files(folder_address, full.names = TRUE)
  files_to_delete <- files[basename(files) != ".keepme"]
  file.remove(files_to_delete)
}

# define functions for data generation & copy ----
generate_app_data <- function(data_day_date) {
  # clean local folders
  walk(c(st_local_data_folder,
         nw_local_data_folder,
         ao_local_data_folder,
         ap_local_data_folder),
       clean_folder)

  source(here("..", "mobile-app", "R", "generate_json_files_state.R"))
  source(here("..", "mobile-app", "R", "generate_json_files_nw.R"))
  source(here("..", "mobile-app", "R", "generate_json_files_ao.R"))
  source(here("..", "mobile-app", "R", "generate_json_files_ap.R"))
}

copy_app_data <- function(data_day_date) {
  # parameters ----
  data_day_text_dash <- data_day_date %>% format("%Y-%m-%d")
  network_data_folder_v3 <- here(destination_dir, "data", "v3", data_day_text_dash)
  network_data_folder_v4 <- here(destination_dir, "data", "v4", data_day_text_dash)

  # check if v3 date folder already exists ----
  if (!dir.exists(network_data_folder_v3)) {
    dir.create(network_data_folder_v3)
  }

  # copy files to the V3 network folder ----
  st_files_to_copy <- list.files(st_local_data_folder, full.names = TRUE)
  nw_files_to_copy <- list.files(nw_local_data_folder, full.names = TRUE)
  ao_files_to_copy <- list.files(ao_local_data_folder, full.names = TRUE)

  file.copy(from = st_files_to_copy, to = network_data_folder_v3, overwrite = TRUE)
  file.copy(from = nw_files_to_copy, to = network_data_folder_v3, overwrite = TRUE)
  file.copy(from = ao_files_to_copy, to = network_data_folder_v3, overwrite = TRUE)

  # copy also one file in the root folder for the checkupdates script to verify
  file.copy(from = here(nw_local_data_folder, "nw_json_app.json"),
            to = here(destination_dir, "data", "v3"),
            overwrite = TRUE)

  # backup json files
  file.copy(network_data_folder_v3,
            archive_dir,
            recursive = TRUE, overwrite = TRUE)

  # check if v4 date folder already exists ----
  if (!dir.exists(network_data_folder_v4)) {
    dir.create(network_data_folder_v4)
  }

  # copy production files into v4 folder
  v3_files_to_copy <- list.files(network_data_folder_v3, full.names = TRUE)

  file.copy(v3_files_to_copy,
            network_data_folder_v4,
            recursive = TRUE, overwrite = TRUE)

  # development files to copy
  ap_files_to_copy <- list.files(ap_local_data_folder, full.names = TRUE)

  # copy development files
  file.copy(from = ap_files_to_copy, to = network_data_folder_v4, overwrite = TRUE)


}

# Define a combined function
process_app_data <- function(data_day_date) {
  generate_app_data(data_day_date)
  copy_app_data(data_day_date)
}


# generate and copy app files ----
if(archive_mode | data_status){
  # get helper functions and common data sets ----
  source(here("..", "mobile-app", "R", "helpers.R"))
  source(here("..", "mobile-app", "R", "get_common_data.R"))

  #temporary cheating "for" loop until I can get the purr setup properly
  data_day_date_temp <- data_day_date
  for (i in length(data_day_date_temp):1) {
    data_day_date <- data_day_date_temp[[i]]
    # generate and copy data for date sequence ----
    walk(data_day_date, .f = process_app_data)
  }
}

# send email ----
## email parameters ----
if (data_status) {
  sbj = "All app datasets copied successfully to folder"
  msg = "All good, relax!"

} else {
  sbj = "App datasets not copied - some tables not updated"
  msg = "Some tables were not updated."

}

from    <- "oscar.alfaro@eurocontrol.int"
to      <- c("oscar.alfaro@eurocontrol.int"
             ,
             "quinten.goens@eurocontrol.int",
             "enrico.spinielli@eurocontrol.int",
             "delia.budulan@eurocontrol.int",
             "nora.cashman@eurocontrol.int"
             ,  "denis.huet@eurocontrol.int"
)
# cc      <- c("enrico.spinielli@eurocontrol.int")
control <- list(smtpServer="mailservices.eurocontrol.int")

## send ----
if (archive_mode == FALSE){
  sendmail(from = from, to = to,
           # cc = cc,
           subject = sbj, msg = msg,
           control = control)
}
