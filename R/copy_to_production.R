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
network_data_folder_prod <- here(destination_dir, "data", "v4")
network_data_folder_dev <- here(destination_dir, "data", "v5")

# set the archive_mode to FALSE to run the scripts for day-1.
# set the archive_mode to TRUE to run the scripts
# for the sequence of dates set below.

archive_mode <- FALSE

if (archive_mode) {
  wef <- "2024-01-01"  #included in output
  til <- "2025-09-03"  #included in output
  data_day_date <- seq(ymd(wef), ymd(til), by = "day")
} else {
  data_day_date <- lubridate::today(tzone = "") +  days(-1)
}

if (!archive_mode) {
## update basic tables
source(here("..", "mobile-app", "R", "update_base_tables.R"))
## create csvs from basic tables
source(here("..", "mobile-app", "R", "csv_from_new_files_ap.R"))
source(here("..", "mobile-app", "R", "csv_from_new_files_ao.R"))
source(here("..", "mobile-app", "R", "csv_from_new_files_st.R"))
source(here("..", "mobile-app", "R", "csv_from_new_files_nw.R"))
}

# set the stakeholders you want to generate when using archive mode
stakeholders <- if(!archive_mode) {
  c("nw","st","ao","ap", "sp", NULL) # don't touch this line
  } else {c(
    # "nw",
    "st",
    # "ao",
    # "ap",
    # "sp",
    NULL)
}

# check data status ----
files_to_read <- setdiff(stakeholders, c("ap", "ao", "st")) %>%
  compact() %>%  # Remove NULLs
  map_chr(~ here(get(paste0(.x, "_base_dir")), get(paste0(.x, "_base_file"))))

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

folders_to_clean <- stakeholders %>%
  compact() %>%
  map_chr(~ get(paste0(.x, "_local_data_folder")))

# define functions for data generation & copy ----
generate_app_data <- function(data_day_date) {
  # clean local folders
  folders_to_clean %>% walk(clean_folder)

# generate json files
  all_jsons <- TRUE #TRUE if we want all json files for the stakeholder(s)
  if(all_jsons) {
    walk(stakeholders, ~ {
      source(here("..", "mobile-app", "R", paste0("generate_json_files_", .x, ".R")))
    })
  } else if (archive_mode) {
    ### to be created, if possible, a way of generating only one json file
    source(here("..", "mobile-app", "R","one_json.R"))
    one_json()
  }
}


copy_app_data <- function(data_day_date) {
  # parameters ----
  data_day_text_dash <- data_day_date %>% format("%Y-%m-%d")
  network_data_folder_prod_date <- here(network_data_folder_prod, data_day_text_dash)
  network_data_folder_dev_date <- here(network_data_folder_dev, data_day_text_dash)

  # check if date folders already exist ----
  if (!dir.exists(network_data_folder_prod_date)) {
    dir.create(network_data_folder_prod_date)
  }

  if (!dir.exists(network_data_folder_dev_date)) {
    dir.create(network_data_folder_dev_date)
  }

  # copy files to the network folders ----
  walk(stakeholders, ~ {
    files_to_copy <- list.files(get(paste0(.x, "_local_data_folder")), full.names = TRUE)
    # assign(paste0(.x, "_files_to_copy"), files_to_copy, envir = .GlobalEnv)  # Assign dynamically to global environment

    if (get(paste0(.x, "_status")) == "prod") {
        file.copy(from = files_to_copy, to = network_data_folder_prod_date, overwrite = TRUE)
    }
    # print(files_to_copy)
    file.copy(from = files_to_copy, to = network_data_folder_dev_date, overwrite = TRUE)
    print(.x)
    }
    )

  # copy also one file in the root folder for the checkupdates script to verify
  if(!archive_mode) {
    file.copy(from = here(nw_local_data_folder, "nw_json_app.json"),
              to = network_data_folder_dev,
              overwrite = TRUE)

    file.copy(from = here(nw_local_data_folder, "nw_json_app.json"),
              to = network_data_folder_prod,
              overwrite = TRUE)
  }

  # backup json files
  file.copy(network_data_folder_prod_date,
            archive_dir,
            recursive = TRUE, overwrite = TRUE)

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
    print(data_day_date)
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

