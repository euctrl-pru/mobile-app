library(here)
library("R.utils")
library(sendmailR)
library(dplyr)
library(stringr)
library(readxl)

# parameters ----
yesterday <- (lubridate::now() +  lubridate::days(-1)) %>% format("%Y-%m-%d")

data_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/LastVersion/'
nw_file <- "099_Traffic_Landing_Page_dataset_new.xlsx"
st_file <- '099a_app_state_dataset.xlsx'

base_dir <- here("..", "mobile-app")
destination_dir <- '//ihx-vdm05/LIVE_var_www_performance$/briefing'
local_data_folder <- here("..", "mobile-app", "data", "V2")
# temporary folder for testing new v3 files
test_local_data_folder <- here("..", "mobile-app", "data", "V3")

network_data_folder_v2 <- here(destination_dir, "data", "v2")
network_data_folder_v3 <- here(destination_dir, "data", "v3", yesterday)

# check data status ----
nw_file_status <- read_xlsx(path = fs::path_abs(str_glue(nw_file),start = data_dir),
  sheet = "Checks",
  range = cell_limits(c(1, 5), c(2, 5))) %>% pull()

st_file_status <- read_xlsx(path = fs::path_abs(str_glue(st_file),start = data_dir),
                            sheet = "checks",
                            range = cell_limits(c(1, 5), c(2, 5))) %>% pull()

# generate json files, if data files updated ----
if (nw_file_status == "OK") {
  source(here("..", "mobile-app", "R", "helpers.R"))
  source(here("..", "mobile-app", "R", "get_common_data.R"))
  source(here("..", "mobile-app", "R", "generate_json_files.R"))
  # rmarkdown::render(here::here("R", "mob_ao_traffic_rank_day.Rmd"), output_dir = here::here("iframes"))
  # rmarkdown::render(here::here("R", "mob_apt_traffic_rank_day.Rmd"), output_dir = here::here("iframes"))
}

if (st_file_status == "OK") {
  source(here("..", "mobile-app", "R", "generate_json_files_state.R"))
}

# copy files to performance folder ----

## copy v1 files ----
if (nw_file_status == "OK") {

  ### set email status parameters
  sbj = "Only nw files updated. App update halted"
  msg = "Some tables of the state dataset were not updated. App datasets not copied"
}

## copy v2, v3 files ----
if (nw_file_status == "OK" & st_file_status == "OK") {

  ### copy that data directory to the V2 network folder
  copyDirectory(local_data_folder, network_data_folder_v2, overwrite = TRUE)

  ### check if v3 date folder already exists
  if (!dir.exists(network_data_folder_v3)) {
    dir.create(network_data_folder_v3)
  }

  ### copy that data directory to the V3 network folder
  copyDirectory(local_data_folder, network_data_folder_v3, overwrite = TRUE)

  ### copy the v3 test files
  v3_files_to_copy <- list.files(test_local_data_folder, full.names = TRUE)
  file.copy(from = v3_files_to_copy, to = network_data_folder_v3, overwrite = FALSE)

  ### set email status parameters
  sbj = "App network and state datasets copied successfully to folder"
  msg = "All good, relax!"

} else if (nw_file_status != "OK" & st_file_status != "OK") {
  ### set email status parameters
  sbj = "App datasets not copied - some tables not updated"
  msg = "Some tables of both nw and state datasets were not updated."

}

# send email ----
from    <- "oscar.alfaro@eurocontrol.int"
to      <- c("oscar.alfaro@eurocontrol.int"
             ,
             "quinten.goens@eurocontrol.int",
             "enrico.spinielli@eurocontrol.int",
             "denis.huet@eurocontrol.int"
)
# cc      <- c("enrico.spinielli@eurocontrol.int")
control <- list(smtpServer="mailservices.eurocontrol.int")

sendmail(from = from, to = to,
         # cc = cc,
         subject = sbj, msg = msg,
         control = control)
