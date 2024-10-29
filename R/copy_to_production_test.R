library(here)
library("R.utils")
library(sendmailR)
library(dplyr)
library(stringr)
library(readxl)
library(tidyverse)
library(lubridate)
library(purrr)

# parameters ----
data_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/LastVersion/'
nw_file <- "099_Traffic_Landing_Page_dataset_new.xlsx"
st_file <- '099a_app_state_dataset.xlsx'

base_dir <- here("..", "mobile-app")
destination_dir <- '//ihx-vdm05/LIVE_var_www_performance$/briefing'

## other params
source(here("..", "mobile-app", "R", "params.R"))

# current day = FALSE, past days = TRUE
archive_mode <- TRUE

if (archive_mode) {
  wef <- "2024-01-03"  #included in output
  til <- "2024-01-03"  #included in output
  data_day_date <- seq(ymd(wef), ymd(til), by = "day")
} else {
  data_day_date <- lubridate::today(tzone = "") +  days(-1)
}

# check data status ----
nw_file_status <- read_xlsx(path = fs::path_abs(str_glue(nw_file),start = data_dir),
                            sheet = "Checks",
                            range = cell_limits(c(1, 5), c(2, 5))) %>% pull()

st_file_status <- read_xlsx(path = fs::path_abs(str_glue(st_file),start = data_dir),
                            sheet = "checks",
                            range = cell_limits(c(1, 5), c(2, 5))) %>% pull()

# define functions for data generation & copy ----
generate_app_data <- function(data_day_date) {
  # json functions ----
  source(here("..", "mobile-app", "R", "functions_json_files_nw.R")) # so it can be launched from the checkupdates script in grounded aircraft

  data_day_text <- data_day_date %>% format("%Y%m%d")
  data_day_year <- as.numeric(format(data_day_date,'%Y'))

  # json for main page ----
  nw_json_app(data_day_date)

  # jsons for graphs -------
  ## traffic -----
  nw_traffic_evo_chart_daily(data_day_date)

  ## delay ----
  nw_delay_category_evo_charts(data_day_date)
  nw_delay_flt_type_evo_charts(data_day_date)

  ## punctuality ----
  nw_punct_evo_chart(data_day_date)

  ## billing ----
  nw_billing_evo_chart(data_day_date)

  ## co2 emissions ----
  nw_co2_evo_chart(data_day_date)

  # jsons for ranking tables ----

  ## Aircraft operators traffic ----
  nw_ao_ranking_traffic(data_day_date)

  ## Airport traffic ----
  nw_apt_ranking_traffic(data_day_date)

  ## Country traffic DAI ----
  nw_ctry_ranking_traffic_DAI(data_day_date)

  ## Airport delay -----
  nw_apt_ranking_delay(data_day_date)

  ## ACC delay ----
  nw_acc_ranking_delay(data_day_date)

  ## Country delay ----
  nw_ctry_ranking_delay(data_day_date)


  ## Airport punctuality ----
  nw_apt_ranking_punctuality(data_day_date)

  ## Country punctuality ----
  nw_ctry_ranking_punctuality(data_day_date)

  # source(here("..", "mobile-app", "R", "generate_json_files_nw.R"))
  # source(here("..", "mobile-app", "R", "generate_json_files_state.R"))
  # source(here("..", "mobile-app", "R", "generate_json_files_ao.R"))
}

copy_app_data <- function(data_day_date) {
  # parameters ----
  data_day_text_dash <- data_day_date %>% format("%Y-%m-%d")
  network_data_folder_v2 <- here(destination_dir, "data", "v2")
  network_data_folder_v3 <- here(destination_dir, "data", "v3", data_day_text_dash)

  # copy files to the V2 network folder ----
  st_prod_files_to_copy <- list.files(st_local_data_folder_prod, full.names = TRUE)
  nw_prod_files_to_copy <- list.files(nw_local_data_folder_prod, full.names = TRUE)

  if (archive_mode == FALSE) {
    file.copy(from = nw_prod_files_to_copy, to = network_data_folder_v2, overwrite = TRUE)
    file.copy(from = st_prod_files_to_copy, to = network_data_folder_v2, overwrite = TRUE)
  }

  # check if v3 date folder already exists ----
  if (!dir.exists(network_data_folder_v3)) {
    dir.create(network_data_folder_v3)
  }

  # copy files to the V3 network folder ----
  nw_dev_files_to_copy <- list.files(nw_local_data_folder_dev, full.names = TRUE)
  file.copy(from = nw_dev_files_to_copy, to = network_data_folder_v3, overwrite = TRUE)
  file.copy(from = st_prod_files_to_copy, to = network_data_folder_v3, overwrite = TRUE)

  ### copy the v3 test files
  ao_dev_files_to_copy <- list.files(ao_local_data_folder_dev, full.names = TRUE)
  file.copy(from = ao_dev_files_to_copy, to = network_data_folder_v3, overwrite = TRUE)
}

# Define a combined function
process_app_data <- function(data_day_date) {
  generate_app_data(data_day_date)
  # copy_app_data(data_day_date)
}


# generate and copy app files ----
if(archive_mode | (nw_file_status == "OK" & st_file_status == "OK")){
  # get helper functions and large data sets ----
  source(here("..", "mobile-app", "R", "helpers.R"))
  source(here("..", "mobile-app", "R", "get_common_data.R"))

  #temporary for loop until I can get the purr setup properly
  data_day_date_temp <- data_day_date
  for (i in 1:length(data_day_date_temp)) {
    data_day_date <- data_day_date_temp[[i]]
    # generate and copy data for date sequence ----
    walk(data_day_date, .f = process_app_data)
  }
}

# send email ----
## email parameters ----
if (nw_file_status == "OK" & st_file_status == "OK") {
  sbj = "App network and state datasets copied successfully to folder"
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
             "denis.huet@eurocontrol.int"
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
