library(here)
library("R.utils")
library(sendmailR)
library(dplyr)
library(stringr)
library(readxl)

# check data status ----
data_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/LastVersion/'
nw_file <- "099_Traffic_Landing_Page_dataset_new.xlsx"
st_file <- '099a_app_state_dataset.xlsx'

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
base_dir <- here("..", "mobile-app")
# base_dir <- here()
destination_dir <- '//ihx-vdm05/LIVE_var_www_performance$/briefing/'

## copy v1 files ----
if (nw_file_status == "OK") {
  # copyDirectory(here("..", "mobile-app", "data"), paste0(destination_dir,"data"), overwrite = TRUE)
  # copyDirectory(here::here("data"), paste0(destination_dir,"data"), overwrite = TRUE)

  ### set email status parameters
  sbj = "Only nw files updated. App update halted"
  msg = "Some tables of the state dataset were not updated. App datasets not copied"
}

## copy v2 files ----
if (nw_file_status == "OK" & st_file_status == "OK") {
  copyDirectory(here("..", "mobile-app", "data", "V2"), paste0(destination_dir,"data/V2"), overwrite = TRUE)

  ### set email status parameters
  sbj = "App network and state datasets copied successfully to folder"
  msg = "All good, relax!"

} else if (nw_file_status != "OK" & st_file_status != "OK") {
  ### set email status parameters
  sbj = "App datasets not copied - some tables not updated"
  msg = "Some tables of both nw and state datasets were not updated."

}

# file.copy(file.path(base_dir,list.files(base_dir)), destination_dir, overwrite = TRUE)
# copyDirectory(here::here("iframes"), paste0(destination_dir,"iframes"), overwrite = TRUE)
# copyDirectory(here::here("images"), paste0(destination_dir,"images"), overwrite = TRUE)
# copyDirectory(here::here("traffic"), paste0(destination_dir,"traffic"), overwrite = TRUE)


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
