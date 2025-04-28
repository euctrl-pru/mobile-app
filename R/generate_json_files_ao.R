## libraries
library(data.table)
library(fs)
library(tibble)
library(dplyr)
library(tidyr)
library(stringr)
library(readxl)
library(DBI)
library(ROracle)
library(lubridate)
library(zoo)
library(jsonlite)
library(here)
library(RODBC)

# functions ----
source(here::here("..", "mobile-app", "R", "helpers.R"))

# Parameters ----
source(here("..", "mobile-app", "R", "params.R"))

if (exists("ansp_list") == FALSE) {
  ansp_list <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(an_base_file),
      start = an_base_dir),
    sheet = "lists",
    range = cell_limits(c(1, 1), c(NA, NA))) %>%
    as_tibble()
}

# archive mode for past dates
if (exists("archive_mode") == FALSE) {archive_mode <- FALSE}
if (exists("data_day_date") == FALSE) {
  data_day_date <- lubridate::today(tzone = "") +  days(-1)
}

data_day_text <- data_day_date %>% format("%Y%m%d")
data_day_year <- as.numeric(format(data_day_date,'%Y'))

an_json_app <-""

# ____________________________________________________________________________________________
#
#    ANSP landing page -----
#
# ____________________________________________________________________________________________

