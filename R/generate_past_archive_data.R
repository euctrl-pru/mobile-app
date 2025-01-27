library(here)
library(lubridate)
library(tidyverse)
library(purrr)
library(stringr)

source(here("..", "mobile-app", "R", "helpers.R"))
source(here("..", "mobile-app", "R", "params.R"))
source(here("..", "mobile-app", "R", "queries_st.R"))
source(here("..", "mobile-app", "R", "queries_ao.R"))
source(here("..", "mobile-app", "R", "queries_nw.R"))
if(exists("mydate") == FALSE) {mydate <- "2024-01-01"}
source(here("..", "mobile-app", "R", "queries_ap.R"))

test_archive_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Project/DDP/AIU app/data_archive'
# files_to_rename <- list.files(here(test_archive_dir, "ap"),
#                               pattern = "ap_ap_des_data_week_raw",
#                               full.names = TRUE)
# new_filenames <- gsub("ap_ap_des_data_week_raw", "ap_ap_des_data_y2d_raw", files_to_rename)
# file.rename(files_to_rename, new_filenames)

# set period
wef <- "2024-11-10"  #included in output
til <- "2025-01-20"  #included in output

myquery_string <- "query_ap_ms_data_y2d_raw" # set the name of the query function here
myarchivefile <- paste0(str_replace(myquery_string, "query", ""), ".csv")
stakeholder <- stringr::str_sub(myarchivefile, 2,
                                regexpr("_", substr(myarchivefile, 2, nchar(myarchivefile))))
myquery <- function(mydate_string) {
  get(myquery_string)(mydate_string)
}

cucu <- function(mydate_string) {
  mydate <- as.Date(mydate_string)
  mydate_prefix <-format(mydate, "%Y%m%d")
  #  myquery <- query_state_ao_day(mydate_string)
  df <- export_query(myquery(mydate_string)) %>%
    as_tibble() %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  write_csv(df,
            here(test_archive_dir, stakeholder, paste0(mydate_prefix, myarchivefile))
  )
}

seq(ymd(wef), ymd(til), by = "day") |>
  walk(.f = cucu)



