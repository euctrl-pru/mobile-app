library(here)
library(lubridate)
library(tidyverse)
library(purrr)

source(here("..", "mobile-app", "R", "helpers.R"))
source(here("..", "mobile-app", "R", "state_queries.R"))
source(here("..", "mobile-app", "R", "ao_queries.R"))
source(here("..", "mobile-app", "R", "nw_queries.R"))

test_archive_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Project/DDP/AIU app/data_archive'
wef <- "2024-01-01"
til <- "2024-10-20"  #included in output
# mydate_string <- "2024-10-18"
myarchivefile <- "_nw_acc_delay_y2d_raw.csv" # set the archive file name here
myquery <- function(mydate_string) {
  query_nw_acc_delay_y2d_raw(mydate_string)   # change the query here
}

cucu <- function(mydate_string) {
  mydate <- as.Date(mydate_string)
  mydate_prefix <-format(mydate, "%Y%m%d")
#  myquery <- query_state_ao_day(mydate_string)
  df <- export_query(myquery(mydate_string)) %>%
    as_tibble() %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  write_csv(df,
            here(test_archive_dir, "nw", paste0(mydate_prefix, myarchivefile))
            )
}

seq(ymd(wef), ymd(til), by = "day") |>
  walk(.f = cucu)



