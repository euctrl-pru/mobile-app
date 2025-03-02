# libraries -----
library(here)
library(lubridate)
library(tidyverse)
library(purrr)
library(stringr)

# load query functions -----
source(here("..", "mobile-app", "R", "helpers.R"))
source(here("..", "mobile-app", "R", "params.R"))
source(here("..", "mobile-app", "R", "queries_st.R"))
source(here("..", "mobile-app", "R", "queries_ao.R"))
source(here("..", "mobile-app", "R", "queries_nw.R"))
if(exists("mydate") == FALSE) {mydate <- "2024-01-01"}
source(here("..", "mobile-app", "R", "queries_ap.R"))

# params -----
test_archive_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Project/DDP/AIU app/data_archive'
# files_to_rename <- list.files(here(test_archive_dir, "ap"),
#                               pattern = "ap_st_des_data_y2d_raw",
#                               full.names = FALSE)
# new_filenames <- gsub("ap_ap_des_data_week_raw", "ap_ap_des_data_y2d_raw", files_to_rename)
# file.rename(files_to_rename, new_filenames)

# set period
wef <- "2025-02-13"  #included in output
til <- "2025-02-28"  #included in output

# functions list -----
# get functions list for multiple queries
# Load script into an environment
get_functions_from_script <- function(script_path) {
  env <- new.env()
  sys.source(script_path, envir = env) # Load script
  funcs <- ls(env) # Get all objects
  funcs[sapply(funcs, function(f) is.function(get(f, env)))] # Filter functions
}

stakeholder <- 'ap' # set the 2 letter stakeholder to retrieve query list
script_path <- here("R", paste0("queries_", stakeholder, ".R"))
function_list <- get_functions_from_script(script_path)
#remove queries I don't need executed
function_list <- function_list[!sapply(function_list, function(x) x %in% c("query_ap_traffic",
                                                                           "query_ap_punct",

                                                                           "query_state_daio_raw",
                                                                           "query_state_dai_raw",
                                                                           "query_state_delay_raw",
                                                                           "query_state_delay_cause_raw"))]

# create archive -----
all_stk_queries <- TRUE
if (all_stk_queries) {
  myquery_string <- function_list
  } else {
  myquery_string <- "query_nw_apt_y2d_raw"
  }


generate_archive <- function (myquery_string) {
  myarchivefile <- paste0(str_replace(myquery_string, "query", ""), ".csv")
  stakeholder <- stringr::str_sub(myarchivefile, 2,
                                  regexpr("_", substr(myarchivefile, 2, nchar(myarchivefile))))
  # stakeholder <- "st"
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
}

walk(myquery_string, generate_archive)
