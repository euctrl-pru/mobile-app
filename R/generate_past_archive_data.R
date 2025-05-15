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
wef <- "2024-12-31"  #included in output
til <- "2024-12-31"  #included in output

# functions list -----
# get functions list for multiple queries
# Load script into an environment
get_functions_from_script <- function(script_path) {
  env <- new.env()
  sys.source(script_path, envir = env) # Load script
  funcs <- ls(env) # Get all objects
  funcs[sapply(funcs, function(f) is.function(get(f, env)))] # Filter functions
}

stakeholder <- 'nw' # set the 2 letter stakeholder to retrieve query list
script_path <- here("R", paste0("queries_", stakeholder, ".R"))
function_list_full <- get_functions_from_script(script_path)
# these queries only need to be executed if the date is 31 december
function_list_exceptions <- c("query_ao_traffic_delay_raw",

                             "query_ap_traffic",
                             "query_ap_punct",
                             ##temp exclusion
                             # 'query_ap_ms_data_y2d_raw', 'query_ap_ms_data_week_raw', 'query_ap_ms_data_day_raw',
                             # 'query_ap_ao_data_y2d_raw', 'query_ap_ao_data_week_raw', 'query_ap_ao_data_day_raw',
                             # 'query_ap_ap_des_data_y2d_raw', 'query_ap_ap_des_data_week_raw', 'query_ap_ap_des_data_day_raw',

                             "query_state_daio_raw",
                             "query_state_dai_raw",
                             "query_state_delay_raw",
                             "query_state_delay_cause_raw")

#remove queries I only need once a year
function_list <- function_list_full[!sapply(function_list_full, function(x) x %in% function_list_exceptions)]

# create archive -----
# set to true to execute all queries for the stakeholder
# set to false to execute only the query below regardless of the stakeholders selected
all_stk_queries <- FALSE
if (all_stk_queries) {
  myquery_string <- function_list
  myquery_string_full <- function_list_full
  } else {
  myquery_string <- "query_nw_delay_data"
  myquery_string_full <- myquery_string
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
    myschema <- if_else(stakeholder == "ao" |
                          stakeholder == "ap", "PRU_READ", "PRU_DEV")

    df <- export_query(myquery(mydate_string), schema=myschema) %>%
      as_tibble() %>%
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    write_csv(df,
              here(test_archive_dir, stakeholder, paste0(mydate_prefix, myarchivefile))
    )

    print(paste0(myquery_string, "-", mydate))
  }

    seq(ymd(til), ymd(wef), by = "-1 day") |>
    walk(.f = cucu)
}

walk(myquery_string, generate_archive)
