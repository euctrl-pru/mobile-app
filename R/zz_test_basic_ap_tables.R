## libraries
library(tibble)
library(dplyr)
library(tidyr)
library(lubridate)
library(here)
library(RODBC)
library(arrow)

# DB params
usr <- Sys.getenv("PRU_DEV_USR")
pwd <- Sys.getenv("PRU_DEV_PWD")
dbn <- Sys.getenv("PRU_DEV_DBNAME")

source(here::here("..", "mobile-app", "R", "params.R"))
source(here::here("..", "mobile-app", "R", "helpers.R"))

query_from <- "TO_DATE ('01-01-2019', 'dd-mm-yyyy')"
source(here::here("..", "mobile-app", "R", "z_base_queries_ap.R"))

mydataframe <- "ap_ao_day_base"
myquery <- get(paste0(mydataframe, "_query"))
myarchivefile <- paste0(mydataframe, ".parquet")

df <- export_query(myquery) %>%
  as_tibble()

df %>% write_parquet(here::here(archive_dir_raw, myarchivefile))
