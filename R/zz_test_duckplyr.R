# library(duckdb)
library(arrow)
library(duckplyr)
library(lubridate)
library(here)
library(RODBC)

source(here::here("..", "mobile-app", "R", "helpers.R"))
source(here::here("..", "mobile-app", "R", "params.R"))

query_from <- "TRUNC (SYSDATE) - 7"
source(here::here("..", "mobile-app", "R", "z_base_queries_ap.R"))

# update base table ----
mydataframes <- c(
  "ap_ao_day_base",
  "ap_st_des_day_base",
  "ap_ap_des_day_base",
  "ap_ms_day_base",
  NULL
)

update_base_tables <- function(mydataframe) {
# mydataframe <-   "ap_st_des_day_base"
  myarchivefile <- paste0(mydataframe, ".parquet")
  mybackupfile <- paste0(mydataframe, "_backup.parquet")
  query_7d <- get(paste0(mydataframe, "_query"))
  
  # import data
  df_tmp <- read_parquet_duckdb(here(archive_dir_raw, myarchivefile))
  
  # define today
  today_date <- today(tzone = "")

  # materialise table
  df <- df_tmp %>% collect() 
  
  # create backup
  df %>% write_parquet(here::here(archive_dir_raw, "backup", mybackupfile))
  
  
  # filter out last 7 days
  df_filtered <- df %>% 
    filter(!(ENTRY_DATE %in% c(seq.Date(today_date + days(-7), today_date))))
  
  # DB params
  usr <- Sys.getenv("PRU_DEV_USR")
  pwd <- Sys.getenv("PRU_DEV_PWD")
  dbn <- Sys.getenv("PRU_DEV_DBNAME")
  
  # run query
  df_lastweek <- export_query(query_7d) 
  
  # join filtered table with last 7 days calculated
  df_updated <- rbind(df_filtered, df_lastweek)
  
  # write new updated table
  df_updated %>% write_parquet(here(archive_dir_raw, myarchivefile))
 
  print(mydataframe) 
}

purrr::walk(mydataframes, update_base_tables)