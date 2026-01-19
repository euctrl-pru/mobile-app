# library(duckdb)
library(arrow)
library(duckplyr)
library(lubridate)
library(here)
library(RODBC)
library(sendmailR)
library(DBI)
library(fs)
library(glue)
library(dplyr)
library(stringr)
library(readxl)
library(tidyverse)
library(purrr)

source(here::here("..", "mobile-app", "R", "helpers.R"), local = TRUE)
source(here::here("..", "mobile-app", "R", "params.R"), local = TRUE)
source(here::here("..", "mobile-app", "R", "duckdb_functions.R"), local = TRUE)

# check if DAG procedure run successfully today
  query_log <- "select * from AIU_RUN_LOG"
  data_log <- export_query(query_log) %>%
    mutate(LOG_DAY = floor_date(LOG_TIME, "day"))
  
  # check if the procedure run today
  last_run_day <- max(data_log$LOG_DAY)
  
  # check last successful run
  last_run_time_ok <- data_log %>%
    filter(IS_FINISHED == 1) %>%
    summarise(max(LOG_TIME)) %>%
    pull()
    
  # setup email message and subject depending on case
  if (last_run_day == today()) {
    if (floor_date(last_run_time_ok, "day",) == today()) {
    check_log <- 1
      
    } else {
      check_log <- 0
      
      sbj = "AIU OPS tables procedure unsuccessfully run today!!"
      msg = paste0("Last time the procedure was run successfully was ", last_run_time_ok)
    }
  } else {
    check_log <- 0
    
    sbj = "AIU OPS tables procedure did not run today!!"
    msg = paste0("Last time the procedure was run successfully was ", last_run_time_ok)
  }
  
# check if v_aiu_agg_global_daily_counts table is updated
  query_check <-"SELECT 
                  a_first_entry_time_date FLIGHT_DATE , 
                  SUM(nvl(a.all_traffic,0)) TOT_TFC
             FROM  prudev.v_aiu_agg_global_daily_counts a
             WHERE a.a_first_entry_time_date  > TRUNC (SYSDATE) -10
             GROUP BY  a.a_first_entry_time_date"
  
  synthesis_last_day <- export_query(query_check)
  
  check_day <- max(synthesis_last_day$FLIGHT_DATE, na.rm = TRUE) == today() - days(1)
  check_flt <- synthesis_last_day %>% filter (FLIGHT_DATE == max(FLIGHT_DATE, na.rm = TRUE)) %>%
    select(TOT_TFC) %>% pull() !=0

  sbj = "Zero flights detected in Synthesis tables"
  msg = "Check Synthesis update"
  
check_synthesis <- check_day + check_flt + check_log

if (check_synthesis != 3) {
  from    <- "oscar.alfaro@eurocontrol.int"
  to      <- c("oscar.alfaro@eurocontrol.int"
               # ,
               # "quinten.goens@eurocontrol.int",
               # "enrico.spinielli@eurocontrol.int",
               # "delia.budulan@eurocontrol.int",
               # "nora.cashman@eurocontrol.int"
               # ,  "denis.huet@eurocontrol.int"
  )
  # cc      <- c("enrico.spinielli@eurocontrol.int")
  control <- list(smtpServer="mailservices.eurocontrol.int")
  sendmail(from = from, to = to,
           # cc = cc,
           subject = sbj, msg = msg,
           control = control)
  
  stop()
}


days_back <- 7
query_from <- glue("TRUNC (SYSDATE) - {days_back}")
source(here::here("..", "mobile-app", "R", "base_queries.R"), local = TRUE)

# update base table ----
mydataframes <- c(
  "nw_delay_cause",

  "ap_traffic_delay",
  "ap_ao",
  "ap_st_des",
  "ap_ap_des",
  "ap_ms",

  "ao_traffic_delay",
  "ao_st_des",
  "ao_ap_dep",
  "ao_ap_pair",
  "ao_ap_arr_delay",

  "st_dai",
  "st_ao",
  "st_st",
  "st_ap",

  "sp_traffic_delay",
  NULL
)

update_base_tables <- function(mydataframe) {
# mydataframe <-   "sp_traffic_delay_new"
  myarchivefile <- paste0(mydataframe, "_day_base.parquet")
  mybackupfile <- paste0(mydataframe, "_day_base_backup.parquet")
  query_7d <- get(paste0(mydataframe, "_day_base_query"))
  
  # create backup
  dest_parent <- here(archive_dir_raw, "backup", mydataframe)
  if (!dir.exists(dest_parent)) dir.create(dest_parent, recursive = TRUE)

  file.copy(from = here(archive_dir_raw, mydataframe), 
            to = dest_parent,
            recursive = TRUE, overwrite = TRUE, copy.mode = TRUE)
  
  # define today
  today_date <- today(tzone = "")
  yesterday_date <- today_date - days(1)
  if (as.integer(format(today_date, '%m%d')) < 115) {
    myyears <- c(year(yesterday_date), year(yesterday_date)-1)
    
  } else {
    myyears <- c(year(yesterday_date))
  }
  # myyears <- c(2025)
  
  # import data
  # df <- read_parquet_duckdb(here(archive_dir_raw, myarchivefile))

  # create connection
  con = DBI::dbConnect(duckdb::duckdb())
  
  # ingest only partitions defined in myyears
  df <- read_selected_years_duck_tbl(con = con,
                                     mydataframe = mydataframe,
                                     years=myyears,
                                     subpattern = "YEAR=*/data_*.parquet")
# df_mod <- df %>%
#   compute(prudence = "lavish") %>%
#   mutate(YEAR = as.integer(year(ENTRY_DATE))) %>%
#   relocate(YEAR, .before = everything()) %>%
#   # rename(BK_AP_ID = DEP_BK_AP_ID) %>%
#   arrange(ISO_CT_CODE, ENTRY_DATE, desc(FLIGHT))
# 
# myyears <- c(2019:2025)

  # filter out last 7 days
  if ("ENTRY_DATE" %in% colnames(df)) {
    df_filtered <- df %>% 
      collect() %>% 
      filter(!(ENTRY_DATE %in% c(seq.Date(today_date - days(days_back), today_date))))
    
  } else if ("FLIGHT_DATE" %in% colnames(df)) {
    df_filtered <- df %>% 
      collect() %>% 
      filter(!(FLIGHT_DATE %in% c(seq.Date(today_date - days(days_back), today_date))))
  } else if ("ARR_DATE" %in% colnames(df)) {
    df_filtered <- df %>% 
      collect() %>% 
      filter(!(ARR_DATE %in% c(seq.Date(today_date - days(days_back), today_date))))
  }
  
  # DB params
  usr <- Sys.getenv("PRU_DEV_USR")
  pwd <- Sys.getenv("PRU_DEV_PWD")
  dbn <- Sys.getenv("PRU_DEV_DBNAME")
  
  # run query
  df_lastweek <- export_query(query_7d) %>% 
    mutate(YEAR = as.integer(YEAR))
  
  if (mydataframe  == "sp_traffic_delay") {
    # the sp query runs the full period
    df_lastweek <- df_lastweek %>% 
      filter(ENTRY_DATE >= today_date - days(days_back))
  }
  
  # join filtered table with last 7 days calculated
  df_updated <- rbind(df_filtered, df_lastweek) %>% 
    mutate(YEAR = as.integer(YEAR))
  
  # write new updated table
  save_partitions_single_copy (con = con, 
                               df = df_updated, 
                               mydataframe,
                               years = myyears)
  
  # close connection
  DBI::dbDisconnect(con, shutdown = TRUE)
  
#   con <- DBI::dbConnect(duckdb::duckdb())
#   DBI::dbWriteTable(con, "t", df_year, overwrite = TRUE)
#   DBI::dbExecute(con, glue::glue("
#   COPY (SELECT * FROM t)
#   TO '{normalizePath(archive_partition_dir, winslash = '/')}'
#   (FORMAT PARQUET, PARTITION_BY (YEAR), OVERWRITE_OR_IGNORE TRUE);
# "))
#   DBI::dbDisconnect(con, shutdown = TRUE)
#   # Produces out_dir/YEAR=2019/part-*.parquet etc.
  
  
  # # write new updated table
  # df_updated %>% write_parquet(here(archive_dir_raw, mydataframe, "YEAR=2025", "data_0.parquet"))
 
  print(paste(format(now(), "%H:%M:%S"), mydataframe, "base table updated")) 
}

purrr::walk(mydataframes, update_base_tables)

