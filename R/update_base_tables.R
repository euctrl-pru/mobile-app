# library(duckdb)
library(arrow)
library(duckplyr)
library(lubridate)
library(here)
library(RODBC)
library(sendmailR)

source(here::here("..", "mobile-app", "R", "helpers.R"))
source(here::here("..", "mobile-app", "R", "params.R"))

# check if base aiu_flt table is updated
check_synthesis <- read_xlsx(
  path  = here(nw_base_dir, "000_Initial_check.xlsx"),
  sheet = "Sheet1",
  range = cell_limits(c(6, 7), c(7, 7))) %>%
  pull()

if (check_synthesis == 0) {
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
  sbj = "Synthesis 0 flights detected - update halted"
  msg = "Check Synthesis update"
  
  sendmail(from = from, to = to,
           # cc = cc,
           subject = sbj, msg = msg,
           control = control)
  
  stop()
}



query_from <- "TRUNC (SYSDATE) - 7"
source(here::here("..", "mobile-app", "R", "z_base_queries_ap.R"))

# update base table ----
mydataframes <- c(
  "ap_ao",
  "ap_st_des",
  "ap_ap_des",
  "ap_ms",
  
  "ao_st_des",
  NULL
)

update_base_tables <- function(mydataframe) {
# mydataframe <-   "ap_st_des_day_base"
  myarchivefile <- paste0(mydataframe, "_day_base.parquet")
  mybackupfile <- paste0(mydataframe, "_day_base_backup.parquet")
  query_7d <- get(paste0(mydataframe, "_day_base_query"))
  
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