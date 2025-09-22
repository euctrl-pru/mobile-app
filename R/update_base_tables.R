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
source(here::here("..", "mobile-app", "R", "base_queries.R"))

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
  NULL
)

update_base_tables <- function(mydataframe) {
# mydataframe <-   "ap_traffic_delay"
  myarchivefile <- paste0(mydataframe, "_day_base.parquet")
  mybackupfile <- paste0(mydataframe, "_day_base_backup.parquet")
  query_7d <- get(paste0(mydataframe, "_day_base_query"))
  
  # create backup
  file.copy(from = here(archive_dir_raw, myarchivefile), 
            to = here(archive_dir_raw, "backup", mybackupfile),
            overwrite = TRUE,
            copy.mode = TRUE,
            recursive = FALSE)
  
  # import data
  df <- read_parquet_duckdb(here(archive_dir_raw, myarchivefile))
  
  # define today
  today_date <- today(tzone = "")

  # materialise table
  # df <- df_tmp %>% collect() 
  
  # create backup
  # df %>% write_parquet(here::here(archive_dir_raw, "backup", mybackupfile))
  
  
  # filter out last 7 days
  if ("ENTRY_DATE" %in% names(df)) {
    df_filtered <- df %>% 
      filter(!(ENTRY_DATE %in% c(seq.Date(today_date + days(-7), today_date))))
  } else if ("FLIGHT_DATE" %in% names(df)) {
    df_filtered <- df %>% 
      filter(!(FLIGHT_DATE %in% c(seq.Date(today_date + days(-7), today_date))))
  } else if ("ARR_DATE" %in% names(df)) {
    df_filtered <- df %>% 
      filter(!(ARR_DATE %in% c(seq.Date(today_date + days(-7), today_date))))
  }
  
  
  # DB params
  usr <- Sys.getenv("PRU_DEV_USR")
  pwd <- Sys.getenv("PRU_DEV_PWD")
  dbn <- Sys.getenv("PRU_DEV_DBNAME")
  
  # run query
  df_lastweek <- export_query(query_7d) 
  
  # join filtered table with last 7 days calculated
  df_updated <- rbind(collect(df_filtered), df_lastweek)
  
  # write new updated table
  df_updated %>% write_parquet(here(archive_dir_raw, myarchivefile))
 
  print(paste(format(now(), "%H:%M:%S"), mydataframe, "base table updated")) 
}

purrr::walk(mydataframes, update_base_tables)

# update base sp table ----
# the sp table is built with the last days embedded in the query
mydataframe <- "sp_traffic_delay"

myarchivefile <- paste0(mydataframe, "_day_base.parquet")
mybackupfile <- paste0(mydataframe, "_day_base_backup.parquet")
query_y2d <- get(paste0(mydataframe, "_day_base_query"))

file.copy(from = here(archive_dir_raw, myarchivefile), 
          to = here(archive_dir_raw, "backup", mybackupfile),
          overwrite = TRUE,
          copy.mode = TRUE,
          recursive = FALSE)

# run query
df <- export_query(query_y2d) 

df %>%  write_parquet(here(archive_dir_raw, myarchivefile))
print(paste(format(now(), "%H:%M:%S"), mydataframe, "base table updated")) 
print(" ") 
