# library(duckdb)
library(arrow)
library(duckplyr)
library(lubridate)
library(here)
library(RODBC)

source(here::here("..", "mobile-app", "R", "helpers.R"))
source(here::here("..", "mobile-app", "R", "params.R"))
source(here::here("..", "mobile-app", "R", "duckdb_functions.R"))

query_from <- "TO_DATE('2018-12-24', 'yyyy-mm-dd')"
source(here::here("..", "mobile-app", "R", "base_queries.R"))

# update base table ----
mydataframe <- "sp_traffic_delay_new"

myarchivefile <- paste0(mydataframe, "_day_base.parquet")
query_y2d <- get(paste0(mydataframe, "_day_base_query"))
  
# run query
df <- export_query(query_y2d) 

# df <-  read_parquet(here(archive_dir_raw, "ao_ap_arr_delay_new_day_base.parquet"))

df <- df %>%
mutate(YEAR = as.integer(YEAR))
 
# df %>%  write_parquet(here(archive_dir_raw, myarchivefile))

con = DBI::dbConnect(duckdb::duckdb())
myyears <- c(2018:2026)

# write new updated table
save_partitions_single_copy (con = con, 
                             df = df, 
                             mydataframe,
                             years = myyears)

# close connection
DBI::dbDisconnect(con, shutdown = TRUE)


