# library(duckdb)
library(arrow)
library(duckplyr)
library(lubridate)
library(here)
library(RODBC)

source(here::here("..", "mobile-app", "R", "helpers.R"))
source(here::here("..", "mobile-app", "R", "params.R"))

query_from <- "TO_DATE('2018-12-24', 'yyyy-mm-dd')"
source(here::here("..", "mobile-app", "R", "base_queries.R"))

# update base table ----
mydataframe <- "ap_traffic_delay"

myarchivefile <- paste0(mydataframe, "_day_base.parquet")
query_y2d <- get(paste0(mydataframe, "_day_base_query"))
  
# run query
df <- export_query(query_y2d) 
  
df %>% write_parquet(here(archive_dir_raw, myarchivefile))
  
