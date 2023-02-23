library(here)
library("R.utils")

##generate all files
source(here::here("R", "generate_json_files.R"))
source(here::here("R", "mob_ao_traffic_rank_day.Rmd"))
source(here::here("R", "mob_apt_traffic_rank_day.Rmd"))

### copy files to performance folder
base_dir <- here::here()
destination_dir <- '//ihx-vdm05/LIVE_var_www_Economics$/DailyBriefing/Prototype2/'


file.copy(file.path(base_dir,list.files(base_dir)), destination_dir, overwrite = TRUE)
copyDirectory(here::here("iframes"), paste0(destination_dir,"iframes"))
