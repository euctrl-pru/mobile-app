library(here)
library("R.utils")

##generate all files
source(here::here("R", "generate_json_files.R"))
rmarkdown::render(here::here("R", "mob_ao_traffic_rank_day.Rmd"), output_dir = here::here("iframes"))
rmarkdown::render(here::here("R", "mob_apt_traffic_rank_day.Rmd"), output_dir = here::here("iframes"))

### copy files to performance folder
base_dir <- here::here()
destination_dir <- '//ihx-vdm05/LIVE_var_www_performance$/daily-data/'


file.copy(file.path(base_dir,list.files(base_dir)), destination_dir, overwrite = TRUE)
copyDirectory(here::here("iframes"), paste0(destination_dir,"iframes"), overwrite = TRUE)
copyDirectory(here::here("data"), paste0(destination_dir,"data"), overwrite = TRUE)
copyDirectory(here::here("images"), paste0(destination_dir,"images"), overwrite = TRUE)