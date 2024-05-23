library(here)
library("R.utils")
library(sendmailR)

##generate all files
source(here("R", "generate_json_files.R"))
# rmarkdown::render(here::here("R", "mob_ao_traffic_rank_day.Rmd"), output_dir = here::here("iframes"))
# rmarkdown::render(here::here("R", "mob_apt_traffic_rank_day.Rmd"), output_dir = here::here("iframes"))

### copy files to performance folder
base_dir <- here()
destination_dir <- '//ihx-vdm05/LIVE_var_www_performance$/briefing/'


# file.copy(file.path(base_dir,list.files(base_dir)), destination_dir, overwrite = TRUE)
# copyDirectory(here::here("iframes"), paste0(destination_dir,"iframes"), overwrite = TRUE)
copyDirectory(here::here("data"), paste0(destination_dir,"data"), overwrite = TRUE)
# copyDirectory(here::here("images"), paste0(destination_dir,"images"), overwrite = TRUE)
# copyDirectory(here::here("traffic"), paste0(destination_dir,"traffic"), overwrite = TRUE)


### send email
sbj = "App dataset copied successfully to folder"
msg = "All good, relax!"

from    <- "oscar.alfaro@eurocontrol.int"
to      <- c("oscar.alfaro@eurocontrol.int"
             ,
             "quinten.goens@eurocontrol.int",
             "enrico.spinielli@eurocontrol.int",
             "denis.huet@eurocontrol.int"
)
# cc      <- c("enrico.spinielli@eurocontrol.int")
control <- list(smtpServer="mailservices.eurocontrol.int")

sendmail(from = from, to = to,
         # cc = cc,
         subject = sbj, msg = msg,
         control = control)
