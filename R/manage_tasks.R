library(taskscheduleR)
library(stringr)
library(fs)

# Sys.info()[["user"]]
base_dir <- "C:/Users/spi/dev/repos/mobile-app/"

if (FALSE) {
  # REMEMBER to set the "Start from:" as the root of this project
  file_name <- path_abs("R/copy_to_production.R", start = base_dir)
  if (file_exists(file_name)) {
    taskscheduler_create(
      taskname = "generate data for mobile app",
      rscript = file_name,
      schedule = "DAILY",
      starttime = "08:30",
      startdate = format(Sys.Date() + 1, "%Y/%m/%d")
    )
  }
}

