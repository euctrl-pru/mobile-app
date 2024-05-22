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

if (FALSE) {
  # REMEMBER to set the "Start from:" as the root of this project
  file_name <- path_abs("R/update_pockethost_data.R", start = base_dir)
  if (file_exists(file_name)) {
    taskscheduler_create(
      taskname = "update API mobile app data",
      rscript = file_name,
      schedule = "DAILY",
      starttime = "08:30",
      startdate = format(Sys.Date() + 1, "%Y/%m/%d")
    )
  }
}



if (FALSE) {
  # manually add a new action for
  #   update API for network situation of AIU Portal

  file_name <- path_abs("R/update_aiu_portal_network_situation.R", start = base_dir)
  taskscheduler_create(
    taskname = "update API network situation",
    rscript = file_name,
    schedule = "DAILY",
    starttime = "08:30",
    startdate = format(Sys.Date() + 1, "%Y/%m/%d")
    # repeat every hour "/RI 60"
    # for a duration of 2 hours "/DU 02:00"
    # kill any pending task at the end of duration
    #schtasks_extra = "/RI 60 /DU 03:00 /K"
  )
}


if (FALSE) {
  # manually add a new action for
  #   update API for daily traffic

  file_name <- path_abs("R/update_daily_traffic.R", start = base_dir)
  taskscheduler_create(
    taskname = "update API network daily traffic",
    rscript = file_name,
    schedule = "DAILY",
    starttime = "08:30",
    startdate = format(Sys.Date() + 1, "%Y/%m/%d")
    # repeat every hour "/RI 60"
    # for a duration of 2 hours "/DU 02:00"
    # kill any pending task at the end of duration
    #schtasks_extra = "/RI 60 /DU 03:00 /K"
  )
}
