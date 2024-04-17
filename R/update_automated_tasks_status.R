# insert latest entries in Automated Tasks Status APIs
library(here)

# pak::pkg_install("euctrl-pru/pockethostr")
library(pockethostr)

source(here::here("R", "helpers.R"))


username <- Sys.getenv("PH_AIU_PORTAL_USR")
password <- Sys.getenv("PH_AIU_PORTAL_PWD")

app_main <- "aiu-data-status"
collection <- "task_check"

# authenticate over main app
adm_main <- ph_authenticate_admin_username_password(
  app_main,
  "/api/admins/auth-with-password",
  username,
  password)

#---------- Automated Tasks Status: check -----------
# see grounded_aircraft/R/check_updates_spi.R
latest <- tasks_status_latest()


#---------- Automated Tasks Status: write API ------------
ph_create_record(
  app = app_main,
  api = "/api/collections",
  collection = collection,
  token = adm_main$token,
  body = latest)

