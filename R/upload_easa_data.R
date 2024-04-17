# insert EASA data


library(here)

# pak::pkg_install("euctrl-pru/pockethostr")
library(pockethostr)
library(readxl)

source(here::here("R", "helpers.R"))

flts <- read_xlsx("data-raw/Exposure data - Airport mvts - FIR flights - 2024_03.xlsx", sheet = "STATE - Flight")
apts <- read_xlsx("data-rawExposure data - Airport mvts - FIR flights - 2024_03.xlsx", sheet = "AIRPORT - Movements")

username <- Sys.getenv("PH_AIU_PORTAL_USR")
password <- Sys.getenv("PH_AIU_PORTAL_PWD")

app_main <- "easa-operational-data"

# authenticate over main app
adm_main <- ph_authenticate_admin_username_password(
  app_main,
  "/api/admins/auth-with-password",
  username,
  password)


# ------ EASA data -----
##---------- State flights ----
collection <- "state_flights"

flts |>
  purrr::pwalk(.f = function(YEAR, MONTH, STATE_EASA, STATE_DB, UNIT_KIND, FLIGHT) {
    body <- list(YEAR       = YEAR,
                 MONTH      = MONTH,
                 STATE_EASA = STATE_EASA,
                 STATE_DB   = STATE_DB,
                 UNIT_KIND  = UNIT_KIND,
                 FLIGHT     = FLIGHT) |>
      purrr::list_transpose() |>
      magrittr::extract2(1)
    # dput(body)
    ph_create_record(
      app = app_main,
      api = "/api/collections",
      collection = collection,
      token = adm_main$token,
      body = body)
  })


##---------- Airport movements ----
collection <- "airport_movements"

apts |>
  purrr::pwalk(.f = function(YEAR, MONTH, AIRPORT_ICAO_CODE, FLTS_DEP, FLTS_ARR, FLTS_DEP_ARR, STATE) {
    body <- list(YEAR = YEAR,
                 MONTH = MONTH,
                 AIRPORT_ICAO_CODE = AIRPORT_ICAO_CODE,
                 FLTS_DEP = FLTS_DEP,
                 FLTS_ARR = FLTS_ARR,
                 FLTS_DEP_ARR = FLTS_DEP_ARR,
                 STATE = STATE) |>
      purrr::list_transpose() |>
      magrittr::extract2(1)
    # dput(body)
    ph_create_record(
      app = app_main,
      api = "/api/collections",
      collection = collection,
      token = adm_main$token,
      body = body)
  })
