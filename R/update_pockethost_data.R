# insert latest entries in
library(here)

# pak::pkg_install("euctrl-pru/pockethostr")
library(pockethostr)

source(here::here("R", "helpers.R"))


username <- Sys.getenv("PH_AIU_PORTAL_USR")
password <- Sys.getenv("PH_AIU_PORTAL_PWD")

app_main <- "eurocontrol-data"
app_test <- "eurocontrol-data-test"

# authenticate over test app
adm_test <- ph_authenticate_admin_username_password(
  app_test,
  "/api/admins/auth-with-password",
  username,
  password)

# authenticate over main app
adm_main <- ph_authenticate_admin_username_password(
  app_main,
  "/api/admins/auth-with-password",
  username,
  password)


#---------- Network traffic ----
collection <- "nw_traffic"

nw_traffic_latest <- network_traffic_latest()

# TODO: cope with
# * update of existing entry
# * need to fill holes, i.e. missing days
# * errors

ph_create_record(
  app = app_test,
  api = "/api/collections",
  collection = collection,
  token = adm_test$token,
  body = nw_traffic_latest)

ph_create_record(
  app = app_main,
  api = "/api/collections",
  collection = collection,
  token = adm_main$token,
  body = nw_traffic_latest)

## ------ traffic init -----
# for (d in seq(from = ymd("2024-01-02"), to = ymd("2024-02-01"), by = "1 day")) {
#   nw_traffic_latest <- network_traffic_latest(as_date(d))
#   ph_create_record(
#     app = app_test,
#     api = "/api/collections",
#     collection = collection,
#     token = adm_test$token,
#     body = nw_traffic_latest)
#
#   ph_create_record(
#     app = app_main,
#     api = "/api/collections",
#     collection = collection,
#     token = adm_main$token,
#     body = nw_traffic_latest)
# }





#---------- Network delay ----
collection <- "nw_delay"

nw_delay_latest_full <- network_delay_latest()

nw_delay_latest <- nw_delay_latest_full |>
  magrittr::extract(
    c(
      "FLIGHT_DATE",
      "DAY_DLY",
      "DAY_DIFF_PREV_YEAR_PERC",
      "DAY_DLY_DIFF_2019_PERC",
      "DAY_DLY_FLT",
      "DAY_DLY_FLT_DIF_PY_PERC",
      "DAY_DLY_FLT_DIF_2019_PERC",
      "AVG_ROLLING_WEEK",
      "DIF_WEEK_PREV_YEAR_PERC",
      "DIF_ROLLING_WEEK_2019_PERC",
      "RWEEK_DLY_FLT",
      "RWEEK_DLY_FLT_DIF_PY_PERC",
      "RWEEK_DLY_FLT_DIF_2019_PERC",
      "Y2D_AVG_DLY_YEAR",
      "Y2D_DIFF_PREV_YEAR_PERC",
      "Y2D_DIFF_2019_PERC",
      "Y2D_DLY_FLT",
      "Y2D_DLY_FLT_DIF_PY_PERC",
      "Y2D_DLY_FLT_DIF_2019_PERC",
      NULL
    )
  )

ph_create_record(
  app = app_test,
  api = "/api/collections",
  collection = collection,
  token = adm_test$token,
  body = nw_delay_latest)

ph_create_record(
  app = app_main,
  api = "/api/collections",
  collection = collection,
  token = adm_main$token,
  body = nw_delay_latest)



#---------- Network billed ----
collection <- "nw_billed"

nw_billed_latest <- network_billed_latest()

ll <- ph_list_records(
  app = app_main,
  api = "/api/collections/",
  collection = collection,
  perPage = 1,
  page = 1,
  sort = "-BILLING_DATE",
  skipTotal = 1) |>
  pull("BILLING_DATE") |>
  as_date()

if (nw_billed_latest$BILLING_DATE > ll) {
  ph_create_record(
    app = app_main,
    api = "/api/collections",
    collection = collection,
    token = adm_main$token,
    body = nw_billed_latest)
}

ll <- ph_list_records(
  app = app_test,
  api = "/api/collections/",
  collection = collection,
  perPage = 1,
  page = 1,
  sort = "-BILLING_DATE",
  skipTotal = 1) |>
  pull("BILLING_DATE") |>
  as_date()

if (nw_billed_latest$BILLING_DATE > ll) {
  ph_create_record(
    app = app_test,
    api = "/api/collections",
    collection = collection,
    token = adm_test$token,
    body = nw_billed_latest)
}


#---------- Network punctuality ----
collection <- "nw_punct"

nw_punct_latest <- network_punctuality_latest()

ph_create_record(
  app = app_test,
  api = "/api/collections",
  collection = collection,
  token = adm_test$token,
  body = nw_punct_latest)

ph_create_record(
  app = app_main,
  api = "/api/collections",
  collection = collection,
  token = adm_main$token,
  body = nw_punct_latest)



#------ create API initiail records -----
# for (d in seq(from = ymd("2024-01-02"), to = ymd("2024-01-31"), by = "1 day")) {
#   nw_punct_latest <- network_punctuality_latest(as_date(d))
#   ph_create_record(
#     app = app_test,
#     api = "/api/collections",
#     collection = collection,
#     token = adm_test$token,
#     body = nw_punct_latest)
#
#   ph_create_record(
#     app = app_main,
#     api = "/api/collections",
#     collection = collection,
#     token = adm_main$token,
#     body = nw_punct_latest)
# }
