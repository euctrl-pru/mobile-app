# extract daily network traffic figures

library(eurocontrol)
library(dplyr)

query <- "
  SELECT
    A_FIRST_ENTRY_TIME_DATE FLIGHT_DATE ,
    SUM(NVL(A.ALL_TRAFFIC, 0)) DAY_TFC
  FROM
    V_AIU_AGG_GLOBAL_DAILY_COUNTS A
  WHERE
    A.A_FIRST_ENTRY_TIME_DATE  >= TO_DATE('01-01-2019','DD-MM-YYYY')
    AND A.A_FIRST_ENTRY_TIME_DATE  < TRUNC (SYSDATE)
  GROUP BY
    A.A_FIRST_ENTRY_TIME_DATE
--  SORT BY ASC(FLIGHT_DATE)
"

conn <- db_connection(schema = "PRU_DEV")
nw_traffic <- tbl(conn, sql(query))

dd <- nw_traffic |>
  collect() |>
  dplyr::mutate(FLIGHT_DATE = as_date(FLIGHT_DATE, tz = "UTC")) |>
  arrange(FLIGHT_DATE) |>
  rename(date = FLIGHT_DATE, flights = DAY_TFC)


DBI::dbDisconnect(conn)



library(pockethostr)
source(here::here("R", "helpers.R"))


username <- Sys.getenv("PH_AIU_PORTAL_USR")
password <- Sys.getenv("PH_AIU_PORTAL_PWD")

app_main <- "aiu-portal"
# authenticate over main app
adm_main <- ph_authenticate_admin_username_password(
  app_main,
  "/api/admins/auth-with-password",
  username,
  password)

collection <- "network_traffic"

dd |>
  purrr::pwalk(.f = function(date, flights) {
    body <- list(date    = date,
                 flights = flights) |>
      purrr::list_transpose() |>
      magrittr::extract2(1)
    ph_create_record(
      app = app_main,
      api = "/api/collections",
      collection = collection,
      token = adm_main$token,
      body = body)
  })

