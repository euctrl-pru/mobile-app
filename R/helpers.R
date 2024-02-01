library(withr)
library(DBI)
library(tibble)
library(dplyr)

export_query <- function(query) {

  # NOTE: to be set before you create your ROracle connection!
  # See http://www.oralytics.com/2015/05/r-roracle-and-oracle-date-formats_27.html
  withr::local_envvar(c("TZ" = "UTC",
                        "ORA_SDTZ" = "UTC"))
  withr::local_namespace("ROracle")
  con <- withr::local_db_connection(
    DBI::dbConnect(
      DBI::dbDriver("Oracle"),
      usr, pwd,
      dbname = dbn,
      timezone = "UTC")
  )

  data <- DBI::dbSendQuery(con, query)
  # ~2.5 min for one day
  DBI::fetch(data, n = -1) %>%
    tibble::as_tibble()
}

