# functions for export query
source(here("..", "mobile-app", "R", "helpers.R"))

# parameters ----
if (!exists("nw_status")) {
  source(here("..", "mobile-app", "R", "params.R"))
}


# billing data ----
if (exists("nw_billed_per_cz") == FALSE) {
  nw_billed_per_cz <- get_billing_data()
}

# co2 data ----
if (exists("co2_data_raw") == FALSE) {
  co2_data_raw <- get_co2_data()
}

# punctuality data spain ----
if (exists("punct_data_spain_raw") == FALSE) {
  punct_data_spain_raw <- get_punct_data_spain()
}

# network punctuality data ----
query <- "
WITH
 DATA_DAY AS (
SELECT
              t.day_date
FROM pru_time_references t
WHERE
t.day_date >= to_date('01-01-2018','DD-MM-YYYY')
AND t.day_date < trunc(sysdate)
)

SELECT
    a.day_date as \"DATE\",
    DEP_PUNCTUALITY_PERCENTAGE,
    DEPARTURE_FLIGHTS,
    AVG_DEPARTURE_SCHEDULE_DELAY,
    ARR_PUNCTUALITY_PERCENTAGE,
    ARRIVAL_FLIGHTS,
    AVG_ARRIVAL_SCHEDULE_DELAY,
    MISSING_SCHEDULES_PERCENTAGE,
    DEP_PUNCTUAL_FLIGHTS,
    ARR_PUNCTUAL_FLIGHTS,
    DEP_SCHED_DELAY,
    ARR_SCHED_DELAY,
    MISSING_SCHED_FLIGHTS,
    DEP_SCHEDULE_FLIGHT,
    ARR_SCHEDULE_FLIGHT,
    DEP_FLIGHTS_NO_OVERFLIGHTS

FROM DATA_DAY a
left join LDW_VDM.VIEW_FAC_PUNCTUALITY_NW_DAY b on a.day_date = b.\"DATE\"
order by a.day_date
 "

nw_punct_data_raw <- export_query(query) %>%
  as_tibble() %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

