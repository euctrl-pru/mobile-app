# functions for export query
source(here("..", "mobile-app", "R", "helpers.R"))

# parameters ----
source(here("..", "mobile-app", "R", "params.R"))

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

# network traffic data ---
nw_traffic_data <- read_xlsx(
  path = fs::path_abs(
    str_glue(nw_base_file),
    start = nw_base_dir
  ),
  sheet = "NM_Daily_Traffic_All",
  range = cell_limits(c(2, 1), c(NA, 39))
) %>%
  as_tibble() %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

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

# dimensions ----
state_iso <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(st_base_file),
    start = st_base_dir),
  sheet = "lists",
  range = cell_limits(c(2, 2), c(NA, 3))) %>%
  as_tibble()

state_crco <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(st_base_file),
    start = st_base_dir),
  sheet = "lists",
  range = cell_limits(c(2, 6), c(NA, 8))) %>%
  as_tibble()

state_daio <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(st_base_file),
    start = st_base_dir),
  sheet = "lists",
  range = cell_limits(c(2, 11), c(NA, 13))) %>%
  as_tibble()

state_co2 <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(st_base_file),
    start = st_base_dir),
  sheet = "lists",
  range = cell_limits(c(2, 16), c(NA, 17))) %>%
  as_tibble()

acc <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(nw_base_file),
    start = nw_base_dir),
  sheet = "ACC_names",
  range = cell_limits(c(2, 3), c(NA, NA))) %>%
  as_tibble()

query <- "select * from PRU_AIRPORT"

airport <- export_query(query) %>%
  as_tibble() %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
  select(ICAO_CODE, ISO_COUNTRY_CODE) %>%
  #in case we need to separate spain from canarias
  # mutate(ISO_COUNTRY_CODE = if_else(substr(ICAO_CODE,1,2) == "GC",
  #                                   "IC", ISO_COUNTRY_CODE)) %>%
  rename(iso_2letter = ISO_COUNTRY_CODE)

ao_grp_icao_full <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(ao_base_file),
    start = ao_base_dir),
  sheet = "lists",
  range = cell_limits(c(1, 1), c(NA, NA))) %>%
  as_tibble()

ao_grp_icao <- ao_grp_icao_full %>%
  select('AO_GRP_CODE', 'AO_GRP_NAME')

apt_icao <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(ap_base_file),
    start = ap_base_dir),
  sheet = "lists",
  range = cell_limits(c(1, 1), c(NA, NA))) %>%
  as_tibble() %>%
  janitor::clean_names()
