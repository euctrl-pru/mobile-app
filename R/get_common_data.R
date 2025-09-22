# functions for export query
source(here("..", "mobile-app", "R", "helpers.R"))

# parameters ----
source(here("..", "mobile-app", "R", "params.R"))

# dimension queries ----
source(here("..", "mobile-app", "R", "dimension_queries.R"))


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

# dimensions ----
state_iso <-  read_xlsx(
  here("stakeholder_lists.xlsx"),
  sheet = "state_lists",
  range = cell_limits(c(2, 2), c(NA, 3))) %>%
  as_tibble()

state_iso_icao <-  read_xlsx(
  here("stakeholder_lists.xlsx"),
  sheet = "state_lists",
  range = cell_limits(c(2, 24), c(NA, 26))) %>%
  as_tibble()

state_crco <-  read_xlsx(
  here("stakeholder_lists.xlsx"),
  sheet = "state_lists",
  range = cell_limits(c(2, 6), c(NA, 8))) %>%
  as_tibble()

state_daio <-  read_xlsx(
  here("stakeholder_lists.xlsx"),
  sheet = "state_lists",
  range = cell_limits(c(2, 11), c(NA, 13))) %>%
  as_tibble()

state_co2 <-  read_xlsx(
  here("stakeholder_lists.xlsx"),
  sheet = "state_lists",
  range = cell_limits(c(2, 16), c(NA, 17))) %>%
  as_tibble()

state_statfor <-  read_xlsx(
  here("stakeholder_lists.xlsx"),
  sheet = "state_lists",
  range = cell_limits(c(2, 20), c(NA, 21))) %>%
  as_tibble()

acc <-  read_xlsx(
  here("stakeholder_lists.xlsx"),
  sheet = "acc_lists",
  range = cell_limits(c(1, 1), c(NA, NA))) %>%
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

ao_grp_icao_full <-  export_query(list_ao_grp_query)

ao_grp_icao <- ao_grp_icao_full %>%
  select('AO_GRP_CODE', 'AO_GRP_NAME')

query <- "SELECT
arp_code AS apt_icao_code,
arp_name AS apt_name,
flag_top_apt,
latitude,
longitude

FROM pruprod.v_aiu_app_dim_airport a
INNER JOIN (
  SELECT ec_ap_code, latitude, longitude
  FROM (
    SELECT ec_ap_code, latitude, longitude,
           ROW_NUMBER() OVER (PARTITION BY ec_ap_code ORDER BY sk_ap_id DESC) AS rn
    FROM swh_fct.dim_airport
  ) t
  WHERE rn = 1
) b ON a.arp_code = b.ec_ap_code
"

apt_icao_full <- export_query(query) %>%
  janitor::clean_names()

apt_icao <- apt_icao_full %>% select (apt_icao_code, apt_name)
