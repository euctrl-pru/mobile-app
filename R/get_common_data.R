
# parameters ----
source(here("..", "mobile-app", "R", "params.R"))

# billing data ----
if (exists("billed_raw") == FALSE) {
  billed_raw <- get_billing_data()
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

# network punctuality data ---
nw_punct_data_raw <- read_xlsx(
  path = fs::path_abs(
    str_glue("098_PUNCTUALITY.xlsx"),
    start = nw_base_dir
  ),
  sheet = "NETWORK",
  range = cell_limits(c(1, 1), c(NA, NA))
) %>%
  as_tibble() %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
