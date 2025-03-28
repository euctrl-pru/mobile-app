# libraries  ----
library(fs)
library(tibble)
library(dplyr)
library(tidyr)
library(stringr)
library(readxl)
library(DBI)
library(ROracle)
library(lubridate)
library(zoo)
library(jsonlite)
library(here)
library(RODBC)

# functions
source(here("..", "mobile-app", "R", "helpers.R")) # so it can be launched from the checkupdates script in grounded aircraft

# parameters ----
source(here("..", "mobile-app", "R", "params.R")) # so it can be launched from the checkupdates script in grounded aircraft

# archive mode for past dates
if (exists("archive_mode") == FALSE) {archive_mode <- FALSE}
if (exists("data_day_date") == FALSE) {
  data_day_date <- lubridate::today(tzone = "") +  days(-1)
  }

# archive_mode <- TRUE
# data_day_date <- ymd("2024-10-31")

data_day_text <- data_day_date %>% format("%Y%m%d")
data_day_year <- as.numeric(format(data_day_date,'%Y'))

nw_json_app <- ""

# common data ----
source(here("..", "mobile-app", "R", "get_common_data.R")) # so it can be launched from the checkupdates script in grounded aircraft

## Network billed ----
#### billing json - we do this first to avoid 'R fatal error'

# so this script is stand alone
if (exists("nw_billed_per_cz") == FALSE) {nw_billed_per_cz <- get_billing_data()}

# calculate network total
nw_billing <- nw_billed_per_cz %>%
  group_by(year, month, billing_period_start_date) %>%
  summarise(total_billing = sum(route_charges)) %>%
  ungroup()

# extract date parameters
last_billing_date <- min(max(nw_billing$billing_period_start_date + days(1)),
                         floor_date(data_day_date, 'month)') + months(-1))
last_billing_year <- year(last_billing_date)
last_billing_month <- month(last_billing_date)

# calcs + format
nw_billed_for_json <- nw_billing %>%
  arrange(year, billing_period_start_date) %>%
  mutate(
    BILLING_DATE = (billing_period_start_date + days(1) + months(1)) + days(-1),
    Year = year,
    MONTH_F = format(billing_period_start_date + days(1), "%B"),
    BILL_MONTH_PY = lag(total_billing, 12),
    BILL_MONTH_2019 = lag(total_billing, (last_billing_year - 2019) * 12),
    DIF_BILL_MONTH_PY = total_billing / BILL_MONTH_PY - 1,
    DIF_BILL_MONTH_2019 = total_billing / BILL_MONTH_2019 - 1,
    BILLED = round(total_billing / 1000000, 0)
  ) %>%
  group_by(Year) %>%
  mutate(
    total_billing_y2d = cumsum(total_billing)
  ) %>%
  ungroup() %>%
  mutate(
    BILL_Y2D_PY = lag(total_billing_y2d, 12),
    BILL_Y2D_2019 = lag(total_billing_y2d, (last_billing_year - 2019) * 12),
    DIF_BILL_Y2D_PY = total_billing_y2d / BILL_Y2D_PY - 1,
    DIF_BILL_Y2D_2019 = total_billing_y2d / BILL_Y2D_2019 - 1,
    BILLED_Y2D = round(total_billing_y2d / 1000000, 0)
  ) %>%
  filter(Year == last_billing_year,
         month == last_billing_month) %>%
  select(
    BILLING_DATE,
    MONTH_TEXT = MONTH_F,
    MM_BILLED = BILLED,
    MM_BILLED_DIF_PREV_YEAR = DIF_BILL_MONTH_PY,
    MM_BILLED_DIF_2019 = DIF_BILL_MONTH_2019,
    Y2D_BILLED = BILLED_Y2D,
    Y2D_BILLED_DIF_PREV_YEAR = DIF_BILL_Y2D_PY,
    Y2D_BILLED_DIF_2019 = DIF_BILL_Y2D_2019
  )


# app json
nw_billed_json <- nw_billed_for_json %>%
  toJSON(., pretty = TRUE) %>%
  substr(., 1, nchar(.) - 1) %>%
  substr(., 2, nchar(.))


## Network traffic ----
# traffic data
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


# get data for last date
nw_traffic_last_day <- nw_traffic_data %>%
  filter(FLIGHT_DATE == min(max(LAST_DATA_DAY),
                            data_day_date))

# select relevant fields
nw_traffic_for_json <- nw_traffic_last_day %>%
  select(
    FLIGHT_DATE,
    DY_TFC = DAY_TFC,
    DY_TFC_DIF_PREV_YEAR_PERC = DAY_DIFF_PREV_YEAR_PERC,
    DY_TFC_DIF_2019_PERC = DAY_TFC_DIFF_2019_PERC,
    WK_TFC_AVG_ROLLING = AVG_ROLLING_WEEK,
    WK_TFC_DIF_PREV_YEAR_PERC = DIF_WEEK_PREV_YEAR_PERC,
    WK_TFC_DIF_2019_PERC = DIF_ROLLING_WEEK_2019_PERC,
    Y2D_TFC = Y2D_TFC_YEAR,
    Y2D_TFC_AVG = Y2D_AVG_TFC_YEAR,
    Y2D_TFC_DIF_PREV_YEAR_PERC = Y2D_DIFF_PREV_YEAR_PERC,
    Y2D_TFC_DIF_2019_PERC = Y2D_DIFF_2019_PERC
  )

# app json
nw_traffic_json <- nw_traffic_for_json %>%
  toJSON(., pretty = TRUE, digits = 10) %>%
  substr(., 1, nchar(.) - 1) %>%
  substr(., 2, nchar(.))

## Network delay ----
# delay data
nw_delay_data <- read_xlsx(
  path = fs::path_abs(
    str_glue(nw_base_file),
    start = nw_base_dir
  ),
  sheet = "NM_Daily_Delay_All",
  range = cell_limits(c(2, 1), c(NA, 39))
) %>%
  as_tibble() %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

# calcs
nw_delay_for_json <- nw_delay_data %>%
  mutate(FLIGHT_DATE = as.Date(FLIGHT_DATE)) %>%
  filter(FLIGHT_DATE == min(max(LAST_DATA_DAY),
                            data_day_date)) %>%
  mutate(
    DAY_DLY_FLT = DAY_DLY / nw_traffic_last_day$DAY_TFC,
    DAY_DLY_FLT_PY = DAY_DLY_PREV_YEAR / nw_traffic_last_day$DAY_TFC_PREV_YEAR,
    DAY_DLY_FLT_2019 = DAY_DLY_2019 / nw_traffic_last_day$DAY_TFC_2019,
    DAY_DLY_FLT_DIF_PY_PERC = if_else(
      DAY_DLY_FLT_PY == 0, NA, DAY_DLY_FLT / DAY_DLY_FLT_PY - 1
    ),
    DAY_DLY_FLT_DIF_2019_PERC = if_else(
      DAY_DLY_FLT_2019 == 0, NA, DAY_DLY_FLT / DAY_DLY_FLT_2019 - 1
    ),
    RWEEK_DLY_FLT = TOTAL_ROLLING_WEEK / nw_traffic_last_day$TOTAL_ROLLING_WEEK,
    RWEEK_DLY_FLT_PY = AVG_ROLLING_WEEK_PREV_YEAR / nw_traffic_last_day$AVG_ROLLING_WEEK_PREV_YEAR,
    RWEEK_DLY_FLT_2019 = AVG_ROLLING_WEEK_2019 / nw_traffic_last_day$AVG_ROLLING_WEEK_2019,
    RWEEK_DLY_FLT_DIF_PY_PERC = if_else(
      RWEEK_DLY_FLT_PY == 0, NA, RWEEK_DLY_FLT / RWEEK_DLY_FLT_PY - 1
    ),
    RWEEK_DLY_FLT_DIF_2019_PERC = if_else(
      RWEEK_DLY_FLT_2019 == 0, NA, RWEEK_DLY_FLT / RWEEK_DLY_FLT_2019 - 1
    ),
    Y2D_DLY_FLT = Y2D_DLY_YEAR / nw_traffic_last_day$Y2D_TFC_YEAR,
    Y2D_DLY_FLT_PY = Y2D_AVG_DLY_PREV_YEAR / nw_traffic_last_day$Y2D_AVG_TFC_PREV_YEAR,
    Y2D_DLY_FLT_2019 = Y2D_AVG_DLY_2019 / nw_traffic_last_day$Y2D_AVG_TFC_2019,
    Y2D_DLY_FLT_DIF_PY_PERC = if_else(
      Y2D_DLY_FLT_PY == 0, NA, Y2D_DLY_FLT / Y2D_DLY_FLT_PY - 1
    ),
    Y2D_DLY_FLT_DIF_2019_PERC = if_else(
      Y2D_DLY_FLT_2019 == 0, NA, Y2D_DLY_FLT / Y2D_DLY_FLT_2019 - 1
    )
  ) %>%
  select(
    FLIGHT_DATE,
    DY_DLY = DAY_DLY,
    DY_DLY_DIF_PREV_YEAR_PERC = DAY_DIFF_PREV_YEAR_PERC,
    DY_DLY_DIF_2019_PERC = DAY_DLY_DIFF_2019_PERC,
    DY_DLY_FLT = DAY_DLY_FLT,
    DY_DLY_FLT_DIF_PREV_YEAR_PERC = DAY_DLY_FLT_DIF_PY_PERC,
    DY_DLY_FLT_DIF_2019_PERC = DAY_DLY_FLT_DIF_2019_PERC,
    WK_DLY_AVG_ROLLING = AVG_ROLLING_WEEK,
    WK_DLY_DIF_PREV_YEAR_PERC = DIF_WEEK_PREV_YEAR_PERC,
    WK_DLY_DIF_2019_PERC = DIF_ROLLING_WEEK_2019_PERC,
    WK_DLY_FLT = RWEEK_DLY_FLT,
    WK_DLY_FLT_DIF_PREV_YEAR_PERC = RWEEK_DLY_FLT_DIF_PY_PERC,
    WK_DLY_FLT_DIF_2019_PERC = RWEEK_DLY_FLT_DIF_2019_PERC,
    Y2D_DLY_AVG = Y2D_AVG_DLY_YEAR,
    Y2D_DLY_DIF_PREV_YEAR_PERC = Y2D_DIFF_PREV_YEAR_PERC,
    Y2D_DLY_DIF_2019_PERC = Y2D_DIFF_2019_PERC,
    Y2D_DLY_FLT,
    Y2D_DLY_FLT_DIF_PREV_YEAR_PERC = Y2D_DLY_FLT_DIF_PY_PERC,
    Y2D_DLY_FLT_DIF_2019_PERC
  )

# app json
nw_delay_json <- nw_delay_for_json %>%
  toJSON(., pretty = TRUE) %>%
  substr(., 1, nchar(.) - 1) %>%
  substr(., 2, nchar(.))

##------ Network punctuality ----
# punctuality data comes from get_common data

# pull out date parameters
last_day_punct <- min(max(nw_punct_data_raw$DATE),
                      data_day_date, na.rm = TRUE)
last_year_punct <- as.numeric(format(last_day_punct, "%Y"))

# day/week calculations
nw_punct_data_d_w <- nw_punct_data_raw %>%
  arrange(DATE) %>%
  mutate(YEAR_FLIGHT = as.numeric(format(DATE, "%Y"))) %>%
  mutate(
    ARR_PUN_PREV_YEAR = lag(ARR_PUNCTUALITY_PERCENTAGE, 364),
    DEP_PUN_PREV_YEAR = lag(DEP_PUNCTUALITY_PERCENTAGE, 364),
    ARR_PUN_2019 = if_else(
      YEAR_FLIGHT == last_year_punct,
      lag(
        ARR_PUNCTUALITY_PERCENTAGE,
        364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7
        ),
      1
      ),
    DEP_PUN_2019 = if_else(
      YEAR_FLIGHT == last_year_punct,
      lag(
        DEP_PUNCTUALITY_PERCENTAGE,
        364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7
        ),
      1
      ),
    DAY_2019 = if_else(
      YEAR_FLIGHT == last_year_punct,
      lag(
        DATE,
        364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7
        ),
      last_day_punct
      ),
    DAY_ARR_PUN_DIF_PY_PERC = ARR_PUNCTUALITY_PERCENTAGE - ARR_PUN_PREV_YEAR,
    DAY_DEP_PUN_DIF_PY_PERC = DEP_PUNCTUALITY_PERCENTAGE - DEP_PUN_PREV_YEAR,
    DAY_ARR_PUN_DIF_2019_PERC = ARR_PUNCTUALITY_PERCENTAGE - ARR_PUN_2019,
    DAY_DEP_PUN_DIF_2019_PERC = DEP_PUNCTUALITY_PERCENTAGE - DEP_PUN_2019
    ) %>%
  mutate(
    ARR_PUN_WK = rollsum(ARR_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") /
      rollsum(ARR_SCHEDULE_FLIGHT, 7, fill = NA, align = "right") * 100,
    DEP_PUN_WK = rollsum(DEP_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") /
      rollsum(DEP_SCHEDULE_FLIGHT, 7, fill = NA, align = "right") * 100
    ) %>%
  mutate(
    ARR_PUN_WK_PREV_YEAR = lag(ARR_PUN_WK, 364),
    DEP_PUN_WK_PREV_YEAR = lag(DEP_PUN_WK, 364),
    ARR_PUN_WK_2019 = if_else(
      YEAR_FLIGHT == last_year_punct,
      lag(ARR_PUN_WK, 364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7),
      1
      ),
    DEP_PUN_WK_2019 = if_else(
      YEAR_FLIGHT == last_year_punct,
      lag(DEP_PUN_WK, 364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7),
      1
      ),
    WK_ARR_PUN_DIF_PY_PERC = ARR_PUN_WK - ARR_PUN_WK_PREV_YEAR,
    WK_DEP_PUN_DIF_PY_PERC = DEP_PUN_WK - DEP_PUN_WK_PREV_YEAR,
    WK_ARR_PUN_DIF_2019_PERC = ARR_PUN_WK - ARR_PUN_WK_2019,
    WK_DEP_PUN_DIF_2019_PERC = DEP_PUN_WK - DEP_PUN_WK_2019
    ) %>%
  filter(DATE == last_day_punct) %>%
  mutate(FLIGHT_DATE = DATE) %>%
  select(
    FLIGHT_DATE,
    DY_ARR_PUN = ARR_PUNCTUALITY_PERCENTAGE,
    DY_DEP_PUN = DEP_PUNCTUALITY_PERCENTAGE,
    DY_ARR_PUN_DIF_PREV_YEAR = DAY_ARR_PUN_DIF_PY_PERC,
    DY_DEP_PUN_DIF_PREV_YEAR = DAY_DEP_PUN_DIF_PY_PERC,
    DY_ARR_PUN_DIF_2019 = DAY_ARR_PUN_DIF_2019_PERC,
    DY_DEP_PUN_DIF_2019 = DAY_DEP_PUN_DIF_2019_PERC,
    WK_ARR_PUN = ARR_PUN_WK,
    WK_DEP_PUN = DEP_PUN_WK,
    WK_ARR_PUN_DIF_PREV_YEAR = WK_ARR_PUN_DIF_PY_PERC,
    WK_DEP_PUN_DIF_PREV_YEAR = WK_DEP_PUN_DIF_PY_PERC,
    WK_ARR_PUN_DIF_2019 = WK_ARR_PUN_DIF_2019_PERC,
    WK_DEP_PUN_DIF_2019 = WK_DEP_PUN_DIF_2019_PERC
  ) %>%
  mutate(INDEX = 1)

# y2d calculations
nw_punct_data_y2d <- nw_punct_data_raw %>%
  arrange(DATE) %>%
  mutate(YEAR_FLIGHT = as.numeric(format(DATE, "%Y"))) %>%
  mutate(MONTH_DAY = as.numeric(format(DATE, format = "%m%d"))) %>%
  filter(MONTH_DAY <= as.numeric(format(last_day_punct, format = "%m%d"))) %>%
  mutate(YEAR = as.numeric(format(DATE, format = "%Y"))) %>%
  group_by(YEAR) %>%
  summarise(
    ARR_PUN_Y2D = sum(ARR_PUNCTUAL_FLIGHTS, na.rm = TRUE) / sum(ARR_SCHEDULE_FLIGHT, na.rm = TRUE) * 100,
    DEP_PUN_Y2D = sum(DEP_PUNCTUAL_FLIGHTS, na.rm = TRUE) / sum(DEP_SCHEDULE_FLIGHT, na.rm = TRUE) * 100
    ) %>%
  mutate(
    Y2D_ARR_PUN_PREV_YEAR = lag(ARR_PUN_Y2D, 1),
    Y2D_DEP_PUN_PREV_YEAR = lag(DEP_PUN_Y2D, 1),
    Y2D_ARR_PUN_2019 = lag(ARR_PUN_Y2D, last_year_punct - 2019),
    Y2D_DEP_PUN_2019 = lag(DEP_PUN_Y2D, last_year_punct - 2019),
    Y2D_ARR_PUN_DIF_PY_PERC = ARR_PUN_Y2D - Y2D_ARR_PUN_PREV_YEAR,
    Y2D_DEP_PUN_DIF_PY_PERC = DEP_PUN_Y2D - Y2D_DEP_PUN_PREV_YEAR,
    Y2D_ARR_PUN_DIF_2019_PERC = ARR_PUN_Y2D - Y2D_ARR_PUN_2019,
    Y2D_DEP_PUN_DIF_2019_PERC = DEP_PUN_Y2D - Y2D_DEP_PUN_2019
    ) %>%
  filter(YEAR == as.numeric(format(last_day_punct, format = "%Y"))) %>%
  select(
    Y2D_ARR_PUN = ARR_PUN_Y2D,
    Y2D_DEP_PUN = DEP_PUN_Y2D,
    Y2D_ARR_PUN_DIF_PREV_YEAR = Y2D_ARR_PUN_DIF_PY_PERC,
    Y2D_DEP_PUN_DIF_PREV_YEAR = Y2D_DEP_PUN_DIF_PY_PERC,
    Y2D_ARR_PUN_DIF_2019 = Y2D_ARR_PUN_DIF_2019_PERC,
    Y2D_DEP_PUN_DIF_2019 = Y2D_DEP_PUN_DIF_2019_PERC
    ) %>%
  mutate(INDEX = 1)

# merge day/week and y2d tables
nw_punct_for_json <- merge(nw_punct_data_d_w, nw_punct_data_y2d, by = "INDEX") %>%
  select(-INDEX)


# app json
nw_punct_json <- nw_punct_for_json %>%
  toJSON(., pretty = TRUE) %>%
  substr(., 1, nchar(.) - 1) %>%
  substr(., 2, nchar(.))


##------ Network CO2 emissions ----
# CO2 data
if (exists("co2_data_raw") == FALSE) {co2_data_raw <- get_co2_data()}

# calcs + format
co2_data_evo_nw <- co2_data_raw %>%
  select(
    FLIGHT_MONTH,
    CO2_QTY_TONNES,
    TF,
    YEAR,
    MONTH
    ) %>%
  group_by(FLIGHT_MONTH) %>%
  summarise(MM_TTF = sum(TF) / 1000000, MM_CO2 = sum(CO2_QTY_TONNES) / 1000000) %>%
  mutate(
    YEAR = as.numeric(format(FLIGHT_MONTH, "%Y")),
    MONTH = as.numeric(format(FLIGHT_MONTH, "%m")),
    MM_CO2_DEP = MM_CO2 / MM_TTF
    ) %>%
  arrange(FLIGHT_MONTH) %>%
  mutate(FLIGHT_MONTH = ceiling_date(as_date(FLIGHT_MONTH), unit = "month") - 1)

# pull out date parameters
co2_last_date <- min(max(co2_data_evo_nw$FLIGHT_MONTH, na.rm = TRUE),
                     floor_date(data_day_date, 'month') -1,
                     na.rm = TRUE)

co2_last_month <- format(co2_last_date, "%B")
co2_last_month_num <- as.numeric(format(co2_last_date, "%m"))
co2_last_year <- lubridate::year(co2_last_date)

# check last month number of flights
check_flights <- co2_data_evo_nw %>%
  filter(YEAR == max(YEAR)) %>%
  filter(MONTH == max(MONTH)) %>%
  select(MM_TTF) %>%
  pull() * 1000000

# if last month has less than 1000 flights, take the previous
if (check_flights < 1000) {
  co2_data_raw <- co2_data_raw %>% filter(FLIGHT_MONTH < max(FLIGHT_MONTH))
  co2_data_evo_nw <- co2_data_evo_nw %>% filter(FLIGHT_MONTH < max(FLIGHT_MONTH))
  co2_last_date <- max(co2_data_evo_nw$FLIGHT_MONTH, na.rm = TRUE)
}

# calcs
co2_for_json <- co2_data_evo_nw %>%
  mutate(
    MONTH_TEXT = format(FLIGHT_MONTH, "%B"),
    MM_CO2_PREV_YEAR = lag(MM_CO2, 12),
    MM_TTF_PREV_YEAR = lag(MM_TTF, 12),
    MM_CO2_2019 = lag(MM_CO2, (as.numeric(co2_last_year) - 2019) * 12),
    MM_TTF_2019 = lag(MM_TTF, (as.numeric(co2_last_year) - 2019) * 12),
    MM_CO2_DEP_PREV_YEAR = lag(MM_CO2_DEP, 12),
    MM_CO2_DEP_2019 = lag(MM_CO2_DEP, (as.numeric(co2_last_year) - 2019) * 12)
    ) %>%
  mutate(
    DIF_CO2_MONTH_PREV_YEAR = MM_CO2 / MM_CO2_PREV_YEAR - 1,
    DIF_TTF_MONTH_PREV_YEAR = MM_TTF / MM_TTF_PREV_YEAR - 1,
    DIF_CO2_DEP_MONTH_PREV_YEAR = MM_CO2_DEP / MM_CO2_DEP_PREV_YEAR - 1,
    DIF_CO2_MONTH_2019 = MM_CO2 / MM_CO2_2019 - 1,
    DIF_TTF_MONTH_2019 = MM_TTF / MM_TTF_2019 - 1,
    DIF_CO2_DEP_MONTH_2019 = MM_CO2_DEP / MM_CO2_DEP_2019 - 1
    ) %>%
  group_by(YEAR) %>%
  mutate(
    YTD_CO2 = cumsum(MM_CO2),
    YTD_TTF = cumsum(MM_TTF),
    YTD_CO2_DEP = cumsum(MM_CO2) / cumsum(MM_TTF)
    ) %>%
  ungroup() %>%
  mutate(
    YTD_CO2_PREV_YEAR = lag(YTD_CO2, 12),
    YTD_TTF_PREV_YEAR = lag(YTD_TTF, 12),
    YTD_CO2_DEP_PREV_YEAR = lag(YTD_CO2_DEP, 12),
    YTD_CO2_2019 = lag(YTD_CO2, (as.numeric(co2_last_year) - 2019) * 12),
    YTD_CO2_DEP_2019 = lag(YTD_CO2_DEP, (as.numeric(co2_last_year) - 2019) * 12),
    YTD_TTF_2019 = lag(YTD_TTF, (as.numeric(co2_last_year) - 2019) * 12)
    ) %>%
  mutate(
    YTD_DIF_CO2_PREV_YEAR = YTD_CO2 / YTD_CO2_PREV_YEAR - 1,
    YTD_DIF_TTF_PREV_YEAR = YTD_TTF / YTD_TTF_PREV_YEAR - 1,
    YTD_DIF_CO2_DEP_PREV_YEAR = YTD_CO2_DEP / YTD_CO2_DEP_PREV_YEAR - 1,
    YTD_DIF_CO2_2019 = YTD_CO2 / YTD_CO2_2019 - 1,
    YTD_DIF_CO2_DEP_2019 = YTD_CO2_DEP / YTD_CO2_DEP_2019 - 1,
    YTD_DIF_TTF_2019 = YTD_TTF / YTD_TTF_2019 - 1
    ) %>%
  select(
    FLIGHT_MONTH,
    MONTH_TEXT,
    MM_CO2,
    MM_CO2_DIF_PREV_YEAR = DIF_CO2_MONTH_PREV_YEAR,
    MM_CO2_DIF_2019 = DIF_CO2_MONTH_2019,
    MM_CO2_DEP,
    MM_CO2_DEP_DIF_PREV_YEAR = DIF_CO2_DEP_MONTH_PREV_YEAR,
    MM_CO2_DEP_DIF_2019 = DIF_CO2_DEP_MONTH_2019
    , Y2D_CO2 = YTD_CO2
    , Y2D_CO2_DIF_PREV_YEAR = YTD_DIF_CO2_PREV_YEAR
    , Y2D_CO2_DIF_2019 = YTD_DIF_CO2_2019
    , Y2D_CO2_DEP = YTD_CO2_DEP
    , Y2D_CO2_DEP_DIF_PREV_YEAR = YTD_DIF_CO2_DEP_PREV_YEAR
    , Y2D_CO2_DEP_DIF_2019 = YTD_DIF_CO2_DEP_2019
  ) %>%
  filter(FLIGHT_MONTH == co2_last_date)

# app json
nw_co2_json <- co2_for_json %>%
  toJSON(., pretty = TRUE) %>%
  substr(., 1, nchar(.) - 1) %>%
  substr(., 2, nchar(.))

##------ update date ----

# add date to json

update_day <- floor_date(lubridate::now(), unit = "days") %>%
  as_tibble() %>%
  rename(APP_UPDATE = 1)

update_day_json <- update_day %>%
  toJSON(., pretty = TRUE) %>%
  substr(., 1, nchar(.) - 1) %>%
  substr(., 2, nchar(.))

# app json
nw_json_app <- paste0(
  "{",
  '"nw_traffic":', nw_traffic_json,
  ', "nw_delay":', nw_delay_json,
  ', "nw_punct":', nw_punct_json,
  ', "nw_co2":', nw_co2_json,
  ', "nw_billed":', nw_billed_json,
  ', "app_update":', update_day_json,
  "}"
)

save_json(nw_json_app, "nw_json_app")


# jsons for graphs -------
## traffic -----
nw_traffic_evo <- nw_traffic_data %>%
  mutate(AVG_ROLLING_WEEK = if_else(FLIGHT_DATE > min(data_day_date,
                                                      max(LAST_DATA_DAY, na.rm = TRUE),na.rm = TRUE), NA, AVG_ROLLING_WEEK)
  ) %>%
  select(
    FLIGHT_DATE, AVG_ROLLING_WEEK, AVG_ROLLING_WEEK_PREV_YEAR,
    AVG_ROLLING_WEEK_2020, AVG_ROLLING_WEEK_2019
  )

column_names <- c("FLIGHT_DATE", data_day_year, data_day_year - 1, 2020, 2019)
colnames(nw_traffic_evo) <- column_names


### nest data
nw_traffic_evo_long <- nw_traffic_evo %>%
  pivot_longer(-c(FLIGHT_DATE), names_to = 'metric', values_to = 'value') %>%
  group_by(FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

### create json
nw_traffic_evo_j <- nw_traffic_evo_long %>% toJSON(., pretty = TRUE)

### save and archive
save_json(nw_traffic_evo_j, "nw_traffic_evo_chart_daily")

## delay ----
nw_delay_raw <- read_xlsx(
path = fs::path_abs(
  str_glue(nw_base_file),
  start = nw_base_dir
),
sheet = "NM_Delay_for_graph",
range = cell_limits(c(2, 1), c(NA, 34))
) %>%
as_tibble()

### delay per cause ----
nw_delay_evo <- nw_delay_raw %>%
  filter(FLIGHT_DATE <= data_day_date) %>%
  mutate(
    ROLL_WK_AVG_DLY_PREV_YEAR = lag(ROLL_WK_AVG_DLY, 364),
    DAY_DLY_PREV_YEAR = lag(DAY_DLY, 364)
  ) %>%
  filter(FLIGHT_DATE >= paste0(data_day_year, "-01-01")) %>%
  mutate(FLIGHT_DATE = as.Date(FLIGHT_DATE))

#### day ----
nw_delay_cause_day <- nw_delay_evo %>%
  filter(FLIGHT_DATE == max(FLIGHT_DATE)) %>%
  mutate(
    SHARE_TDM_G = if_else(DAY_DLY == 0, 0, DAY_DLY_APT_CAP / DAY_DLY),
    SHARE_TDM_CS = if_else(DAY_DLY == 0, 0, DAY_DLY_CAP_STAF_NOG / DAY_DLY),
    SHARE_TDM_IT = if_else(DAY_DLY == 0, 0, DAY_DLY_DISR / DAY_DLY),
    SHARE_TDM_WD = if_else(DAY_DLY == 0, 0, DAY_DLY_WTH / DAY_DLY),
    SHARE_TDM_NOCSGITWD = if_else(DAY_DLY == 0, 0, DAY_DLY_OTH / DAY_DLY)
  ) %>%
  select(FLIGHT_DATE,
         DAY_DLY_APT_CAP,
         DAY_DLY_CAP_STAF_NOG,
         DAY_DLY_DISR,
         DAY_DLY_WTH,
         DAY_DLY_OTH,
         DAY_DLY_PREV_YEAR,
         SHARE_TDM_G,
         SHARE_TDM_CS,
         SHARE_TDM_IT,
         SHARE_TDM_WD,
         SHARE_TDM_NOCSGITWD
  )

column_names <- c(
  "FLIGHT_DATE",
  "Aerodrome capacity",
  "Capacity/Staffing (ATC)",
  "Disruptions (ATC)",
  "Weather",
  "Other",
  paste0("Total delay ", data_day_year - 1),
  "share_aerodrome_capacity",
  "share_capacity_staffing_atc",
  "share_disruptions_atc",
  "share_weather",
  "share_other"
)

colnames(nw_delay_cause_day) <- column_names

### nest data
#### values
nw_delay_value_day_long <- nw_delay_cause_day %>%
  select(-c(share_aerodrome_capacity,
            share_capacity_staffing_atc,
            share_disruptions_atc,
            share_weather,
            share_other)
  ) %>%
  pivot_longer(-c(FLIGHT_DATE), names_to = 'metric', values_to = 'value')

#### share
nw_delay_share_day_long <- nw_delay_cause_day %>%
  select(-c("Aerodrome capacity",
            "Capacity/Staffing (ATC)",
            "Disruptions (ATC)",
            "Weather",
            "Other",
            paste0("Total delay ", data_day_year - 1)
            )
         )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

#### join values and share
nw_delay_cause_day_long <- cbind(nw_delay_value_day_long, nw_delay_share_day_long) %>%
  select(-name) %>%
  group_by(FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

#### convert to json and save in data folder and archive
nw_delay_cause_evo_dy_j <- nw_delay_cause_day_long %>% toJSON(., pretty = TRUE)
save_json(nw_delay_cause_evo_dy_j, "nw_delay_category_evo_chart_dy")

#### week ----
nw_delay_cause_wk <- nw_delay_evo %>%
  filter(FLIGHT_DATE >= max(FLIGHT_DATE) + lubridate::days(-6)) %>%
  mutate(
    SHARE_TDM_G = if_else(sum(DAY_DLY) == 0, 0, sum(DAY_DLY_APT_CAP) / sum(DAY_DLY)),
    SHARE_TDM_CS = if_else(sum(DAY_DLY) == 0, 0, sum(DAY_DLY_CAP_STAF_NOG) / sum(DAY_DLY)),
    SHARE_TDM_IT = if_else(sum(DAY_DLY) == 0, 0, sum(DAY_DLY_DISR) / sum(DAY_DLY)),
    SHARE_TDM_WD = if_else(sum(DAY_DLY) == 0, 0, sum(DAY_DLY_WTH) / sum(DAY_DLY)),
    SHARE_TDM_NOCSGITWD = if_else(sum(DAY_DLY) == 0, 0, sum(DAY_DLY_OTH) / sum(DAY_DLY))
  ) %>%
  select(FLIGHT_DATE,
         DAY_DLY_APT_CAP,
         DAY_DLY_CAP_STAF_NOG,
         DAY_DLY_DISR,
         DAY_DLY_WTH,
         DAY_DLY_OTH,
         DAY_DLY_PREV_YEAR,
         SHARE_TDM_G,
         SHARE_TDM_CS,
         SHARE_TDM_IT,
         SHARE_TDM_WD,
         SHARE_TDM_NOCSGITWD
  )

colnames(nw_delay_cause_wk) <- column_names

### nest data
#### values
nw_delay_value_wk_long <- nw_delay_cause_wk %>%
  select(-c(share_aerodrome_capacity,
            share_capacity_staffing_atc,
            share_disruptions_atc,
            share_weather,
            share_other)
  ) %>%
  pivot_longer(-c(FLIGHT_DATE), names_to = 'metric', values_to = 'value')

#### share
nw_delay_share_wk_long <- nw_delay_cause_wk %>%
  select(-c("Aerodrome capacity",
            "Capacity/Staffing (ATC)",
            "Disruptions (ATC)",
            "Weather",
            "Other",
            paste0("Total delay ", data_day_year - 1)
            )
         )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

#### join values and share
nw_delay_cause_wk_long <- cbind(nw_delay_value_wk_long, nw_delay_share_wk_long) %>%
  select(-name) %>%
  group_by(FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

#### convert to json and save in data folder and archive
nw_delay_cause_evo_wk_j <- nw_delay_cause_wk_long %>% toJSON(., pretty = TRUE)
save_json(nw_delay_cause_evo_wk_j, "nw_delay_category_evo_chart_wk")

#### y2d ----
nw_delay_cause_y2d <- nw_delay_evo %>%
  mutate(
    SHARE_TDM_G = if_else(sum(DAY_DLY) == 0, 0, sum(DAY_DLY_APT_CAP) / sum(DAY_DLY)),
    SHARE_TDM_CS = if_else(sum(DAY_DLY) == 0, 0, sum(DAY_DLY_CAP_STAF_NOG) / sum(DAY_DLY)),
    SHARE_TDM_IT = if_else(sum(DAY_DLY) == 0, 0, sum(DAY_DLY_DISR) / sum(DAY_DLY)),
    SHARE_TDM_WD = if_else(sum(DAY_DLY) == 0, 0, sum(DAY_DLY_WTH) / sum(DAY_DLY)),
    SHARE_TDM_NOCSGITWD = if_else(sum(DAY_DLY) == 0, 0, sum(DAY_DLY_OTH) / sum(DAY_DLY))
  ) %>%
  select(FLIGHT_DATE,
         ROLL_WK_AVG_DLY_APT_CAP,
         ROLL_WK_AVG_DLY_CAP_STAF_NOG,
         ROLL_WK_AVG_DLY_DISR,
         ROLL_WK_AVG_DLY_WTH,
         ROLL_WK_AVG_DLY_OTH,
         ROLL_WK_AVG_DLY_PREV_YEAR,
         SHARE_TDM_G,
         SHARE_TDM_CS,
         SHARE_TDM_IT,
         SHARE_TDM_WD,
         SHARE_TDM_NOCSGITWD
  )

colnames(nw_delay_cause_y2d) <- column_names

### nest data
#### values
nw_delay_value_y2d_long <- nw_delay_cause_y2d %>%
  select(-c(share_aerodrome_capacity,
            share_capacity_staffing_atc,
            share_disruptions_atc,
            share_weather,
            share_other)
  ) %>%
  pivot_longer(-c(FLIGHT_DATE), names_to = 'metric', values_to = 'value')

#### share
nw_delay_share_y2d_long <- nw_delay_cause_y2d %>%
  select(-c("Aerodrome capacity",
            "Capacity/Staffing (ATC)",
            "Disruptions (ATC)",
            "Weather",
            "Other",
            paste0("Total delay ", data_day_year - 1)
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

#### join values and share
nw_delay_cause_y2d_long <- cbind(nw_delay_value_y2d_long, nw_delay_share_y2d_long) %>%
  select(-name) %>%
  group_by(FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

#### convert to json and save in data folder and archive
nw_delay_cause_evo_y2d_j <- nw_delay_cause_y2d_long %>% toJSON(., pretty = TRUE)
save_json(nw_delay_cause_evo_y2d_j, "nw_delay_category_evo_chart_y2d")


### delay per flight per type v1----
nw_delay_flt_evo <- nw_delay_raw %>%
  filter(FLIGHT_DATE <= data_day_date) %>%
  mutate(
    ROLL_WK_AVG_FLT = rollmeanr(DAY_FLT, 7, fill = NA, align = "right"),
    ROLL_WK_AVG_DLY_FLT_ERT = ROLL_WK_AVG_DLY_ERT / ROLL_WK_AVG_FLT,
    ROLL_WK_AVG_DLY_FLT_APT = ROLL_WK_AVG_DLY_APT / ROLL_WK_AVG_FLT,
    ROLL_WK_AVG_DLY_FLT_PREV_YEAR = lag(ROLL_WK_AVG_DLY, 364) / lag(ROLL_WK_AVG_FLT, 364),

    DAY_DLY_PER_FLT_ERT = if_else(DAY_FLT == 0, 0, DAY_DLY_ERT / DAY_FLT),
    DAY_DLY_PER_FLT_APT = if_else(DAY_FLT == 0, 0, DAY_DLY_APT / DAY_FLT),
    DAY_DLY_PER_FLT_PREV_YEAR = lag(DAY_DLY_PER_FLT, 364)
  ) %>%
  filter(FLIGHT_DATE >= paste0(data_day_year, "-01-01")) %>%
  mutate(FLIGHT_DATE = as.Date(FLIGHT_DATE)) %>%
  select(
    DAY_FLT,
    DAY_DLY,
    DAY_DLY_ERT,
    DAY_DLY_APT,
    DAY_DLY_PER_FLT,
    DAY_DLY_PER_FLT_ERT,
    DAY_DLY_PER_FLT_APT,
    DAY_DLY_PER_FLT_PREV_YEAR,
    FLIGHT_DATE,
    ROLL_WK_AVG_DLY_FLT_ERT,
    ROLL_WK_AVG_DLY_FLT_APT,
    ROLL_WK_AVG_DLY_FLT_PREV_YEAR
  )

nw_delay_flt_evo_app <- nw_delay_flt_evo %>%
  select(
    FLIGHT_DATE,
    ROLL_WK_AVG_DLY_FLT_ERT,
    ROLL_WK_AVG_DLY_FLT_APT,
    ROLL_WK_AVG_DLY_FLT_PREV_YEAR
  )

column_names <- c(
  "FLIGHT_DATE",
  "En-route ATFM delay/flight",
  "Airport ATFM delay/flight",
  paste0("Total ATFM delay/flight ", data_day_year - 1)
)

colnames(nw_delay_flt_evo_app) <- column_names

### delay per flight per type v2----
#graphs not implemented, maybe for v4
#### day ----
nw_delay_flt_day <- nw_delay_flt_evo %>%
  filter(FLIGHT_DATE == max(FLIGHT_DATE)) %>%
  mutate(
    SHARE_DLY_FLT_ERT = if_else(DAY_DLY_PER_FLT == 0, 0, DAY_DLY_PER_FLT_ERT / DAY_DLY_PER_FLT),
    SHARE_DLY_FLT_APT = if_else(DAY_DLY_PER_FLT == 0, 0, DAY_DLY_PER_FLT_APT / DAY_DLY_PER_FLT)
  ) %>%
  select(
    FLIGHT_DATE,
    DAY_DLY_PER_FLT_ERT,
    DAY_DLY_PER_FLT_APT,
    DAY_DLY_PER_FLT_PREV_YEAR,
    SHARE_DLY_FLT_ERT,
    SHARE_DLY_FLT_APT
  )

column_names <- c(
  "FLIGHT_DATE",
  "En-route ATFM delay/flight",
  "Airport ATFM delay/flight",
  paste0("Total ATFM delay/flight ", data_day_year - 1),
  "share_en_route",
  "share_airport"
)

colnames(nw_delay_flt_day) <- column_names

### nest data
#### values
nw_delay_flt_value_day_long <- nw_delay_flt_day %>%
  select(-c(share_en_route,
            share_airport)
  ) %>%
  pivot_longer(-c(FLIGHT_DATE), names_to = 'metric', values_to = 'value')

#### share
nw_delay_flt_share_day_long <- nw_delay_flt_day %>%
  select(-c("En-route ATFM delay/flight",
            "Airport ATFM delay/flight",
            paste0("Total ATFM delay/flight ", data_day_year - 1),
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

#### join values and share
nw_delay_flt_day_long <- cbind(nw_delay_flt_value_day_long, nw_delay_flt_share_day_long) %>%
  select(-name) %>%
  group_by(FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

#### convert to json and save in data folder and archive
nw_delay_flt_day_j <- nw_delay_flt_day_long %>% toJSON(., pretty = TRUE)
save_json(nw_delay_flt_day_j, "nw_delay_flt_type_evo_chart_dy")

#### week ----
nw_delay_flt_wk <- nw_delay_flt_evo %>%
  filter(FLIGHT_DATE >= max(FLIGHT_DATE) + lubridate::days(-6)) %>%
  mutate(
    SHARE_DLY_FLT_ERT = if_else(sum(DAY_DLY_PER_FLT) == 0, 0, sum(DAY_DLY_PER_FLT_ERT) / sum(DAY_DLY_PER_FLT)),
    SHARE_DLY_FLT_APT = if_else(sum(DAY_DLY_PER_FLT) == 0, 0, sum(DAY_DLY_PER_FLT_APT) / sum(DAY_DLY_PER_FLT))
  ) %>%
  select(
    FLIGHT_DATE,
    DAY_DLY_PER_FLT_ERT,
    DAY_DLY_PER_FLT_APT,
    DAY_DLY_PER_FLT_PREV_YEAR,
    SHARE_DLY_FLT_ERT,
    SHARE_DLY_FLT_APT
  )

colnames(nw_delay_flt_wk) <- column_names

### nest data
#### values
nw_delay_flt_value_wk_long <- nw_delay_flt_wk %>%
  select(-c(share_en_route,
            share_airport)
  ) %>%
  pivot_longer(-c(FLIGHT_DATE), names_to = 'metric', values_to = 'value')

#### share
nw_delay_flt_share_wk_long <- nw_delay_flt_wk %>%
  select(-c("En-route ATFM delay/flight",
            "Airport ATFM delay/flight",
            paste0("Total ATFM delay/flight ", data_day_year - 1),
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

#### join values and share
nw_delay_flt_wk_long <- cbind(nw_delay_flt_value_wk_long, nw_delay_flt_share_wk_long) %>%
  select(-name) %>%
  group_by(FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

#### convert to json and save in data folder and archive
nw_delay_flt_wk_j <- nw_delay_flt_wk_long %>% toJSON(., pretty = TRUE)
save_json(nw_delay_flt_wk_j, "nw_delay_flt_type_evo_chart_wk")

#### y2d ----
nw_delay_flt_y2d <- nw_delay_flt_evo %>%
  mutate(
    SHARE_DLY_FLT_ERT = if_else(sum(DAY_DLY_PER_FLT) == 0, 0, sum(DAY_DLY_PER_FLT_ERT) / sum(DAY_DLY_PER_FLT)),
    SHARE_DLY_FLT_APT = if_else(sum(DAY_DLY_PER_FLT) == 0, 0, sum(DAY_DLY_PER_FLT_APT) / sum(DAY_DLY_PER_FLT))
  ) %>%
  select(
    FLIGHT_DATE,
    ROLL_WK_AVG_DLY_FLT_ERT,
    ROLL_WK_AVG_DLY_FLT_APT,
    ROLL_WK_AVG_DLY_FLT_PREV_YEAR,
    SHARE_DLY_FLT_ERT,
    SHARE_DLY_FLT_APT
  )

colnames(nw_delay_flt_y2d) <- column_names

### nest data
#### values
nw_delay_flt_value_y2d_long <- nw_delay_flt_y2d %>%
  select(-c(share_en_route,
            share_airport)
  ) %>%
  pivot_longer(-c(FLIGHT_DATE), names_to = 'metric', values_to = 'value')

#### share
nw_delay_flt_share_y2d_long <- nw_delay_flt_y2d %>%
  select(-c("En-route ATFM delay/flight",
            "Airport ATFM delay/flight",
            paste0("Total ATFM delay/flight ", data_day_year - 1),
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

#### join values and share
nw_delay_flt_y2d_long <- cbind(nw_delay_flt_value_y2d_long, nw_delay_flt_share_y2d_long) %>%
  select(-name) %>%
  group_by(FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

#### convert to json and save in data folder and archive
nw_delay_flt_y2d_j <- nw_delay_flt_y2d_long %>% toJSON(., pretty = TRUE)
save_json(nw_delay_flt_y2d_j, "nw_delay_flt_type_evo_chart_y2d")

## punctuality ----
nw_punct_evo_app <- nw_punct_data_raw %>%
  filter(DATE >= as.Date(paste0("01-01-", data_day_year - 2), format = "%d-%m-%Y")) %>%
  arrange(DATE) %>%
  mutate(DEP_PUN = DEP_PUNCTUALITY_PERCENTAGE, ARR_PUN = ARR_PUNCTUALITY_PERCENTAGE, OPERATED = 100 - MISSING_SCHEDULES_PERCENTAGE) %>%
  mutate(
    DEP_PUN_WK = rollsum(DEP_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") / rollsum(DEP_SCHEDULE_FLIGHT, 7, fill = NA, align = "right") * 100,
    ARR_PUN_WK = rollsum(ARR_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") / rollsum(ARR_SCHEDULE_FLIGHT, 7, fill = NA, align = "right") * 100,
    OP_FLT_WK = 100 - rollsum(MISSING_SCHED_FLIGHTS, 7, fill = NA, align = "right") / rollsum((MISSING_SCHED_FLIGHTS + DEP_FLIGHTS_NO_OVERFLIGHTS), 7, fill = NA, align = "right") * 100
  ) %>%
  select(DATE, DEP_PUN_WK, ARR_PUN_WK, OP_FLT_WK) %>%
  filter(DATE >= as.Date(paste0("01-01-", data_day_year - 1), format = "%d-%m-%Y"),
         DATE <= last_day_punct)

column_names <- c(
  "FLIGHT_DATE",
  "Departure punct.",
  "Arrival punct.",
  "Operated schedules"
)
colnames(nw_punct_evo_app) <- column_names

### nest data
nw_punct_evo_app_v2_long <- nw_punct_evo_app %>%
  pivot_longer(-c(FLIGHT_DATE), names_to = 'metric', values_to = 'value') %>%
  group_by(FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

nw_punct_evo_v2_j <- nw_punct_evo_app_v2_long %>% toJSON(., pretty = TRUE)
save_json(nw_punct_evo_v2_j, "nw_punct_evo_chart")

## billing ----
nw_billing_evo <- nw_billing %>%
  arrange(year, month) %>%
  mutate(
    total_billing = total_billing / 10^6,
    total_billing_py = lag(total_billing, 12),
    total_billing_dif_mm_perc = total_billing / total_billing_py - 1
  ) %>%
  group_by(year) %>%
  mutate(
    total_billing_y2d = cumsum(total_billing)
  ) %>%
  ungroup() %>%
  mutate(
    total_billing_y2d_py = lag(total_billing_y2d, 12),
    total_billing_dif_y2d_perc = total_billing_y2d / total_billing_y2d_py - 1
  ) %>%
  filter(year == last_billing_year,
         month <= last_billing_month) %>%
  select(
    month,
    total_billing,
    total_billing_py,
    total_billing_dif_mm_perc,
    total_billing_dif_y2d_perc
  ) %>%
  mutate(
    month = month.name[month],
    min_right_axis = -0.2,
    max_right_axis = 1.3
  )

column_names <- c(
  "month",
  last_billing_year,
  last_billing_year - 1,
  paste0("Monthly variation vs ", last_billing_year - 1),
  paste0("Year-to-date variation vs ", last_billing_year - 1),
  "min_right_axis",
  "max_right_axis"
)

colnames(nw_billing_evo) <- column_names

### nest data
nw_billing_evo_v2_long <- nw_billing_evo %>%
  pivot_longer(-c(month), names_to = 'metric', values_to = 'value') %>%
  group_by(month) %>%
  nest_legacy(.key = "statistics")

nw_billing_evo_v2_j <- nw_billing_evo_v2_long %>% toJSON(., pretty = TRUE)
save_json(nw_billing_evo_v2_j, "nw_billing_evo_chart")


## co2 emissions ----
nw_co2_evo <- co2_data_raw %>%
  filter(YEAR >= 2019,
         YEAR <= co2_last_year,
         MONTH <= co2_last_month_num
  ) %>%
  select(
    FLIGHT_MONTH,
    CO2_QTY_TONNES,
    TF,
    YEAR,
    MONTH
  ) %>%
  group_by(FLIGHT_MONTH) %>%
  summarise(TTF = sum(TF), TCO2 = sum(CO2_QTY_TONNES)) %>%
  mutate(
    YEAR = as.numeric(format(FLIGHT_MONTH, "%Y")),
    MONTH = as.numeric(format(FLIGHT_MONTH, "%m"))
  ) %>%
  arrange(FLIGHT_MONTH) %>%
  mutate(
    DEP_IDX = TTF / first(TTF) * 100,
    CO2_IDX = TCO2 / first(TCO2) * 100,
    FLIGHT_MONTH = ceiling_date(as_date(FLIGHT_MONTH), unit = "month") - 1
  ) %>%
  select(
    FLIGHT_MONTH,
    CO2_IDX,
    DEP_IDX
  )

column_names <- c(
  "month",
  "CO2 index",
  "Departures index"
)

colnames(nw_co2_evo) <- column_names

### nest data
nw_co2_evo_v2_long <- nw_co2_evo %>%
  pivot_longer(-c(month), names_to = 'metric', values_to = 'value') %>%
  group_by(month) %>%
  nest_legacy(.key = "statistics")

nw_co2_evo_v2_j <- nw_co2_evo_v2_long %>% toJSON(., pretty = TRUE)
save_json(nw_co2_evo_v2_j, "nw_co2_evo_chart")


# jsons for ranking tables ----

## Aircraft operators traffic ----
### day ----
mydataframe <- "nw_ao_day_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

} else {
  df <- read_xlsx(
  path = fs::path_abs(
    str_glue(nw_base_file),
    start = nw_base_dir
  ),
  sheet = "AO_DAY_DATA",
  range = cell_limits(c(5, 2), c(NA, NA))
) %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  # save pre-processed file in archive for generation of past json files
  write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
}

ao_data_dy <- assign(mydataframe, df) %>%
  filter(R_RANK_BY_DAY <= 10 &
           R_RANK_BY_DAY != 0 &
           is.na(R_RANK_BY_DAY) == FALSE) %>%
  mutate(DY_RANK_DIF_PREV_WEEK = RANK_BY_DAY_7DAY - RANK_BY_DAY) %>%
  select(
    WK_R_RANK_BY_DAY = R_RANK_BY_DAY,
    DY_AO_GRP_NAME = AO_GRP_NAME,
    DY_TO_DATE = ENTRY_DATE,
    DY_FLIGHT = FLIGHT,
    DY_DIF_PREV_WEEK_PERC = FLIGHT_DIFF_7DAY_PERC,
    DY_DIF_PREV_YEAR_PERC = FLIGHT_DIFF_PERC,
    DY_RANK_DIF_PREV_WEEK,
    DY_AO_GRP_CODE = AO_GRP_CODE)

### week ----
mydataframe <- "nw_ao_week_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

} else {
  df <- read_xlsx(
    path = fs::path_abs(
      str_glue(nw_base_file),
      start = nw_base_dir
    ),
    sheet = "AO_WEEK_DATA",
    range = cell_limits(c(5, 2), c(NA, NA))
  ) %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  # save pre-processed file in archive for generation of past json files
  write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
}

ao_data_wk <- assign(mydataframe, df) %>%
  filter(R_RANK_BY_DAY <= 10 &
           R_RANK_BY_DAY != 0 &
           is.na(R_RANK_BY_DAY) == FALSE) %>%
  mutate(WK_RANK_DIF_PREV_WEEK = RANK_BY_DAY_7DAY -  RANK_BY_DAY) %>%
  select(WK_R_RANK_BY_DAY = R_RANK_BY_DAY,
         WK_AO_GRP_NAME = AO_GRP_NAME,
         WK_TO_DATE = MAX_ENTRY_DATE,
         WK_DAILY_FLIGHT = DAILY_FLIGHT,
         WK_DIF_PREV_WEEK_PERC =  FLIGHT_DIFF_PREV_YEAR_PERC,
         WK_DIF_PREV_YEAR_PERC = FLIGHT_DIFF_7DAY_PERC,
         WK_RANK_DIF_PREV_WEEK)

### y2d ----
mydataframe <- "nw_ao_y2d_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

} else {
  df <- read_xlsx(
  path = fs::path_abs(
    str_glue(nw_base_file),
    start = nw_base_dir
  ),
  sheet = "TOP40_AO_Y2D",
  range = cell_limits(c(5, 2), c(NA, NA))
  ) %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  # save pre-processed file in archive for generation of past json files
  write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
}

ao_data_y2d <- assign(mydataframe, df) %>%
  mutate(TO_DATE = max(TO_DATE, na.rm = TRUE),
         Y2D_YEAR = case_when(
           Y2D_YEAR == "1_Y2D_CURRENT_YEAR" ~ "Y2D_CURRENT_YEAR",
           Y2D_YEAR == "2_Y2D_PREV_YEAR" ~ "Y2D_PREV_YEAR",
           .default = Y2D_YEAR
         )) %>%
  filter(R_RANK <= 10) %>%
  select(Y2D_YEAR,
         AO_GRP_NAME,
         AO_GRP_CODE,
         TO_DATE,
         R_RANK,
         RANK,
         RANK_PY,
         FLT) %>%
  pivot_wider(names_from = "Y2D_YEAR", values_from = FLT) %>%
  mutate(
    Y2D_DIF_PREV_YEAR_PERC = if_else(Y2D_PREV_YEAR == 0 , 0,
                                     Y2D_CURRENT_YEAR / Y2D_PREV_YEAR -1),
    Y2D_DIF_2019_PERC = if_else(Y2D_2019 == 0 , 0,
                                Y2D_CURRENT_YEAR / Y2D_2019 -1),
    Y2D_RANK_DIF_PREV_YEAR = RANK_PY - RANK
    ) %>%
  select(
    WK_R_RANK_BY_DAY = R_RANK,
    Y2D_AO_GRP_NAME = AO_GRP_NAME,
    Y2D_TO_DATE = TO_DATE,
    Y2D_DAILY_FLIGHT = Y2D_CURRENT_YEAR,
    Y2D_DIF_PREV_YEAR_PERC,
    Y2D_DIF_2019_PERC,
    Y2D_RANK_DIF_PREV_YEAR
    )

### main card ----
ao_main_traffic <- ao_data_dy %>%
  mutate(across(-WK_R_RANK_BY_DAY, ~ ifelse(WK_R_RANK_BY_DAY > 4, NA, .))) %>%
  select(WK_R_RANK_BY_DAY,
         MAIN_TFC_AO_GRP_NAME = DY_AO_GRP_NAME,
         MAIN_TFC_AO_GRP_CODE = DY_AO_GRP_CODE,
         MAIN_TFC_AO_GRP_FLIGHT = DY_FLIGHT)

ao_main_traffic_dif <- nw_ao_day_raw %>%
  select(AO_GRP_CODE,
         AO_GRP_NAME,
         FLIGHT_7DAY_DIFF) %>%
  arrange(desc(abs(FLIGHT_7DAY_DIFF))) %>%
  mutate(WK_R_RANK_BY_DAY = row_number()) %>%
  filter(WK_R_RANK_BY_DAY <= 10) %>%
  mutate(across(-WK_R_RANK_BY_DAY, ~ ifelse(WK_R_RANK_BY_DAY > 4, NA, .))) %>%
  arrange(abs(FLIGHT_7DAY_DIFF)) %>%
  mutate(WK_R_RANK_BY_DAY = row_number()) %>%
  select(
    WK_R_RANK_BY_DAY,
    MAIN_TFC_DIF_AO_GRP_NAME = AO_GRP_NAME,
    MAIN_TFC_DIF_AO_GRP_CODE = AO_GRP_CODE,
    MAIN_TFC_AO_GRP_DIF = FLIGHT_7DAY_DIFF
    )

### merge and reorder tables ----
ao_data <- merge(x = ao_data_wk, y = ao_data_dy, by = "WK_R_RANK_BY_DAY")
ao_data <- merge(x = ao_data, y = ao_data_y2d, by = "WK_R_RANK_BY_DAY")
ao_data <- merge(x = ao_data, y = ao_main_traffic, by = "WK_R_RANK_BY_DAY")
ao_data <- merge(x = ao_data, y = ao_main_traffic_dif, by = "WK_R_RANK_BY_DAY")

ao_data <- ao_data %>%
  mutate(WK_FROM_DATE = WK_TO_DATE - 6)

ao_data <- ao_data %>%
  select(
    RANK = WK_R_RANK_BY_DAY,
    MAIN_TFC_AO_GRP_NAME,
    MAIN_TFC_AO_GRP_CODE,
    MAIN_TFC_AO_GRP_FLIGHT,
    MAIN_TFC_DIF_AO_GRP_NAME,
    MAIN_TFC_DIF_AO_GRP_CODE,
    MAIN_TFC_AO_GRP_DIF,
    DY_RANK_DIF_PREV_WEEK,
    DY_AO_GRP_NAME,
    DY_TO_DATE,
    DY_FLIGHT,
    DY_DIF_PREV_WEEK_PERC,
    DY_DIF_PREV_YEAR_PERC,
    WK_RANK_DIF_PREV_WEEK,
    WK_AO_GRP_NAME,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_DAILY_FLIGHT,
    WK_DIF_PREV_WEEK_PERC,
    WK_DIF_PREV_YEAR_PERC,
    Y2D_RANK_DIF_PREV_YEAR,
    Y2D_AO_GRP_NAME,
    Y2D_TO_DATE,
    Y2D_DAILY_FLIGHT,
    Y2D_DIF_PREV_YEAR_PERC,
    Y2D_DIF_2019_PERC
  )

### covert to json and save in app data folder and archive ----
ao_data_j <- ao_data %>% toJSON(., pretty = TRUE)
save_json(ao_data_j, "nw_ao_ranking_traffic")


## Airport traffic ----
### day ----
mydataframe <- "nw_apt_day_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

} else {
  df <- read_xlsx(
  path = fs::path_abs(
    str_glue(nw_base_file),
    start = nw_base_dir
  ),
  sheet = "APT_DAY_DATA",
  range = cell_limits(c(5, 2), c(NA, NA))
) %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  # save pre-processed file in archive for generation of past json files
  write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
}

apt_data_dy <- assign(mydataframe, df) %>%
  filter(R_RANK_BY_DAY <= 10 &
           R_RANK_BY_DAY != 0 &
           is.na(R_RANK_BY_DAY) == FALSE) %>%
  mutate(DY_RANK_DIF_PREV_WEEK = RANK_BY_DAY_7DAY - RANK_BY_DAY) %>%
  select(
    DY_R_RANK_BY_DAY = R_RANK_BY_DAY,
    DY_AIRPORT_NAME = AIRPORT_NAME,
    DY_TO_DATE = ENTRY_DATE,
    DY_DEP_ARR = DEP_ARR,
    DY_DIF_PREV_WEEK_PERC =  DEP_ARR_7DAY_DIFF_PERC,
    DY_DIF_PREV_YEAR_PERC =  DEP_ARR_PREV_YEAR_DIFF_PERC,
    DY_RANK_DIF_PREV_WEEK
    )

### week ----
mydataframe <- "nw_apt_week_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

} else {
  df <- read_xlsx(
  path = fs::path_abs(
    str_glue(nw_base_file),
    start = nw_base_dir
  ),
  sheet = "APT_WEEK_DATA",
  range = cell_limits(c(6, 1), c(NA, NA))
) %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  # save pre-processed file in archive for generation of past json files
  write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
}

apt_data_wk <- assign(mydataframe, df) %>%
  filter(R_RANK_BY_DAY <= 10 &
           R_RANK_BY_DAY != 0 &
           is.na(R_RANK_BY_DAY) == FALSE) %>%
  mutate(WK_RANK_DIF_PREV_WEEK = RANK_BY_DAY_7DAY - RANK_BY_DAY) %>%
  select(
    DY_R_RANK_BY_DAY = R_RANK_BY_DAY,
    WK_AIRPORT_NAME = APT_NAME,
    WK_FROM_DATE = MIN_ENTRY_DATE,
    WK_DAILY_DEP_ARR =  DAILY_TTF_DEP_ARR,
    WK_DIF_PREV_WEEK_PERC =  TTF_DEP_ARR_DIFF_7DAY_PERC,
    WK_DIF_PREV_YEAR_PERC =  TTF_DEP_ARR_DIFF_PREV_YY_PERC,
    WK_RANK_DIF_PREV_WEEK
  )

### y2d ----
mydataframe <- "nw_apt_y2d_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

} else {
  df <- read_xlsx(
  path = fs::path_abs(
    str_glue(nw_base_file),
    start = nw_base_dir
  ),
  sheet = "TOP40_APT_Y2D",
  range = cell_limits(c(5, 2), c(NA, 10))
) %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  # save pre-processed file in archive for generation of past json files
  write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
}

apt_data_y2d <- assign(mydataframe, df) %>%
  filter(R_RANK <= 10 & R_RANK != 0 & is.na(R_RANK) == FALSE) %>%
  mutate(FLAG_LAST_YEAR = case_when(
    YEAR == data_day_year ~ "Y2D_DEP_ARR_CURRENT_YEAR",
    YEAR == data_day_year - 1 ~ "Y2D_DEP_ARR_PREV_YEAR",
    .default = paste0("Y2D_DEP_ARR_", as.character(YEAR))
  )) %>%
  pivot_wider(id_cols = -c(YEAR, FROM_DATE,  TO_DATE), names_from = "FLAG_LAST_YEAR", values_from = DAILY_DEP_ARR) %>%
  mutate(Y2D_TO_DATE = data_day_date) %>%
  mutate(R_RANK = row_number(),
         Y2D_RANK_DIF_PREV_YEAR = RANK_PY - RANK,
         Y2D_DIF_PREV_YEAR_PERC = if_else(Y2D_DEP_ARR_PREV_YEAR == 0,
                                          0,
                                          Y2D_DEP_ARR_CURRENT_YEAR/Y2D_DEP_ARR_PREV_YEAR-1),
         Y2D_DIF_2019_PERC = if_else(Y2D_DEP_ARR_2019 == 0,
                                          0,
                                          Y2D_DEP_ARR_CURRENT_YEAR/Y2D_DEP_ARR_2019-1)
  ) %>%
  select(
    DY_R_RANK_BY_DAY = R_RANK,
    Y2D_AIRPORT_NAME = ARP_NAME,
    Y2D_DEP_ARR = Y2D_DEP_ARR_CURRENT_YEAR,
    Y2D_DIF_PREV_YEAR_PERC,
    Y2D_DIF_2019_PERC,
    Y2D_TO_DATE,
    Y2D_RANK_DIF_PREV_YEAR
  )

### main card ----
apt_main_traffic <- apt_data_dy %>%
  mutate(across(-DY_R_RANK_BY_DAY, ~ ifelse(DY_R_RANK_BY_DAY > 4, NA, .))) %>%
  select(DY_R_RANK_BY_DAY,
         MAIN_TFC_AIRPORT_NAME = DY_AIRPORT_NAME,
         MAIN_TFC_AIRPORT_DEP_ARR = DY_DEP_ARR)

apt_main_traffic_dif <- nw_apt_day_raw %>%
  filter(R_RANK_BY_DAY <= 40) %>%
  select(AIRPORT_NAME,
         DEP_ARR_7DAY_DIFF) %>%
  arrange(desc(abs(DEP_ARR_7DAY_DIFF))) %>%
  mutate(DY_R_RANK_BY_DAY = row_number()) %>%
  filter(DY_R_RANK_BY_DAY <= 10) %>%
  mutate(across(-DY_R_RANK_BY_DAY, ~ ifelse(DY_R_RANK_BY_DAY > 4, NA, .))) %>%
  arrange(abs(DEP_ARR_7DAY_DIFF)) %>%
  mutate(DY_R_RANK_BY_DAY = row_number()) %>%
  select(
    DY_R_RANK_BY_DAY,
    MAIN_TFC_DIF_AIRPORT_NAME = AIRPORT_NAME,
    MAIN_TFC_AIRPORT_DIF = DEP_ARR_7DAY_DIFF
  )

### merge and reorder tables ----
apt_data <- merge(x = apt_data_dy, y = apt_data_wk, by = "DY_R_RANK_BY_DAY")
apt_data <- merge(x = apt_data, y = apt_data_y2d, by = "DY_R_RANK_BY_DAY")
apt_data <- merge(x = apt_data, y = apt_main_traffic, by = "DY_R_RANK_BY_DAY")
apt_data <- merge(x = apt_data, y = apt_main_traffic_dif, by = "DY_R_RANK_BY_DAY")

apt_data <- apt_data %>%
  mutate(WK_TO_DATE = WK_FROM_DATE + 6) %>%
  select(
    RANK = DY_R_RANK_BY_DAY,
    MAIN_TFC_AIRPORT_NAME,
    MAIN_TFC_AIRPORT_DEP_ARR,
    MAIN_TFC_DIF_AIRPORT_NAME,
    MAIN_TFC_AIRPORT_DIF,
    DY_RANK_DIF_PREV_WEEK,
    DY_AIRPORT_NAME,
    DY_TO_DATE,
    DY_DEP_ARR,
    DY_DIF_PREV_WEEK_PERC,
    DY_DIF_PREV_YEAR_PERC,
    WK_RANK_DIF_PREV_WEEK,
    WK_AIRPORT_NAME,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_DAILY_DEP_ARR,
    WK_DIF_PREV_WEEK_PERC,
    WK_DIF_PREV_YEAR_PERC,
    Y2D_RANK_DIF_PREV_YEAR,
    Y2D_AIRPORT_NAME,
    Y2D_TO_DATE,
    Y2D_DEP_ARR,
    Y2D_DIF_PREV_YEAR_PERC,
    Y2D_DIF_2019_PERC
  )

### covert to json and save in app data folder and archive ----
apt_data_j <- apt_data %>% toJSON(., pretty = TRUE)
save_json(apt_data_j, "nw_apt_ranking_traffic")

## Country traffic DAI ----
### day----
mydataframe <- "nw_st_dai_day_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

} else {
  df <- read_xlsx(
  path = fs::path_abs(
    str_glue(nw_base_file),
    start = nw_base_dir
  ),
  sheet = "CTRY_DAI_DAY_DATA",
  range = cell_limits(c(4, 3), c(NA, NA))
) %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  # save pre-processed file in archive for generation of past json files
  write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
}

st_dai_data_dy <- assign(mydataframe, df) %>%
  pivot_wider(names_from = "FLAG_DAY", values_from = TOT_MVT) %>%
  mutate(R_RANK = as.numeric(R_RANK)) %>%
  filter(R_RANK <= 10 & R_RANK != 0 & is.na(R_RANK) == FALSE) %>%
  mutate(
    DY_RANK_DIF_PREV_WEEK = RANK_PREV_WEEK - RANK,
    DY_DIF_PREV_WEEK_PERC = if_else(DAY_PREV_WEEK ==0, 0, CURRENT_DAY/DAY_PREV_WEEK-1),
    DY_DIF_PREV_YEAR_PERC = if_else(DAY_PREV_YEAR ==0, 0, CURRENT_DAY/DAY_PREV_YEAR-1)
    ) %>%
  select(
    DY_R_RANK_BY_DAY = R_RANK,
    DY_COUNTRY_NAME = COUNTRY_NAME,
    DY_CTRY_ISO_CODE = ISO_COUNTRY_CODE,
    DY_TO_DATE = TO_DATE,
    DY_CTRY_DAI = CURRENT_DAY,
    DY_DIF_PREV_WEEK_PERC,
    DY_DIF_PREV_YEAR_PERC,
    DY_RANK_DIF_PREV_WEEK
)

### week ----
mydataframe <- "nw_st_dai_week_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

} else {
 df <- read_xlsx(
  path = fs::path_abs(
    str_glue(nw_base_file),
    start = nw_base_dir
  ),
  sheet = "CTRY_DAI_WK_DATA",
  range = cell_limits(c(4, 2), c(NA, NA))
) %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

 # save pre-processed file in archive for generation of past json files
 write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))

}

st_dai_data_wk <- assign(mydataframe, df) %>%
  mutate(R_RANK = as.numeric(R_RANK)) %>%
  pivot_wider(names_from = "FLAG_ROLLING_WEEK", values_from = TOT_MVT) %>%
  filter(R_RANK <= 10 & R_RANK != 0 & is.na(R_RANK) == FALSE) %>%
  mutate(
    WK_RANK_DIF_PREV_WEEK = RANK_PREV_WEEK - RANK,
    WK_CTRY_DAI = CURRENT_ROLLING_WEEK/7,
    WK_DIF_PREV_WEEK_PERC = if_else(PREV_ROLLING_WEEK == 0, 0, CURRENT_ROLLING_WEEK/PREV_ROLLING_WEEK-1),
    WK_DIF_PREV_YEAR_PERC = if_else(ROLLING_WEEK_PREV_YEAR == 0, 0, CURRENT_ROLLING_WEEK/ROLLING_WEEK_PREV_YEAR-1)
  ) %>%
  select(
    DY_R_RANK_BY_DAY = R_RANK,
    WK_COUNTRY_NAME = COUNTRY_NAME,
    WK_CTRY_ISO_CODE = ISO_COUNTRY_CODE,
    WK_FROM_DATE = FROM_DATE,
    WK_TO_DATE = TO_DATE,
    WK_CTRY_DAI,
    WK_DIF_PREV_WEEK_PERC,
    WK_DIF_PREV_YEAR_PERC,
    WK_RANK_DIF_PREV_WEEK
  )

### y2d----
mydataframe <- "nw_st_dai_y2d_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

} else {
  df <- read_xlsx(
  path = fs::path_abs(
    str_glue(nw_base_file),
    start = nw_base_dir
  ),
  sheet = "CTRY_DAI_Y2D_DATA",
  range = cell_limits(c(4, 2), c(NA, NA))
) %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  # save pre-processed file in archive for generation of past json files
  write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))

}

st_dai_data_y2d <- assign(mydataframe, df) %>%
  mutate(R_RANK = as.numeric(R_RANK),
         TO_DATE = max(TO_DATE, na.rm = TRUE),
         FLAG_YEAR = case_when(
           YEAR == data_day_year ~ "CURRENT_YEAR",
           YEAR == data_day_year-1 ~ "PREVIOUS_YEAR",
           .default = as.character(YEAR))) %>%
  pivot_wider(id_cols = -c(YEAR, TOT_MVT, FROM_DATE, NO_DAYS),
              names_from = "FLAG_YEAR",
              values_from = AVG_MVT,
              names_prefix = "AVG_MVT_") %>%
  filter(R_RANK <= 10 & R_RANK != 0 & is.na(R_RANK) == FALSE) %>%
  mutate(
    Y2D_RANK_DIF_PREV_YEAR = RANK_PREV_YEAR - RANK,
    Y2D_CTRY_DAI = AVG_MVT_CURRENT_YEAR ,
    Y2D_CTRY_DAI_PREV_YEAR_PERC = if_else(AVG_MVT_PREVIOUS_YEAR == 0, 0, AVG_MVT_CURRENT_YEAR /AVG_MVT_PREVIOUS_YEAR-1),
    Y2D_CTRY_DAI_2019_PERC = if_else(AVG_MVT_2019 == 0, 0, AVG_MVT_CURRENT_YEAR /AVG_MVT_2019-1)
  ) %>%
  select(
    DY_R_RANK_BY_DAY = R_RANK,
    Y2D_COUNTRY_NAME = COUNTRY_NAME,
    Y2D_CTRY_ISO_CODE = ISO_COUNTRY_CODE,
    Y2D_TO_DATE = TO_DATE,
    Y2D_CTRY_DAI,
    Y2D_CTRY_DAI_PREV_YEAR_PERC,
    Y2D_CTRY_DAI_2019_PERC,
    Y2D_RANK_DIF_PREV_YEAR
  )


### main card ----
st_main_traffic <- st_dai_data_dy %>%
  mutate(across(-DY_R_RANK_BY_DAY, ~ ifelse(DY_R_RANK_BY_DAY > 4, NA, .))) %>%
  select(DY_R_RANK_BY_DAY,
         MAIN_TFC_CTRY_NAME = DY_COUNTRY_NAME,
         MAIN_TFC_CTRY_DAI = DY_CTRY_DAI,
         MAIN_TFC_CTRY_CODE = DY_CTRY_ISO_CODE)

st_main_traffic_dif <- nw_st_dai_day_raw %>%
  pivot_wider(names_from = "FLAG_DAY", values_from = TOT_MVT) %>%
  mutate(MAIN_TFC_CTRY_DIF = CURRENT_DAY - DAY_PREV_WEEK) %>%
  select(COUNTRY_NAME, ISO_COUNTRY_CODE, MAIN_TFC_CTRY_DIF) %>%
  arrange(desc(abs(MAIN_TFC_CTRY_DIF))) %>%
  mutate(DY_R_RANK_BY_DAY = row_number()) %>%
  filter(DY_R_RANK_BY_DAY <= 10) %>%
  mutate(across(-DY_R_RANK_BY_DAY,
                ~ ifelse(DY_R_RANK_BY_DAY > 4, NA, .))) %>%
  arrange(abs(MAIN_TFC_CTRY_DIF)) %>%
  mutate(DY_R_RANK_BY_DAY = row_number()) %>%
  select(
    DY_R_RANK_BY_DAY,
    MAIN_TFC_DIF_CTRY_NAME = COUNTRY_NAME,
    MAIN_TFC_CTRY_DIF,
    MAIN_TFC_DIF_CTRY_CODE = ISO_COUNTRY_CODE
  )

### merge and reorder tables ----
st_dai_data <- merge(x = st_dai_data_dy, y = st_dai_data_wk, by = "DY_R_RANK_BY_DAY")
st_dai_data <- merge(x = st_dai_data, y = st_dai_data_y2d, by = "DY_R_RANK_BY_DAY")
st_dai_data <- merge(x = st_dai_data, y = st_main_traffic, by = "DY_R_RANK_BY_DAY")
st_dai_data <- merge(x = st_dai_data, y = st_main_traffic_dif, by = "DY_R_RANK_BY_DAY")

st_dai_data <- st_dai_data %>%
  relocate(c(
    RANK = DY_R_RANK_BY_DAY,
    MAIN_TFC_CTRY_NAME,
    MAIN_TFC_CTRY_DAI,
    MAIN_TFC_CTRY_CODE,
    MAIN_TFC_DIF_CTRY_NAME,
    MAIN_TFC_CTRY_DIF,
    MAIN_TFC_DIF_CTRY_CODE,
    DY_RANK_DIF_PREV_WEEK,
    DY_COUNTRY_NAME,
    DY_TO_DATE,
    DY_CTRY_DAI,
    DY_DIF_PREV_WEEK_PERC,
    DY_DIF_PREV_YEAR_PERC,
    WK_RANK_DIF_PREV_WEEK,
    WK_COUNTRY_NAME,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_CTRY_DAI,
    WK_DIF_PREV_WEEK_PERC,
    WK_DIF_PREV_YEAR_PERC,
    Y2D_RANK_DIF_PREV_YEAR,
    Y2D_COUNTRY_NAME,
    Y2D_TO_DATE,
    Y2D_CTRY_DAI,
    Y2D_CTRY_DAI_PREV_YEAR_PERC,
    Y2D_CTRY_DAI_2019_PERC,
    Y2D_RANK_DIF_PREV_YEAR
  ))

### covert to json and save in app data folder and archive ----
st_dai_data_j <- st_dai_data %>% toJSON(., pretty = TRUE)
save_json(st_dai_data_j, "nw_ctry_ranking_traffic_DAI")

## Airport delay -----
### raw data
mydataframe <- "nw_apt_delay_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

} else {
  df <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(nw_base_file),
      start = nw_base_dir),
    sheet = "APT_DELAY",
    range = cell_limits(c(5, 2), c(NA, 50))) %>%
    as_tibble() %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  # save pre-processed file in archive for generation of past json files
  write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
}

assign(mydataframe, df)

### day data ----
apt_rank_data_day <- nw_apt_delay_raw %>%
  arrange(desc(DLY_ARR), ARP_NAME) %>%
  mutate(
    R_RANK_DLY_DAY = row_number(),
    ARP_NAME_DAY = ARP_NAME,
    DLY_PER_FLT = ifelse(FLT_ARR == 0, 0, round(DLY_ARR / FLT_ARR, 2))
  ) %>%
  select(R_RANK_DLY_DAY, ARP_NAME_DAY, FLIGHT_DATE, DLY_ARR, DLY_PER_FLT) %>%
  as.data.frame() %>%
  filter(R_RANK_DLY_DAY <= 10)

### week ----
apt_rank_data_week <- nw_apt_delay_raw %>%
  arrange(desc(ROLL_WEEK_DLY_ARR), ARP_NAME) %>%
  mutate(
    R_RANK_DLY_DAY = row_number(),
    WK_RANK = R_RANK_DLY_DAY,
    ARP_NAME_WK = ARP_NAME,
    DLY_PER_FLT_WEEK = ifelse(ROLL_WEEK_ARR == 0, 0, round(ROLL_WEEK_DLY_ARR / ROLL_WEEK_ARR, 2)),
    WK_FROM_DATE = FLIGHT_DATE + days(-6),
    WK_TO_DATE = FLIGHT_DATE
  ) %>%
  select(
    R_RANK_DLY_DAY, WK_RANK, ARP_NAME_WK, WK_FROM_DATE, WK_TO_DATE,
    ROLL_WEEK_DLY_ARR, DLY_PER_FLT_WEEK
  ) %>%
  as.data.frame() %>%
  filter(R_RANK_DLY_DAY <= 10)

### y2d ----
apt_rank_data_y2d <- nw_apt_delay_raw %>%
  arrange(desc(Y2D_AVG_DLY_ARR), ARP_NAME) %>%
  mutate(
    R_RANK_DLY_DAY = row_number(),
    Y2D_RANK = R_RANK_DLY_DAY,
    ARP_NAME_Y2D = ARP_NAME,
    DLY_PER_FLT_Y2D = ifelse(Y2D_AVG_FLT == 0,
      0,
      round(Y2D_AVG_DLY / Y2D_AVG_ARR, 2)
    ),
    Y2D_TO_DATE = FLIGHT_DATE
  ) %>%
  select(R_RANK_DLY_DAY, Y2D_RANK, ARP_NAME_Y2D, Y2D_TO_DATE, Y2D_AVG_DLY_ARR, DLY_PER_FLT_Y2D) %>%
  as.data.frame()

### main card ----
apt_main_delay <- apt_rank_data_day %>%
  mutate(across(-R_RANK_DLY_DAY, ~ ifelse(R_RANK_DLY_DAY > 4, NA, .))) %>%
  select(R_RANK_DLY_DAY,
         MAIN_DLY_APT_NAME = ARP_NAME_DAY,
         MAIN_DLY_APT_DLY = DLY_ARR)

apt_main_delay_flt <- apt_rank_data_day %>%
  arrange(desc(DLY_PER_FLT), ARP_NAME_DAY) %>%
  mutate(R_RANK_DLY_DAY = row_number(),
         across(-R_RANK_DLY_DAY,
                ~ ifelse(R_RANK_DLY_DAY > 4, NA, .))) %>%
  select(R_RANK_DLY_DAY,
         MAIN_DLY_FLT_APT_NAME = ARP_NAME_DAY,
         MAIN_DLY_FLT_APT_DLY_FLT = DLY_PER_FLT)

### merge and reorder tables ----
apt_rank_data <- merge(x = apt_rank_data_day, y = apt_rank_data_week, by = "R_RANK_DLY_DAY")
apt_rank_data <- merge(x = apt_rank_data, y = apt_rank_data_y2d, by = "R_RANK_DLY_DAY")
apt_rank_data <- merge(x = apt_rank_data, y = apt_main_delay, by = "R_RANK_DLY_DAY")
apt_rank_data <- merge(x = apt_rank_data, y = apt_main_delay_flt, by = "R_RANK_DLY_DAY")

apt_rank_data <- apt_rank_data %>%
  select(
    RANK = R_RANK_DLY_DAY,
    MAIN_DLY_APT_NAME,
    MAIN_DLY_APT_DLY,
    MAIN_DLY_FLT_APT_NAME,
    MAIN_DLY_FLT_APT_DLY_FLT,
    DY_RANK = R_RANK_DLY_DAY,
    DY_AIRPORT_NAME = ARP_NAME_DAY,
    DY_TO_DATE = FLIGHT_DATE,
    DY_AIRPORT_DLY = DLY_ARR,
    DY_AIRPORT_DLY_PER_FLT = DLY_PER_FLT,
    WK_RANK,
    WK_AIRPORT_NAME = ARP_NAME_WK,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_AIRPORT_DLY = ROLL_WEEK_DLY_ARR,
    WK_AIRPORT_DLY_PER_FLT = DLY_PER_FLT_WEEK,
    Y2D_RANK,
    Y2D_AIRPORT_NAME = ARP_NAME_Y2D,
    Y2D_TO_DATE,
    Y2D_AIRPORT_DLY = Y2D_AVG_DLY_ARR,
    Y2D_AIRPORT_DLY_PER_FLT = DLY_PER_FLT_Y2D
  )

### covert to json and save in app data folder and archive ----
apt_rank_data_j <- apt_rank_data %>% toJSON(., pretty = TRUE)
save_json(apt_rank_data_j, "nw_apt_ranking_delay")

## ACC delay ----
### day ----
mydataframe <- "nw_acc_delay_day_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

} else {
  df <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(nw_base_file),
      start = nw_base_dir),
    sheet = "ACC_DAY_DELAY",
    range = cell_limits(c(5, 2), c(NA, 20))) %>%
    as_tibble() %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  # save pre-processed file in archive for generation of past json files
  write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
}

# process data
acc_rank_data_day_all <- assign(mydataframe, df) %>%
  left_join(distinct(acc, Name, ICAO_code), by = c("UNIT_CODE" = "ICAO_code")) %>%
  relocate(Name, .before = everything()) %>%
  rename(NAME = Name) %>%
  arrange(desc(DLY_ER), NAME) %>%
  mutate(
    DY_RANK = row_number(),
    DY_ACC_DLY_PER_FLT = DLY_ER / FLIGHT,
  ) %>%
  select(DY_RANK,
         R_RANK_DLY_DAY,
         DY_ACC_NAME = NAME,
         DY_TO_DATE = ENTRY_DATE,
         DY_ACC_DLY = DLY_ER,
         DY_ACC_DLY_PER_FLT)

acc_rank_data_day <- acc_rank_data_day_all %>%
  filter(DY_RANK <= 10)

### week ----
mydataframe <- "nw_acc_delay_week_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

} else {
  df <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(nw_base_file),
      start = nw_base_dir),
    sheet = "ACC_WEEK_DELAY",
    range = cell_limits(c(5, 2), c(NA, 16))) %>%
    as_tibble() %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  # save pre-processed file in archive for generation of past json files
  write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
}

# process data
acc_rank_data_week <- assign(mydataframe, df) %>%
  left_join(distinct(acc, Name, ICAO_code), by = c("UNIT_CODE" = "ICAO_code")) %>%
  relocate(Name, .before = everything()) %>%
  rename(NAME = Name) %>%

  arrange(desc(DAILY_DLY_ER), NAME) %>%
  mutate(
    DY_RANK = row_number(),
    WK_RANK = DY_RANK,
    WK_ACC_DLY_PER_FLT = DAILY_DLY_ER / DAILY_FLIGHT
  ) %>%
  select(DY_RANK,
         WK_RANK,
         WK_ACC_NAME = NAME,
         WK_FROM_DATE = MIN_ENTRY_DATE,
         WK_TO_DATE = MAX_ENTRY_DATE,
         WK_ACC_DLY = DAILY_DLY_ER,
         WK_ACC_DLY_PER_FLT) %>%
  filter(DY_RANK <= 10)

### y2d ----
mydataframe <- "nw_acc_delay_y2d_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

} else {
  df <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(nw_base_file),
      start = nw_base_dir),
    sheet = "ACC_Y2D_DELAY",
    range = cell_limits(c(7, 2), c(NA, 13))) %>%
    as_tibble() %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  # save pre-processed file in archive for generation of past json files
  write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
}

# process data
acc_rank_data_y2d <- assign(mydataframe, df) %>%
  rename(Y2D_FROM_DATE = MIN_DATE) %>%
  left_join(distinct(acc, Name, ICAO_code), by = c("UNIT_CODE" = "ICAO_code")) %>%
  relocate(Name, .before = everything()) %>%
  rename(NAME = Name) %>%

  arrange(desc(Y2D_AVG_DLY_ER), NAME) %>%
  mutate(
    DY_RANK = row_number(),
    Y2D_RANK = DY_RANK,
    Y2D_ACC_DLY_PER_FLT = Y2D_AVG_DLY_ER / Y2D_AVG_FLIGHT
  ) %>%
  select(DY_RANK,
         Y2D_RANK,
         Y2D_ACC_NAME = NAME,
         Y2D_TO_DATE = ENTRY_DATE,
         Y2D_ACC_DLY = Y2D_AVG_DLY_ER,
         Y2D_ACC_DLY_PER_FLT
         ) %>%
  filter(DY_RANK <= 10)

### main card ----
acc_main_delay <- acc_rank_data_day %>%
  mutate(across(-DY_RANK, ~ ifelse(DY_RANK > 4, NA, .))) %>%
  select(DY_RANK,
         MAIN_DLY_ACC_NAME = DY_ACC_NAME,
         MAIN_DLY_ACC_DLY = DY_ACC_DLY)

acc_main_delay_flt <- acc_rank_data_day_all %>%
  arrange(desc(DY_ACC_DLY_PER_FLT), DY_ACC_NAME) %>%
  mutate(
    DY_RANK = row_number(),
    across(-DY_RANK, ~ ifelse(DY_RANK > 4, NA, .))
    ) %>%
  select(DY_RANK,
         MAIN_DLY_FLT_ACC_NAME = DY_ACC_NAME,
         MAIN_DLY_FLT_ACC_DLY_FLT = DY_ACC_DLY_PER_FLT) %>%
  filter(DY_RANK <= 10)

### merge and reorder tables ----
acc_rank_data <- merge(x = acc_rank_data_day, y = acc_rank_data_week, by = "DY_RANK")
acc_rank_data <- merge(x = acc_rank_data, y = acc_rank_data_y2d, by = "DY_RANK")
acc_rank_data <- merge(x = acc_rank_data, y = acc_main_delay, by = "DY_RANK")
acc_rank_data <- merge(x = acc_rank_data, y = acc_main_delay_flt, by = "DY_RANK")

acc_rank_data <- acc_rank_data %>%
  select(
    RANK = DY_RANK,
    MAIN_DLY_ACC_NAME,
    MAIN_DLY_ACC_DLY,
    MAIN_DLY_FLT_ACC_NAME,
    MAIN_DLY_FLT_ACC_DLY_FLT,
    DY_RANK,
    DY_ACC_NAME,
    DY_TO_DATE,
    DY_ACC_DLY,
    DY_ACC_DLY_PER_FLT,
    WK_RANK,
    WK_ACC_NAME,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_ACC_DLY,
    WK_ACC_DLY_PER_FLT,
    Y2D_RANK,
    Y2D_ACC_NAME,
    Y2D_TO_DATE,
    Y2D_ACC_DLY,
    Y2D_ACC_DLY_PER_FLT
  )

### covert to json and save in app data folder and archive ----
acc_rank_data_j <- acc_rank_data %>% toJSON(., pretty = TRUE)
save_json(acc_rank_data_j, "nw_acc_ranking_delay")

## Country delay ----
### day ----
mydataframe <- "nw_st_delay_day_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

} else {
  df <- read_xlsx(
    path = fs::path_abs(
      str_glue(nw_base_file),
      start = nw_base_dir
    ),
    sheet = "CTRY_DLY_DAY",
    range = cell_limits(c(5, 2), c(NA, 5))
  ) %>%
    as_tibble() %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  # save pre-processed file in archive for generation of past json files
  write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
}

# process data
st_rank_delay_day <- assign(mydataframe, df) %>%
  left_join(distinct(state_iso, state, iso_2letter),
            by = c("DY_CTRY_DLY_NAME" = "state")) %>%
  mutate(DY_CTRY_DLY_CODE = case_when(
    DY_CTRY_DLY_NAME == "Maastricht" ~ "MUAC",
    DY_CTRY_DLY_NAME == "Serbia/Montenegro" ~ "RSME",
    .default = iso_2letter
  )) %>%
  arrange(desc(DY_CTRY_DLY), DY_CTRY_DLY_NAME) %>%
  mutate(
    RANK = row_number(),
    DY_RANK = RANK
  ) %>%
  filter(DY_RANK <= 10) %>%
  select(
    RANK,
    DY_RANK,
    DY_CTRY_DLY_NAME,
    DY_TO_DATE,
    DY_CTRY_DLY,
    DY_CTRY_DLY_PER_FLT,
    DY_CTRY_DLY_CODE
  )

### week ----
mydataframe <- "nw_st_delay_week_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

} else {
  df <- read_xlsx(
    path = fs::path_abs(
      str_glue(nw_base_file),
      start = nw_base_dir
    ),
    sheet = "CTRY_DLY_WK_DATA",
    range = cell_limits(c(5, 2), c(NA, NA))
  ) %>%
    as_tibble() %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  # save pre-processed file in archive for generation of past json files
  write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
}

# process data
st_rank_delay_week <- assign(mydataframe, df) %>%
  filter(PERIOD_TYPE == "1_ROL_WEEK") %>%
  arrange(desc(AVG_DELAY), COUNTRY_NAME) %>%
  mutate(
    DY_RANK = row_number(),
    WK_RANK = DY_RANK,
    WK_FROM_DATE = TO_DATE + lubridate::days(-6),
    WK_CTRY_DLY_PER_FLT = if_else(AVG_FLIGHT == 0, 0,AVG_DELAY/ AVG_FLIGHT)
  ) %>%
  filter(DY_RANK <= 10) %>%
  select(
    DY_RANK,
    WK_RANK,
    WK_CTRY_DLY_NAME = COUNTRY_NAME,
    WK_FROM_DATE,
    WK_TO_DATE = TO_DATE,
    WK_CTRY_DLY = AVG_DELAY,
    WK_CTRY_DLY_PER_FLT
  )

### y2d ----
mydataframe <- "nw_st_delay_y2d_raw"
myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
stakeholder <- str_sub(mydataframe, 1, 2)

if (archive_mode) {
  df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

} else {
  df <- read_xlsx(
    path = fs::path_abs(
      str_glue(nw_base_file),
      start = nw_base_dir
    ),
    sheet = "CTRY_DLY_Y2D_DATA",
    range = cell_limits(c(5, 2), c(NA, NA))
  ) %>%
    as_tibble() %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  # save pre-processed file in archive for generation of past json files
  write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
}

# process data
st_rank_delay_y2d <- assign(mydataframe, df) %>%
  filter(YEAR == data_day_year) %>%
  arrange(desc(AVG_DELAY), COUNTRY_NAME) %>%
  mutate(
    DY_RANK = row_number(),
    Y2D_RANK = DY_RANK,
    Y2D_CTRY_DLY_PER_FLT = if_else(AVG_FLIGHT == 0, 0,AVG_DELAY/ AVG_FLIGHT)
  ) %>%
  filter(DY_RANK <= 10) %>%
  select(
    DY_RANK,
    Y2D_RANK,
    Y2D_CTRY_DLY_NAME = COUNTRY_NAME,
    Y2D_TO_DATE = TO_DATE,
    Y2D_CTRY_DLY = AVG_DELAY,
    Y2D_CTRY_DLY_PER_FLT
  )

### main card ----
st_main_delay <- st_rank_delay_day %>%
  mutate(across(-DY_RANK, ~ ifelse(DY_RANK > 4, NA, .))) %>%
  select(DY_RANK,
         MAIN_DLY_CTRY_NAME = DY_CTRY_DLY_NAME,
         MAIN_DLY_CTRY_DLY = DY_CTRY_DLY,
         MAIN_DLY_CTRY_CODE = DY_CTRY_DLY_CODE)

st_main_delay_flt <- st_rank_delay_day %>%
  arrange(desc(DY_CTRY_DLY_PER_FLT), DY_CTRY_DLY_NAME) %>%
  mutate(DY_RANK = row_number(),
         across(-DY_RANK, ~ ifelse(DY_RANK > 4, NA, .))) %>%
  select(DY_RANK,
         MAIN_DLY_FLT_CTRY_NAME = DY_CTRY_DLY_NAME,
         MAIN_DLY_FLT_CTRY_DLY_FLT = DY_CTRY_DLY_PER_FLT,
         MAIN_DLY_FLT_CTRY_CODE = DY_CTRY_DLY_CODE)

### merge and reorder tables ----
st_rank_delay <- merge(x = st_rank_delay_day, y = st_rank_delay_week, by = "DY_RANK")
st_rank_delay <- merge(x = st_rank_delay, y = st_rank_delay_y2d, by = "DY_RANK")
st_rank_delay <- merge(x = st_rank_delay, y = st_main_delay, by = "DY_RANK")
st_rank_delay <- merge(x = st_rank_delay, y = st_main_delay_flt, by = "DY_RANK")

st_rank_delay <- st_rank_delay %>%
  select(
    RANK,
    MAIN_DLY_CTRY_NAME,
    MAIN_DLY_CTRY_DLY,
    MAIN_DLY_CTRY_CODE,
    MAIN_DLY_FLT_CTRY_NAME,
    MAIN_DLY_FLT_CTRY_DLY_FLT,
    MAIN_DLY_FLT_CTRY_CODE,
    DY_RANK,
    DY_CTRY_DLY_NAME,
    DY_TO_DATE,
    DY_CTRY_DLY,
    DY_CTRY_DLY_PER_FLT,
    WK_RANK,
    WK_CTRY_DLY_NAME,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_CTRY_DLY,
    WK_CTRY_DLY_PER_FLT,
    Y2D_RANK,
    Y2D_CTRY_DLY_NAME,
    Y2D_TO_DATE,
    Y2D_CTRY_DLY,
    Y2D_CTRY_DLY_PER_FLT
  )

### covert to json and save in app data folder and archive ----
st_rank_delay_j <- st_rank_delay %>% toJSON(., pretty = TRUE)
save_json(st_rank_delay_j, "nw_ctry_ranking_delay")

## Airport punctuality ----
if(exists("nw_apt_punct_raw") == FALSE) {
  nw_apt_punct_raw <- get_punct_data_apt()
}

last_punctuality_day <- min(max(nw_apt_punct_raw$DAY_DATE),
                            data_day_date, na.rm = TRUE)

### calc
apt_punct_calc <- nw_apt_punct_raw %>%
  group_by(DAY_DATE) %>%
  arrange(desc(ARR_PUNCTUALITY_PERCENTAGE), ARP_NAME) %>%
  mutate(RANK = row_number()) %>%
  ungroup() %>%
  # select(DAY_DATE, ARP_NAME, ARR_PUNCTUALITY_PERCENTAGE, RANK)
  group_by(ARP_NAME) %>%
  arrange(DAY_DATE) %>%
  mutate(
    DY_RANK_DIF_PREV_WEEK = lag(RANK, 7) - RANK,
    DY_PUNCT_DIF_PREV_WEEK_PERC = (ARR_PUNCTUALITY_PERCENTAGE - lag(ARR_PUNCTUALITY_PERCENTAGE, 7)) / 100,
    DY_PUNCT_DIF_PREV_YEAR_PERC = (ARR_PUNCTUALITY_PERCENTAGE - lag(ARR_PUNCTUALITY_PERCENTAGE, 364)) / 100,
    WK_APT_ARR_PUNCT = rollsum(ARR_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") / rollsum(ARR_SCHEDULE_FLIGHT, 7, fill = NA, align = "right")
  ) %>%
  ungroup()

### day ----
#### top
apt_punct_dy_top <- apt_punct_calc %>%
  filter(DAY_DATE == last_punctuality_day, RANK < 11) %>%
  mutate(
   DY_APT_NAME = ARP_NAME,
    DY_APT_ARR_PUNCT = ARR_PUNCTUALITY_PERCENTAGE / 100,
    DY_TO_DATE = round_date(DAY_DATE, "day")
  ) %>%
  select(
    RANK,
    DY_RANK_DIF_PREV_WEEK,
    DY_APT_NAME,
    DY_TO_DATE,
    DY_APT_ARR_PUNCT,
    DY_PUNCT_DIF_PREV_WEEK_PERC,
    DY_PUNCT_DIF_PREV_YEAR_PERC
  )

#### bottom
apt_punct_dy_bottom <- apt_punct_calc %>%
  filter(DAY_DATE == last_punctuality_day) %>%
  arrange(ARR_PUNCTUALITY_PERCENTAGE, ARP_NAME) %>%
  mutate(
    RANK = max(RANK) + 1 - RANK,
    DY_APT_NAME_BOTTOM = ARP_NAME,
    DY_APT_ARR_PUNCT_BOTTOM = ARR_PUNCTUALITY_PERCENTAGE / 100,
    DY_TO_DATE_BOTTOM = round_date(DAY_DATE, "day")
  ) %>%
  filter(RANK < 11) %>%
  select(
    RANK,
    DY_RANK_DIF_PREV_WEEK_BOTTOM = DY_RANK_DIF_PREV_WEEK,
    DY_APT_NAME_BOTTOM,
    DY_TO_DATE_BOTTOM,
    DY_APT_ARR_PUNCT_BOTTOM,
    DY_PUNCT_DIF_PREV_WEEK_PERC_BOTTOM = DY_PUNCT_DIF_PREV_WEEK_PERC,
    DY_PUNCT_DIF_PREV_YEAR_PERC_BOTTOM = DY_PUNCT_DIF_PREV_YEAR_PERC
  )

### week ----
apt_punct_wk <- apt_punct_calc %>%
  group_by(DAY_DATE) %>%
  arrange(desc(WK_APT_ARR_PUNCT), ARP_NAME) %>%
  mutate(
    RANK = row_number(),
    WK_RANK = RANK
  ) %>%
  ungroup() %>%
  group_by(ARP_NAME) %>%
  arrange(DAY_DATE) %>%
  mutate(
    WK_RANK_DIF_PREV_WEEK = lag(RANK, 7) - RANK,
    WK_PUNCT_DIF_PREV_WEEK_PERC = (WK_APT_ARR_PUNCT - lag(WK_APT_ARR_PUNCT, 7)),
    WK_PUNCT_DIF_PREV_YEAR_PERC = (WK_APT_ARR_PUNCT - lag(WK_APT_ARR_PUNCT, 364))
  ) %>%
  ungroup()

apt_punct_wk_top <- apt_punct_wk %>%
  filter(DAY_DATE == last_punctuality_day,
         RANK < 11) %>%
  mutate(
    WK_APT_NAME = ARP_NAME,
    WK_FROM_DATE = round_date(DAY_DATE, "day") + lubridate::days(-6),
    WK_TO_DATE = round_date(DAY_DATE, "day")
  ) %>%
  select(
    RANK,
    WK_RANK_DIF_PREV_WEEK,
    WK_APT_NAME,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_APT_ARR_PUNCT,
    WK_PUNCT_DIF_PREV_WEEK_PERC,
    WK_PUNCT_DIF_PREV_YEAR_PERC
  )

#### bottom
apt_punct_wk_bottom <- apt_punct_wk %>%
  filter(DAY_DATE == last_punctuality_day) %>%
  mutate(
    RANK = max(RANK) + 1 - RANK,
    WK_APT_NAME_BOTTOM = ARP_NAME,
    WK_FROM_DATE_BOTTOM = round_date(DAY_DATE, "day") + lubridate::days(-6),
    WK_TO_DATE_BOTTOM = round_date(DAY_DATE, "day")
  ) %>%
  filter(RANK < 11) %>%
  arrange(RANK) %>%
  select(
    RANK,
    WK_RANK_DIF_PREV_WEEK_BOTTOM = WK_RANK_DIF_PREV_WEEK,
    WK_APT_NAME_BOTTOM,
    WK_FROM_DATE_BOTTOM,
    WK_TO_DATE_BOTTOM,
    WK_APT_ARR_PUNCT_BOTTOM = WK_APT_ARR_PUNCT,
    WK_PUNCT_DIF_PREV_WEEK_PERC_BOTTOM = WK_PUNCT_DIF_PREV_WEEK_PERC,
    WK_PUNCT_DIF_PREV_YEAR_PERC_BOTTOM = WK_PUNCT_DIF_PREV_YEAR_PERC
  )

### y2d ----
apt_punct_y2d <- apt_punct_calc %>%
  mutate(MONTH_DAY = as.numeric(format(DAY_DATE, format = "%m%d"))) %>%
  filter(MONTH_DAY <= as.numeric(format(last_punctuality_day, format = "%m%d"))) %>%
  mutate(YEAR = as.numeric(format(DAY_DATE, format = "%Y"))) %>%
  group_by(ARP_NAME, ARP_CODE, YEAR) %>%
  summarise(Y2D_APT_ARR_PUNCT = sum(ARR_PUNCTUAL_FLIGHTS, na.rm = TRUE) / sum(ARR_SCHEDULE_FLIGHT, na.rm = TRUE)) %>%
  ungroup() %>%
  group_by(YEAR) %>%
  arrange(desc(Y2D_APT_ARR_PUNCT), ARP_NAME) %>%
  mutate(
    RANK = row_number(),
    Y2D_RANK = RANK
  ) %>%
  ungroup() %>%
  group_by(ARP_NAME, ARP_CODE) %>%
  arrange(YEAR) %>%
  mutate(
    Y2D_RANK_DIF_PREV_YEAR = lag(RANK, 1) - RANK,
    Y2D_PUNCT_DIF_PREV_YEAR_PERC = (Y2D_APT_ARR_PUNCT - lag(Y2D_APT_ARR_PUNCT, 1)),
    Y2D_PUNCT_DIF_2019_PERC = (Y2D_APT_ARR_PUNCT - lag(Y2D_APT_ARR_PUNCT, max(YEAR) - 2019))
  ) %>%
  ungroup()

#### top
apt_punct_y2d_top <- apt_punct_y2d %>%
  filter(YEAR == max(YEAR), RANK < 11) %>%
  mutate(Y2D_APT_NAME = ARP_NAME) %>%
  select(
    RANK,
    Y2D_RANK_DIF_PREV_YEAR,
    Y2D_APT_NAME,
    Y2D_APT_ARR_PUNCT,
    Y2D_PUNCT_DIF_PREV_YEAR_PERC,
    Y2D_PUNCT_DIF_2019_PERC
  )

#### bottom
apt_punct_y2d_bottom <- apt_punct_y2d %>%
  filter(YEAR == max(YEAR)) %>%
  mutate(RANK = max(RANK) + 1 - RANK) %>%
  filter(RANK < 11) %>%
  arrange(RANK) %>%
  select(
    RANK,
    Y2D_RANK_DIF_PREV_YEAR_BOTTOM = Y2D_RANK_DIF_PREV_YEAR,
    Y2D_APT_NAME_BOTTOM = ARP_NAME,
    Y2D_APT_ARR_PUNCT_BOTTOM = Y2D_APT_ARR_PUNCT,
    Y2D_PUNCT_DIF_PREV_YEAR_PERC_BOTTOM = Y2D_PUNCT_DIF_PREV_YEAR_PERC,
    Y2D_PUNCT_DIF_2019_PERC_BOTTOM = Y2D_PUNCT_DIF_2019_PERC
  )

### main card ----
apt_main_punct_top <- apt_punct_dy_top %>%
  mutate(across(-RANK, ~ ifelse(RANK > 4, NA, .))) %>%
  select(RANK,
         MAIN_PUNCT_APT_NAME = DY_APT_NAME,
         MAIN_PUNCT_APT_ARR_PUNCT = DY_APT_ARR_PUNCT)

apt_main_punct_bottom <- apt_punct_calc %>%
  filter(DAY_DATE == last_punctuality_day) %>%
  mutate(
    RANK = max(RANK) + 1 - RANK,
    DY_APT_NAME = ARP_NAME,
    DY_APT_ARR_PUNCT = ARR_PUNCTUALITY_PERCENTAGE / 100
  ) %>%
  mutate(across(-RANK, ~ ifelse(RANK > 4, NA, .))) %>%
  select(RANK,
         MAIN_PUNCT_APT_NAME_BOTTOM = DY_APT_NAME,
         MAIN_PUNCT_APT_ARR_PUNCT_BOTTOM = DY_APT_ARR_PUNCT) %>%
  filter(RANK < 11) %>%
  arrange(RANK)

### merge and reorder tables ----
apt_punct_data <- merge(x = apt_punct_dy_top, y = apt_punct_wk_top, by = "RANK")
apt_punct_data <- merge(x = apt_punct_data, y = apt_punct_y2d_top, by = "RANK")
apt_punct_data <- merge(x = apt_punct_data, y = apt_main_punct_top, by = "RANK")
apt_punct_data <- merge(x = apt_punct_data, y = apt_main_punct_bottom, by = "RANK")

apt_punct_data <- apt_punct_data %>%
  mutate(Y2D_TO_DATE = DY_TO_DATE) %>%
  select(
    RANK,
    MAIN_PUNCT_APT_NAME,
    MAIN_PUNCT_APT_ARR_PUNCT,
    MAIN_PUNCT_APT_NAME_BOTTOM,
    MAIN_PUNCT_APT_ARR_PUNCT_BOTTOM,
    DY_RANK_DIF_PREV_WEEK,
    DY_APT_NAME,
    DY_TO_DATE,
    DY_APT_ARR_PUNCT,
    DY_PUNCT_DIF_PREV_WEEK_PERC,
    DY_PUNCT_DIF_PREV_YEAR_PERC,
    WK_RANK_DIF_PREV_WEEK,
    WK_APT_NAME,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_APT_ARR_PUNCT,
    WK_PUNCT_DIF_PREV_WEEK_PERC,
    WK_PUNCT_DIF_PREV_YEAR_PERC,
    Y2D_RANK_DIF_PREV_YEAR,
    Y2D_APT_NAME,
    Y2D_TO_DATE,
    Y2D_APT_ARR_PUNCT,
    Y2D_PUNCT_DIF_PREV_YEAR_PERC,
    Y2D_PUNCT_DIF_2019_PERC
  )

### add bottom ranking at the end of the df
apt_punct_data <- merge(x = apt_punct_data, y = apt_punct_dy_bottom, by = "RANK")
apt_punct_data <- merge(x = apt_punct_data, y = apt_punct_wk_bottom, by = "RANK")
apt_punct_data <- merge(x = apt_punct_data, y = apt_punct_y2d_bottom, by = "RANK")

apt_punct_data <- apt_punct_data %>%
mutate(Y2D_TO_DATE_BOTTOM = Y2D_TO_DATE) %>%
relocate (Y2D_TO_DATE_BOTTOM, .before = Y2D_APT_ARR_PUNCT_BOTTOM)

### covert to json and save in app data folder and archive ----
apt_punct_data_j <- apt_punct_data %>% toJSON(., pretty = TRUE)
save_json(apt_punct_data_j, "nw_apt_ranking_punctuality")

## Country punctuality ----
if(exists("st_punct_raw") == FALSE) {
  st_punct_raw <- get_punct_data_state()
}

st_punct_raw <- st_punct_raw %>%
  as_tibble() %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
  filter(
    ISO_CT_CODE != "GI",
    ISO_CT_CODE != "FO",
    ISO_CT_CODE != "SJ",
    ISO_CT_CODE != "MC",
    ISO_CT_CODE != "PM",
    ISO_CT_CODE != "UA",
    ISO_CT_CODE != "SM"
  ) %>%
  mutate_at("EC_ISO_CT_NAME", ~ if_else(. == "Turkiye", "Türkiye", .))

last_punctuality_day <- min(max(st_punct_raw$DATE),
                            data_day_date, na.rm = TRUE)

### calc
st_punct_calc <- st_punct_raw %>%
group_by(DATE) %>%
arrange(desc(ARR_PUNCTUALITY_PERCENTAGE), EC_ISO_CT_NAME) %>%
mutate(RANK = row_number()) %>%
ungroup() %>%
group_by(EC_ISO_CT_NAME) %>%
arrange(DATE) %>%
mutate(
  DY_RANK_DIF_PREV_WEEK = lag(RANK, 7) - RANK,
  DY_PUNCT_DIF_PREV_WEEK_PERC = (ARR_PUNCTUALITY_PERCENTAGE - lag(ARR_PUNCTUALITY_PERCENTAGE, 7)) / 100,
  DY_PUNCT_DIF_PREV_YEAR_PERC = (ARR_PUNCTUALITY_PERCENTAGE - lag(ARR_PUNCTUALITY_PERCENTAGE, 364)) / 100,
  WK_CTRY_ARR_PUNCT = rollsum(ARR_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") / rollsum(ARR_SCHEDULE_FLIGHT, 7, fill = NA, align = "right")
) %>%
ungroup()

### day ----
####top
st_punct_dy_calc_top <- st_punct_calc %>%
filter(DATE == last_punctuality_day, RANK < 11) %>%
mutate(
  DY_CTRY_NAME = EC_ISO_CT_NAME,
  DY_CTRY_ARR_PUNCT = ARR_PUNCTUALITY_PERCENTAGE / 100,
  DY_TO_DATE = round_date(DATE, "day")
)

st_punct_dy_top <- st_punct_dy_calc_top %>%
select(
  RANK,
  DY_RANK_DIF_PREV_WEEK,
  DY_CTRY_NAME,
  DY_TO_DATE,
  DY_CTRY_ARR_PUNCT,
  DY_PUNCT_DIF_PREV_WEEK_PERC,
  DY_PUNCT_DIF_PREV_YEAR_PERC
)

####bottom
st_punct_dy_calc_bottom <- st_punct_calc %>%
filter(DATE == last_punctuality_day,
       RANK > max(RANK) - 11) %>%
arrange(ARR_PUNCTUALITY_PERCENTAGE) %>%
mutate(
  RANK = max(RANK) + 1 - RANK,
  DY_CTRY_NAME_BOTTOM = EC_ISO_CT_NAME,
  DY_CTRY_ARR_PUNCT_BOTTOM = ARR_PUNCTUALITY_PERCENTAGE / 100,
  DY_TO_DATE_BOTTOM = round_date(DATE, "day")
)

st_punct_dy_bottom <-st_punct_dy_calc_bottom %>%
select(
  RANK,
  DY_RANK_DIF_PREV_WEEK_BOTTOM = DY_RANK_DIF_PREV_WEEK,
  DY_CTRY_NAME_BOTTOM,
  DY_TO_DATE_BOTTOM,
  DY_CTRY_ARR_PUNCT_BOTTOM,
  DY_PUNCT_DIF_PREV_WEEK_PERC_BOTTOM = DY_PUNCT_DIF_PREV_WEEK_PERC,
  DY_PUNCT_DIF_PREV_YEAR_PERC_BOTTOM = DY_PUNCT_DIF_PREV_YEAR_PERC
)

### week ----
st_punct_wk <- st_punct_calc %>%
group_by(DATE) %>%
arrange(desc(WK_CTRY_ARR_PUNCT), EC_ISO_CT_NAME) %>%
mutate(
  RANK = row_number(),
  WK_RANK = RANK
) %>%
ungroup() %>%
group_by(EC_ISO_CT_NAME) %>%
arrange(DATE) %>%
mutate(
  WK_RANK_DIF_PREV_WEEK = lag(RANK, 7) - RANK,
  WK_PUNCT_DIF_PREV_WEEK_PERC = (WK_CTRY_ARR_PUNCT - lag(WK_CTRY_ARR_PUNCT, 7)),
  WK_PUNCT_DIF_PREV_YEAR_PERC = (WK_CTRY_ARR_PUNCT - lag(WK_CTRY_ARR_PUNCT, 364))
) %>%
ungroup()

#### top
st_punct_wk_top <- st_punct_wk %>%
filter(DATE == last_punctuality_day, RANK < 11) %>%
mutate(
  WK_CTRY_NAME = EC_ISO_CT_NAME,
  WK_FROM_DATE = round_date(DATE, "day") + lubridate::days(-6),
  WK_TO_DATE = round_date(DATE, "day")
) %>%
select(
  RANK,
  WK_RANK_DIF_PREV_WEEK,
  WK_CTRY_NAME,
  WK_FROM_DATE,
  WK_TO_DATE,
  WK_CTRY_ARR_PUNCT,
  WK_PUNCT_DIF_PREV_WEEK_PERC,
  WK_PUNCT_DIF_PREV_YEAR_PERC
)

#### bottom
st_punct_wk_bottom <- st_punct_wk %>%
filter(DATE == last_punctuality_day,
       RANK > max(RANK) - 11) %>%
arrange(WK_CTRY_ARR_PUNCT, EC_ISO_CT_NAME) %>%
mutate(
  RANK = max(RANK) + 1 - RANK,
  WK_CTRY_NAME_BOTTOM = EC_ISO_CT_NAME,
  WK_FROM_DATE_BOTTOM = round_date(DATE, "day") + lubridate::days(-6),
  WK_TO_DATE_BOTTOM = round_date(DATE, "day")
) %>%
select(
  RANK,
  WK_RANK_DIF_PREV_WEEK_BOTTOM = WK_RANK_DIF_PREV_WEEK,
  WK_CTRY_NAME_BOTTOM,
  WK_FROM_DATE_BOTTOM,
  WK_TO_DATE_BOTTOM,
  WK_CTRY_ARR_PUNCT_BOTTOM = WK_CTRY_ARR_PUNCT,
  WK_PUNCT_DIF_PREV_WEEK_PERC_BOTTOM = WK_PUNCT_DIF_PREV_WEEK_PERC,
  WK_PUNCT_DIF_PREV_YEAR_PERC_BOTTOM = WK_PUNCT_DIF_PREV_YEAR_PERC
)

### y2d ----
st_punct_y2d <- st_punct_calc %>%
mutate(MONTH_DAY = as.numeric(format(DATE, format = "%m%d"))) %>%
filter(MONTH_DAY <= as.numeric(format(last_punctuality_day, format = "%m%d"))) %>%
mutate(YEAR = as.numeric(format(DATE, format = "%Y"))) %>%
group_by(EC_ISO_CT_NAME, YEAR) %>%
summarise(Y2D_CTRY_ARR_PUNCT = sum(ARR_PUNCTUAL_FLIGHTS, na.rm = TRUE) / sum(ARR_SCHEDULE_FLIGHT, na.rm = TRUE)) %>%
ungroup() %>%
group_by(YEAR) %>%
arrange(desc(Y2D_CTRY_ARR_PUNCT), EC_ISO_CT_NAME) %>%
mutate(
  RANK = row_number(),
  Y2D_RANK = RANK
) %>%
ungroup() %>%
group_by(EC_ISO_CT_NAME) %>%
arrange(YEAR) %>%
mutate(
  Y2D_RANK_DIF_PREV_YEAR = lag(RANK, 1) - RANK,
  Y2D_PUNCT_DIF_PREV_YEAR_PERC = (Y2D_CTRY_ARR_PUNCT - lag(Y2D_CTRY_ARR_PUNCT, 1)),
  Y2D_PUNCT_DIF_2019_PERC = (Y2D_CTRY_ARR_PUNCT - lag(Y2D_CTRY_ARR_PUNCT, max(YEAR) - 2019))
) %>%
ungroup()

#### top
st_punct_y2d_top <- st_punct_y2d %>%
filter(YEAR == max(YEAR), RANK < 11) %>%
mutate(Y2D_CTRY_NAME = EC_ISO_CT_NAME) %>%
select(
  RANK,
  Y2D_RANK_DIF_PREV_YEAR,
  Y2D_CTRY_NAME,
  Y2D_CTRY_ARR_PUNCT,
  Y2D_PUNCT_DIF_PREV_YEAR_PERC,
  Y2D_PUNCT_DIF_2019_PERC
)

#### bottom
st_punct_y2d_bottom <- st_punct_y2d %>%
filter(YEAR == max(YEAR),
       RANK > max(RANK) - 11) %>%
arrange(Y2D_CTRY_ARR_PUNCT, EC_ISO_CT_NAME) %>%
mutate(
  RANK = max(RANK) + 1 - RANK,
  Y2D_CTRY_NAME_BOTTOM = EC_ISO_CT_NAME
  ) %>%
select(
  RANK,
  Y2D_RANK_DIF_PREV_YEAR_BOTTOM = Y2D_RANK_DIF_PREV_YEAR,
  Y2D_CTRY_NAME_BOTTOM,
  Y2D_CTRY_ARR_PUNCT_BOTTOM = Y2D_CTRY_ARR_PUNCT,
  Y2D_PUNCT_DIF_PREV_YEAR_PERC_BOTTOM = Y2D_PUNCT_DIF_PREV_YEAR_PERC,
  Y2D_PUNCT_DIF_2019_PERC_BOTTOM = Y2D_PUNCT_DIF_2019_PERC
)

### main card ----
st_main_punct_top <- st_punct_dy_calc_top %>%
  mutate(across(-RANK, ~ ifelse(RANK > 4, NA, .))) %>%
  select(RANK,
         MAIN_PUNCT_CTRY_NAME = DY_CTRY_NAME,
         MAIN_PUNCT_CTRY_ARR_PUNCT = DY_CTRY_ARR_PUNCT,
         MAIN_PUNCT_CTRY_CODE = ISO_CT_CODE)

st_main_punct_bottom <- st_punct_calc %>%
filter(DATE == last_punctuality_day) %>%
mutate(
  RANK = max(RANK) + 1 - RANK,
  DY_CTRY_NAME = EC_ISO_CT_NAME,
  DY_CTRY_ARR_PUNCT = ARR_PUNCTUALITY_PERCENTAGE / 100
) %>%
  mutate(across(-RANK, ~ ifelse(RANK > 4, NA, .))) %>%
  select(RANK,
         MAIN_PUNCT_CTRY_NAME_BOTTOM = DY_CTRY_NAME,
         MAIN_PUNCT_CTRY_ARR_PUNCT_BOTTOM = DY_CTRY_ARR_PUNCT,
         MAIN_PUNCT_CTRY_CODE_BOTTOM = ISO_CT_CODE) %>%
  filter(RANK < 11) %>%
  arrange(RANK)

### merge and reorder tables ----
st_punct_data <- merge(x = st_punct_dy_top, y = st_punct_wk_top, by = "RANK")
st_punct_data <- merge(x = st_punct_data, y = st_punct_y2d_top, by = "RANK")
st_punct_data <- merge(x = st_punct_data, y = st_main_punct_top, by = "RANK")
st_punct_data <- merge(x = st_punct_data, y = st_main_punct_bottom, by = "RANK")

st_punct_data <- st_punct_data %>%
mutate(Y2D_TO_DATE = DY_TO_DATE) %>%
select(
  RANK,
  MAIN_PUNCT_CTRY_NAME,
  MAIN_PUNCT_CTRY_ARR_PUNCT,
  MAIN_PUNCT_CTRY_CODE,
  MAIN_PUNCT_CTRY_NAME_BOTTOM,
  MAIN_PUNCT_CTRY_ARR_PUNCT_BOTTOM,
  MAIN_PUNCT_CTRY_CODE_BOTTOM,
  DY_RANK_DIF_PREV_WEEK,
  DY_CTRY_NAME,
  DY_TO_DATE,
  DY_CTRY_ARR_PUNCT,
  DY_PUNCT_DIF_PREV_WEEK_PERC,
  DY_PUNCT_DIF_PREV_YEAR_PERC,
  WK_RANK_DIF_PREV_WEEK,
  WK_CTRY_NAME,
  WK_FROM_DATE,
  WK_TO_DATE,
  WK_CTRY_ARR_PUNCT,
  WK_PUNCT_DIF_PREV_WEEK_PERC,
  WK_PUNCT_DIF_PREV_YEAR_PERC,
  Y2D_RANK_DIF_PREV_YEAR,
  Y2D_CTRY_NAME,
  Y2D_TO_DATE,
  Y2D_CTRY_ARR_PUNCT,
  Y2D_PUNCT_DIF_PREV_YEAR_PERC,
  Y2D_PUNCT_DIF_2019_PERC
)

### add bottom ranking at the end of the df
st_punct_data <- merge(x = st_punct_data, y = st_punct_dy_bottom, by = "RANK")
st_punct_data <- merge(x = st_punct_data, y = st_punct_wk_bottom, by = "RANK")
st_punct_data <- merge(x = st_punct_data, y = st_punct_y2d_bottom, by = "RANK")

st_punct_data <- st_punct_data %>%
mutate(Y2D_TO_DATE_BOTTOM = Y2D_TO_DATE) %>%
relocate (Y2D_TO_DATE_BOTTOM, .before = Y2D_CTRY_ARR_PUNCT_BOTTOM)

### covert to json and save in app data folder and archive ----
st_punct_data_j <- st_punct_data %>% toJSON(., pretty = TRUE)
save_json(st_punct_data_j, "nw_ctry_ranking_punctuality")

