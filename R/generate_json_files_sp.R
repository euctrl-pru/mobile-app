## libraries
library(data.table)
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

# functions ----
source(here::here("..", "mobile-app", "R", "helpers.R"))

# Parameters ----
source(here("..", "mobile-app", "R", "params.R"))

if (exists("ansp_list") == FALSE) {
  ansp_list <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(sp_base_file),
      start = sp_base_dir),
    sheet = "lists",
    range = cell_limits(c(1, 1), c(NA, NA))) %>%
    as_tibble() %>%
    select(-PRU_ID)
}

# archive mode for past dates
if (exists("archive_mode") == FALSE) {archive_mode <- FALSE}
if (exists("data_day_date") == FALSE) {
  data_day_date <- lubridate::today(tzone = "") +  days(-1)
}

data_day_text <- data_day_date %>% format("%Y%m%d")
data_day_year <- as.numeric(format(data_day_date,'%Y'))

sp_json_app <-""

# ____________________________________________________________________________________________
#
#    ANSP landing page -----
#
# ____________________________________________________________________________________________

#### Traffic data ----
sp_traffic_data <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(sp_base_file),
      start = sp_base_dir),
    sheet = "ansp_traffic",
    range = cell_limits(c(1, 1), c(NA, NA))) %>%
    as_tibble() %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
  left_join(ansp_list, by = c("ANSP_NAME"))


sp_traffic_calc <- sp_traffic_data %>%
  mutate(
    DY_TFC_DIF_PREV_YEAR_PERC = if_else(DAY_FLT_DAIO_PREV_YEAR == 0 | is.na(DAY_FLT_DAIO_PREV_YEAR),
                                        NA,
                                        DAY_FLT_DAIO / DAY_FLT_DAIO_PREV_YEAR -1),
    DY_TFC_DIF_2019_PERC = if_else(DAY_FLT_DAIO_2019 == 0 | is.na(DAY_FLT_DAIO_2019),
                                   NA,
                                   DAY_FLT_DAIO / DAY_FLT_DAIO_2019 -1),

    WK_TFC_DIF_PREV_YEAR_PERC = if_else(RW_AVG_FLT_DAIO_PREV_YEAR == 0 | is.na(RW_AVG_FLT_DAIO_PREV_YEAR),
                                        NA,
                                        RW_AVG_FLT_DAIO / RW_AVG_FLT_DAIO_PREV_YEAR -1),
    WK_TFC_DIF_2019_PERC = if_else(RW_AVG_FLT_DAIO_2019 == 0 | is.na(RW_AVG_FLT_DAIO_2019),
                                   NA,
                                   RW_AVG_FLT_DAIO / RW_AVG_FLT_DAIO_2019 -1),

    Y2D_TFC_DIF_PREV_YEAR_PERC = if_else(Y2D_FLT_DAIO_PREV_YEAR == 0 | is.na(Y2D_FLT_DAIO_PREV_YEAR),
                                         NA,
                                         Y2D_FLT_DAIO_YEAR / Y2D_FLT_DAIO_PREV_YEAR -1),
    Y2D_TFC_DIF_2019_PERC = if_else(Y2D_FLT_DAIO_2019 == 0 | is.na(Y2D_FLT_DAIO_2019),
                                    NA,
                                    Y2D_FLT_DAIO_YEAR / Y2D_FLT_DAIO_2019 -1)
  )

sp_traffic_last_day <- sp_traffic_calc %>%
  filter(ENTRY_DATE == min(data_day_date,
                            max(LAST_DATA_DAY, na.rm = TRUE),
                            na.rm = TRUE)
  ) %>%
  arrange(ANSP_NAME, ENTRY_DATE)

sp_traffic_for_json <- sp_traffic_last_day %>%
  select(
    ANSP_NAME,
    ANSP_CODE,
    FLIGHT_DATE = ENTRY_DATE,
    DY_TFC = DAY_FLT_DAIO,
    DY_TFC_DIF_PREV_YEAR_PERC,
    DY_TFC_DIF_2019_PERC,
    RWK_AVG_TFC = RW_AVG_FLT_DAIO,
    WK_TFC_DIF_PREV_YEAR_PERC,
    WK_TFC_DIF_2019_PERC,
    Y2D_TFC_YEAR = Y2D_FLT_DAIO_YEAR,Y2D_AVG_TFC_YEAR = Y2D_AVG_FLT_DAIO_YEAR,
    Y2D_TFC_DIF_PREV_YEAR_PERC,
    Y2D_TFC_DIF_2019_PERC
  )


#### Join strings and save  ----

sp_json_app_j <- ansp_list %>% arrange(ANSP_NAME)
sp_json_app_j$sp_traffic <- select(arrange(sp_traffic_for_json, ANSP_NAME), -c(ANSP_CODE, ANSP_NAME))
# sp_json_app_j$sp_delay <- select(arrange(sp_delay_for_json, ANSP_NAME), -c(ANSP_CODE, ANSP_NAME))

update_day <- floor_date(lubridate::now(), unit = "days") %>%
  as_tibble() %>%
  rename(APP_UPDATE = 1)

sp_json_app_j$sp_update <- update_day

sp_json_app_j <- sp_json_app_j %>%   group_by(ANSP_CODE, ANSP_NAME)

sp_json_app <- sp_json_app_j %>%
  toJSON(., pretty = TRUE)

save_json(sp_json_app, "sp_json_app")


# ____________________________________________________________________________________________
#
#    ANSP graphs  -----
#
# ____________________________________________________________________________________________

## TRAFFIC ----
### 7-day traffic avg ----

#### create date sequence
wef <- paste0(year(data_day_date), "-01-01")
til <- paste0(year(data_day_date), "-12-31")
days_current_year <- seq(ymd(wef), ymd(til), by = "1 day") %>%
  as_tibble() %>%
  select(ENTRY_DATE = value)

#### combine with ansp list to get full sequence
days_ansp <- crossing(days_current_year, ansp_list) %>%
  arrange(ANSP_CODE, ENTRY_DATE)

#### create dataset
sp_traffic_evo <- days_ansp %>%
  left_join(sp_traffic_data, by = c("ENTRY_DATE", "ANSP_CODE", "ANSP_NAME"))  %>%
  mutate(RWK_AVG_TFC = if_else(ENTRY_DATE > min(data_day_date,
                                                 max(LAST_DATA_DAY, na.rm = TRUE),na.rm = TRUE), NA, RW_AVG_FLT_DAIO)) %>%
  select(
    ANSP_CODE,
    ANSP_NAME,
    FLIGHT_DATE = ENTRY_DATE,
    RWK_AVG_TFC,
    RWK_AVG_TFC_PREV_YEAR = RW_AVG_FLT_DAIO_PREV_YEAR,
    RWK_AVG_TFC_2020 = RW_AVG_FLT_DAIO_2020,
    RWK_AVG_TFC_2019 = RW_AVG_FLT_DAIO_2019
  )

column_names <- c('ANSP_CODE', 'ANSP_NAME', 'FLIGHT_DATE', data_day_year, data_day_year-1, 2020, 2019)
colnames(sp_traffic_evo) <- column_names

### nest data
sp_traffic_evo_long <- sp_traffic_evo %>%
  pivot_longer(-c(ANSP_CODE, ANSP_NAME, FLIGHT_DATE), names_to = 'year', values_to = 'daio') %>%
  group_by(ANSP_CODE, ANSP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

sp_traffic_evo_j <- sp_traffic_evo_long %>% toJSON(., pretty = TRUE)

save_json(sp_traffic_evo_j, "sp_traffic_evo_chart_daily")


