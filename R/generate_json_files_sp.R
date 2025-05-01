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
    as_tibble()
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
    Y2D_TFC_YEAR = Y2D_FLT_DAIO_YEAR,
    Y2D_AVG_TFC_YEAR = Y2D_AVG_FLT_DAIO_YEAR,
    Y2D_TFC_DIF_PREV_YEAR_PERC,
    Y2D_TFC_DIF_2019_PERC
  )

#### Delay data ----
sp_delay_data <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(sp_base_file),
    start = sp_base_dir),
  sheet = "ansp_delay",
  range = cell_limits(c(1, 1), c(NA, NA))) %>%
  as_tibble() %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
  left_join(ansp_list, by = c("ANSP_NAME"))


sp_delay_calc <-  sp_delay_data %>%
  group_by(ANSP_NAME) %>%
  mutate(
    TDM_ERT_CS = TDM_ERT_C + TDM_ERT_S,
    TDM_ERT_IT = TDM_ERT_I + TDM_ERT_T,
    TDM_ERT_WD = TDM_ERT_W + TDM_ERT_D,
    TDM_ERT_OTHER = TDM_ERT - TDM_ERT_CS - TDM_ERT_IT - TDM_ERT_WD,

    DY_DLY_FLT = TDM_ERT / FLT_DAIO,
    DY_DELAYED_TFC_PERC = TDF_ERT / FLT_DAIO,
    DY_DELAYED_TFC_15_PERC = TDF_15_ERT / FLT_DAIO,

    # rolling week
    WK_DLY_AVG_ROLLING = rollmean(TDM_ERT, k = 7, fill = NA, align = "right"),
    WK_DLY_CS_AVG_ROLLING = rollmean(TDM_ERT_CS, k = 7, fill = NA, align = "right"),
    WK_DLY_IT_AVG_ROLLING = rollmean(TDM_ERT_IT, k = 7, fill = NA, align = "right"),
    WK_DLY_WD_AVG_ROLLING = rollmean(TDM_ERT_WD, k = 7, fill = NA, align = "right"),
    WK_DLY_OTHER_AVG_ROLLING = rollmean(TDM_ERT_OTHER, k = 7, fill = NA, align = "right"),
    RWK_FLT_DAIO_AVG = rollmean(FLT_DAIO, k = 7, fill = NA, align = "right"),

    WK_DLY_FLT = WK_DLY_AVG_ROLLING / RWK_FLT_DAIO_AVG,
    WK_DELAYED_TFC_PERC = rollmean(TDF_ERT, k = 7, fill = NA, align = "right") / RWK_FLT_DAIO_AVG,
    WK_DELAYED_TFC_15_PERC = rollmean(TDF_15_ERT, k = 7, fill = NA, align = "right") / RWK_FLT_DAIO_AVG

  ) %>%
  group_by(YEAR, .add = TRUE) %>%
  mutate(
    # year to date
    Y2D_DLY_AVG = cumsum(coalesce(TDM_ERT, 0)) / yday(ENTRY_DATE),
    Y2D_DLY_CS_AVG = cumsum(coalesce(TDM_ERT_CS, 0)) / yday(ENTRY_DATE),
    Y2D_DLY_IT_AVG = cumsum(coalesce(TDM_ERT_IT, 0)) / yday(ENTRY_DATE),
    Y2D_DLY_WD_AVG = cumsum(coalesce(TDM_ERT_WD, 0)) / yday(ENTRY_DATE),
    Y2D_DLY_OTHER_AVG = cumsum(coalesce(TDM_ERT_OTHER, 0)) / yday(ENTRY_DATE),
    Y2D_TFC_AVG = cumsum(coalesce(FLT_DAIO, 0)) / yday(ENTRY_DATE),

    Y2D_DLY_FLT = Y2D_DLY_AVG / Y2D_TFC_AVG,
    Y2D_DELAYED_TFC_PERC = cumsum(coalesce(TDF_ERT, 0)) / cumsum(coalesce(FLT_DAIO, 0)),
    Y2D_DELAYED_TFC_15_PERC = cumsum(coalesce(TDF_15_ERT, 0)) / cumsum(coalesce(FLT_DAIO, 0))

  ) %>%
  group_by(ANSP_NAME) %>%
  arrange(ANSP_NAME, ENTRY_DATE) %>%
  mutate(
    # prev year
    TDM_ERT_PREV_YEAR = lag(TDM_ERT, 364),
    DY_DLY_FLT_PREV_YEAR = lag(DY_DLY_FLT, 364),
    DY_DELAYED_TFC_PERC_PREV_YEAR = lag(DY_DELAYED_TFC_PERC, 364),
    DY_DELAYED_TFC_15_PERC_PREV_YEAR = lag(DY_DELAYED_TFC_15_PERC, 364),

    WK_DLY_AVG_ROLLING_PREV_YEAR = lag(WK_DLY_AVG_ROLLING, 364),
    WK_DLY_FLT_PREV_YEAR = lag(WK_DLY_FLT, 364),
    WK_DELAYED_TFC_PERC_PREV_YEAR = lag(WK_DELAYED_TFC_PERC, 364),
    WK_DELAYED_TFC_15_PERC_PREV_YEAR = lag(WK_DELAYED_TFC_15_PERC, 364),

    ## dif prev year
    DY_DLY_DIF_PREV_YEAR_PERC = if_else(TDM_ERT_PREV_YEAR == 0,
                                        NA, TDM_ERT/TDM_ERT_PREV_YEAR -1),
    DY_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(DY_DLY_FLT_PREV_YEAR == 0,
                                             NA, DY_DLY_FLT/DY_DLY_FLT_PREV_YEAR -1),
    DY_DELAYED_TFC_PERC_DIF_PREV_YEAR = DY_DELAYED_TFC_PERC - DY_DELAYED_TFC_PERC_PREV_YEAR,
    DY_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = DY_DELAYED_TFC_15_PERC - DY_DELAYED_TFC_15_PERC_PREV_YEAR,

    WK_DLY_DIF_PREV_YEAR_PERC = if_else(WK_DLY_AVG_ROLLING_PREV_YEAR == 0,
                                        NA, WK_DLY_AVG_ROLLING/WK_DLY_AVG_ROLLING_PREV_YEAR -1),
    WK_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(WK_DLY_FLT_PREV_YEAR == 0,
                                        NA, WK_DLY_FLT / WK_DLY_FLT_PREV_YEAR -1),
    WK_DELAYED_TFC_PERC_DIF_PREV_YEAR = WK_DELAYED_TFC_PERC - WK_DELAYED_TFC_PERC_PREV_YEAR,
    WK_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = WK_DELAYED_TFC_15_PERC - WK_DELAYED_TFC_15_PERC_PREV_YEAR,

    # 2019
    TDM_ERT_2019 = lag( TDM_ERT,  364 * (data_day_year - 2019) + floor((data_day_year - 2019) / 4) * 7),
    DY_DLY_FLT_2019 = lag( DY_DLY_FLT,  364 * (data_day_year - 2019) + floor((data_day_year - 2019) / 4) * 7),
    DY_DELAYED_TFC_PERC_2019 = lag(DY_DELAYED_TFC_PERC,  364 * (data_day_year - 2019) + floor((data_day_year - 2019) / 4) * 7),
    DY_DELAYED_TFC_15_PERC_2019 = lag(DY_DELAYED_TFC_15_PERC,  364 * (data_day_year - 2019) + floor((data_day_year - 2019) / 4) * 7),

    WK_DLY_AVG_ROLLING_2019 = lag( WK_DLY_AVG_ROLLING,  364 * (data_day_year - 2019) + floor((data_day_year - 2019) / 4) * 7),
    WK_DLY_FLT_2019 = lag( WK_DLY_FLT,  364 * (data_day_year - 2019) + floor((data_day_year - 2019) / 4) * 7),
    WK_DELAYED_TFC_PERC_2019 = lag(WK_DELAYED_TFC_PERC,  364 * (data_day_year - 2019) + floor((data_day_year - 2019) / 4) * 7),
    WK_DELAYED_TFC_15_PERC_2019 = lag(WK_DELAYED_TFC_15_PERC,  364 * (data_day_year - 2019) + floor((data_day_year - 2019) / 4) * 7),

    ## dif 2019
    DY_DLY_DIF_2019_PERC = if_else(TDM_ERT_2019 == 0,
                                        NA, TDM_ERT/TDM_ERT_2019 -1),
    DY_DLY_FLT_DIF_2019_PERC = if_else(DY_DLY_FLT_2019 == 0,
                                       NA, DY_DLY_FLT/DY_DLY_FLT_2019 -1),
    DY_DELAYED_TFC_PERC_DIF_2019 = DY_DELAYED_TFC_PERC - DY_DELAYED_TFC_PERC_2019,
    DY_DELAYED_TFC_15_PERC_DIF_2019 = DY_DELAYED_TFC_15_PERC - DY_DELAYED_TFC_15_PERC_2019,

    WK_DLY_FLT_DIF_2019_PERC = if_else(WK_DLY_FLT_2019 == 0,
                                            NA, WK_DLY_FLT / WK_DLY_FLT_2019 -1),
    WK_DLY_DIF_2019_PERC = if_else(WK_DLY_AVG_ROLLING_2019 == 0,
                                   NA, WK_DLY_AVG_ROLLING/WK_DLY_AVG_ROLLING_2019 -1),
    WK_DELAYED_TFC_PERC_DIF_2019 = WK_DELAYED_TFC_PERC - WK_DELAYED_TFC_PERC_2019,
    WK_DELAYED_TFC_15_PERC_DIF_2019 = WK_DELAYED_TFC_15_PERC - WK_DELAYED_TFC_15_PERC_2019,

  )

## y2d prev year and 2019

sp_delay_calc_prev_year <- sp_delay_calc %>%
  mutate(
    ENTRY_DATE_PREV_YEAR = add_with_rollback(ENTRY_DATE, years(1))
  ) %>%
  select(
    ANSP_NAME,
    ENTRY_DATE_PREV_YEAR,
    Y2D_DLY_AVG_PREV_YEAR = Y2D_DLY_AVG,
    Y2D_DLY_FLT_PREV_YEAR = Y2D_DLY_FLT,
    Y2D_DELAYED_TFC_PERC_PREV_YEAR = Y2D_DELAYED_TFC_PERC,
    Y2D_DELAYED_TFC_15_PERC_PREV_YEAR = Y2D_DELAYED_TFC_15_PERC
  )

sp_delay_calc_2019 <- sp_delay_calc %>%
  mutate(
    ENTRY_DATE_2019 = add_with_rollback(ENTRY_DATE, years((data_day_year -2019 )))) %>%
  select(
    ANSP_NAME,
    ENTRY_DATE_2019,
    Y2D_DLY_AVG_2019 = Y2D_DLY_AVG,
    Y2D_DLY_FLT_2019 = Y2D_DLY_FLT,
    Y2D_DELAYED_TFC_PERC_2019 = Y2D_DELAYED_TFC_PERC,
    Y2D_DELAYED_TFC_15_PERC_2019 = Y2D_DELAYED_TFC_15_PERC
  )

##

sp_delay <- sp_delay_calc %>%
  left_join(sp_delay_calc_prev_year, by = c("ANSP_NAME", "ENTRY_DATE" = "ENTRY_DATE_PREV_YEAR")) %>%
  left_join(sp_delay_calc_2019, by = c("ANSP_NAME", "ENTRY_DATE" = "ENTRY_DATE_2019")) %>%
  mutate(
    Y2D_DLY_DIF_PREV_YEAR_PERC = if_else(Y2D_DLY_AVG_PREV_YEAR == 0,
                                        NA, Y2D_DLY_AVG/Y2D_DLY_AVG_PREV_YEAR -1),

    Y2D_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(Y2D_DLY_FLT_PREV_YEAR == 0,
                                             NA, Y2D_DLY_FLT/Y2D_DLY_FLT_PREV_YEAR -1),

    Y2D_DLY_DIF_2019_PERC = if_else(Y2D_DLY_AVG_2019 == 0,
                                         NA, Y2D_DLY_AVG/Y2D_DLY_AVG_2019 -1),

    Y2D_DLY_FLT_DIF_2019_PERC = if_else(Y2D_DLY_FLT_2019 == 0,
                                             NA, Y2D_DLY_FLT/Y2D_DLY_FLT_2019 -1),
    Y2D_DELAYED_TFC_PERC_DIF_PREV_YEAR = Y2D_DELAYED_TFC_PERC - Y2D_DELAYED_TFC_PERC_PREV_YEAR,
    Y2D_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = Y2D_DELAYED_TFC_15_PERC - Y2D_DELAYED_TFC_15_PERC_PREV_YEAR,

    Y2D_DELAYED_TFC_PERC_DIF_2019 = Y2D_DELAYED_TFC_PERC - Y2D_DELAYED_TFC_PERC_2019,
    Y2D_DELAYED_TFC_15_PERC_DIF_2019 = Y2D_DELAYED_TFC_15_PERC - Y2D_DELAYED_TFC_15_PERC_2019
  )

sp_delay_last_day <- sp_delay %>%
  filter(ENTRY_DATE == min(data_day_date,
                           max(ENTRY_DATE, na.rm = TRUE),
                           na.rm = TRUE)
  ) %>%
  arrange(ANSP_NAME, ENTRY_DATE)

sp_delay_for_json <- sp_delay_last_day %>%
  select(
    ANSP_NAME,
    ANSP_CODE,
    FLIGHT_DATE = ENTRY_DATE,

    # delay
    DY_DLY = TDM_ERT,
    DY_DLY_DIF_PREV_YEAR_PERC,
    DY_DLY_DIF_2019_PERC,

    WK_DLY_AVG_ROLLING,
    WK_DLY_DIF_PREV_YEAR_PERC,
    WK_DLY_DIF_2019_PERC,

    Y2D_DLY_AVG,
    Y2D_DLY_DIF_PREV_YEAR_PERC,
    Y2D_DLY_DIF_2019_PERC,

    #delay per flight
    DY_DLY_FLT,
    DY_DLY_FLT_DIF_PREV_YEAR_PERC,
    DY_DLY_FLT_DIF_2019_PERC,

    WK_DLY_FLT,
    WK_DLY_FLT_DIF_PREV_YEAR_PERC,
    WK_DLY_FLT_DIF_2019_PERC,

    Y2D_DLY_FLT,
    Y2D_DLY_FLT_DIF_PREV_YEAR_PERC,
    Y2D_DLY_FLT_DIF_2019_PERC,

    #% of delayed flights
    DY_DELAYED_TFC_PERC,
    DY_DELAYED_TFC_PERC_DIF_PREV_YEAR,
    DY_DELAYED_TFC_PERC_DIF_2019,

    WK_DELAYED_TFC_PERC,
    WK_DELAYED_TFC_PERC_DIF_PREV_YEAR,
    WK_DELAYED_TFC_PERC_DIF_2019,

    Y2D_DELAYED_TFC_PERC,
    Y2D_DELAYED_TFC_PERC_DIF_PREV_YEAR,
    Y2D_DELAYED_TFC_PERC_DIF_2019,

    #% of delayed flights >15'
    DY_DELAYED_TFC_15_PERC,
    DY_DELAYED_TFC_15_PERC_DIF_PREV_YEAR,
    DY_DELAYED_TFC_15_PERC_DIF_2019,

    WK_DELAYED_TFC_15_PERC,
    WK_DELAYED_TFC_15_PERC_DIF_PREV_YEAR,
    WK_DELAYED_TFC_15_PERC_DIF_2019,

    Y2D_DELAYED_TFC_15_PERC,
    Y2D_DELAYED_TFC_15_PERC_DIF_PREV_YEAR,
    Y2D_DELAYED_TFC_15_PERC_DIF_2019

  ) %>%
  ungroup()



#### Join strings and save  ----

sp_json_app_j <- ansp_list %>% arrange(ANSP_NAME)
sp_json_app_j$sp_traffic <- select(arrange(sp_traffic_for_json, ANSP_NAME), -c(ANSP_CODE, ANSP_NAME))
sp_json_app_j$sp_delay <- select(arrange(sp_delay_for_json, ANSP_NAME), -c(ANSP_CODE, ANSP_NAME))

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


