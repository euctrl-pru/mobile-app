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

# Dimensions ----
if (exists("ansp_list") == FALSE) {
  ansp_list <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(sp_base_file),
      start = sp_base_dir),
    sheet = "lists",
    range = cell_limits(c(1, 1), c(NA, NA))) %>%
    as_tibble()
}

rel_ansp_acc <- ansp_list %>% 
  mutate(
    iso_2letter = str_remove_all(ANSP_CODE, "_ANSP"),
    ## smatsa exception
    iso_2letter = str_replace(iso_2letter, "MERS", "RS"),
  ) %>% 
  left_join(acc, by = "iso_2letter") %>% 
  select(
    ICAO_CODE = ICAO_code,
    PRU_ID,
    ANSP_CODE,
    ANSP_NAME
  )

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
  ### rank calculation
  mutate(
    DY_TFC_RANK = min_rank(desc(DAY_FLT_DAIO)),
    WK_TFC_RANK = min_rank(desc(RW_AVG_FLT_DAIO)),
    Y2D_TFC_RANK = min_rank(desc(Y2D_AVG_FLT_DAIO_YEAR)),
    TFC_RANK_TEXT = "*Top rank for highest.",

  ) %>%
  select(
    ANSP_NAME,
    ANSP_CODE,
    FLIGHT_DATE = ENTRY_DATE,

    DY_TFC_RANK,
    DY_TFC = DAY_FLT_DAIO,
    DY_TFC_DIF_PREV_YEAR_PERC,
    DY_TFC_DIF_2019_PERC,

    WK_TFC_RANK,
    WK_TFC_AVG_ROLLING = RW_AVG_FLT_DAIO,
    WK_TFC_DIF_PREV_YEAR_PERC,
    WK_TFC_DIF_2019_PERC,

    Y2D_TFC_RANK,
    Y2D_TFC = Y2D_FLT_DAIO_YEAR,
    Y2D_TFC_AVG = Y2D_AVG_FLT_DAIO_YEAR,
    Y2D_TFC_DIF_PREV_YEAR_PERC,
    Y2D_TFC_DIF_2019_PERC
  )

#### Delay data ----
##### import ----
sp_delay_data <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(sp_base_file),
    start = sp_base_dir),
  sheet = "ansp_delay",
  range = cell_limits(c(1, 1), c(NA, NA))) %>%
  as_tibble() %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
  left_join(ansp_list, by = c("ANSP_NAME"))

##### calc ----
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

##### y2d prev year and 2019 ----
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

##### full dataset ----
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

##### last day -----
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
  ##iceland exception
  mutate(
    DY_DLY_DIF_PREV_YEAR_PERC = if_else(ANSP_CODE == "IS_ANSP" & year(FLIGHT_DATE) < 2025, NA, DY_DLY_DIF_PREV_YEAR_PERC),
    DY_DLY_DIF_2019_PERC = if_else(ANSP_CODE == "IS_ANSP", NA, DY_DLY_DIF_2019_PERC),

    WK_DLY_DIF_PREV_YEAR_PERC = if_else(ANSP_CODE == "IS_ANSP" & year(FLIGHT_DATE) < 2025, NA, WK_DLY_DIF_PREV_YEAR_PERC),
    WK_DLY_DIF_2019_PERC = if_else(ANSP_CODE == "IS_ANSP", NA, WK_DLY_DIF_2019_PERC),

    Y2D_DLY_DIF_PREV_YEAR_PERC = if_else(ANSP_CODE == "IS_ANSP" & year(FLIGHT_DATE) < 2025, NA, Y2D_DLY_DIF_PREV_YEAR_PERC),
    Y2D_DLY_DIF_2019_PERC = if_else(ANSP_CODE == "IS_ANSP", NA, Y2D_DLY_DIF_2019_PERC),

    #delay per flight
    DY_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(ANSP_CODE == "IS_ANSP" & year(FLIGHT_DATE) < 2025, NA, DY_DLY_FLT_DIF_PREV_YEAR_PERC),
    DY_DLY_FLT_DIF_2019_PERC = if_else(ANSP_CODE == "IS_ANSP", NA, DY_DLY_FLT_DIF_2019_PERC),

    WK_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(ANSP_CODE == "IS_ANSP" & year(FLIGHT_DATE) < 2025, NA, WK_DLY_FLT_DIF_PREV_YEAR_PERC),
    WK_DLY_FLT_DIF_2019_PERC = if_else(ANSP_CODE == "IS_ANSP", NA, WK_DLY_FLT_DIF_2019_PERC),

    Y2D_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(ANSP_CODE == "IS_ANSP" & year(FLIGHT_DATE) < 2025, NA, Y2D_DLY_FLT_DIF_PREV_YEAR_PERC),
    Y2D_DLY_FLT_DIF_2019_PERC = if_else(ANSP_CODE == "IS_ANSP", NA, Y2D_DLY_FLT_DIF_2019_PERC),

    #% of delayed flights
    DY_DELAYED_TFC_PERC_DIF_PREV_YEAR = if_else(ANSP_CODE == "IS_ANSP" & year(FLIGHT_DATE) < 2025, NA, DY_DELAYED_TFC_PERC_DIF_PREV_YEAR),
    DY_DELAYED_TFC_PERC_DIF_2019 = if_else(ANSP_CODE == "IS_ANSP", NA, DY_DELAYED_TFC_PERC_DIF_2019),

    WK_DELAYED_TFC_PERC_DIF_PREV_YEAR = if_else(ANSP_CODE == "IS_ANSP" & year(FLIGHT_DATE) < 2025, NA, WK_DELAYED_TFC_PERC_DIF_PREV_YEAR),
    WK_DELAYED_TFC_PERC_DIF_2019 = if_else(ANSP_CODE == "IS_ANSP", NA, WK_DELAYED_TFC_PERC_DIF_2019),

    Y2D_DELAYED_TFC_PERC_DIF_PREV_YEAR = if_else(ANSP_CODE == "IS_ANSP" & year(FLIGHT_DATE) < 2025, NA, Y2D_DELAYED_TFC_PERC_DIF_PREV_YEAR),
    Y2D_DELAYED_TFC_PERC_DIF_2019 = if_else(ANSP_CODE == "IS_ANSP", NA, Y2D_DELAYED_TFC_PERC_DIF_2019),

    #% of delayed flights >15'
    DY_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = if_else(ANSP_CODE == "IS_ANSP" & year(FLIGHT_DATE) < 2025, NA, DY_DELAYED_TFC_15_PERC_DIF_PREV_YEAR),
    DY_DELAYED_TFC_15_PERC_DIF_2019 = if_else(ANSP_CODE == "IS_ANSP", NA, DY_DELAYED_TFC_15_PERC_DIF_2019),

    WK_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = if_else(ANSP_CODE == "IS_ANSP" & year(FLIGHT_DATE) < 2025, NA, WK_DELAYED_TFC_15_PERC_DIF_PREV_YEAR),
    WK_DELAYED_TFC_15_PERC_DIF_2019 = if_else(ANSP_CODE == "IS_ANSP", NA, WK_DELAYED_TFC_15_PERC_DIF_2019),

    Y2D_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = if_else(ANSP_CODE == "IS_ANSP" & year(FLIGHT_DATE) < 2025, NA, Y2D_DELAYED_TFC_15_PERC_DIF_PREV_YEAR),
    Y2D_DELAYED_TFC_15_PERC_DIF_2019 = if_else(ANSP_CODE == "IS_ANSP", NA, Y2D_DELAYED_TFC_15_PERC_DIF_2019),

  ) %>%
  ### rank calculation
  mutate(
    ## delay
    DY_DLY_RANK = min_rank(DY_DLY),
    WK_DLY_RANK = min_rank(WK_DLY_AVG_ROLLING),
    Y2D_DLY_RANK = min_rank(Y2D_DLY_AVG),

    ## delay per flight
    DY_DLY_FLT_RANK = min_rank(DY_DLY_FLT),
    WK_DLY_FLT_RANK = min_rank(WK_DLY_FLT),
    Y2D_DLY_FLT_RANK = min_rank(Y2D_DLY_FLT),

    ## % delayed flights
    DY_DELAYED_TFC_PERC_RANK = min_rank(DY_DELAYED_TFC_PERC),
    WK_DELAYED_TFC_PERC_RANK = min_rank(WK_DELAYED_TFC_PERC),
    Y2D_DELAYED_TFC_PERC_RANK = min_rank(Y2D_DELAYED_TFC_PERC),

    ## % delayed flights
    DY_DELAYED_TFC_15_PERC_RANK = min_rank(DY_DELAYED_TFC_15_PERC),
    WK_DELAYED_TFC_15_PERC_RANK = min_rank(WK_DELAYED_TFC_15_PERC),
    Y2D_DELAYED_TFC_15_PERC_RANK = min_rank(Y2D_DELAYED_TFC_15_PERC),

    DLY_RANK_TEXT = "*Top rank for lowest."
  )


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
wef <- paste0(data_day_year, "-01-01")
til <- paste0(data_day_year, "-12-31")
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

## DELAY ----
### Delay category ----
#### day ----
sp_delay_cause_day <- sp_delay_calc %>%
  ungroup() %>%
  filter(ENTRY_DATE == min(max(ENTRY_DATE),
                            data_day_date,
                            na.rm = TRUE)
  ) %>%
  mutate(
    SHARE_TDM_ERT_CS = if_else(TDM_ERT == 0, 0, TDM_ERT_CS / TDM_ERT),
    SHARE_TDM_ERT_IT = if_else(TDM_ERT == 0, 0, TDM_ERT_IT / TDM_ERT),
    SHARE_TDM_ERT_WD = if_else(TDM_ERT == 0, 0, TDM_ERT_WD / TDM_ERT),
    SHARE_TDM_ERT_OTHER = if_else(TDM_ERT == 0, 0, TDM_ERT_OTHER / TDM_ERT)
  ) %>%
  select(ANSP_CODE,
         ANSP_NAME,
         FLIGHT_DATE = ENTRY_DATE,
         TDM_ERT_CS,
         TDM_ERT_IT,
         TDM_ERT_WD,
         TDM_ERT_OTHER,
         TDM_ERT_PREV_YEAR,
         SHARE_TDM_ERT_CS,
         SHARE_TDM_ERT_IT,
         SHARE_TDM_ERT_WD,
         SHARE_TDM_ERT_OTHER
  ) %>%   # iceland exception
  mutate(
    TDM_ERT_PREV_YEAR = if_else(ANSP_CODE == "IS_ANSP" & year(FLIGHT_DATE) < 2025, NA, TDM_ERT_PREV_YEAR)
  )

column_names <- c(
  "ANSP_CODE",
  "ANSP_NAME",
  "FLIGHT_DATE",
  "Capacity/Staffing (ATC)",
  "Disruptions (ATC)",
  "Weather",
  "Other",
  paste0("En-route delay ", data_day_year - 1),
  "share_capacity_staffing_atc",
  "share_disruptions_atc",
  "share_weather",
  "share_other"
)

colnames(sp_delay_cause_day) <- column_names

### nest data
sp_delay_value_day_long <- sp_delay_cause_day %>%
  select(-c(share_capacity_staffing_atc,
            share_disruptions_atc,
            share_weather,
            share_other)
  ) %>%
  pivot_longer(-c(ANSP_CODE, ANSP_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

sp_delay_share_day_long <- sp_delay_cause_day %>%
  select(-c("Capacity/Staffing (ATC)",
            "Disruptions (ATC)",
            "Weather",
            "Other",
            paste0("En-route delay ", data_day_year - 1)
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(ANSP_CODE, ANSP_NAME, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

sp_delay_cause_day_long <- cbind(sp_delay_value_day_long, sp_delay_share_day_long) %>%
  select(-name) %>%
  group_by(ANSP_CODE, ANSP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

sp_delay_cause_evo_dy_j <- sp_delay_cause_day_long %>% toJSON(., pretty = TRUE)

save_json(sp_delay_cause_evo_dy_j, "sp_delay_category_evo_chart_dy")

#### week ----
sp_delay_cause_week <- sp_delay_calc %>%
  filter(ENTRY_DATE >= min(max(ENTRY_DATE),
                           data_day_date,
                           na.rm = TRUE) -6,
         ENTRY_DATE <= min(max(ENTRY_DATE),
                           data_day_date,
                           na.rm = TRUE)
  )  %>%
  reframe(
    ANSP_CODE,
    ANSP_NAME,
    FLIGHT_DATE = ENTRY_DATE,
    TDM_ERT_CS,
    TDM_ERT_IT,
    TDM_ERT_WD,
    TDM_ERT_OTHER,
    TDM_ERT_PREV_YEAR,
    WK_TDM_ERT_CS = sum(TDM_ERT_CS),
    WK_TDM_ERT_IT = sum(TDM_ERT_IT),
    WK_TDM_ERT_WD = sum(TDM_ERT_WD),
    WK_TDM_ERT_OTHER = sum(TDM_ERT_OTHER),
    WK_TDM_ERT = sum(TDM_ERT)
  ) %>%
  ungroup()%>%
  mutate(
    WK_SHARE_TDM_ERT_CS = if_else(WK_TDM_ERT == 0, 0, WK_TDM_ERT_CS / WK_TDM_ERT),
    WK_SHARE_TDM_ERT_IT = if_else(WK_TDM_ERT == 0, 0, WK_TDM_ERT_IT / WK_TDM_ERT),
    WK_SHARE_TDM_ERT_WD = if_else(WK_TDM_ERT == 0, 0, WK_TDM_ERT_WD / WK_TDM_ERT),
    WK_SHARE_TDM_ERT_OTHER = if_else(WK_TDM_ERT == 0, 0, WK_TDM_ERT_OTHER / WK_TDM_ERT)
  ) %>%
  select(
    ANSP_CODE,
    ANSP_NAME,
    FLIGHT_DATE,
    TDM_ERT_CS,
    TDM_ERT_IT,
    TDM_ERT_WD,
    TDM_ERT_OTHER,
    TDM_ERT_PREV_YEAR,
    WK_SHARE_TDM_ERT_CS,
    WK_SHARE_TDM_ERT_IT,
    WK_SHARE_TDM_ERT_WD,
    WK_SHARE_TDM_ERT_OTHER
  ) %>%   # iceland exception
  mutate(
    TDM_ERT_PREV_YEAR = if_else(ANSP_CODE == "IS_ANSP" & year(FLIGHT_DATE) < 2025, NA, TDM_ERT_PREV_YEAR)
  )

colnames(sp_delay_cause_week) <- column_names

### nest data
sp_delay_value_week_long <- sp_delay_cause_week %>%
  select(-c(share_capacity_staffing_atc,
            share_disruptions_atc,
            share_weather,
            share_other)
  ) %>%
  pivot_longer(-c(ANSP_CODE, ANSP_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

sp_delay_share_week_long <- sp_delay_cause_week %>%
  select(-c("Capacity/Staffing (ATC)",
            "Disruptions (ATC)",
            "Weather",
            "Other",
            paste0("En-route delay ", data_day_year - 1)
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(ANSP_CODE, ANSP_NAME, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

sp_delay_cause_week_long <- cbind(sp_delay_value_week_long, sp_delay_share_week_long) %>%
  select(-name) %>%
  group_by(ANSP_CODE, ANSP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

sp_delay_cause_evo_wk_j <- sp_delay_cause_week_long %>% toJSON(., pretty = TRUE)

save_json(sp_delay_cause_evo_wk_j, "sp_delay_category_evo_chart_wk")

#### y2d ----
sp_delay_cause_y2d <- sp_delay_calc %>%
  filter(ENTRY_DATE <= data_day_date,
         year(ENTRY_DATE) == year(data_day_date)) %>%
  reframe(
    ANSP_CODE,
    ANSP_NAME,
    FLIGHT_DATE = ENTRY_DATE,
    WK_DLY_CS_AVG_ROLLING,
    WK_DLY_IT_AVG_ROLLING,
    WK_DLY_WD_AVG_ROLLING,
    WK_DLY_OTHER_AVG_ROLLING,
    WK_DLY_AVG_ROLLING_PREV_YEAR,
    Y2D_TDM_ERT_CS = sum(TDM_ERT_CS),
    Y2D_TDM_ERT_IT = sum(TDM_ERT_IT),
    Y2D_TDM_ERT_WD = sum(TDM_ERT_WD),
    Y2D_TDM_ERT_OTHER = sum(TDM_ERT_OTHER),
    Y2D_TDM_ERT = sum(TDM_ERT)
  ) %>%
  ungroup() %>%
  mutate(
    Y2D_SHARE_TDM_ERT_CS = if_else(Y2D_TDM_ERT == 0, 0, Y2D_TDM_ERT_CS / Y2D_TDM_ERT),
    Y2D_SHARE_TDM_ERT_IT = if_else(Y2D_TDM_ERT == 0, 0, Y2D_TDM_ERT_IT / Y2D_TDM_ERT),
    Y2D_SHARE_TDM_ERT_WD = if_else(Y2D_TDM_ERT == 0, 0, Y2D_TDM_ERT_WD / Y2D_TDM_ERT),
    Y2D_SHARE_TDM_ERT_OTHER = if_else(Y2D_TDM_ERT == 0, 0, Y2D_TDM_ERT_OTHER / Y2D_TDM_ERT)
  ) %>%
  select(
    ANSP_CODE,
    ANSP_NAME,
    FLIGHT_DATE,
    WK_DLY_CS_AVG_ROLLING,
    WK_DLY_IT_AVG_ROLLING,
    WK_DLY_WD_AVG_ROLLING,
    WK_DLY_OTHER_AVG_ROLLING,
    WK_DLY_AVG_ROLLING_PREV_YEAR,
    Y2D_SHARE_TDM_ERT_CS,
    Y2D_SHARE_TDM_ERT_IT,
    Y2D_SHARE_TDM_ERT_WD,
    Y2D_SHARE_TDM_ERT_OTHER
  ) %>%   # iceland exception
  mutate(
    WK_DLY_AVG_ROLLING_PREV_YEAR = if_else(ANSP_CODE == "IS_ANSP" & year(FLIGHT_DATE) < 2025, NA, WK_DLY_AVG_ROLLING_PREV_YEAR)
  )


colnames(sp_delay_cause_y2d) <- column_names

### nest data
sp_delay_value_y2d_long <- sp_delay_cause_y2d %>%
  select(-c(share_capacity_staffing_atc,
            share_disruptions_atc,
            share_weather,
            share_other)
  ) %>%
  pivot_longer(-c(ANSP_CODE, ANSP_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

sp_delay_share_y2d_long <- sp_delay_cause_y2d %>%
  select(-c("Capacity/Staffing (ATC)",
            "Disruptions (ATC)",
            "Weather",
            "Other",
            paste0("En-route delay ", data_day_year - 1)
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(ANSP_CODE, ANSP_NAME, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

sp_delay_cause_y2d_long <- cbind(sp_delay_value_y2d_long, sp_delay_share_y2d_long) %>%
  select(-name) %>%
  group_by(ANSP_CODE, ANSP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")


sp_delay_cause_evo_y2d_j <- sp_delay_cause_y2d_long %>% toJSON(., pretty = TRUE)

save_json(sp_delay_cause_evo_y2d_j, "sp_delay_category_evo_chart_y2d")


### Delay per flight ----
sp_delay_flt_evo <- sp_delay_calc %>%
  filter(ENTRY_DATE <= min(data_day_date,
                            max(ENTRY_DATE, na.rm = TRUE),
                            na.rm = TRUE),
         year(ENTRY_DATE) == year(data_day_date)
  ) %>%
  mutate(
    WK_DLY_FLT = coalesce(WK_DLY_FLT, 0),
    WK_DLY_FLT_PREV_YEAR = coalesce(WK_DLY_FLT_PREV_YEAR, 0)
  ) %>%
  select(
    ANSP_CODE,
    ANSP_NAME,
    FLIGHT_DATE = ENTRY_DATE,
    WK_DLY_FLT,
    WK_DLY_FLT_PREV_YEAR
    )

y2d_delay_flt <- sp_delay_last_day %>% ungroup() %>% select(ANSP_CODE, Y2D_DLY_FLT, Y2D_DLY_FLT_PREV_YEAR)


column_names <- c('ANSP_CODE',
                  'ANSP_NAME',
                  'FLIGHT_DATE',
                  paste0('En-route ATFM delay/flight ', data_day_year),
                  paste0('En-route ATFM delay/flight ', data_day_year -1)
)

colnames(sp_delay_flt_evo) <- column_names

### nest data
sp_delay_flt_evo_long <- sp_delay_flt_evo %>%
  pivot_longer(-c(ANSP_CODE, ANSP_NAME, FLIGHT_DATE), names_to = 'year', values_to = 'daio') %>%
  left_join(y2d_delay_flt, by = "ANSP_CODE") %>%
  mutate(
    year = if_else(str_detect(year, as.character(data_day_year)),
                   paste0(year, " (", format(round(Y2D_DLY_FLT,2), nsmall=2),"')"),
                   paste0(year, " (", format(round(Y2D_DLY_FLT_PREV_YEAR,2), nsmall=2),"')"))
  ) %>%
  select(-Y2D_DLY_FLT, -Y2D_DLY_FLT_PREV_YEAR) %>%
  group_by(ANSP_CODE, ANSP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

###convert to json and save
sp_delay_flt_evo_j <- sp_delay_flt_evo_long %>% toJSON(., pretty = TRUE)

save_json(sp_delay_flt_evo_j, "sp_delay_per_flight_evo_chart_daily")

### % of delayed flights ----
#### create date sequence
wef <- paste0(data_day_year-1, "-01-01")
til <- paste0(data_day_year, "-12-31")
days_current_year <- seq(ymd(wef), ymd(til), by = "1 day") %>%
  as_tibble() %>%
  select(ENTRY_DATE = value)

#### combine with ansp list to get full sequence
days_ansp <- crossing(days_current_year, ansp_list) %>%
  arrange(ANSP_CODE, ENTRY_DATE)

sp_delayed_flights_evo <- days_ansp %>%
  left_join(sp_delay_calc, by = c("ENTRY_DATE", "ANSP_CODE", "ANSP_NAME")) %>%   mutate(
    WK_DELAYED_TFC_PERC = case_when(
      ENTRY_DATE > min(data_day_date,
                        max(ENTRY_DATE, na.rm = TRUE),
                        na.rm = TRUE) ~ NA,
      .default = coalesce(WK_DELAYED_TFC_PERC,0)),

    WK_DELAYED_TFC_15_PERC = case_when(
      ENTRY_DATE > min(data_day_date,
                        max(ENTRY_DATE, na.rm = TRUE),
                        na.rm = TRUE) ~ NA,
      .default = coalesce(WK_DELAYED_TFC_15_PERC,0)),
  ) %>%
  group_by(ANSP_CODE) %>%
  arrange(ANSP_CODE, ENTRY_DATE) %>%
  mutate(
    WK_DELAYED_TFC_PERC_PREV_YEAR = lag(WK_DELAYED_TFC_PERC, 364),
    WK_DELAYED_TFC_15_PERC_PREV_YEAR = lag(WK_DELAYED_TFC_15_PERC, 364)
  ) %>%
  ungroup() %>%
  filter(year(ENTRY_DATE) == data_day_year) %>%
  select(
    ANSP_CODE,
    ANSP_NAME,
    FLIGHT_DATE = ENTRY_DATE,
    WK_DELAYED_TFC_PERC,
    WK_DELAYED_TFC_PERC_PREV_YEAR,
    WK_DELAYED_TFC_15_PERC,
    WK_DELAYED_TFC_15_PERC_PREV_YEAR
  )

column_names <- c('ANSP_CODE',
                  'ANSP_NAME',
                  'FLIGHT_DATE',
                  paste0('% of delayed flights ', data_day_year),
                  paste0('% of delayed flights ', data_day_year -1),
                  paste0("% of delayed flights >15' ", data_day_year),
                  paste0("% of delayed flights >15' ", data_day_year -1)
)

colnames(sp_delayed_flights_evo) <- column_names

### nest data
sp_delayed_flights_evo_long <- sp_delayed_flights_evo %>%
  pivot_longer(-c(ANSP_CODE, ANSP_NAME, FLIGHT_DATE), names_to = 'year', values_to = 'daio') %>%
  group_by(ANSP_CODE, ANSP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")


sp_delayed_flights_evo_j <- sp_delayed_flights_evo_long %>% toJSON(., pretty = TRUE)

save_json(sp_delayed_flights_evo_j, "sp_delayed_flights_evo_chart_daily")

# ____________________________________________________________________________________________
#
#    ANSP ranking tables  -----
#
# ____________________________________________________________________________________________

## TRAFFIC ----
### ACC ----
#### day ----
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
    range = cell_limits(c(5, 1), c(NA, NA))) %>%
    as_tibble() %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
}

# process data
sp_acc_traffic_day_int <- assign(mydataframe, df) %>% 
  left_join(rel_ansp_acc, by = c("UNIT_CODE" = "ICAO_CODE")) %>% 
  group_by(PRU_ID) %>% 
  mutate(
    DY_RANK = rank(desc(FLIGHT), ties.method = "max"),
    SP_RANK = paste0(ANSP_CODE, DY_RANK),
    DY_FLT_DIF_PREV_WEEK_PERC = if_else(FLIGHT_7DAY == 0, NA, FLIGHT / FLIGHT_7DAY -1),
    DY_FLT_DIF_PREV_YEAR_PERC = if_else(FLIGHT_PREV_YEAR == 0, NA, FLIGHT / FLIGHT_PREV_YEAR -1),
     
    DY_FLT_DIF_PREV_WEEK = FLIGHT - FLIGHT_7DAY
    ) %>% 
  arrange(PRU_ID, SP_RANK) %>% 
  ungroup()

sp_acc_traffic_day <- sp_acc_traffic_day_int %>% 
  select(
    SP_RANK,
    DY_RANK,
    DY_ACC_NAME = NAME,
    DY_TO_DATE = ENTRY_DATE,
    DY_FLT = FLIGHT,
    DY_FLT_DIF_PREV_WEEK_PERC,
    DY_FLT_DIF_PREV_YEAR_PERC
  )
  
#### week ----
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
    range = cell_limits(c(5, 1), c(NA, NA))) %>%
    as_tibble() %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
}

# process data
sp_acc_traffic_week <- assign(mydataframe, df) %>% 
  left_join(rel_ansp_acc, by = c("UNIT_CODE" = "ICAO_CODE")) %>% 
  group_by(PRU_ID) %>% 
  mutate(
    WK_RANK = rank(desc(FLIGHT), ties.method = "max"),
    SP_RANK = paste0(ANSP_CODE, WK_RANK),
    WK_FLT_DIF_PREV_WEEK_PERC = if_else(FLIGHT_7DAY == 0, NA, FLIGHT / FLIGHT_7DAY -1),
    WK_FLT_DIF_PREV_YEAR_PERC = if_else(FLIGHT_PREV_YEAR == 0, NA, FLIGHT / FLIGHT_PREV_YEAR -1)
  ) %>% 
  arrange(PRU_ID, SP_RANK) %>% 
  ungroup() %>% 
  select(
    SP_RANK,
    WK_RANK,
    WK_ACC_NAME = NAME,
    WK_FROM_DATE = MIN_ENTRY_DATE,
    WK_TO_DATE = MAX_ENTRY_DATE,
    WK_FLT = FLIGHT,
    WK_FLT_DIF_PREV_WEEK_PERC,
    WK_FLT_DIF_PREV_YEAR_PERC
  )

#### y2d ----
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
    range = cell_limits(c(7, 1), c(NA, NA))) %>%
    as_tibble() %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
}

# process data
sp_acc_traffic_y2d <- assign(mydataframe, df) %>% 
  left_join(rel_ansp_acc, by = c("UNIT_CODE" = "ICAO_CODE")) %>% 
  group_by(PRU_ID) %>% 
  mutate(
    Y2D_RANK = rank(desc(FLIGHT), ties.method = "max"),
    SP_RANK = paste0(ANSP_CODE, Y2D_RANK),
    Y2D_FLT_DIF_PREV_YEAR_PERC = if_else(Y2D_AVG_FLIGHT_PY == 0, NA,  Y2D_AVG_FLIGHT /  Y2D_AVG_FLIGHT_PY-1),
    Y2D_FLT_DIF_2019_PERC = if_else(Y2D_AVG_FLIGHT_2019 == 0, NA, Y2D_AVG_FLIGHT / Y2D_AVG_FLIGHT_2019 -1)
  ) %>% 
  arrange(PRU_ID, SP_RANK) %>% 
  ungroup() %>% 
  select(
    SP_RANK,
    Y2D_RANK,
    Y2D_ACC_NAME = NAME,
    Y2D_FROM_DATE = MIN_DATE,
    Y2D_TO_DATE = ENTRY_DATE,
    Y2D_FLT = FLIGHT,
    Y2D_FLT_DIF_PREV_YEAR_PERC,
    Y2D_FLT_DIF_2019_PERC
  )


#### main card ----
sp_acc_main_traffic <- sp_acc_traffic_day_int %>%
  select(
    SP_RANK,
    MAIN_TFC_ACC_NAME = NAME,
    MAIN_TFC_ACC_CODE = UNIT_CODE,
    MAIN_TFC_ACC_FLT = FLIGHT
    )

sp_acc_main_traffic_dif <- sp_acc_traffic_day_int %>%
  arrange(ANSP_CODE, desc(abs(DY_FLT_DIF_PREV_WEEK)), DY_RANK) %>%
  group_by(ANSP_CODE) %>%
  mutate(RANK_DIF_AO_TFC = row_number()) %>%
  ungroup() %>%
  mutate(
    MAIN_TFC_DIF_ACC_NAME = NAME,
    MAIN_TFC_DIF_ACC_CODE = UNIT_CODE,
    MAIN_TFC_DIF_ACC_FLT_DIF = DY_FLT_DIF_PREV_WEEK
  ) %>%
  arrange(ANSP_CODE, desc(MAIN_TFC_DIF_ACC_FLT_DIF)) %>%
  group_by(ANSP_CODE) %>%
  mutate(
    RANK_MAIN_DIF = row_number(),
    SP_RANK = paste0(ANSP_CODE, RANK_MAIN_DIF)
  ) %>%
  ungroup() %>%
  select(SP_RANK, MAIN_TFC_DIF_ACC_NAME, MAIN_TFC_DIF_ACC_CODE, MAIN_TFC_DIF_ACC_FLT_DIF)

#### join tables ----
# create list of state/rankings for left join
ansp_ranking <- list()
i = 0
for (i in 1:10) {
  i = i + 1
  ansp_ranking <- ansp_ranking %>%
    bind_rows(ansp_list, .)
}

ansp_ranking <- ansp_ranking %>%
  select(-PRU_ID) %>% 
  arrange(ANSP_CODE) %>%
  group_by(ANSP_CODE) %>%
  mutate(
    RANK = row_number(),
    SP_RANK = paste0(ANSP_CODE, RANK)
  )

# join and reorder tables
sp_acc_data <- ansp_ranking %>%
  left_join(sp_acc_main_traffic, by = "SP_RANK") %>%
  left_join(sp_acc_main_traffic_dif, by = "SP_RANK") %>%
  left_join(sp_acc_traffic_day, by = "SP_RANK") %>%
  left_join(sp_acc_traffic_week, by = "SP_RANK") %>%
  left_join(sp_acc_traffic_y2d, by = "SP_RANK") %>%
  ungroup() %>%
  select(-SP_RANK) %>%
  arrange (ANSP_CODE, RANK)

# covert to json and save in app data folder and archive
st_ao_data_j <- st_ao_data %>% toJSON(., pretty = TRUE)

save_json(st_ao_data_j, "sp_acc_ranking_traffic")