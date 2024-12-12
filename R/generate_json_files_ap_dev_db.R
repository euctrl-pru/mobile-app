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


# Queries ----
source(here("..", "mobile-app", "R", "queries_ap.R"))


# airport dimension table (lists the airports and their ICAO codes)
if (exists("apt_icao") == FALSE) {
  apt_icao <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(ap_base_file),
      start = ap_base_dir),
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


ap_json_app <-""

# ____________________________________________________________________________________________
#
#    APT landing page -----
#
# ____________________________________________________________________________________________



#### Traffic data ----



# last line of this section
#save_json(apt_json_app, "apt_json_app", archive_file = FALSE)



# ____________________________________________________________________________________________
#
#    APT ranking tables -----
#
# ____________________________________________________________________________________________

## TRAFFIC ----
### Aircraft operator ----

#### day ----
apt_ao_day <- read_xlsx(
  path  = fs::path_abs(
    str_glue(ap_base_file),
    start = ap_base_dir),
  sheet = "apt_ao_day",
  range = cell_limits(c(1, 1), c(NA, NA))) |>
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

# process data
apt_ao_data_day_int <- apt_ao_day |>
  select(-TO_DATE) |>
  spread(key = FLAG_DAY, value = DEP_ARR) |>
  arrange(ARP_CODE, R_RANK) |>
  mutate(
    DY_RANK_DIF_PREV_WEEK = case_when(
      is.na(RANK_PREV) ~ RANK,
      .default = RANK_PREV - RANK
    ),
    DY_FLT_DIF_PREV_WEEK_PERC =   case_when(
      DAY_PREV_WEEK == 0 | is.na(DAY_PREV_WEEK) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_WEEK - 1
    ),
    DY_FLT_DIF_PREV_YEAR_PERC = case_when(
      DAY_PREV_YEAR == 0 | is.na(DAY_PREV_YEAR) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_YEAR - 1
    ),
    APT_TFC_AO_GRP_DIF = CURRENT_DAY - DAY_PREV_WEEK
  )

apt_ao_data_day <- apt_ao_data_day_int |>
  select(
    APT_CODE = ARP_CODE,
    APT_NAME = ARP_NAME,
    RANK = R_RANK,
    DY_RANK_DIF_PREV_WEEK,
    DY_AO_GRP_NAME = AO_GRP_NAME,
    DY_TO_DATE = LAST_DATA_DAY,
    DY_FLT = CURRENT_DAY,
    DY_FLT_DIF_PREV_WEEK_PERC,
    DY_FLT_DIF_PREV_YEAR_PERC
  )


#### week ----
apt_ao_week <- read_xlsx(
  path  = fs::path_abs(
    str_glue(ap_base_file),
    start = ap_base_dir),
  sheet = "apt_ao_week",
  range = cell_limits(c(1, 1), c(NA, NA))) |>
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

# process data
apt_ao_data_week <- apt_ao_week |>
  mutate(
    WK_FROM_DATE = max(FROM_DATE),
    WK_TO_DATE = max(TO_DATE)
  ) |>
  select(-FROM_DATE, -TO_DATE, -LAST_DATA_DAY) |>
  spread(key = PERIOD_TYPE, value = DEP_ARR) |>
  arrange(ARP_CODE, R_RANK) |>
  mutate(
    WK_RANK_DIF_PREV_WEEK =  case_when(
      is.na(RANK_PREV) ~ RANK,
      .default = RANK_PREV - RANK
    ),
    WK_FLT_DIF_PREV_WEEK_PERC =   case_when(
      PREV_ROLLING_WEEK == 0 | is.na(PREV_ROLLING_WEEK) ~ NA,
      .default = round((CURRENT_ROLLING_WEEK / PREV_ROLLING_WEEK - 1), 3)
    ),
    WK_FLT_DIF_PREV_YEAR_PERC = case_when(
      ROLLING_WEEK_PREV_YEAR == 0 | is.na(ROLLING_WEEK_PREV_YEAR) ~ NA,
      .default = round((CURRENT_ROLLING_WEEK / ROLLING_WEEK_PREV_YEAR - 1), 3)
    ),
    WK_FLT_AVG = round((CURRENT_ROLLING_WEEK/7), 2)
  ) |>
  select(
    APT_CODE = ARP_CODE,
    APT_NAME = ARP_NAME,
    RANK = R_RANK,
    WK_RANK_DIF_PREV_WEEK,
    WK_AO_GRP_NAME = AO_GRP_NAME,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_FLT_AVG,
    WK_FLT_DIF_PREV_WEEK_PERC,
    WK_FLT_DIF_PREV_YEAR_PERC
  )

#### y2d ----
apt_ao_y2d <- read_xlsx(
  path  = fs::path_abs(
    str_glue(ap_base_file),
    start = ap_base_dir),
  sheet = "apt_ao_y2d",
  range = cell_limits(c(1, 1), c(NA, NA))) |>
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

# process data
apt_ao_data_year <- apt_ao_y2d |>
  arrange(ARP_CODE, AO_GRP_CODE, YEAR) |>
  mutate(
    Y2D_RANK_DIF_PREV_YEAR =  RANK_PY - RANK,
    Y2D_FLT_DIF_PREV_YEAR_PERC = ifelse(YEAR == "2024",
                                            round((DEP_ARR/lag(DEP_ARR)-1), 3), NA),
    Y2D_FLT_DIF_2019_PERC = ifelse(YEAR == "2024",
                                       round((DEP_ARR/lag(DEP_ARR, 5)-1), 3), NA)
  ) |>
  filter(YEAR == 2024) |>
  mutate(TO_DATE = max(TO_DATE)) |>
  arrange(ARP_CODE, ARP_NAME, R_RANK) |>
  select(
    APT_CODE = ARP_CODE,
    APT_NAME = ARP_NAME,
    RANK = R_RANK,
    Y2D_RANK_DIF_PREV_YEAR,
    Y2D_AO_GRP_NAME = AO_GRP_NAME,
    Y2D_TO_DATE = TO_DATE,
    Y2D_FLT_AVG = DEP_ARR,
    Y2D_FLT_DIF_PREV_YEAR_PERC,
    Y2D_FLT_DIF_2019_PERC
  )

#### main card ----
apt_ao_main_traffic <- apt_ao_data_day_int |>
  mutate(
    MAIN_TFC_AO_GRP_NAME = if_else(
      R_RANK <= 4,
      AO_GRP_NAME,
      NA
    ),
    MAIN_TFC_AO_GRP_CODE = if_else(
      R_RANK <= 4,
      AO_GRP_CODE,
      NA
    ),
    MAIN_TFC_AO_GRP_FLT = if_else(
      R_RANK <= 4,
      CURRENT_DAY,
      NA
    )
  ) |>
  select(APT_CODE = ARP_CODE, APT_NAME = ARP_NAME, RANK = R_RANK,
         MAIN_TFC_AO_GRP_NAME, MAIN_TFC_AO_GRP_CODE, MAIN_TFC_AO_GRP_FLT)

apt_ao_main_traffic_dif <- apt_ao_data_day_int |>
  arrange(ARP_CODE, desc(abs(APT_TFC_AO_GRP_DIF))) |>
  group_by(ARP_CODE) |>
  mutate(RANK_DIF_AO_TFC = row_number()) |>
  ungroup() |>
  arrange(ARP_CODE, R_RANK) |>
  mutate(
    MAIN_TFC_DIF_AO_GRP_NAME = if_else(
      RANK_DIF_AO_TFC <= 4,
      AO_GRP_NAME,
      NA
    ),
    MAIN_TFC_DIF_AO_GRP_CODE = if_else(
      RANK_DIF_AO_TFC <= 4,
      AO_GRP_CODE,
      NA
    ),
    MAIN_TFC_DIF_AO_GRP_FLT_DIF = if_else(
      RANK_DIF_AO_TFC <= 4,
      APT_TFC_AO_GRP_DIF,
      NA
    )
  ) |>
  arrange(ARP_CODE, RANK_DIF_AO_TFC) |>
  select(APT_CODE = ARP_CODE, APT_NAME = ARP_NAME, RANK = RANK_DIF_AO_TFC,
         MAIN_TFC_DIF_AO_GRP_NAME, MAIN_TFC_DIF_AO_GRP_CODE,
         MAIN_TFC_DIF_AO_GRP_FLT_DIF)


#### join tables ----
apt_ao_ranking_traffic <- apt_ao_main_traffic |>
  left_join(apt_ao_main_traffic_dif, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_ao_data_day, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_ao_data_week, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_ao_data_year, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  arrange(APT_CODE, APT_NAME, RANK) |>
  distinct(RANK, APT_CODE, APT_NAME, .keep_all = TRUE)

# convert to json and save in app data folder
apt_ao_ranking_traffic_j <- apt_ao_ranking_traffic |> toJSON(pretty = TRUE)

save_json(apt_ao_ranking_traffic_j, "apt_ao_ranking_traffic", archive_file = FALSE)



### ### Airports ----

#### day ----
apt_apt_day <- read_xlsx(
  path  = fs::path_abs(
    str_glue(ap_base_file),
    start = ap_base_dir),
  sheet = "apt_apt_des_day",
  range = cell_limits(c(1, 1), c(NA, NA))) |>
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

# process data
apt_apt_data_day_int <- apt_apt_day |>
  select(-TO_DATE) |>
  spread(key = FLAG_PERIOD, value = DEP) |>
  arrange(ARP_CODE_DEP, R_RANK) |>
  mutate(
    DY_RANK_DIF_PREV_WEEK = case_when(
      is.na(RANK_PREV) ~ RANK,
      .default = RANK_PREV - RANK
    ),
    DY_FLT_DIF_PREV_WEEK_PERC =   case_when(
      DAY_PREV_WEEK == 0 | is.na(DAY_PREV_WEEK) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_WEEK - 1
    ),
    DY_FLT_DIF_PREV_YEAR_PERC = case_when(
      DAY_PREV_YEAR == 0 | is.na(DAY_PREV_YEAR) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_YEAR - 1
    ),
    APT_TFC_APT_DIF = CURRENT_DAY - DAY_PREV_WEEK
  )

apt_apt_data_day <- apt_apt_data_day_int |>
  select(
    APT_CODE = ARP_CODE_DEP,
    APT_NAME = ARP_NAME_DEP,
    RANK = R_RANK,
    DY_RANK_DIF_PREV_WEEK,
    DY_APT_DES_NAME = ARP_NAME_ARR,
    DY_TO_DATE = LAST_DATA_DAY,
    DY_FLT = CURRENT_DAY,
    DY_FLT_DIF_PREV_WEEK_PERC,
    DY_FLT_DIF_PREV_YEAR_PERC
  )


#### week ----
apt_apt_week <- read_xlsx(
  path  = fs::path_abs(
    str_glue(ap_base_file),
    start = ap_base_dir),
  sheet = "apt_apt_des_week",
  range = cell_limits(c(1, 1), c(NA, NA))) |>
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

# process data
apt_apt_data_week <- apt_apt_week |>
  mutate(
    WK_FROM_DATE = max(FROM_DATE),
    WK_TO_DATE = max(TO_DATE)
  ) |>
  select(-FROM_DATE, -TO_DATE, -LAST_DATA_DAY) |>
  spread(key = FLAG_PERIOD, value = DEP) |>
  arrange(ARP_CODE_DEP, R_RANK) |>
  mutate(
    WK_RANK_DIF_PREV_WEEK =  case_when(
      is.na(RANK_PREV) ~ RANK,
      .default = RANK_PREV - RANK
    ),
    WK_FLT_DIF_PREV_WEEK_PERC =   case_when(
      PREV_ROLLING_WEEK == 0 | is.na(PREV_ROLLING_WEEK) ~ NA,
      .default = round((CURRENT_ROLLING_WEEK / PREV_ROLLING_WEEK - 1), 3)
    ),
    WK_FLT_DIF_PREV_YEAR_PERC = case_when(
      ROLLING_WEEK_PREV_YEAR == 0 | is.na(ROLLING_WEEK_PREV_YEAR) ~ NA,
      .default = round((CURRENT_ROLLING_WEEK / ROLLING_WEEK_PREV_YEAR - 1), 3)
    ),
    WK_FLT_AVG = round((CURRENT_ROLLING_WEEK/7), 2)
  ) |>
  select(
    APT_CODE = ARP_CODE_DEP,
    APT_NAME = ARP_NAME_DEP,
    RANK = R_RANK,
    WK_RANK_DIF_PREV_WEEK,
    WK_APT_DES_NAME = ARP_NAME_ARR,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_FLT_AVG,
    WK_FLT_DIF_PREV_WEEK_PERC,
    WK_FLT_DIF_PREV_YEAR_PERC
  )

#### y2d ----
apt_apt_y2d <- read_xlsx(
  path  = fs::path_abs(
    str_glue(ap_base_file),
    start = ap_base_dir),
  sheet = "apt_apt_des_y2d",
  range = cell_limits(c(1, 1), c(NA, NA))) |>
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

# process data
apt_apt_data_year <- apt_apt_y2d |>
  arrange(ARP_CODE_DEP, ARP_NAME_ARR, YEAR) |>
  mutate(
    Y2D_RANK_DIF_PREV_YEAR =  RANK_PREV - RANK,
    Y2D_FLT_DIF_PREV_YEAR_PERC = ifelse(YEAR == "2024",
                                        round((DEP/lag(DEP)-1), 3), NA),
    Y2D_FLT_DIF_2019_PERC = ifelse(YEAR == "2024",
                                   round((DEP/lag(DEP, 5)-1), 3), NA)
  ) |>
  filter(YEAR == 2024) |>
  arrange(ARP_CODE_DEP, ARP_NAME_DEP, R_RANK) |>
  select(
    APT_CODE = ARP_CODE_DEP,
    APT_NAME = ARP_NAME_DEP,
    RANK = R_RANK,
    Y2D_RANK_DIF_PREV_YEAR,
    Y2D_APT_DES_NAME = ARP_NAME_ARR,
    Y2D_TO_DATE = LAST_DATA_DAY,
    Y2D_FLT_AVG = DEP,
    Y2D_FLT_DIF_PREV_YEAR_PERC,
    Y2D_FLT_DIF_2019_PERC
  )

#### main card ----
apt_apt_main_traffic <- apt_apt_data_day_int |>
  mutate(
    MAIN_TFC_APT_DES_NAME = if_else(
      R_RANK <= 4,
      ARP_NAME_ARR,
      NA
    ),
    MAIN_TFC_APT_DES_CODE = if_else(
      R_RANK <= 4,
      ARP_CODE_ARR,
      NA
    ),
    MAIN_TFC_APT_DES_FLT = if_else(
      R_RANK <= 4,
      CURRENT_DAY,
      NA
    )
  ) |>
  select(APT_CODE = ARP_CODE_DEP, APT_NAME = ARP_NAME_DEP, RANK = R_RANK,
         MAIN_TFC_APT_DES_NAME, MAIN_TFC_APT_DES_CODE, MAIN_TFC_APT_DES_FLT)

apt_apt_main_traffic_dif <- apt_apt_data_day_int |>
  arrange(ARP_CODE_DEP, desc(abs(APT_TFC_APT_DIF))) |>
  group_by(ARP_CODE_DEP) |>
  mutate(RANK_DIF_APT_TFC = row_number()) |>
  ungroup() |>
  arrange(ARP_CODE_DEP, R_RANK) |>
  mutate(
    MAIN_TFC_DIF_APT_DES_NAME = if_else(
      RANK_DIF_APT_TFC <= 4,
      ARP_NAME_ARR,
      NA
    ),
    MAIN_TFC_DIF_APT_DES_CODE = if_else(
      RANK_DIF_APT_TFC <= 4,
      ARP_CODE_ARR,
      NA
    ),
    MAIN_TFC_DIF_APT_DES_FLT_DIF = if_else(
      RANK_DIF_APT_TFC <= 4,
      APT_TFC_APT_DIF,
      NA
    )
  ) |>
  arrange(ARP_CODE_DEP, RANK_DIF_APT_TFC) |>
  select(APT_CODE = ARP_CODE_DEP, APT_NAME = ARP_NAME_DEP, RANK = RANK_DIF_APT_TFC,
         MAIN_TFC_DIF_APT_DES_NAME, MAIN_TFC_DIF_APT_DES_CODE,
         MAIN_TFC_DIF_APT_DES_FLT_DIF)


#### join tables ----
apt_apt_ranking_traffic <- apt_apt_main_traffic |>
  left_join(apt_apt_main_traffic_dif, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_apt_data_day, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_apt_data_week, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_apt_data_year, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  arrange(APT_CODE, APT_NAME, RANK) |>
  distinct(RANK, APT_CODE, APT_NAME, .keep_all = TRUE)

# convert to json and save in app data folder
apt_apt_ranking_traffic_j <- apt_apt_ranking_traffic |> toJSON(pretty = TRUE)

save_json(apt_apt_ranking_traffic_j, "apt_apt_ranking_traffic", archive_file = FALSE)


### ### States ----

#### day ----
apt_st_day <- read_xlsx(
  path  = fs::path_abs(
    str_glue(ap_base_file),
    start = ap_base_dir),
  sheet = "apt_state_des_day",
  range = cell_limits(c(1, 1), c(NA, NA))) |>
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

# process data
apt_st_data_day_int <- apt_st_day |>
  select(-TO_DATE) |>
  spread(key = FLAG_PERIOD, value = DEP) |>
  arrange(ARP_CODE, R_RANK) |>
  mutate(
    DY_RANK_DIF_PREV_WEEK = case_when(
      is.na(RANK_PREV) ~ RANK,
      .default = RANK_PREV - RANK
    ),
    DY_FLT_DIF_PREV_WEEK_PERC =   case_when(
      DAY_PREV_WEEK == 0 | is.na(DAY_PREV_WEEK) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_WEEK - 1
    ),
    DY_FLT_DIF_PREV_YEAR_PERC = case_when(
      DAY_PREV_YEAR == 0 | is.na(DAY_PREV_YEAR) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_YEAR - 1
    ),
    APT_TFC_ST_DIF = CURRENT_DAY - DAY_PREV_WEEK
  )

apt_st_data_day <- apt_st_data_day_int |>
  select(
    APT_CODE = ARP_CODE,
    APT_NAME = ARP_NAME,
    RANK = R_RANK,
    DY_RANK_DIF_PREV_WEEK,
    DY_ST_DES_NAME = ISO_CT_NAME_ARR,
    DY_TO_DATE = LAST_DATA_DAY,
    DY_FLT = CURRENT_DAY,
    DY_FLT_DIF_PREV_WEEK_PERC,
    DY_FLT_DIF_PREV_YEAR_PERC
  )


#### week ----
apt_st_week <- read_xlsx(
  path  = fs::path_abs(
    str_glue(ap_base_file),
    start = ap_base_dir),
  sheet = "apt_state_des_week",
  range = cell_limits(c(1, 1), c(NA, NA))) |>
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

# process data
apt_st_data_week <- apt_st_week |>
  mutate(
    WK_FROM_DATE = max(FROM_DATE),
    WK_TO_DATE = max(TO_DATE)
  ) |>
  select(-FROM_DATE, -TO_DATE, -LAST_DATA_DAY) |>
  spread(key = FLAG_PERIOD, value = DEP) |>
  arrange(ARP_CODE, R_RANK) |>
  mutate(
    WK_RANK_DIF_PREV_WEEK =  case_when(
      is.na(RANK_PREV) ~ RANK,
      .default = RANK_PREV - RANK
    ),
    WK_FLT_DIF_PREV_WEEK_PERC =   case_when(
      PREV_ROLLING_WEEK == 0 | is.na(PREV_ROLLING_WEEK) ~ NA,
      .default = round((CURRENT_ROLLING_WEEK / PREV_ROLLING_WEEK - 1), 3)
    ),
    WK_FLT_DIF_PREV_YEAR_PERC = case_when(
      ROLLING_WEEK_PREV_YEAR == 0 | is.na(ROLLING_WEEK_PREV_YEAR) ~ NA,
      .default = round((CURRENT_ROLLING_WEEK / ROLLING_WEEK_PREV_YEAR - 1), 3)
    ),
    WK_FLT_AVG = round((CURRENT_ROLLING_WEEK/7), 2)
  ) |>
  select(
    APT_CODE = ARP_CODE,
    APT_NAME = ARP_NAME,
    RANK = R_RANK,
    WK_RANK_DIF_PREV_WEEK,
    WK_ST_DES_NAME = ISO_CT_NAME_ARR,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_FLT_AVG,
    WK_FLT_DIF_PREV_WEEK_PERC,
    WK_FLT_DIF_PREV_YEAR_PERC
  )

#### y2d ----
apt_st_y2d <- read_xlsx(
  path  = fs::path_abs(
    str_glue(ap_base_file),
    start = ap_base_dir),
  sheet = "apt_state_des_y2d",
  range = cell_limits(c(1, 1), c(NA, NA))) |>
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

# process data
apt_st_data_year <- apt_st_y2d |>
  arrange(ARP_CODE, ISO_CT_NAME_ARR, YEAR) |>
  mutate(
    Y2D_RANK_DIF_PREV_YEAR =  RANK_PREV - RANK,
    Y2D_FLT_DIF_PREV_YEAR_PERC = ifelse(YEAR == "2024",
                                        round((DEP/lag(DEP)-1), 3), NA),
    Y2D_FLT_DIF_2019_PERC = ifelse(YEAR == "2024",
                                   round((DEP/lag(DEP, 5)-1), 3), NA)
  ) |>
  filter(YEAR == 2024) |>
  arrange(ARP_CODE, ISO_CT_NAME_ARR, R_RANK) |>
  select(
    APT_CODE = ARP_CODE,
    APT_NAME = ARP_NAME,
    RANK = R_RANK,
    Y2D_RANK_DIF_PREV_YEAR,
    Y2D_ST_DES_NAME = ISO_CT_NAME_ARR,
    Y2D_TO_DATE = LAST_DATA_DAY,
    Y2D_FLT_AVG = DEP,
    Y2D_FLT_DIF_PREV_YEAR_PERC,
    Y2D_FLT_DIF_2019_PERC
  )

#### main card ----
apt_st_main_traffic <- apt_st_data_day_int |>
  mutate(
    MAIN_TFC_ST_DES_NAME = if_else(
      R_RANK <= 4,
      ISO_CT_NAME_ARR,
      NA
    ),
    MAIN_TFC_ST_DES_CODE = if_else(
      R_RANK <= 4,
      ISO_CT_CODE_ARR,
      NA
    ),
    MAIN_TFC_ST_DES_FLT = if_else(
      R_RANK <= 4,
      CURRENT_DAY,
      NA
    )
  ) |>
  select(APT_CODE = ARP_CODE, APT_NAME = ARP_NAME, RANK = R_RANK,
         MAIN_TFC_ST_DES_NAME, MAIN_TFC_ST_DES_CODE, MAIN_TFC_ST_DES_FLT)

apt_st_main_traffic_dif <- apt_st_data_day_int |>
  arrange(ARP_CODE, desc(abs(APT_TFC_ST_DIF))) |>
  group_by(ARP_CODE) |>
  mutate(RANK_DIF_ST_TFC = row_number()) |>
  ungroup() |>
  arrange(ARP_CODE, R_RANK) |>
  mutate(
    MAIN_TFC_DIF_ST_DES_NAME = if_else(
      RANK_DIF_ST_TFC <= 4,
      ISO_CT_NAME_ARR,
      NA
    ),
    MAIN_TFC_DIF_ST_DES_CODE = if_else(
      RANK_DIF_ST_TFC <= 4,
      ISO_CT_CODE_ARR,
      NA
    ),
    MAIN_TFC_DIF_ST_DES_FLT_DIF = if_else(
      RANK_DIF_ST_TFC <= 4,
      APT_TFC_ST_DIF,
      NA
    )
  ) |>
  arrange(ARP_CODE, RANK_DIF_ST_TFC) |>
  select(APT_CODE = ARP_CODE, APT_NAME = ARP_NAME, RANK = RANK_DIF_ST_TFC,
         MAIN_TFC_DIF_ST_DES_NAME, MAIN_TFC_DIF_ST_DES_CODE,
         MAIN_TFC_DIF_ST_DES_FLT_DIF)


#### join tables ----
apt_st_ranking_traffic <- apt_st_main_traffic |>
  left_join(apt_st_main_traffic_dif, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_st_data_day, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_st_data_week, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_st_data_year, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  arrange(APT_CODE, APT_NAME, RANK) |>
  distinct(RANK, APT_CODE, APT_NAME, .keep_all = TRUE)

# convert to json and save in app data folder
apt_st_ranking_traffic_j <- apt_st_ranking_traffic |> toJSON(pretty = TRUE)

save_json(apt_st_ranking_traffic_j, "apt_st_ranking_traffic", archive_file = FALSE)



# ____________________________________________________________________________________________
#
#    APT market segments -----
#
# ____________________________________________________________________________________________


#### day ----
apt_ms_day <- read_xlsx(
  path  = fs::path_abs(
    str_glue(ap_base_file),
    start = ap_base_dir),
  sheet = "apt_ms_day",
  range = cell_limits(c(1, 1), c(NA, NA))) |>
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

# process data
apt_ms_data_day <- apt_ms_day |>
  select(-TO_DATE) |>
  spread(key = FLAG_PERIOD, value = DEP_ARR) |>
  arrange(ARP_CODE, R_RANK) |>
  mutate(
    DY_RANK_DIF_PREV_WEEK = case_when(
      is.na(RANK_PREV) ~ RANK,
      .default = RANK_PREV - RANK
    ),
    DY_FLT_DIF_PREV_WEEK_PERC =   case_when(
      DAY_PREV_WEEK == 0 | is.na(DAY_PREV_WEEK) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_WEEK - 1
    ),
    DY_FLT_DIF_PREV_YEAR_PERC = case_when(
      DAY_PREV_YEAR == 0 | is.na(DAY_PREV_YEAR) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_YEAR - 1
    )
  ) |>
  group_by(ARP_CODE) |>
  mutate(DY_MS_SHARE = ifelse(CURRENT_DAY == 0, 0,
                              CURRENT_DAY/sum(CURRENT_DAY, na.rm = TRUE))) |>
  ungroup() |>
  select(
    APT_CODE = ARP_CODE,
    APT_NAME = ARP_NAME,
    RANK = R_RANK,
    DY_RANK_DIF_PREV_WEEK,
    DY_MARKET_SEGMENT = MARKET_SEGMENT,
    DY_MS_SHARE,
    DY_TO_DATE = LAST_DATA_DAY,
    DY_FLT = CURRENT_DAY,
    DY_FLT_DIF_PREV_WEEK_PERC,
    DY_FLT_DIF_PREV_YEAR_PERC
  )


#### week ----
apt_ms_week <- read_xlsx(
  path  = fs::path_abs(
    str_glue(ap_base_file),
    start = ap_base_dir),
  sheet = "apt_ms_week",
  range = cell_limits(c(1, 1), c(NA, NA))) |>
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

# process data
apt_ms_data_week <- apt_ms_week |>
  mutate(
    WK_FROM_DATE = max(FROM_DATE),
    WK_TO_DATE = max(TO_DATE)
  ) |>
  select(-FROM_DATE, -TO_DATE, -LAST_DATA_DAY) |>
  spread(key = FLAG_PERIOD, value = DEP_ARR) |>
  arrange(ARP_CODE, R_RANK) |>
  mutate(
    WK_RANK_DIF_PREV_WEEK =  case_when(
      is.na(RANK_PREV) ~ RANK,
      .default = RANK_PREV - RANK
    ),
    WK_FLT_DIF_PREV_WEEK_PERC =   case_when(
      PREV_ROLLING_WEEK == 0 | is.na(PREV_ROLLING_WEEK) ~ NA,
      .default = round((CURRENT_ROLLING_WEEK / PREV_ROLLING_WEEK - 1), 3)
    ),
    WK_FLT_DIF_PREV_YEAR_PERC = case_when(
      ROLLING_WEEK_PREV_YEAR == 0 | is.na(ROLLING_WEEK_PREV_YEAR) ~ NA,
      .default = round((CURRENT_ROLLING_WEEK / ROLLING_WEEK_PREV_YEAR - 1), 3)
    ),
    WK_FLT_AVG = round((CURRENT_ROLLING_WEEK/7), 2)
  ) |>
  group_by(ARP_CODE) |>
  mutate(WK_MS_SHARE = ifelse(CURRENT_ROLLING_WEEK == 0, 0,
                              CURRENT_ROLLING_WEEK/sum(CURRENT_ROLLING_WEEK,
                                                       na.rm = TRUE))) |>
  ungroup() |>
  select(
    APT_CODE = ARP_CODE,
    APT_NAME = ARP_NAME,
    RANK = R_RANK,
    WK_RANK_DIF_PREV_WEEK,
    WK_MARKET_SEGMENT = MARKET_SEGMENT,
    WK_MS_SHARE,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_FLT_AVG,
    WK_FLT_DIF_PREV_WEEK_PERC,
    WK_FLT_DIF_PREV_YEAR_PERC
  )


#### y2d ----
apt_ms_y2d <- read_xlsx(
  path  = fs::path_abs(
    str_glue(ap_base_file),
    start = ap_base_dir),
  sheet = "apt_ms_y2d",
  range = cell_limits(c(1, 1), c(NA, NA))) |>
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

# process data
apt_ms_data_year <- apt_ms_y2d |>
  arrange(ARP_CODE, MARKET_SEGMENT, YEAR) |>
  mutate(
    Y2D_RANK_DIF_PREV_YEAR =  RANK_PREV - RANK,
    Y2D_FLT_DIF_PREV_YEAR_PERC = ifelse(YEAR == "2024",
                                            round((DEP_ARR/lag(DEP_ARR)-1), 3), NA),
    Y2D_FLT_DIF_2019_PERC = ifelse(YEAR == "2024",
                                       round((DEP_ARR/lag(DEP_ARR, 5)-1), 3), NA)
  ) |>
  filter(YEAR == 2024) |>
  group_by(ARP_CODE) |>
  mutate(Y2D_MS_SHARE = ifelse(DEP_ARR == 0, 0,
                               DEP_ARR/sum(DEP_ARR, na.rm = TRUE))) |>
  ungroup() |>
  arrange(ARP_CODE, ARP_NAME, R_RANK) |>
  select(
    APT_CODE = ARP_CODE,
    APT_NAME = ARP_NAME,
    RANK = R_RANK,
    Y2D_RANK_DIF_PREV_YEAR,
    Y2D_MARKET_SEGMENT = MARKET_SEGMENT,
    Y2D_MS_SHARE,
    Y2D_TO_DATE = LAST_DATA_DAY,
    Y2D_FLT_AVG = DEP_ARR,
    Y2D_FLT_DIF_PREV_YEAR_PERC,
    Y2D_FLT_DIF_2019_PERC
  )


#### join tables ----
apt_ms_ranking_traffic <- apt_ms_data_day |>
  left_join(apt_ms_data_week, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  left_join(apt_ms_data_year, by = c("RANK", "APT_CODE", "APT_NAME"),
            relationship = "many-to-many") |>
  arrange(APT_CODE, APT_NAME, RANK) |>
  distinct(RANK, APT_CODE, APT_NAME, .keep_all = TRUE)

# convert to json and save in app data folder
apt_ms_ranking_traffic_j <- apt_ms_ranking_traffic |> toJSON(pretty = TRUE)

save_json(ap_apt_ms_traffic_j, "apt_ms_ranking_traffic", archive_file = FALSE)



