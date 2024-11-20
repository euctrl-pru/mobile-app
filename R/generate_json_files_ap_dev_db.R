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
  mutate(TO_DATE = max(TO_DATE)) |>
  spread(key = FLAG_DAY, value = DEP_ARR) |>
  arrange(ARP_CODE, R_RANK) |>
  mutate(
    DY_RANK_DIF_PREV_WEEK = case_when(
      is.na(RANK_PREV) ~ RANK,
      .default = RANK_PREV - RANK
    ),
    DY_DEP_ARR_DIF_PREV_WEEK_PERC =   case_when(
      DAY_PREV_WEEK == 0 | is.na(DAY_PREV_WEEK) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_WEEK - 1
    ),
    DY_DEP_ARR_DIF_PREV_YEAR_PERC = case_when(
      DAY_PREV_YEAR == 0 | is.na(DAY_PREV_YEAR) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_YEAR - 1
    ),
    APT_RANK = paste0(tolower(ARP_CODE), R_RANK),
    APT_TFC_AO_GRP_DIF = CURRENT_DAY - DAY_PREV_WEEK
  )

ap_apt_data_day <- apt_ao_data_day_int |>
  select(
    APT_RANK,
    DY_RANK_DIF_PREV_WEEK,
    DY_AO_GRP_NAME = AO_GRP_NAME,
    DY_TO_DATE = TO_DATE,
    DY_DEP_ARR = CURRENT_DAY,
    DY_DEP_ARR_DIF_PREV_WEEK_PERC,
    DY_DEP_ARR_DIF_PREV_YEAR_PERC
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
ap_apt_data_week <- apt_ao_week |>
  mutate(DEP_ARR = DEP_ARR / 7) |>
  spread(key = PERIOD_TYPE, value = DEP_ARR) |>
  arrange(ARP_CODE, R_RANK) |>
  mutate(
    WK_RANK_DIF_PREV_WEEK = case_when(
      is.na(RANK_PREV) ~ RANK,
      .default = RANK_PREV - RANK
    ),
    WK_DEP_ARR_DIF_PREV_WEEK_PERC =   case_when(
      PREV_ROLLING_WEEK == 0 | is.na(PREV_ROLLING_WEEK) ~ NA,
      .default = CURRENT_ROLLING_WEEK / PREV_ROLLING_WEEK - 1
    ),
    WK_DEP_ARR_DIF_PREV_YEAR_PERC = case_when(
      ROLLING_WEEK_PREV_YEAR == 0 | is.na(ROLLING_WEEK_PREV_YEAR) ~ NA,
      .default = CURRENT_ROLLING_WEEK / ROLLING_WEEK_PREV_YEAR - 1
    ),
    APT_RANK = paste0(tolower(ARP_CODE), R_RANK)
  ) |>
  select(
    APT_RANK,
    WK_RANK_DIF_PREV_WEEK,
    WK_AO_GRP_NAME = AO_GRP_NAME,
    WK_FROM_DATE = FROM_DATE,
    WK_TO_DATE = TO_DATE,
    WK_DEP_ARR_AVG = CURRENT_ROLLING_WEEK,
    WK_DEP_ARR_DIF_PREV_WEEK_PERC,
    WK_DEP_ARR_DIF_PREV_YEAR_PERC
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
ap_apt_data_year <- apt_ao_y2d |>
  mutate(
    FROM_DATE = max(FROM_DATE),
    TO_DATE = max(TO_DATE),
    PERIOD =   case_when(
      YEAR == max(YEAR) ~ 'CURRENT_YEAR',
      YEAR == max(YEAR) - 1 ~ 'PREV_YEAR',
      .default = paste0('PERIOD_', YEAR)
    )
  ) |>
  select(-YEAR) |>
  spread(key = PERIOD, value = DEP_ARR) |>
  arrange(ARP_CODE, R_RANK) |>
  mutate(
    Y2D_RANK_DIF_PREV_YEAR = case_when(
      is.na(RANK_PY) ~ RANK,
      .default = RANK_PY - RANK
    ),
    Y2D_DEP_ARR_DIF_PREV_YEAR_PERC =   case_when(
      PREV_YEAR == 0 | is.na(PREV_YEAR) ~ NA,
      .default = CURRENT_YEAR / PREV_YEAR - 1
    ),
    Y2D_DEP_ARR_DIF_2019_PERC  = case_when(
      PERIOD_2019 == 0 | is.na(PERIOD_2019) ~ NA,
      .default = CURRENT_YEAR / PERIOD_2019 - 1
    ),
    APT_RANK = paste0(tolower(ARP_CODE), R_RANK)
  ) |>
  select(
    APT_RANK,
    Y2D_RANK_DIF_PREV_YEAR,
    Y2D_AO_GRP_NAME = AO_GRP_NAME,
    Y2D_TO_DATE = TO_DATE,
    Y2D_DEP_ARR_AVG = CURRENT_YEAR,
    Y2D_DEP_ARR_DIF_PREV_YEAR_PERC,
    Y2D_DEP_ARR_DIF_2019_PERC
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
    MAIN_TFC_AO_GRP_DEP_ARR = if_else(
      R_RANK <= 4,
      CURRENT_DAY,
      NA
    ),
    APT_RANK = paste0(tolower(ARP_CODE), R_RANK)
  ) |>
  select(APT_RANK, MAIN_TFC_AO_GRP_NAME, MAIN_TFC_AO_GRP_CODE,
         MAIN_TFC_AO_GRP_DEP_ARR)

apt_ao_main_traffic_dif <- apt_ao_data_day_int |>
  arrange(ARP_CODE, desc(abs(APT_TFC_AO_GRP_DIF)), R_RANK) |>
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
    MAIN_TFC_DIF_AO_GRP_DEP_ARR_DIF = if_else(
      RANK_DIF_AO_TFC <= 4,
      APT_TFC_AO_GRP_DIF,
      NA
    )
  ) |>
  arrange(ARP_CODE, desc(MAIN_TFC_DIF_AO_GRP_DEP_ARR_DIF)) |>
  group_by(ARP_CODE) |>
  mutate(
    RANK_MAIN_DIF = row_number(),
    APT_RANK = paste0(tolower(ARP_CODE), RANK_MAIN_DIF)
  ) |>
  ungroup() |>
  select(APT_RANK, MAIN_TFC_DIF_AO_GRP_NAME, MAIN_TFC_DIF_AO_GRP_CODE,
         MAIN_TFC_DIF_AO_GRP_DEP_ARR_DIF)

# last line of this section
#save_json(ap_ao_traffic_j, "ap_ao_traffic", archive_file = FALSE)




# ____________________________________________________________________________________________
#
#    APT Group graphs  -----
#
# ____________________________________________________________________________________________

## TRAFFIC ----
### 7-day traffic avg ----
ap_traffic_evo <- ap_traffic_delay_data  %>%
  mutate(RWK_AVG_TFC = if_else(FLIGHT_DATE > min(data_day_date,
                                                 max(LAST_DATA_DAY, na.rm = TRUE),na.rm = TRUE), NA, RWK_AVG_DEP_ARR)) %>%
  select(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE,
    RWK_AVG_TFC,
    RWK_AVG_TFC_PREV_YEAR = RWK_AVG_DEP_ARR_PREV_YEAR,
    RWK_AVG_TFC_2020 = RWK_AVG_DEP_ARR_2020,
    RWK_AVG_TFC_2019 = RWK_AVG_DEP_ARR_2019
  )



column_names <- c('ARP_CODE', 'ARP_NAME', 'FLIGHT_DATE', data_day_year, data_day_year-1, 2020, 2019)
colnames(ap_traffic_evo) <- column_names

### nest data
ap_traffic_evo_long <- ap_traffic_evo %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME,FLIGHT_DATE), names_to = 'year', values_to = 'RWK_AVG_TFC') %>%
  group_by(ARP_CODE, ARP_NAME,FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")


ap_traffic_evo_j <- ap_traffic_evo_long %>% toJSON(., pretty = TRUE)

save_json(ap_traffic_evo_j, "ap_traffic_evo_chart_daily")


## PUNCTUALITY ----
### 7-day punctuality avg ----

ap_punct_evo <- ap_punct_raw %>%
  filter(DAY_DATE >= as.Date(paste0("01-01-", data_day_year-2), format = "%d-%m-%Y")) %>%
  arrange(ARP_CODE, DAY_DATE) %>%
  mutate(
    #ARR_PUNCTUAL_FLIGHTS = 0,  ## while the figures are not showable
    #DEP_PUNCTUAL_FLIGHTS = 0,  ## while the figures are not showable
    DEP_PUN_WK = rollsum(DEP_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") /
      rollsum(DEP_SCHEDULE_FLIGHT,7, fill = NA, align = "right") * 100,
    ARR_PUN_WK = rollsum(ARR_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") /
      rollsum(ARR_SCHEDULE_FLIGHT,7, fill = NA, align = "right") * 100,
    OP_FLT_WK = 100 - rollsum(MISSING_SCHED_FLIGHTS, 7, fill = NA, align = "right") /
    rollsum((MISSING_SCHED_FLIGHTS+DEP_FLIGHTS_NO_OVERFLIGHTS),7, fill = NA, align = "right")*100
  ) %>%
  filter(DAY_DATE >= as.Date(paste0("01-01-", data_day_year-1), format = "%d-%m-%Y"),
         DAY_DATE <= last_day_punct) %>%
  right_join(apt_icao, join_by("ARP_CODE"=="apt_icao_code")) %>%
  select(
    ARP_CODE,
    ARP_NAME,
    DAY_DATE,
    DEP_PUN_WK,
    ARR_PUN_WK,
    OP_FLT_WK
  )

column_names <- c('ARP_CODE',
                  'ARP_NAME',
                  'FLIGHT_DATE',
                  "Departure punct.",
                  "Arrival punct.",
                  "Operated schedules"
                  )

colnames(ap_punct_evo) <- column_names

### nest data
ap_punct_evo_long <- ap_punct_evo %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value') %>%
  group_by(ARP_CODE, ARP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")


ap_punct_evo_j <- ap_punct_evo_long %>% toJSON(., pretty = TRUE)

save_json(ap_punct_evo_j, "ap_punct_evo_chart")


## DELAY ----
### Delay category ----

ap_delay_cause_data <-  ap_delay_data_ %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
  mutate(
    TDM = DAY_DLY,
    TDM_G = TDM_ARP_ARR_G,
    TDM_CS = TDM_ARP_ARR_CS,
    TDM_IT = TDM_ARP_ARR_IT ,
    TDM_WD = TDM_ARP_ARR_WD,
    TDM_NOCSGITWD = TDM - TDM_G - TDM_CS - TDM_IT - TDM_WD,
    TDM_PREV_YEAR = DAY_DLY_PREV_YEAR
  ) %>%                              # create 7day average for y2d graph
  mutate(
    RWK_TDM_G = rollsum(TDM_G, 7, fill = NA, align = "right") / 7,
    RWK_TDM_CS = rollsum(TDM_CS, 7, fill = NA, align = "right") / 7,
    RWK_TDM_IT = rollsum(TDM_IT, 7, fill = NA, align = "right") / 7,
    RWK_TDM_WD = rollsum(TDM_WD, 7, fill = NA, align = "right") / 7,
    RWK_TDM_NOCSGITWD = rollsum(TDM_NOCSGITWD, 7, fill = NA, align = "right") / 7,
    RWK_TDM_PREV_YEAR = rollsum(TDM_PREV_YEAR, 7, fill = NA, align = "right") / 7
  ) %>%
  filter(FLIGHT_DATE >= as.Date(paste0("01-01-", data_day_year), format = "%d-%m-%Y"))

#### day ----
ap_delay_cause_day <- ap_delay_cause_data %>%
  filter(FLIGHT_DATE == min(max(FLIGHT_DATE),
                            data_day_date,
                            na.rm = TRUE)
  )%>%
  mutate(
    SHARE_TDM_G = if_else(TDM == 0, 0, TDM_G / TDM),
    SHARE_TDM_CS = if_else(TDM == 0, 0, TDM_CS / TDM),
    SHARE_TDM_IT = if_else(TDM == 0, 0, TDM_IT / TDM),
    SHARE_TDM_WD = if_else(TDM == 0, 0, TDM_WD / TDM),
    SHARE_TDM_NOCSGITWD = if_else(TDM == 0, 0, TDM_NOCSGITWD / TDM)
  ) %>%
  select(ARP_CODE,
         ARP_NAME,
         FLIGHT_DATE,
         TDM_G,
         TDM_CS,
         TDM_IT,
         TDM_WD,
         TDM_NOCSGITWD,
         TDM_PREV_YEAR,
         SHARE_TDM_G,
         SHARE_TDM_CS,
         SHARE_TDM_IT,
         SHARE_TDM_WD,
         SHARE_TDM_NOCSGITWD
  )

column_names <- c(
  "ARP_CODE",
  "ARP_NAME",
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

colnames(ap_delay_cause_day) <- column_names

### nest data
ap_delay_value_day_long <- ap_delay_cause_day %>%
  select(-c(share_aerodrome_capacity,
            share_capacity_staffing_atc,
            share_disruptions_atc,
            share_weather,
            share_other)
  ) %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

ap_delay_share_day_long <- ap_delay_cause_day %>%
  select(-c("Aerodrome capacity",
            "Capacity/Staffing (ATC)",
            "Disruptions (ATC)",
            "Weather",
            "Other",
            paste0("Total delay ", data_day_year - 1)
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

ap_delay_cause_day_long <- cbind(ap_delay_value_day_long, ap_delay_share_day_long) %>%
  select(-name) %>%
  group_by(ARP_CODE, ARP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

# for consistency with v1 we use the word category in the name files... should have been cause
ap_delay_cause_evo_dy_j <- ap_delay_cause_day_long %>% toJSON(., pretty = TRUE)

save_json(ap_delay_cause_evo_dy_j, "ap_delay_category_evo_chart_dy")





#### week ----
ap_delay_cause_wk <- ap_delay_cause_data %>%
  filter(FLIGHT_DATE >= min(max(FLIGHT_DATE), data_day_date, na.rm  = TRUE) + lubridate::days(-6),
         FLIGHT_DATE <= min(max(FLIGHT_DATE), data_day_date, na.rm  = TRUE)
  )  %>%
  group_by(ARP_CODE) %>%
  reframe(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE,
    TDM_G,
    TDM_CS,
    TDM_IT,
    TDM_WD,
    TDM_NOCSGITWD,
    TDM_PREV_YEAR,
    WK_TDM_G = sum(TDM_G),
    WK_TDM_CS = sum(TDM_CS),
    WK_TDM_IT = sum(TDM_IT),
    WK_TDM_WD = sum(TDM_WD),
    WK_TDM_NOCSGITWD = sum(TDM_NOCSGITWD),
    WK_TDM = sum(TDM)
  ) %>%
  ungroup() %>%
  mutate(
    WK_SHARE_TDM_G = if_else(WK_TDM == 0, 0, WK_TDM_G / WK_TDM),
    WK_SHARE_TDM_CS = if_else(WK_TDM == 0, 0, WK_TDM_CS / WK_TDM),
    WK_SHARE_TDM_IT = if_else(WK_TDM == 0, 0, WK_TDM_IT / WK_TDM),
    WK_SHARE_TDM_WD = if_else(WK_TDM == 0, 0, WK_TDM_WD / WK_TDM),
    WK_SHARE_TDM_NOCSGITWD = if_else(WK_TDM == 0, 0, WK_TDM_NOCSGITWD / WK_TDM)
  ) %>%
  select(ARP_CODE,
         ARP_NAME,
         FLIGHT_DATE,
         TDM_G,
         TDM_CS,
         TDM_IT,
         TDM_WD,
         TDM_NOCSGITWD,
         TDM_PREV_YEAR,
         WK_SHARE_TDM_G,
         WK_SHARE_TDM_CS,
         WK_SHARE_TDM_IT,
         WK_SHARE_TDM_WD,
         WK_SHARE_TDM_NOCSGITWD
  )

#check <- apt_delay_cause_wk %>% group_by(ARP_CODE,
#                                ARP_NAME) %>%
#  summarise(ARR= sum(DAY_ARR),
#            TDM = sum(TDM),
#
#            `Aerodrome capacity` = sum(TDM_G),
#            `Capacity/Staffing (ATC)` = sum(TDM_CS),
#            `Disruptions (ATC)` = sum(TDM_IT),
#            `Weather` = sum(TDM_WD),
#            `Other`  =  sum(TDM_NOCSGITWD),
#            `Aerodrome capacity_share` = sum(TDM_G)/sum(TDM),
#            `Capacity/Staffing (ATC)_share` = sum(TDM_CS)/sum(TDM),
#            `Disruptions (ATC)_share` = sum(TDM_IT)/sum(TDM),
#            `Weather_share` = sum(TDM_WD)/sum(TDM),
#            `Other_share`  =  sum(TDM_NOCSGITWD)/sum(TDM)
#  )


colnames(ap_delay_cause_wk) <- column_names


### nest data
ap_delay_value_wk_long <- ap_delay_cause_wk %>%
  select(-c(share_aerodrome_capacity,
            share_capacity_staffing_atc,
            share_disruptions_atc,
            share_weather,
            share_other)
  ) %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

ap_delay_share_wk_long <- ap_delay_cause_wk %>%
  select(-c("Aerodrome capacity",
            "Capacity/Staffing (ATC)",
            "Disruptions (ATC)",
            "Weather",
            "Other",
            paste0("Total delay ", data_day_year - 1)
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

ap_delay_cause_wk_long <- cbind(ap_delay_value_wk_long, ap_delay_share_wk_long) %>%
  select(-name) %>%
  group_by(ARP_CODE, ARP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")


# for consistency with v1 we use the word category in the name files... should have been cause
ap_delay_cause_evo_wk_j <- ap_delay_cause_wk_long %>% toJSON(., pretty = TRUE)

save_json(ap_delay_cause_evo_wk_j, "apt_delay_category_evo_chart_wk")


#### y2d ----
ap_delay_cause_y2d <- ap_delay_cause_data %>%
  filter(FLIGHT_DATE <= data_day_date) %>%
  group_by(ARP_CODE) %>%
  reframe(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE,
    RWK_TDM_G,
    RWK_TDM_CS,
    RWK_TDM_IT,
    RWK_TDM_WD,
    RWK_TDM_NOCSGITWD,
    RWK_TDM_PREV_YEAR,
    Y2D_TDM_G = sum(TDM_G),
    Y2D_TDM_CS = sum(TDM_CS),
    Y2D_TDM_IT = sum(TDM_IT),
    Y2D_TDM_WD = sum(TDM_WD),
    Y2D_TDM_NOCSGITWD = sum(TDM_NOCSGITWD),
    Y2D_TDM = sum(TDM)
  ) %>%
  ungroup() %>%
  mutate(
    Y2D_SHARE_TDM_G = if_else(Y2D_TDM == 0, 0, Y2D_TDM_G / Y2D_TDM),
    Y2D_SHARE_TDM_CS = if_else(Y2D_TDM == 0, 0, Y2D_TDM_CS / Y2D_TDM),
    Y2D_SHARE_TDM_IT = if_else(Y2D_TDM == 0, 0, Y2D_TDM_IT / Y2D_TDM),
    Y2D_SHARE_TDM_WD = if_else(Y2D_TDM == 0, 0, Y2D_TDM_WD / Y2D_TDM),
    Y2D_SHARE_TDM_NOCSGITWD = if_else(Y2D_TDM == 0, 0, Y2D_TDM_NOCSGITWD / Y2D_TDM)
  ) %>%
  select(ARP_CODE,
         ARP_NAME,
         FLIGHT_DATE,
         RWK_TDM_G,
         RWK_TDM_CS,
         RWK_TDM_IT,
         RWK_TDM_WD,
         RWK_TDM_NOCSGITWD,
         RWK_TDM_PREV_YEAR,
         Y2D_SHARE_TDM_G,
         Y2D_SHARE_TDM_CS,
         Y2D_SHARE_TDM_IT,
         Y2D_SHARE_TDM_WD,
         Y2D_SHARE_TDM_NOCSGITWD
  )

colnames(ap_delay_cause_y2d) <- column_names

### nest data
ap_delay_value_y2d_long <- ap_delay_cause_y2d %>%
  select(-c(share_aerodrome_capacity,
            share_capacity_staffing_atc,
            share_disruptions_atc,
            share_weather,
            share_other)
  ) %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

ap_delay_share_y2d_long <- ap_delay_cause_y2d %>%
  select(-c("Aerodrome capacity",
            "Capacity/Staffing (ATC)",
            "Disruptions (ATC)",
            "Weather",
            "Other",
            paste0("Total delay ", data_day_year - 1)
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

ap_delay_cause_y2d_long <- cbind(ap_delay_value_y2d_long, ap_delay_share_y2d_long) %>%
  select(-name) %>%
  group_by(ARP_CODE, ARP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")


# for consistency with v1 we use the word category in the name files... should have been cause
ap_delay_cause_evo_y2d_j <- ap_delay_cause_y2d_long %>% toJSON(., pretty = TRUE)

save_json(ap_delay_cause_evo_y2d_j, "ap_delay_category_evo_chart_y2d")


### Delay type ----
ap_delay_type_data <- ap_delay_data_ %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
  group_by(ARP_CODE) %>%
  arrange(ARP_CODE, FLIGHT_DATE) %>%
  mutate(
    DAY_TFC,
    DY_DLY_FLT = if_else(DAY_TFC == 0, 0, DAY_DLY/DAY_TFC),
    #DY_DLY_FLT_ERT = if_else(DAY_TFC == 0, 0, DAY_ERT_DLY/DAY_TFC),
    #DY_DLY_FLT_APT = if_else(DAY_TFC == 0, 0, DAY_ARP_DLY/DAY_TFC),
    DY_DLY_FLT_PREV_YEAR = if_else(DAY_TFC_PREV_YEAR == 0, 0, DAY_DLY_PREV_YEAR/DAY_TFC_PREV_YEAR),

    #DY_SHARE_DLY_FLT_ERT = if_else(DY_DLY_FLT == 0, 0, DY_DLY_FLT_ERT/DY_DLY_FLT),
    #DY_SHARE_DLY_FLT_APT = if_else(DY_DLY_FLT == 0, 0, DY_DLY_FLT_APT/DY_DLY_FLT),

    RWK_DLY_FLT = if_else(AVG_TFC_ROLLING_WEEK == 0, 0, AVG_DLY_ROLLING_WEEK/AVG_TFC_ROLLING_WEEK),
    #RWK_DLY_FLT_ERT = if_else(AVG_TFC_ROLLING_WEEK == 0, 0, AVG_ERT_DLY_ROLLING_WEEK/AVG_TFC_ROLLING_WEEK),
    #RWK_DLY_FLT_APT = if_else(AVG_TFC_ROLLING_WEEK == 0, 0, AVG_ARP_DLY_ROLLING_WEEK/AVG_TFC_ROLLING_WEEK),
    RWK_DLY_FLT_PREV_YEAR = if_else(AVG_TFC_ROLLING_WEEK_PREV_YEAR == 0, 0, AVG_DLY_ROLLING_WEEK_PREV_YEAR / AVG_TFC_ROLLING_WEEK_PREV_YEAR),

    Y2D_DLY_FLT = if_else(Y2D_TFC_YEAR == 0, 0, Y2D_DLY_YEAR/Y2D_TFC_YEAR),
    #Y2D_DLY_FLT_ERT = if_else(Y2D_TFC_YEAR == 0, 0, Y2D_ERT_DLY_YEAR/Y2D_TFC_YEAR),
    #Y2D_DLY_FLT_APT = if_else(Y2D_TFC_YEAR == 0, 0, Y2D_ARP_DLY_YEAR/Y2D_TFC_YEAR),
    Y2D_DLY_FLT_PREV_YEAR = if_else(Y2D_TFC_YEAR_PREV_YEAR == 0, 0, Y2D_DLY_YEAR_PREV_YEAR / Y2D_TFC_YEAR_PREV_YEAR)
  ) %>%
  ungroup()  %>%
  filter(FLIGHT_DATE >= as.Date(paste0("01-01-", data_day_year), format = "%d-%m-%Y")) %>%
  arrange(ARP_CODE, FLIGHT_DATE)

#### day ----
ap_delay_type_day <- ap_delay_type_data %>%
  filter(FLIGHT_DATE == min(max(FLIGHT_DATE),
                            data_day_date,
                            na.rm = TRUE)) %>%
  select(ARP_CODE,
         ARP_NAME,
         FLIGHT_DATE,
         DY_DLY_FLT,
         #DY_DLY_FLT_ERT,
         #DY_DLY_FLT_APT,
         DY_DLY_FLT_PREV_YEAR,
         #DY_SHARE_DLY_FLT_ERT,
         #DY_SHARE_DLY_FLT_APT
  )

column_names <- c(
  "ARP_CODE",
  "ARP_NAME",
  "FLIGHT_DATE",
  "Arrival ATFM delay/flight",
  #"En-route ATFM delay/flight",
  #"Airport ATFM delay/flight",
  paste0("Arrival ATFM delay/flight ", data_day_year - 1)
  #,
  #"share_en_route",
  #"share_airport"
)

colnames(ap_delay_type_day) <- column_names

### nest data
ap_delay_type_value_day_long <- ap_delay_type_day %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

ap_delay_type_share_day_long <- ap_delay_type_day %>%
  select(-c(paste0("Arrival ATFM delay/flight ", data_day_year - 1)
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

ap_delay_type_day_long <- cbind(ap_delay_type_value_day_long, ap_delay_type_share_day_long) %>%
  select(-name) %>%
  group_by(ARP_CODE, ARP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

ap_delay_type_evo_dy_j <- ap_delay_type_day_long %>% toJSON(., pretty = TRUE)

save_json(ap_delay_type_evo_dy_j, "ap_delay_flt_type_evo_chart_dy")


#### week ----
ap_delay_type_wk <- ap_delay_type_data %>%
  filter(FLIGHT_DATE >= min(max(FLIGHT_DATE), data_day_date, na.rm  = TRUE) + lubridate::days(-6),
         FLIGHT_DATE <= min(max(FLIGHT_DATE), data_day_date, na.rm  = TRUE)
  ) %>%
  select(ARP_CODE,
         ARP_NAME,
         FLIGHT_DATE,
         DY_DLY_FLT,
         #DY_DLY_FLT_ERT,
         #DY_DLY_FLT_APT,
         DY_DLY_FLT_PREV_YEAR,
         DAY_TFC,
         DAY_DLY
         #,
         #DAY_ERT_DLY,
         #DAY_ARP_DLY
  ) %>%
  group_by(ARP_CODE) %>%
  reframe(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE,
    DY_DLY_FLT,
    #DY_DLY_FLT_ERT,
    #DY_DLY_FLT_APT,
    DY_DLY_FLT_PREV_YEAR,

    WK_TFC = sum(DAY_TFC),
    WK_DLY = sum(DAY_DLY),
    #WK_DLY_ERT = sum(DAY_ERT_DLY),
    #WK_DLY_APT = sum(DAY_ARP_DLY),

    WK_DLY_FLT = if_else(WK_TFC == 0, 0, WK_DLY/WK_TFC)
    #,
    #WK_DLY_FLT_ERT = if_else(WK_TFC == 0, 0, WK_DLY_ERT/WK_TFC),
    #WK_DLY_FLT_APT = if_else(WK_TFC == 0, 0, WK_DLY_APT/WK_TFC),

    #WK_SHARE_DLY_FLT_ERT = if_else(WK_DLY_FLT == 0, 0, WK_DLY_FLT_ERT/WK_DLY_FLT),
    #WK_SHARE_DLY_FLT_APT = if_else(WK_DLY_FLT == 0, 0, WK_DLY_FLT_APT/WK_DLY_FLT)
  ) %>%
  select(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE,
    DY_DLY_FLT,
    #DY_DLY_FLT_ERT,
    #DY_DLY_FLT_APT,
    DY_DLY_FLT_PREV_YEAR
    #,
    #WK_SHARE_DLY_FLT_ERT,
    #WK_SHARE_DLY_FLT_APT
  )

colnames(ap_delay_type_wk) <- column_names

### nest data
ap_delay_type_value_wk_long <- ap_delay_type_wk  %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

ap_delay_type_share_wk_long <- ap_delay_type_wk %>%
  select(-c(paste0("Arrival ATFM delay/flight ", data_day_year - 1)
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(ARP_CODE,ARP_NAME, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

ap_delay_type_wk_long <- cbind(ap_delay_type_value_wk_long, ap_delay_type_share_wk_long) %>%
  select(-name) %>%
  group_by(ARP_CODE, ARP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

ap_delay_type_evo_wk_j <- ap_delay_type_wk_long %>% toJSON(., pretty = TRUE)

save_json(ap_delay_type_evo_wk_j, "ap_delay_flt_type_evo_chart_wk")

#### y2d ----
ap_delay_type_y2d <- ap_delay_type_data %>%
  filter(FLIGHT_DATE <= data_day_date) %>%
  group_by(ARP_CODE) %>%
  reframe(
    ARP_CODE,
    ARP_NAME,
    FLIGHT_DATE,
    RWK_DLY_FLT,
    #RWK_DLY_FLT_ERT,
    #RWK_DLY_FLT_APT,
    RWK_DLY_FLT_PREV_YEAR,
    Y2D_TFC = sum(DAY_TFC),
    Y2D_DLY_FLT = if_else(Y2D_TFC == 0, 0, sum(DAY_DLY)/Y2D_TFC)
    #,
    #Y2D_DLY_FLT_ERT = if_else(Y2D_TFC == 0, 0, sum(DAY_ERT_DLY)/Y2D_TFC),
    #Y2D_DLY_FLT_APT = if_else(Y2D_TFC == 0, 0, sum(DAY_ARP_DLY)/Y2D_TFC),
  ) %>%
  #mutate(
  #  Y2D_SHARE_DLY_FLT_ERT = if_else(Y2D_DLY_FLT == 0, 0, Y2D_DLY_FLT_ERT / Y2D_DLY_FLT),
  #  Y2D_SHARE_DLY_FLT_APT = if_else(Y2D_DLY_FLT == 0, 0, Y2D_DLY_FLT_APT / Y2D_DLY_FLT)
  #) %>%
  select(ARP_CODE,
         ARP_NAME,
         FLIGHT_DATE,
         RWK_DLY_FLT,
         #RWK_DLY_FLT_ERT,
         #RWK_DLY_FLT_APT,
         RWK_DLY_FLT_PREV_YEAR
         #,
         #Y2D_SHARE_DLY_FLT_ERT,
         #Y2D_SHARE_DLY_FLT_APT
  )

colnames(ap_delay_type_y2d) <- column_names

### nest data
ap_delay_type_value_y2d_long <- ap_delay_type_y2d %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

ap_delay_type_share_y2d_long <- ap_delay_type_y2d %>%
  select(-c(paste0("Arrival ATFM delay/flight ", data_day_year - 1)
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(ARP_CODE, ARP_NAME, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

ap_delay_type_y2d_long <- cbind(ap_delay_type_value_y2d_long, ap_delay_type_share_y2d_long) %>%
  select(-name) %>%
  group_by(ARP_CODE, ARP_NAME, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

ap_delay_type_evo_y2d_j <- ap_delay_type_y2d_long %>% toJSON(., pretty = TRUE)

save_json(ap_delay_type_evo_y2d_j, "ap_delay_flt_type_evo_chart_y2d")

