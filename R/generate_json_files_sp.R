## libraries
library(arrow)
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

# dimensions ----
if (!exists("dim_iso_country")) {
  source(here("..", "mobile-app", "R", "dimensions.R")) 
}

rel_ansp_acc <- list_ansp %>% 
  mutate(
    iso_2letter = str_remove_all(ANSP_CODE, "_ANSP"),
    ## smatsa exception
    iso_2letter = str_replace(iso_2letter, "MERS", "RS"),
  ) %>% 
  left_join(list_acc, by = c("iso_2letter" = "ISO_2LETTER")) %>% 
  select(
    ICAO_CODE,
    ANSP_ID,
    ANSP_CODE,
    ANSP_NAME
  )

# Parameters ----
source(here("..", "mobile-app", "R", "params.R"))

# archive mode for past dates
if (exists("archive_mode") == FALSE) {archive_mode <- FALSE}
if (exists("data_day_date") == FALSE) {
  data_day_date <- lubridate::today(tzone = "") +  days(-1)
}

data_day_text <- data_day_date %>% format("%Y%m%d")
data_day_year <- as.numeric(format(data_day_date,'%Y'))

# queries ----
source(here("..", "mobile-app", "R", "data_queries.R")) 

print(paste("Generating sp json files", format(data_day_date, "%Y-%m-%d"), "..."))

sp_json_app <-""

# ____________________________________________________________________________________________
#
#    ANSP landing page -----
#
# ____________________________________________________________________________________________

#### Import data ----
mydatafile <- paste0("sp_traffic_delay_day.parquet")
stakeholder <- substr(mydatafile, 1,2)

sp_traffic_delay_data <- read_parquet(here(app_tables_dir, stakeholder, mydatafile)) %>% 
  filter(YEAR == data_day_year) %>% 
  rename_with(~ sub("DAY_", "DY_", .x, fixed = TRUE), contains("DAY_")) %>% 
  rename_with(~ sub("RWK_", "WK_", .x, fixed = TRUE), contains("RWK_")) %>% 
  rename(ANSP_NAME = STK_NAME, ANSP_CODE = STK_CODE) %>%
  arrange(ANSP_NAME, FLIGHT_DATE)
  
sp_traffic_delay_last_day <- sp_traffic_delay_data %>%
 filter(FLIGHT_DATE == min(data_day_date,
                           max(DATA_DAY, na.rm = TRUE),
                           na.rm = TRUE)
  ) 

#### Traffic ----
sp_traffic_for_json <- sp_traffic_delay_last_day %>%
  ### rank calculation
  mutate(
    DY_TFC_RANK = min_rank(desc(DY_TFC)),
    WK_TFC_RANK = min_rank(desc(WK_AVG_TFC)),
    Y2D_TFC_RANK = min_rank(desc(Y2D_AVG_TFC)),
    S2D_TFC_RANK = min_rank(desc(S2D_AVG_TFC)),
    TFC_RANK_TEXT = "*Top rank for highest.",
    
  ) %>%
  select(
    ANSP_NAME,
    ANSP_CODE,
    FLIGHT_DATE,
    
    DY_TFC_RANK,
    DY_TFC,
    DY_TFC_DIF_PREV_YEAR_PERC,
    DY_TFC_DIF_2019_PERC,
    
    WK_TFC_RANK,
    WK_TFC_AVG_ROLLING = WK_AVG_TFC,
    WK_TFC_DIF_PREV_YEAR_PERC,
    WK_TFC_DIF_2019_PERC,
    
    Y2D_TFC_RANK,
    Y2D_TFC,
    Y2D_TFC_AVG = Y2D_AVG_TFC,
    Y2D_TFC_DIF_PREV_YEAR_PERC,
    Y2D_TFC_DIF_2019_PERC,
    
    S2D_TFC_RANK,
    S2D_TFC,
    S2D_TFC_AVG = S2D_AVG_TFC,
    S2D_TFC_DIF_PREV_YEAR_PERC,
    S2D_TFC_DIF_2019_PERC
  )

#### Delay ----
sp_delay_for_json <- sp_traffic_delay_last_day %>%
  select(
    ANSP_NAME,
    ANSP_CODE,
    FLIGHT_DATE,

    # delay
    DY_DLY,
    DY_DLY_DIF_PREV_YEAR_PERC,
    DY_DLY_DIF_2019_PERC,

    WK_DLY_AVG_ROLLING = WK_AVG_DLY,
    WK_DLY_DIF_PREV_YEAR_PERC,
    WK_DLY_DIF_2019_PERC,

    Y2D_DLY_AVG = Y2D_AVG_DLY,
    Y2D_DLY_DIF_PREV_YEAR_PERC,
    Y2D_DLY_DIF_2019_PERC,

    S2D_DLY_AVG = S2D_AVG_DLY,
    S2D_DLY_DIF_PREV_YEAR_PERC,
    S2D_DLY_DIF_2019_PERC,
    
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

    S2D_DLY_FLT,
    S2D_DLY_FLT_DIF_PREV_YEAR_PERC,
    S2D_DLY_FLT_DIF_2019_PERC,
    
    #% of delayed flights
    DY_DELAYED_TFC_PERC = DY_DLYED_PERC,
    DY_DELAYED_TFC_PERC_DIF_PREV_YEAR = DY_DLYED_PERC_DIF_PREV_YEAR,
    DY_DELAYED_TFC_PERC_DIF_2019 = DY_DLYED_PERC_DIF_2019,

    WK_DELAYED_TFC_PERC = WK_DLYED_PERC,
    WK_DELAYED_TFC_PERC_DIF_PREV_YEAR = WK_DLYED_PERC_DIF_PREV_YEAR,
    WK_DELAYED_TFC_PERC_DIF_2019 = WK_DLYED_PERC_DIF_2019,

    Y2D_DELAYED_TFC_PERC = Y2D_DLYED_PERC,
    Y2D_DELAYED_TFC_PERC_DIF_PREV_YEAR = Y2D_DLYED_PERC_DIF_PREV_YEAR,
    Y2D_DELAYED_TFC_PERC_DIF_2019 = Y2D_DLYED_PERC_DIF_2019,

    S2D_DELAYED_TFC_PERC = S2D_DLYED_PERC,
    S2D_DELAYED_TFC_PERC_DIF_PREV_YEAR = S2D_DLYED_PERC_DIF_PREV_YEAR,
    S2D_DELAYED_TFC_PERC_DIF_2019 = S2D_DLYED_PERC_DIF_2019,
    
    #% of delayed flights >15'
    DY_DELAYED_TFC_15_PERC = DY_DLYED_15_PERC,
    DY_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = DY_DLYED_15_PERC_DIF_PREV_YEAR,
    DY_DELAYED_TFC_15_PERC_DIF_2019 = DY_DLYED_15_PERC_DIF_2019,
    
    WK_DELAYED_TFC_15_PERC = WK_DLYED_15_PERC,
    WK_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = WK_DLYED_15_PERC_DIF_PREV_YEAR,
    WK_DELAYED_TFC_15_PERC_DIF_2019 = WK_DLYED_15_PERC_DIF_2019,
    
    Y2D_DELAYED_TFC_15_PERC = Y2D_DLYED_15_PERC,
    Y2D_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = Y2D_DLYED_15_PERC_DIF_PREV_YEAR,
    Y2D_DELAYED_TFC_15_PERC_DIF_2019 = Y2D_DLYED_15_PERC_DIF_2019,
    
    S2D_DELAYED_TFC_15_PERC = S2D_DLYED_15_PERC,
    S2D_DELAYED_TFC_15_PERC_DIF_PREV_YEAR = S2D_DLYED_15_PERC_DIF_PREV_YEAR,
    S2D_DELAYED_TFC_15_PERC_DIF_2019 = S2D_DLYED_15_PERC_DIF_2019
    
  ) %>%  
  ungroup() %>% 
  ### rank calculation
  mutate(
    ## delay
    DY_DLY_RANK = rank(desc(DY_DLY), ties.method = "max"),
    WK_DLY_RANK = rank(desc(WK_DLY_AVG_ROLLING), ties.method = "max"),
    Y2D_DLY_RANK = rank(desc(Y2D_DLY_AVG), ties.method = "max"),
    S2D_DLY_RANK = rank(desc(S2D_DLY_AVG), ties.method = "max"),
    
    ## delay per flight
    DY_DLY_FLT_RANK = rank(desc(DY_DLY_FLT), ties.method = "max"),
    WK_DLY_FLT_RANK = rank(desc(WK_DLY_FLT), ties.method = "max"),
    Y2D_DLY_FLT_RANK = rank(desc(Y2D_DLY_FLT), ties.method = "max"),
    S2D_DLY_FLT_RANK = rank(desc(S2D_DLY_FLT), ties.method = "max"),
    
    ## % delayed flights
    DY_DELAYED_TFC_PERC_RANK = rank(desc(DY_DELAYED_TFC_PERC), ties.method = "max"),
    WK_DELAYED_TFC_PERC_RANK = rank(desc(WK_DELAYED_TFC_PERC), ties.method = "max"),
    Y2D_DELAYED_TFC_PERC_RANK = rank(desc(Y2D_DELAYED_TFC_PERC), ties.method = "max"),
    S2D_DELAYED_TFC_PERC_RANK = rank(desc(S2D_DELAYED_TFC_PERC), ties.method = "max"),
    
    ## % delayed flights
    DY_DELAYED_TFC_15_PERC_RANK = rank(desc(DY_DELAYED_TFC_15_PERC), ties.method = "max"),
    WK_DELAYED_TFC_15_PERC_RANK = rank(desc(WK_DELAYED_TFC_15_PERC), ties.method = "max"),
    Y2D_DELAYED_TFC_15_PERC_RANK = rank(desc(Y2D_DELAYED_TFC_15_PERC), ties.method = "max"),
    S2D_DELAYED_TFC_15_PERC_RANK = rank(desc(S2D_DELAYED_TFC_15_PERC), ties.method = "max"),
    
    DLY_RANK_TEXT = "*Top rank for highest."
  )

#### Join strings and save  ----

sp_json_app_j <- list_ansp %>% arrange(ANSP_NAME)
sp_json_app_j$sp_traffic <- select(arrange(sp_traffic_for_json, ANSP_NAME), -c(ANSP_CODE, ANSP_NAME))
sp_json_app_j$sp_delay <- select(arrange(sp_delay_for_json, ANSP_NAME), -c(ANSP_CODE, ANSP_NAME))

update_day <- floor_date(lubridate::now(), unit = "days") %>%
  as_tibble() %>%
  rename(APP_UPDATE = 1)

sp_json_app_j$sp_update <- update_day

sp_json_app_j <- sp_json_app_j %>%   group_by(ANSP_CODE, ANSP_NAME) %>% 
  # xxx to ensure the comparison with the old version works. it can be removed later 
  relocate(ANSP_NAME, .before= everything()) %>%
  relocate(ANSP_ID, .before= everything()) %>% 
  relocate(ANSP_CODE, .before= everything())

sp_json_app <- sp_json_app_j %>%
  toJSON(., pretty = TRUE)

save_json(sp_json_app, "sp_json_app")
print(paste(format(now(), "%H:%M:%S"), "sp_json_app"))


# ____________________________________________________________________________________________
#
#    ANSP graphs  -----
#
# ____________________________________________________________________________________________

## TRAFFIC ----
### 7-day traffic avg ----
sp_traffic_evo <- sp_traffic_delay_data %>%
  mutate(RWK_AVG_TFC = if_else(FLIGHT_DATE > min(data_day_date,
                                                 max(DATA_DAY, na.rm = TRUE),na.rm = TRUE),
                               NA,
                               WK_AVG_TFC)
         ) %>%
  select(
    ANSP_CODE,
    ANSP_NAME,
    FLIGHT_DATE,
    WK_AVG_TFC,
    WK_AVG_TFC_PREV_YEAR,
    WK_AVG_TFC_2020,
    WK_AVG_TFC_2019
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
print(paste(format(now(), "%H:%M:%S"), "sp_traffic_evo_chart_daily"))

## DELAY ----
### Delay category ----
#### day ----
sp_delay_cause_day <- sp_traffic_delay_data %>%
  ungroup() %>%
  filter(FLIGHT_DATE == min(max(FLIGHT_DATE),
                            data_day_date,
                            na.rm = TRUE)
  ) %>%
  mutate(
    SHARE_DLY_CS = if_else(DY_DLY == 0, 0, DY_DLY_CS / DY_DLY),
    SHARE_DLY_IT = if_else(DY_DLY == 0, 0, DY_DLY_IT / DY_DLY),
    SHARE_DLY_WD = if_else(DY_DLY == 0, 0, DY_DLY_WD / DY_DLY),
    SHARE_DLY_OTHER = if_else(DY_DLY == 0, 0, DY_DLY_OTHER / DY_DLY)
  ) %>%
  select(ANSP_CODE,
         ANSP_NAME,
         FLIGHT_DATE,
         DY_DLY_CS,
         DY_DLY_IT,
         DY_DLY_WD,
         DY_DLY_OTHER,
         DY_DLY_PREV_YEAR,
         SHARE_DLY_CS,
         SHARE_DLY_IT,
         SHARE_DLY_WD,
         SHARE_DLY_OTHER
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
print(paste(format(now(), "%H:%M:%S"), "sp_delay_category_evo_chart_dy"))

#### week ----
sp_delay_cause_week <- sp_traffic_delay_data %>%
  filter(FLIGHT_DATE >= min(max(FLIGHT_DATE),
                           data_day_date,
                           na.rm = TRUE) -6,
         FLIGHT_DATE <= min(max(FLIGHT_DATE),
                           data_day_date,
                           na.rm = TRUE)
  )  %>%
  group_by(ANSP_CODE) %>% 
  mutate(
    WK_SHARE_DLY_CS = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_CS) / sum(DY_DLY)),
    WK_SHARE_DLY_IT = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_IT) / sum(DY_DLY)),
    WK_SHARE_DLY_WD = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_WD) / sum(DY_DLY)),
    WK_SHARE_DLY_OTHER = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_OTHER) / sum(DY_DLY))
  ) %>%
  ungroup() %>% 
  select(
    ANSP_CODE,
    ANSP_NAME,
    FLIGHT_DATE,
    DY_DLY_CS,
    DY_DLY_IT,
    DY_DLY_WD,
    DY_DLY_OTHER,
    DY_DLY_PREV_YEAR,
    WK_SHARE_DLY_CS,
    WK_SHARE_DLY_IT,
    WK_SHARE_DLY_WD,
    WK_SHARE_DLY_OTHER
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
print(paste(format(now(), "%H:%M:%S"), "sp_delay_category_evo_chart_wk"))

#### y2d ----
sp_delay_cause_y2d <- sp_traffic_delay_data %>%
  filter(FLIGHT_DATE <= data_day_date,
         year(FLIGHT_DATE) == year(data_day_date)) %>%
  group_by(ANSP_CODE) %>% 
  mutate(
    Y2D_SHARE_DLY_CS = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_CS) / sum(DY_DLY)),
    Y2D_SHARE_DLY_IT = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_IT) / sum(DY_DLY)),
    Y2D_SHARE_DLY_WD = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_WD) / sum(DY_DLY)),
    Y2D_SHARE_DLY_OTHER = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_OTHER) / sum(DY_DLY))
  ) %>%
  ungroup() %>% 
  select(
    ANSP_CODE,
    ANSP_NAME,
    FLIGHT_DATE,
    WK_AVG_DLY_CS,
    WK_AVG_DLY_IT,
    WK_AVG_DLY_WD,
    WK_AVG_DLY_OTHER,
    WK_AVG_DLY_PREV_YEAR,
    Y2D_SHARE_DLY_CS,
    Y2D_SHARE_DLY_IT,
    Y2D_SHARE_DLY_WD,
    Y2D_SHARE_DLY_OTHER
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
print(paste(format(now(), "%H:%M:%S"), "sp_delay_category_evo_chart_y2d"))


### Delay per flight ----
sp_delay_flt_evo <- sp_traffic_delay_data %>%
  filter(FLIGHT_DATE <= min(data_day_date,
                            max(FLIGHT_DATE, na.rm = TRUE),
                            na.rm = TRUE),
         year(FLIGHT_DATE) == data_day_year
  )  %>% 
  select(
    ANSP_CODE,
    ANSP_NAME,
    FLIGHT_DATE,
    WK_DLY_FLT,
    WK_DLY_FLT_PREV_YEAR
    )

y2d_delay_flt <- sp_traffic_delay_last_day %>% ungroup() %>% select(ANSP_CODE, Y2D_DLY_FLT, Y2D_DLY_FLT_PREV_YEAR)


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

print(paste(format(now(), "%H:%M:%S"), "sp_delay_per_flight_evo_chart_daily"))

### % of delayed flights ----

sp_delayed_flights_evo <- sp_traffic_delay_data %>% 
  select(
    ANSP_CODE,
    ANSP_NAME,
    FLIGHT_DATE,
    WK_DLYED_PERC,
    WK_DLYED_PERC_PREV_YEAR,
    WK_DLYED_15_PERC,
    WK_DLYED_15_PERC_PREV_YEAR
  ) %>% 
  arrange(ANSP_CODE, FLIGHT_DATE)

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
print(paste(format(now(), "%H:%M:%S"), "sp_delayed_flights_evo_chart_daily"))

# ____________________________________________________________________________________________
#
#    ANSP ranking tables  -----
#
# ____________________________________________________________________________________________

## TRAFFIC ----
### ACC ----
#### day ----
if(!exists("nw_acc_delay_day_raw")) {
    nw_acc_delay_day_raw <- export_query(query_nw_acc_delay_day_raw(format(data_day_date, "%Y-%m%-%d"))) 
}

if (max(nw_acc_delay_day_raw$ENTRY_DATE) != data_day_date) {
  nw_acc_delay_day_raw <- export_query(query_nw_acc_delay_day_raw(format(data_day_date, "%Y-%m%-%d"))) 
}

# process data
sp_acc_traffic_day_int <- nw_acc_delay_day_raw %>%
  left_join(unique(select(list_acc, NAME, ICAO_CODE)), by = c("UNIT_CODE" = "ICAO_CODE")) %>% 
  left_join(rel_ansp_acc, by = c("UNIT_CODE" = "ICAO_CODE")) %>% 
  mutate(
    DY_FLT_RANK = rank(desc(FLIGHT), ties.method = "max"),
  ) %>% 
  group_by(ANSP_ID) %>% 
  mutate(
    SP_RANK = paste0(ANSP_CODE, row_number()),
    DY_FLT_DIF_PREV_WEEK_PERC = if_else(FLIGHT_7DAY == 0, NA, FLIGHT / FLIGHT_7DAY -1),
    DY_FLT_DIF_PREV_YEAR_PERC = if_else(FLIGHT_PREV_YEAR == 0, NA, FLIGHT / FLIGHT_PREV_YEAR -1),
    
    DY_FLT_DIF_PREV_WEEK = FLIGHT - FLIGHT_7DAY
  ) %>% 
  arrange(ANSP_ID, SP_RANK) %>% 
  ungroup()

sp_acc_traffic_day <- sp_acc_traffic_day_int %>% 
  select(
    SP_RANK,
    DY_FLT_RANK,
    DY_FLT_ACC_NAME = NAME,
    DY_TO_DATE = ENTRY_DATE,
    # DY_FLT_TO_DATE = ENTRY_DATE,
    DY_FLT = FLIGHT,
    DY_FLT_DIF_PREV_WEEK_PERC,
    DY_FLT_DIF_PREV_YEAR_PERC
  )


#### week ----
if(!exists("nw_acc_delay_week_raw")) {
  nw_acc_delay_week_raw <- export_query(query_nw_acc_delay_week_raw(format(data_day_date, "%Y-%m%-%d"))) 
}

if (max(nw_acc_delay_week_raw$MAX_ENTRY_DATE) != data_day_date) {
  nw_acc_delay_week_raw <- export_query(query_nw_acc_delay_week_raw(format(data_day_date, "%Y-%m%-%d"))) 
}

sp_acc_traffic_week_int <- nw_acc_delay_week_raw %>% 
  left_join(unique(select(list_acc, NAME, ICAO_CODE)), by = c("UNIT_CODE" = "ICAO_CODE")) %>% 
  left_join(rel_ansp_acc, by = c("UNIT_CODE" = "ICAO_CODE")) %>% 
  mutate(
    WK_FLT_RANK = rank(desc(FLIGHT), ties.method = "max")
  ) %>% 
  arrange(ANSP_ID, NAME) %>% 
  group_by(ANSP_ID) %>% 
  mutate(
    SP_RANK = paste0(ANSP_CODE, row_number()),
    WK_FLT_DIF_PREV_WEEK_PERC = if_else(FLIGHT_7DAY == 0, NA, FLIGHT / FLIGHT_7DAY -1),
    WK_FLT_DIF_PREV_YEAR_PERC = if_else(FLIGHT_PREV_YEAR == 0, NA, FLIGHT / FLIGHT_PREV_YEAR -1)
  ) %>% 
  arrange(ANSP_ID, SP_RANK) %>% 
  ungroup() 

sp_acc_traffic_week <- sp_acc_traffic_week_int %>% 
  select(
    SP_RANK,
    WK_FLT_RANK,
    WK_FLT_ACC_NAME = NAME,
    WK_FROM_DATE = MIN_ENTRY_DATE,
    WK_TO_DATE = MAX_ENTRY_DATE,
    WK_FLT =  DAILY_FLIGHT,
    WK_FLT_DIF_PREV_WEEK_PERC,
    WK_FLT_DIF_PREV_YEAR_PERC
  ) %>% 
  arrange(WK_FLT_RANK)


#### y2d ----
if(!exists("nw_acc_delay_y2d_raw")) {
  nw_acc_delay_y2d_raw <- export_query(query_nw_acc_delay_y2d_raw(format(data_day_date, "%Y-%m%-%d"))) 
}

if (max(nw_acc_delay_y2d_raw$ENTRY_DATE) != data_day_date) {
  nw_acc_delay_y2d_raw <- export_query(query_nw_acc_delay_y2d_raw(format(data_day_date, "%Y-%m%-%d"))) 
}

# process data
sp_acc_traffic_y2d_int <- nw_acc_delay_y2d_raw %>% 
  left_join(unique(select(list_acc, NAME, ICAO_CODE)), by = c("UNIT_CODE" = "ICAO_CODE")) %>% 
  left_join(rel_ansp_acc, by = c("UNIT_CODE" = "ICAO_CODE")) %>% 
  mutate(
    Y2D_FLT_RANK = rank(desc(FLIGHT), ties.method = "max"),
  ) %>% 
  group_by(ANSP_ID) %>% 
  arrange(ANSP_ID, ANSP_NAME) %>% 
  mutate(
    SP_RANK = paste0(ANSP_CODE, row_number()),
    Y2D_FLT_DIF_PREV_YEAR_PERC = if_else(Y2D_AVG_FLIGHT_PY == 0, NA,  Y2D_AVG_FLIGHT /  Y2D_AVG_FLIGHT_PY-1),
    Y2D_FLT_DIF_2019_PERC = if_else(Y2D_AVG_FLIGHT_2019 == 0, NA, Y2D_AVG_FLIGHT / Y2D_AVG_FLIGHT_2019 -1)
  ) %>% 
  arrange(ANSP_ID, SP_RANK) %>% 
  ungroup()

sp_acc_traffic_y2d <- sp_acc_traffic_y2d_int %>% 
  select(
    SP_RANK,
    Y2D_FLT_RANK,
    Y2D_FLT_ACC_NAME = NAME,
    Y2D_FROM_DATE = MIN_DATE,
    Y2D_TO_DATE = ENTRY_DATE,
    Y2D_FLT =  Y2D_AVG_FLIGHT,
    Y2D_FLT_DIF_PREV_YEAR_PERC,
    Y2D_FLT_DIF_2019_PERC
  )%>% 
  arrange(Y2D_FLT_RANK)

#### s2d ----
if(!exists("nw_acc_delay_s2d_raw")) {
  nw_acc_delay_s2d_raw <- export_query(query_nw_acc_delay_y2d_raw(format(data_day_date, "%Y-%m%-%d"), initial_date = summer_start)) %>% 
    rename_with(~ gsub("Y2D", "S2D", .x), .cols = contains("Y2D"))
}

if (max(nw_acc_delay_s2d_raw$ENTRY_DATE) != data_day_date) {
  nw_acc_delay_s2d_raw <- export_query(query_nw_acc_delay_y2d_raw(format(data_day_date, "%Y-%m%-%d"), initial_date = summer_start)) %>% 
    rename_with(~ gsub("Y2D", "S2D", .x), .cols = contains("Y2D"))
}

# process data
sp_acc_traffic_s2d_int <- nw_acc_delay_s2d_raw %>% 
  left_join(unique(select(list_acc, NAME, ICAO_CODE)), by = c("UNIT_CODE" = "ICAO_CODE")) %>% 
  left_join(rel_ansp_acc, by = c("UNIT_CODE" = "ICAO_CODE")) %>% 
  mutate(
    S2D_FLT_RANK = rank(desc(FLIGHT), ties.method = "max"),
  ) %>% 
  group_by(ANSP_ID) %>% 
  arrange(ANSP_ID, ANSP_NAME) %>% 
  mutate(
    SP_RANK = paste0(ANSP_CODE, row_number()),
    S2D_FLT_DIF_PREV_YEAR_PERC = if_else(S2D_AVG_FLIGHT_PY == 0, NA,  S2D_AVG_FLIGHT /  S2D_AVG_FLIGHT_PY-1),
    S2D_FLT_DIF_2019_PERC = if_else(S2D_AVG_FLIGHT_2019 == 0, NA, S2D_AVG_FLIGHT / S2D_AVG_FLIGHT_2019 -1)
  ) %>% 
  arrange(ANSP_ID, SP_RANK) %>% 
  ungroup()

sp_acc_traffic_s2d <- sp_acc_traffic_s2d_int %>% 
  select(
    SP_RANK,
    S2D_FLT_RANK,
    S2D_FLT_ACC_NAME = NAME,
    S2D_FROM_DATE = MIN_DATE,
    S2D_TO_DATE = ENTRY_DATE,
    S2D_FLT =  S2D_AVG_FLIGHT,
    S2D_FLT_DIF_PREV_YEAR_PERC,
    S2D_FLT_DIF_2019_PERC
  )%>% 
  arrange(S2D_FLT_RANK)


#### main card ----
sp_acc_main_traffic <- sp_acc_traffic_day_int %>%
  select(
    SP_RANK,
    MAIN_TFC_ACC_RANK = DY_FLT_RANK,
    MAIN_TFC_ACC_NAME = NAME,
    MAIN_TFC_ACC_FLT = FLIGHT
    )

sp_acc_main_traffic_dif <- sp_acc_traffic_day_int %>%
  mutate(
    MAIN_TFC_DIF_ACC_RANK = rank(desc(abs(DY_FLT_DIF_PREV_WEEK)), ties.method = "max"),
  ) %>% 
  arrange(ANSP_CODE, desc(abs(DY_FLT_DIF_PREV_WEEK)), NAME) %>%
  group_by(ANSP_CODE) %>%
  mutate(
    SP_RANK = paste0(ANSP_CODE, row_number()),
    MAIN_TFC_DIF_ACC_RANK,
    MAIN_TFC_DIF_ACC_NAME = NAME,
    MAIN_TFC_DIF_ACC_FLT_DIF = DY_FLT_DIF_PREV_WEEK
  ) %>%
  ungroup() %>%
  select(SP_RANK,
         MAIN_TFC_DIF_ACC_RANK,
         MAIN_TFC_DIF_ACC_NAME, 
         MAIN_TFC_DIF_ACC_FLT_DIF)


#### join tables ----
# create list of state/rankings for left join
ansp_ranking <- list()
i = 0
for (i in 1:10) {
  i = i + 1
  ansp_ranking <- ansp_ranking %>%
    bind_rows(list_ansp, .)
}

ansp_ranking <- ansp_ranking %>%
  select(-ANSP_ID) %>% 
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
  left_join(sp_acc_traffic_s2d, by = "SP_RANK") %>%
  ungroup() %>%
  select(-SP_RANK) %>%
  arrange (ANSP_CODE, RANK) %>% 
  relocate(ANSP_CODE, .before = everything())

# covert to json and save in app data folder and archive
sp_acc_data_j <- sp_acc_data %>% toJSON(., pretty = TRUE)
save_json(sp_acc_data_j, "sp_acc_ranking_traffic")
print(paste(format(now(), "%H:%M:%S"), "sp_acc_ranking_traffic"))


## DELAY ----
### ACC ----
#### day ----
# process data
sp_acc_delay_day_int <- sp_acc_traffic_day_int %>% 
  mutate(
    DY_DLY_RANK = rank(desc(DLY), ties.method = "max")
  ) %>% 
  group_by(ANSP_ID) %>% 
  mutate(
    SP_RANK = paste0(ANSP_CODE, row_number()),
    DY_DLY_DIF_PREV_WEEK_PERC = if_else(DLY_7DAY == 0, NA, DLY / DLY_7DAY -1),
    DY_DLY_DIF_PREV_YEAR_PERC = if_else(DLY_PREV_YEAR == 0, NA, DLY / DLY_PREV_YEAR -1),
    
    DY_DLY_FLT = DLY / FLIGHT,
    DY_DLY_FLT_PREV_WEEK = DLY_7DAY / FLIGHT_7DAY,
    DY_DLY_FLT_PREV_YEAR = DLY_PREV_YEAR / FLIGHT_PREV_YEAR,
    DY_DLY_FLT_DIF_PREV_WEEK_PERC = if_else(DY_DLY_FLT_PREV_WEEK == 0, NA, DY_DLY_FLT / DY_DLY_FLT_PREV_WEEK -1),
    DY_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(DY_DLY_FLT_PREV_YEAR == 0, NA, DY_DLY_FLT / DY_DLY_FLT_PREV_YEAR -1),
    
    
  ) %>% 
  arrange(ANSP_ID, SP_RANK) %>% 
  ungroup()

sp_acc_delay_day <- sp_acc_delay_day_int %>% 
  select(
    SP_RANK,
    DY_DLY_RANK,
    DY_DLY_ACC_NAME = NAME,
    DY_TO_DATE = ENTRY_DATE,
    DY_DLY = DLY,
    DY_DLY_DIF_PREV_WEEK_PERC,
    DY_DLY_DIF_PREV_YEAR_PERC
  )

sp_acc_delay_flight_day <- sp_acc_delay_day_int %>% 
  mutate(
    DY_DLY_FLT_RANK = rank(desc(DY_DLY_FLT), ties.method = "max"),
  ) %>% 
  group_by(ANSP_ID) %>% 
  mutate(
    SP_RANK = paste0(ANSP_CODE, row_number()),
  ) %>% 
  arrange(ANSP_ID, SP_RANK) %>% 
  ungroup() %>% 
  select(
    SP_RANK,
    DY_DLY_FLT_RANK,
    DY_DLY_FLT_ACC_NAME = NAME,
    DY_DLY_FLT,
    DY_DLY_FLT_DIF_PREV_WEEK_PERC,
    DY_DLY_FLT_DIF_PREV_YEAR_PERC
  ) 


#### week ----
# process data
sp_acc_delay_week_int <- sp_acc_traffic_week_int %>% 
  mutate(
    WK_DLY_RANK = rank(desc(DAILY_DLY), ties.method = "max")
  ) %>%
  group_by(ANSP_ID) %>% 
  mutate(
    SP_RANK = paste0(ANSP_CODE, row_number()),
    WK_DLY_DIF_PREV_WEEK_PERC = if_else(DAILY_DLY_7DAY == 0, NA,  DAILY_DLY / DAILY_DLY_7DAY -1),
    WK_DLY_DIF_PREV_YEAR_PERC = if_else( DAILY_DLY_PREV_YEAR == 0, NA, DAILY_DLY /  DAILY_DLY_PREV_YEAR -1),
    
    WK_DLY_FLT = DAILY_DLY /  DAILY_FLIGHT,
    WK_DLY_FLT_PREV_WEEK = DAILY_DLY_7DAY / DAILY_FLIGHT_7DAY,
    WK_DLY_FLT_PREV_YEAR = DAILY_DLY_PREV_YEAR / DAILY_FLIGHT_PREV_YEAR,
    WK_DLY_FLT_DIF_PREV_WEEK_PERC = if_else(WK_DLY_FLT_PREV_WEEK == 0, NA, WK_DLY_FLT / WK_DLY_FLT_PREV_WEEK -1),
    WK_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(WK_DLY_FLT_PREV_YEAR == 0, NA, WK_DLY_FLT / WK_DLY_FLT_PREV_YEAR -1),
    
    
  ) %>% 
  arrange(ANSP_ID, SP_RANK) %>% 
  ungroup()

sp_acc_delay_week <- sp_acc_delay_week_int %>% 
  select(
    SP_RANK,
    WK_DLY_RANK,
    WK_DLY_ACC_NAME = NAME,
    WK_FROM_DATE = MIN_ENTRY_DATE,
    WK_TO_DATE = MAX_ENTRY_DATE,
    WK_DLY = DAILY_DLY,
    WK_DLY_DIF_PREV_WEEK_PERC,
    WK_DLY_DIF_PREV_YEAR_PERC
  )

sp_acc_delay_flight_week <- sp_acc_delay_week_int %>% 
  mutate(
    WK_DLY_FLT_RANK = rank(desc(WK_DLY_FLT), ties.method = "max"),
  ) %>% 
  group_by(ANSP_ID) %>% 
  mutate(
    SP_RANK = paste0(ANSP_CODE, row_number()),
  ) %>% 
  arrange(ANSP_ID, SP_RANK) %>% 
  ungroup() %>% 
  select(
    SP_RANK,
    WK_DLY_FLT_RANK,
    WK_DLY_FLT_ACC_NAME = NAME,
    WK_DLY_FLT,
    WK_DLY_FLT_DIF_PREV_WEEK_PERC,
    WK_DLY_FLT_DIF_PREV_YEAR_PERC
  )

#### y2d ----
# process data
sp_acc_delay_y2d_int <- sp_acc_traffic_y2d_int %>% 
  mutate(
    Y2D_DLY_RANK = rank(desc(Y2D_AVG_DLY), ties.method = "max"),
    ) %>% 
  group_by(ANSP_ID) %>% 
  mutate(
    SP_RANK = paste0(ANSP_CODE, row_number()),
    Y2D_DLY_DIF_PREV_YEAR_PERC = if_else(Y2D_AVG_DLY_PY == 0, NA,  Y2D_AVG_DLY /  Y2D_AVG_DLY_PY -1),
    Y2D_DLY_DIF_2019_PERC = if_else(Y2D_AVG_DLY_2019 == 0, NA,  Y2D_AVG_DLY /  Y2D_AVG_DLY_2019 -1),
    
    Y2D_DLY_FLT = Y2D_AVG_DLY / Y2D_AVG_FLIGHT,
    Y2D_DLY_FLT_PREV_YEAR = Y2D_AVG_DLY_PY / Y2D_AVG_FLIGHT_PY,
    Y2D_DLY_FLT_2019 = Y2D_AVG_DLY_2019 / Y2D_AVG_FLIGHT_2019,
    Y2D_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(Y2D_DLY_FLT_PREV_YEAR == 0, NA, Y2D_DLY_FLT / Y2D_DLY_FLT_PREV_YEAR -1),
    Y2D_DLY_FLT_DIF_2019_PERC = if_else(Y2D_DLY_FLT_2019 == 0, NA, Y2D_DLY_FLT / Y2D_DLY_FLT_2019 -1),
    
  ) %>% 
  arrange(ANSP_ID, SP_RANK) %>% 
  ungroup()

sp_acc_delay_y2d <- sp_acc_delay_y2d_int %>% 
  select(
    SP_RANK,
    Y2D_DLY_RANK,
    Y2D_DLY_ACC_NAME = NAME,
    Y2D_TO_DATE = ENTRY_DATE,
    Y2D_DLY = Y2D_AVG_DLY,
    Y2D_DLY_DIF_PREV_YEAR_PERC,
    Y2D_DLY_DIF_2019_PERC
  )

sp_acc_delay_flight_y2d <- sp_acc_delay_y2d_int %>% 
  mutate(
    Y2D_DLY_FLT_RANK = rank(desc(Y2D_DLY_FLT), ties.method = "max"),
  ) %>% 
  group_by(ANSP_ID) %>% 
  mutate(
    SP_RANK = paste0(ANSP_CODE, row_number()),
  ) %>% 
  arrange(ANSP_ID, SP_RANK) %>% 
  ungroup() %>% 
  select(
    SP_RANK,
    Y2D_DLY_FLT_RANK,
    Y2D_DLY_FLT_ACC_NAME = NAME,
    Y2D_DLY_FLT,
    Y2D_DLY_FLT_DIF_PREV_YEAR_PERC,
    Y2D_DLY_FLT_DIF_2019_PERC
  )

#### s2d ----
# process data
sp_acc_delay_s2d_int <- sp_acc_traffic_s2d_int %>% 
  mutate(
    S2D_DLY_RANK = rank(desc(S2D_AVG_DLY), ties.method = "max"),
  ) %>% 
  group_by(ANSP_ID) %>% 
  mutate(
    SP_RANK = paste0(ANSP_CODE, row_number()),
    S2D_DLY_DIF_PREV_YEAR_PERC = if_else(S2D_AVG_DLY_PY == 0, NA,  S2D_AVG_DLY /  S2D_AVG_DLY_PY -1),
    S2D_DLY_DIF_2019_PERC = if_else(S2D_AVG_DLY_2019 == 0, NA,  S2D_AVG_DLY /  S2D_AVG_DLY_2019 -1),
    
    S2D_DLY_FLT = S2D_AVG_DLY / S2D_AVG_FLIGHT,
    S2D_DLY_FLT_PREV_YEAR = S2D_AVG_DLY_PY / S2D_AVG_FLIGHT_PY,
    S2D_DLY_FLT_2019 = S2D_AVG_DLY_2019 / S2D_AVG_FLIGHT_2019,
    S2D_DLY_FLT_DIF_PREV_YEAR_PERC = if_else(S2D_DLY_FLT_PREV_YEAR == 0, NA, S2D_DLY_FLT / S2D_DLY_FLT_PREV_YEAR -1),
    S2D_DLY_FLT_DIF_2019_PERC = if_else(S2D_DLY_FLT_2019 == 0, NA, S2D_DLY_FLT / S2D_DLY_FLT_2019 -1),
    
  ) %>% 
  arrange(ANSP_ID, SP_RANK) %>% 
  ungroup()

sp_acc_delay_s2d <- sp_acc_delay_s2d_int %>% 
  select(
    SP_RANK,
    S2D_DLY_RANK,
    S2D_DLY_ACC_NAME = NAME,
    S2D_TO_DATE = ENTRY_DATE,
    S2D_DLY = S2D_AVG_DLY,
    S2D_DLY_DIF_PREV_YEAR_PERC,
    S2D_DLY_DIF_2019_PERC
  )

sp_acc_delay_flight_s2d <- sp_acc_delay_s2d_int %>% 
  mutate(
    S2D_DLY_FLT_RANK = rank(desc(S2D_DLY_FLT), ties.method = "max"),
  ) %>% 
  group_by(ANSP_ID) %>% 
  mutate(
    SP_RANK = paste0(ANSP_CODE, row_number()),
  ) %>% 
  arrange(ANSP_ID, SP_RANK) %>% 
  ungroup() %>% 
  select(
    SP_RANK,
    S2D_DLY_FLT_RANK,
    S2D_DLY_FLT_ACC_NAME = NAME,
    S2D_DLY_FLT,
    S2D_DLY_FLT_DIF_PREV_YEAR_PERC,
    S2D_DLY_FLT_DIF_2019_PERC
  )



#### main card ----
sp_acc_main_delay <- sp_acc_delay_day_int %>%
  select(
    SP_RANK,
    MAIN_DLY_ACC_RANK = DY_DLY_RANK,
    MAIN_DLY_ACC_NAME = NAME,
    MAIN_DLY_ACC_DLY = DLY
  )

sp_acc_main_delay_flight <- sp_acc_delay_day_int %>% 
  mutate(
    MAIN_DLY_FLT_ACC_RANK = rank(desc(DY_DLY_FLT), ties.method = "max"),
  ) %>% 
  arrange(ANSP_CODE, desc(DY_DLY_FLT), MAIN_DLY_FLT_ACC_RANK) %>%
  group_by(ANSP_CODE) %>%
  mutate(
    SP_RANK = paste0(ANSP_CODE, row_number()),
    MAIN_DLY_FLT_ACC_NAME = NAME,
    MAIN_DLY_FLT_ACC_DLY_FLT = DY_DLY_FLT
  ) %>%
  ungroup() %>%
  select(SP_RANK, 
         MAIN_DLY_FLT_ACC_RANK,
         MAIN_DLY_FLT_ACC_NAME,
         MAIN_DLY_FLT_ACC_DLY_FLT)

#### join tables ----
# join and reorder tables
sp_acc_delay_data <- ansp_ranking %>%
  left_join(sp_acc_main_delay, by = "SP_RANK") %>%
  left_join(sp_acc_main_delay_flight, by = "SP_RANK") %>%
  left_join(sp_acc_delay_day, by = "SP_RANK") %>%
  left_join(sp_acc_delay_week, by = "SP_RANK") %>%
  left_join(sp_acc_delay_y2d, by = "SP_RANK") %>%
  left_join(sp_acc_delay_flight_day, by = "SP_RANK") %>%
  left_join(sp_acc_delay_flight_week, by = "SP_RANK") %>%
  left_join(sp_acc_delay_flight_y2d, by = "SP_RANK") %>%
  ungroup() %>%
  select(-SP_RANK) %>%
  arrange (ANSP_CODE, RANK)%>% 
  relocate(ANSP_CODE, .before = everything())

# covert to json and save in app data folder and archive
sp_acc_delay_data_j <- sp_acc_delay_data %>% toJSON(., pretty = TRUE)

save_json(sp_acc_delay_data_j, "sp_acc_ranking_delay")
print(paste(format(now(), "%H:%M:%S"), "sp_acc_ranking_delay"))

print(" ")
