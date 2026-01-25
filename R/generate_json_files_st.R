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
source(here("..", "mobile-app", "R", "helpers.R")) 

# Dimensions ----
if (!exists("dim_iso_country")) {
  source(here("..", "mobile-app", "R", "dimensions.R")) 
  # so I don't have to redo all the joins
}

state_iso <- list_iso_country %>% 
  select( iso_2letter = ISO_COUNTRY_CODE,
          state = COUNTRY_NAME)

# queries ----
source(here("..", "mobile-app", "R", "data_queries.R")) 

# parameters ----
# archive mode for past dates
# run this before the params script to set up the forecast params
if (!exists("archive_mode")) {archive_mode <- FALSE}
if (!exists("data_day_date")) {
  data_day_date <- lubridate::today(tzone = "") +  days(-1)
}

data_day_text <- data_day_date %>% format("%Y%m%d")
data_day_year <- as.numeric(format(data_day_date,'%Y'))

source(here("..", "mobile-app", "R", "params.R")) 

print(paste("Generating st json files", format(data_day_date, "%Y-%m-%d"), "..."))

# common data ----
if (exists("co2_data_raw") == FALSE) {
  source(here("..", "mobile-app", "R", "get_common_data.R")) 
}


# ____________________________________________________________________________________________
#
#    State landing page -----
#
# ____________________________________________________________________________________________

st_json_app <-""

#### Billing data ----
## we do this first to avoid 'R fatal error'

# so this script is stand alone
if (exists("billed_raw") == FALSE) {billed_raw <- get_billing_data()}

## process billing data
st_billed_clean <- billed_raw %>%
  janitor::clean_names() %>%
  mutate(billing_period_start_date = as.Date(billing_period_start_date, format = "%d-%m-%Y"))

last_billing_date <- min(max(st_billed_clean$billing_period_start_date + days(1)),
                         floor_date(data_day_date, 'month)')+ months(-1))
last_billing_year <- year(last_billing_date)
last_billing_month <- month(last_billing_date)

st_billing <- st_billed_clean %>%
  group_by(corrected_cz, year, month, billing_period_start_date) %>%
  summarise(total_billing = sum(route_charges), .groups = "drop")

st_billing <- list_state_crco %>%
  left_join(st_billing, by = "corrected_cz", relationship = "many-to-many")

st_billed_for_json <- st_billing %>%
  arrange(iso_2letter, year, billing_period_start_date) %>%
  mutate(
         BILLING_DATE = (billing_period_start_date + days(1) + months(1)) + days(-1),
         Year = year,
         MONTH_TEXT = format(billing_period_start_date + days(1),'%B'),
         MM_BILLED_PY = lag(total_billing, 12),
         MM_BILLED_2019 = lag(total_billing, (last_billing_year - 2019) * 12),
         MM_BILLED_DIF_PREV_YEAR = total_billing / MM_BILLED_PY - 1,
         MM_BILLED_DIF_2019 = total_billing / MM_BILLED_2019 - 1,
         MM_BILLED = round(total_billing / 1000000, 1)
  ) %>%
  group_by(iso_2letter, Year) %>%
  mutate(
    total_billing_y2d = cumsum(total_billing)
  ) %>%
  ungroup() %>%
  mutate(
    Y2D_BILLED_PY = lag(total_billing_y2d, 12),
    Y2D_BILLED_2019 = lag(total_billing_y2d, (last_billing_year - 2019) * 12),
    Y2D_BILLED_DIF_PREV_YEAR = total_billing_y2d / Y2D_BILLED_PY -1,
    Y2D_BILLED_DIF_2019 = total_billing_y2d / Y2D_BILLED_2019 -1,
    Y2D_BILLED = round(total_billing_y2d / 1000000, 1)
  ) %>%
  filter(Year == last_billing_year,
         month == last_billing_month) %>%
  ### rank calculation
  mutate(
    MM_BILLED_RANK = min_rank(desc(MM_BILLED)),
    Y2D_BILLED_RANK = min_rank(desc(Y2D_BILLED)),
    BILLED_RANK_TEXT = "*Top rank for highest.",
  ) %>%
  select(iso_2letter,
         BILLING_DATE,
         MONTH_TEXT,
         MM_BILLED_RANK,
         MM_BILLED,
         MM_BILLED_DIF_PREV_YEAR,
         MM_BILLED_DIF_2019,
         Y2D_BILLED_RANK,
         Y2D_BILLED,
         Y2D_BILLED_DIF_PREV_YEAR,
         Y2D_BILLED_DIF_2019,
         BILLED_RANK_TEXT
  ) %>%
  right_join(state_iso, by ="iso_2letter") %>%
  select(-state) %>%
  arrange(iso_2letter)

#### Import traffic/delay data ----
mydatafile <- paste0("st_daio_delay_day.parquet")
stakeholder <- substr(mydatafile, 1,2)

st_daio_delay_data <- read_parquet(here(archive_dir_raw, stakeholder, mydatafile)) %>% 
  filter(YEAR == data_day_year) %>% 
  rename_with(~ sub("DAY_", "DY_", .x, fixed = TRUE), contains("DAY_")) %>% 
  rename_with(~ sub("RWK_", "WK_", .x, fixed = TRUE), contains("RWK_")) %>% 
  rename(COUNTRY_CODE = STK_CODE, COUNTRY_NAME = STK_NAME)%>%
  arrange(COUNTRY_NAME, FLIGHT_DATE) %>% 
  mutate(daio_zone_lc = tolower(COUNTRY_NAME))

#getting the latest date's traffic data
st_daio_delay_data_last_day <- st_daio_delay_data %>%
  filter(FLIGHT_DATE == min(data_day_date,
                            max(DATA_DAY, na.rm = TRUE),
                            na.rm = TRUE)
  ) 

#### Traffic DAIO  ----
#selecting columns and renaming
st_daio_for_json <- st_daio_delay_data_last_day %>%
  mutate(
    DY_DAIO_RANK = min_rank(desc(DY_TFC)),
    WK_DAIO_RANK = min_rank(desc(WK_AVG_TFC)),
    Y2D_DAIO_RANK = min_rank(desc(Y2D_AVG_TFC))
  ) %>%
  right_join(rel_iso_country_daio_zone, by = "daio_zone_lc", relationship = "many-to-many") %>%
  arrange(iso_2letter, daio_zone_lc, FLIGHT_DATE) %>% 
  select(
    iso_2letter,
    FLIGHT_DATE,
    
    DY_DAIO_RANK,
    DY_DAIO = DY_TFC,
    DY_DAIO_DIF_PREV_YEAR_PERC = DY_TFC_DIF_PREV_YEAR_PERC,
    DY_DAIO_DIF_2019_PERC = DY_TFC_DIF_2019_PERC,
    
    WK_DAIO_RANK,
    WK_DAIO_AVG_ROLLING = WK_AVG_TFC,
    WK_DAIO_DIF_PREV_YEAR_PERC = WK_TFC_DIF_PREV_YEAR_PERC,
    WK_DAIO_DIF_2019_PERC = WK_TFC_DIF_2019_PERC,
    
    Y2D_DAIO_RANK,
    Y2D_DAIO = Y2D_TFC,
    Y2D_DAIO_AVG = Y2D_AVG_TFC,
    Y2D_DAIO_DIF_PREV_YEAR_PERC = Y2D_TFC_DIF_PREV_YEAR_PERC,
    Y2D_DAIO_DIF_2019_PERC = Y2D_TFC_DIF_2019_PERC,
    # DAIO_RANK_TEXT
  ) %>%
    right_join(state_iso, by ="iso_2letter") %>%
    select(-state) %>%
    arrange(iso_2letter)

#### Traffic DAI  ----
mydatafile <- paste0("st_dai_day.parquet")
stakeholder <- substr(mydatafile, 1,2)

st_dai_data <- read_parquet(here(archive_dir_raw, stakeholder, mydatafile)) %>% 
  filter(YEAR == data_day_year) %>% 
  rename_with(~ sub("DAY_", "DY_", .x, fixed = TRUE), contains("DAY_")) %>% 
  rename_with(~ sub("RWK_", "WK_", .x, fixed = TRUE), contains("RWK_")) %>% 
  rename(COUNTRY_ICAO_CODE = STK_CODE, COUNTRY_NAME = STK_NAME)%>%
  arrange(COUNTRY_NAME, FLIGHT_DATE) %>% 
  mutate(daio_zone_lc = tolower(COUNTRY_NAME))

st_dai_last_day <- st_dai_data %>%
  filter(FLIGHT_DATE == data_day_date)

st_dai_for_json <- st_dai_last_day %>%
  ### rank calculation
  mutate(
    DY_DAI_RANK = min_rank(desc(DY_TFC)),
    WK_DAI_RANK = min_rank(desc(WK_AVG_TFC)),
    Y2D_DAI_RANK = min_rank(desc(Y2D_AVG_TFC))
  ) %>%
  right_join(rel_iso_country_daio_zone, by = "daio_zone_lc", relationship = "many-to-many") %>%
  select(
    iso_2letter,
    FLIGHT_DATE,
    
    DY_DAI_RANK,
    DY_DAI = DY_TFC,
    DY_DAI_DIF_PREV_YEAR_PERC = DY_TFC_DIF_PREV_YEAR_PERC,
    DY_DAI_DIF_2019_PERC = DY_TFC_DIF_2019_PERC,
    
    WK_DAI_RANK,
    WK_DAI_AVG_ROLLING = WK_AVG_TFC,
    WK_DAI_DIF_PREV_YEAR_PERC = WK_TFC_DIF_PREV_YEAR_PERC,
    WK_DAI_DIF_2019_PERC = WK_TFC_DIF_2019_PERC,
    
    Y2D_DAI_RANK,
    Y2D_DAI = Y2D_TFC,
    Y2D_DAI_AVG = Y2D_AVG_TFC,
    Y2D_DAI_DIF_PREV_YEAR_PERC = Y2D_TFC_DIF_PREV_YEAR_PERC,
    Y2D_DAI_DIF_2019_PERC = Y2D_TFC_DIF_2019_PERC,
    # DAI_RANK_TEXT
  ) %>%
  right_join(state_iso, by ="iso_2letter") %>%
  select(-state) %>%
  arrange(iso_2letter)

### in case there are more DAI than DAIO
temp_check <- st_daio_for_json %>%
  left_join(st_dai_for_json, by ="iso_2letter") %>%
  mutate (MODIFIED_DAIO = if_else(DY_DAI>DY_DAIO, DY_DAI, DY_DAIO)) %>%
  select(iso_2letter, MODIFIED_DAIO)

st_daio_for_json <- st_daio_for_json %>%
  left_join(temp_check, by ="iso_2letter") %>%
  mutate(DY_DAIO = MODIFIED_DAIO) %>%
  select(-MODIFIED_DAIO)

#### Traffic overflight  ----
st_dai_data_p <- st_dai_data %>%
  mutate(flight_type = 'dai') %>%
  # mutate_all(~replace(., is.na(.), 0)) %>%
  select(FLIGHT_DATE,
         # iso_2letter,
         daio_zone_lc,
         flight_type,
         DY_TFC,
         DY_TFC_PREV_WEEK,
         DY_TFC_PREV_YEAR,
         DY_TFC_2019,
         
         WK_AVG_TFC,
         WK_AVG_TFC_PREV_WEEK,
         WK_AVG_TFC_PREV_YEAR,
         WK_AVG_TFC_2020,
         WK_AVG_TFC_2019,
         
         Y2D_TFC,
         Y2D_TFC_PREV_YEAR,
         Y2D_TFC_2019,
         Y2D_AVG_TFC,
         Y2D_AVG_TFC_PREV_YEAR,
         Y2D_AVG_TFC_2019,
         DATA_DAY
  ) %>%
  mutate(across(-c(FLIGHT_DATE, flight_type, daio_zone_lc, DATA_DAY), ~ .* -1 ))

st_daio_data_p <- st_daio_delay_data %>%
  mutate(flight_type = 'daio') %>%
  # mutate_all(~replace(., is.na(.), 0)) %>%
  select(FLIGHT_DATE,
         # iso_2letter,
         daio_zone_lc,
         flight_type,
         DY_TFC,
         DY_TFC_PREV_WEEK,
         DY_TFC_PREV_YEAR,
         DY_TFC_2019,
         
         WK_AVG_TFC,
         WK_AVG_TFC_PREV_WEEK,
         WK_AVG_TFC_PREV_YEAR,
         WK_AVG_TFC_2020,
         WK_AVG_TFC_2019,
         
         Y2D_TFC,
         Y2D_TFC_PREV_YEAR,
         Y2D_TFC_2019,
         Y2D_AVG_TFC,
         Y2D_AVG_TFC_PREV_YEAR,
         Y2D_AVG_TFC_2019,
         DATA_DAY
  ) 

st_overflight_data <- rbind(st_daio_data_p, st_dai_data_p) %>%
  group_by(daio_zone_lc, FLIGHT_DATE) %>%
  summarise(DY_OVF = sum(DY_TFC, na.rm = TRUE),
            DY_OVF_PREV_WEEK = sum(DY_TFC_PREV_WEEK, na.rm = TRUE),
            DY_OVF_PREV_YEAR = sum(DY_TFC_PREV_YEAR, na.rm = TRUE),
            DY_OVF_2019 = sum(DY_TFC_2019, na.rm = TRUE),
            
            WK_AVG_OVF = sum(WK_AVG_TFC, na.rm = TRUE),
            WK_AVG_OVF_PREV_WEEK = sum(WK_AVG_TFC_PREV_WEEK, na.rm = TRUE),
            WK_AVG_OVF_PREV_YEAR = sum(WK_AVG_TFC_PREV_YEAR, na.rm = TRUE),
            WK_AVG_OVF_2020 = sum(WK_AVG_TFC_2020, na.rm = TRUE),
            WK_AVG_OVF_2019 = sum(WK_AVG_TFC_2019, na.rm = TRUE),
            
            Y2D_OVF = sum(Y2D_TFC, na.rm = TRUE),
            Y2D_OVF_PREV_YEAR = sum(Y2D_TFC_PREV_YEAR, na.rm = TRUE),
            Y2D_OVF_2019 = sum(Y2D_TFC_2019, na.rm = TRUE),
            Y2D_AVG_OVF = sum(Y2D_AVG_TFC, na.rm = TRUE),
            Y2D_AVG_OVF_PREV_YEAR = sum(Y2D_AVG_TFC_PREV_YEAR, na.rm = TRUE),
            Y2D_AVG_OVF_2019 = sum(Y2D_AVG_TFC_2019, na.rm = TRUE),
            
            LAST_DATA_DAY = max(DATA_DAY, na.rm = TRUE),
            .groups = "drop"
  ) %>%
  mutate(
    DY_OVF_DIF_PREV_WEEK = coalesce(DY_OVF,0) - coalesce(DY_OVF_PREV_WEEK,0),
    DY_OVF_DIF_PREV_YEAR = coalesce(DY_OVF,0) - coalesce(DY_OVF_PREV_YEAR, 0),
    DY_OVF_DIF_2019 = coalesce(DY_OVF,0) - coalesce(DY_OVF_2019,  0),
    DY_OVF_PREV_WEEK_PERC = if_else(DY_OVF_PREV_WEEK != 0, coalesce(DY_OVF,0)/DY_OVF_PREV_WEEK - 1, NA),
    DY_OVF_DIF_PREV_YEAR_PERC	= if_else(DY_OVF_PREV_YEAR != 0, coalesce(DY_OVF,0)/DY_OVF_PREV_YEAR - 1, NA),
    DY_OVF_DIF_2019_PERC = if_else(DY_OVF_2019 != 0, coalesce(DY_OVF,0)/DY_OVF_2019 - 1, NA),
    
    WK_OVF_DIF_PREV_YEAR_PERC = if_else(WK_AVG_OVF_PREV_YEAR != 0, coalesce(WK_AVG_OVF, 0)/WK_AVG_OVF_PREV_YEAR - 1, NA),
    WK_OVF_DIF_2019_PERC = if_else(WK_AVG_OVF_2019 != 0, coalesce(WK_AVG_OVF, 0)/WK_AVG_OVF_2019 - 1, NA),
    
    Y2D_OVF_DIF_PREV_YEAR_PERC	= if_else(Y2D_AVG_OVF_PREV_YEAR != 0, coalesce(Y2D_AVG_OVF, 0)/Y2D_AVG_OVF_PREV_YEAR - 1, NA),
    Y2D_OVF_DIF_2019_PERC = if_else(Y2D_AVG_OVF_2019 != 0, coalesce(Y2D_AVG_OVF, 0)/Y2D_AVG_OVF_2019 - 1, NA)
  ) %>% 
  # Iceland exception
  mutate(
    WK_AVG_OVF_2019 =  if_else(daio_zone_lc == "iceland", NA, WK_AVG_OVF_2019),
    WK_AVG_OVF_2020 =  if_else(daio_zone_lc == "iceland", NA, WK_AVG_OVF_2020),
    
    DY_OVF_DIF_PREV_YEAR_PERC =  if_else(daio_zone_lc == "iceland" & year(FLIGHT_DATE) < 2025, NA, DY_OVF_DIF_PREV_YEAR_PERC),
    WK_OVF_DIF_PREV_YEAR_PERC =  if_else(daio_zone_lc == "iceland" & year(FLIGHT_DATE) < 2025, NA, WK_OVF_DIF_PREV_YEAR_PERC),
    Y2D_OVF_DIF_PREV_YEAR_PERC = if_else(daio_zone_lc == "iceland" & year(FLIGHT_DATE) < 2025, NA, Y2D_OVF_DIF_PREV_YEAR_PERC),
    
    DY_OVF_DIF_2019_PERC =  if_else(daio_zone_lc == "iceland", NA, DY_OVF_DIF_2019_PERC),
    WK_OVF_DIF_2019_PERC =  if_else(daio_zone_lc == "iceland", NA, WK_OVF_DIF_2019_PERC),
    Y2D_OVF_DIF_2019_PERC = if_else(daio_zone_lc == "iceland", NA, Y2D_OVF_DIF_2019_PERC)
  )
  

st_overflight_last_day <- st_overflight_data %>%
  filter(FLIGHT_DATE == data_day_date)

st_overflight_for_json <- st_overflight_last_day %>%
  mutate(DY_OVF =  if_else(DY_OVF<0, 0, DY_OVF)) %>%    
  ### rank calculation
  mutate(
    DY_OVF_RANK = min_rank(desc(DY_OVF)),
    WK_OVF_RANK = min_rank(desc(WK_AVG_OVF)),
    Y2D_OVF_RANK = min_rank(desc(Y2D_AVG_OVF))
  ) %>%
  right_join(rel_iso_country_daio_zone, by = "daio_zone_lc", relationship = "many-to-many") %>% 
  select(
    iso_2letter,
    FLIGHT_DATE,
    
    DY_OVF_RANK,
    DY_OVF,
    DY_OVF_DIF_PREV_YEAR_PERC,
    DY_OVF_DIF_2019_PERC,
    
    WK_OVF_RANK,
    WK_OVF_AVG_ROLLING = WK_AVG_OVF,
    WK_OVF_DIF_PREV_YEAR_PERC,
    WK_OVF_DIF_2019_PERC,
    
    Y2D_OVF_RANK,
    Y2D_OVF,
    Y2D_OVF_AVG = Y2D_AVG_OVF,
    Y2D_OVF_DIF_PREV_YEAR_PERC,
    Y2D_OVF_DIF_2019_PERC
  ) %>%
  right_join(state_iso, by ="iso_2letter") %>%
  select(-state) %>%
  arrange(iso_2letter)


#### Delay  ----
st_delay_for_json <- st_daio_delay_data_last_day %>% 
  mutate(
    DY_DLY_ERT_SHARE = if_else(DY_DLY == 0 , 0 , DY_DLY_ERT / DY_DLY),
    DY_DLY_ARP_SHARE = if_else(DY_DLY == 0 , 0 , DY_DLY_ARP / DY_DLY),
    
    WK_DLY_ERT_SHARE = if_else(WK_AVG_DLY == 0 , 0 , WK_AVG_DLY_ERT / WK_AVG_DLY),
    WK_DLY_ARP_SHARE = if_else(WK_AVG_DLY == 0 , 0 , WK_AVG_DLY_ARP / WK_AVG_DLY),
    
    Y2D_DLY_ERT_SHARE = if_else(Y2D_DLY == 0 , 0 , Y2D_DLY_ERT / Y2D_DLY),
    Y2D_DLY_ARP_SHARE = if_else(Y2D_DLY == 0 , 0 , Y2D_DLY_ARP / Y2D_DLY)
    
  ) %>% 
  rename_with(~ sub("ARP", "APT", .x, fixed = TRUE), contains("ARP")) %>% 
  select(
    daio_zone_lc,
    FLIGHT_DATE,
    DY_DLY,
    DY_DLY_DIF_PREV_YEAR_PERC,
    DY_DLY_DIF_2019_PERC,
    DY_DLY_FLT,
    DY_DLY_FLT_DIF_PREV_YEAR_PERC,
    DY_DLY_FLT_DIF_2019_PERC,
    
    WK_DLY_AVG_ROLLING = WK_AVG_DLY,
    WK_DLY_DIF_PREV_YEAR_PERC,
    WK_DLY_DIF_2019_PERC,
    WK_DLY_FLT,
    WK_DLY_FLT_DIF_PREV_YEAR_PERC,
    WK_DLY_FLT_DIF_2019_PERC,
    
    Y2D_DLY_AVG = Y2D_AVG_DLY,
    Y2D_DLY_DIF_PREV_YEAR_PERC,
    Y2D_DLY_DIF_2019_PERC,
    Y2D_DLY_FLT,
    Y2D_DLY_FLT_DIF_PREV_YEAR_PERC,
    Y2D_DLY_FLT_DIF_2019_PERC,
    
    #En-route delay
    DY_DLY_ERT_SHARE,
    DY_DLY_ERT,
    DY_DLY_ERT_DIF_PREV_YEAR_PERC,
    DY_DLY_ERT_DIF_2019_PERC,
    DY_DLY_ERT_FLT,
    DY_DLY_ERT_FLT_DIF_PREV_YEAR_PERC,
    DY_DLY_ERT_FLT_DIF_2019_PERC,
    
    WK_DLY_ERT_SHARE,
    WK_DLY_ERT_AVG_ROLLING = WK_AVG_DLY_ERT,
    WK_DLY_ERT_DIF_PREV_YEAR_PERC,
    WK_DLY_ERT_DIF_2019_PERC,
    WK_DLY_ERT_FLT,
    WK_DLY_ERT_FLT_DIF_PREV_YEAR_PERC,
    WK_DLY_ERT_FLT_DIF_2019_PERC,
    
    Y2D_DLY_ERT_SHARE,
    Y2D_DLY_ERT_AVG = Y2D_AVG_DLY_ERT,
    Y2D_DLY_ERT_DIF_PREV_YEAR_PERC,
    Y2D_DLY_ERT_DIF_2019_PERC,
    Y2D_DLY_ERT_FLT,
    Y2D_DLY_ERT_FLT_DIF_PREV_YEAR_PERC,
    Y2D_DLY_ERT_FLT_DIF_2019_PERC,
    
    
    #Airport delay
    DY_DLY_APT_SHARE,
    DY_DLY_APT,
    DY_DLY_APT_DIF_PREV_YEAR_PERC,
    DY_DLY_APT_DIF_2019_PERC,
    DY_DLY_APT_FLT,
    DY_DLY_APT_FLT_DIF_PREV_YEAR_PERC,
    DY_DLY_APT_FLT_DIF_2019_PERC,
    
    WK_DLY_APT_SHARE,
    WK_DLY_APT_AVG_ROLLING = WK_AVG_DLY_APT,
    WK_DLY_APT_DIF_PREV_YEAR_PERC,
    WK_DLY_APT_DIF_2019_PERC,
    WK_DLY_APT_FLT,
    WK_DLY_APT_FLT_DIF_PREV_YEAR_PERC,
    WK_DLY_APT_FLT_DIF_2019_PERC,
    
    Y2D_DLY_APT_SHARE,
    Y2D_DLY_APT_AVG = Y2D_AVG_DLY_APT,
    Y2D_DLY_APT_DIF_PREV_YEAR_PERC,
    Y2D_DLY_APT_DIF_2019_PERC,
    Y2D_DLY_APT_FLT,
    Y2D_DLY_APT_FLT_DIF_PREV_YEAR_PERC,
    Y2D_DLY_APT_FLT_DIF_2019_PERC
    
  ) %>% 
  ### rank calculation
  mutate(
    ## delay
    DY_DLY_RANK = rank(desc(DY_DLY), ties.method = "max"),
    WK_DLY_RANK = rank(desc(WK_DLY_AVG_ROLLING), ties.method = "max"),
    Y2D_DLY_RANK = rank(desc(Y2D_DLY_AVG), ties.method = "max"),
    
    DY_DLY_ERT_RANK = rank(desc(DY_DLY_ERT), ties.method = "max"),
    WK_DLY_ERT_RANK = rank(desc(WK_DLY_ERT_AVG_ROLLING), ties.method = "max"),
    Y2D_DLY_ERT_RANK = rank(desc(Y2D_DLY_ERT_AVG), ties.method = "max"),
    
    DY_DLY_APT_RANK = rank(desc(DY_DLY_APT), ties.method = "max"),
    WK_DLY_APT_RANK = rank(desc(WK_DLY_APT_AVG_ROLLING), ties.method = "max"),
    Y2D_DLY_APT_RANK = rank(desc(Y2D_DLY_APT_AVG), ties.method = "max"),
    
    ## delay per flight
    DY_DLY_FLT_RANK = rank(desc(DY_DLY_FLT), ties.method = "max"),
    WK_DLY_FLT_RANK = rank(desc(WK_DLY_FLT), ties.method = "max"),
    Y2D_DLY_FLT_RANK = rank(desc(Y2D_DLY_FLT), ties.method = "max"),
    
    DY_DLY_ERT_FLT_RANK = rank(desc(DY_DLY_ERT_FLT), ties.method = "max"),
    WK_DLY_ERT_FLT_RANK = rank(desc(WK_DLY_ERT_FLT), ties.method = "max"),
    Y2D_DLY_ERT_FLT_RANK = rank(desc(Y2D_DLY_ERT_FLT), ties.method = "max"),
    
    DY_DLY_APT_FLT_RANK = rank(desc(DY_DLY_APT_FLT), ties.method = "max"),
    WK_DLY_APT_FLT_RANK = rank(desc(WK_DLY_APT_FLT), ties.method = "max"),
    Y2D_DLY_APT_FLT_RANK = rank(desc(Y2D_DLY_APT_FLT), ties.method = "max"),
    
    DLY_RANK_TEXT = "*Top rank for highest."
  )  %>%
  right_join(rel_iso_country_daio_zone, by = "daio_zone_lc", relationship = "many-to-many") %>% 
  right_join(state_iso, by ="iso_2letter") %>%
  select(-state, -daio_zone_lc, -daio_zone) %>%
  relocate(iso_2letter, .before = everything()) %>% 
  arrange(iso_2letter)


#### Punctuality data ----
if(exists("st_punct_raw") == FALSE) {
  st_punct_raw <- get_punct_data_state()
}

last_day_punct <-  min(max(st_punct_raw$DAY_DATE),
                       data_day_date, na.rm = TRUE)
last_year_punct <- as.numeric(format(last_day_punct,'%Y'))

# we separate continental and canarias until the flight table is created
if (exists("punct_data_spain_raw") == FALSE) {
  punct_data_spain_raw <- get_punct_data_spain()
}

# a bit of cheating until we fix the overall table
st_punct_miss_sched_spain <- st_punct_raw %>%
  filter(ISO_2LETTER == 'ES') %>%
  select(DAY_DATE,
         MISSING_SCHED_FLIGHTS,
         DEP_FLIGHTS_NO_OVERFLIGHTS)

punct_data_spain <- punct_data_spain_raw %>%
  left_join(st_punct_miss_sched_spain, by = "DAY_DATE") %>%
  mutate(YEAR = as.numeric(format(DAY_DATE, "%Y"))) %>%
  select(ISO_2LETTER,
         YEAR,
         DAY_DATE,
         ARR_PUNCTUAL_FLIGHTS,
         ARR_SCHEDULE_FLIGHT,
         DEP_PUNCTUAL_FLIGHTS,
         DEP_SCHEDULE_FLIGHT,
         MISSING_SCHED_FLIGHTS,
         DEP_FLIGHTS_NO_OVERFLIGHTS)

# end of cheating

st_punct_data_joined <- st_punct_raw %>%
  filter(ISO_2LETTER != 'ES') %>%
  select(ISO_2LETTER,
         YEAR,
         DAY_DATE,
         ARR_PUNCTUAL_FLIGHTS,
         ARR_SCHEDULE_FLIGHT,
         DEP_PUNCTUAL_FLIGHTS,
         DEP_SCHEDULE_FLIGHT,
         MISSING_SCHED_FLIGHTS,
         DEP_FLIGHTS_NO_OVERFLIGHTS) %>%
  rbind(punct_data_spain)

st_punct_data <- st_punct_data_joined %>%
  arrange(ISO_2LETTER, DAY_DATE) %>%
  mutate(
         DAY_ARR_PUNCT = case_when(
           ARR_SCHEDULE_FLIGHT == 0 ~ NA,
           .default = ARR_PUNCTUAL_FLIGHTS/ARR_SCHEDULE_FLIGHT * 100
           ),
         DAY_DEP_PUNCT = case_when(
           DEP_SCHEDULE_FLIGHT == 0 ~ NA,
           .default = DEP_PUNCTUAL_FLIGHTS/DEP_SCHEDULE_FLIGHT * 100
         ),
         DAY_ARR_PUNCT_PY = lag(DAY_ARR_PUNCT, 364),
         DAY_DEP_PUNCT_PY = lag(DAY_DEP_PUNCT, 364),
         DAY_ARR_PUNCT_2019 =if_else(YEAR == last_year_punct,
                               lag(DAY_ARR_PUNCT,
                                   364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7),
                               NA),
         DAY_DEP_PUNCT_2019 =if_else(YEAR == last_year_punct,
                               lag(DAY_DEP_PUNCT,
                                   364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7),
                               NA),
         DAY_2019 = if_else(YEAR == last_year_punct,
                            lag(DAY_DATE,
                                364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7)
                            , NA),
         DAY_ARR_PUNCT_DIF_PY = DAY_ARR_PUNCT - DAY_ARR_PUNCT_PY,
         DAY_DEP_PUNCT_DIF_PY = DAY_DEP_PUNCT - DAY_DEP_PUNCT_PY,
         DAY_ARR_PUNCT_DIF_2019 = DAY_ARR_PUNCT - DAY_ARR_PUNCT_2019,
         DAY_DEP_PUNCT_DIF_2019 = DAY_DEP_PUNCT - DAY_DEP_PUNCT_2019
  ) %>%
  mutate(
    WEEK_ARR_PUNCT = rollsum((ARR_PUNCTUAL_FLIGHTS), 7, fill = NA, align = "right") / rollsum(ARR_SCHEDULE_FLIGHT, 7, fill = NA, align = "right") * 100,
    WEEK_DEP_PUNCT = rollsum((DEP_PUNCTUAL_FLIGHTS), 7, fill = NA, align = "right") / rollsum(DEP_SCHEDULE_FLIGHT, 7, fill = NA, align = "right") * 100
  ) %>%
  mutate(WEEK_ARR_PUNCT_PY = lag(WEEK_ARR_PUNCT, 364),
         WEEK_DEP_PUNCT_PY = lag(WEEK_DEP_PUNCT, 364),
         WEEK_ARR_PUNCT_2019 =if_else(YEAR == last_year_punct,
                                  lag(WEEK_ARR_PUNCT, 364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7),
                                  NA),
         WEEK_DEP_PUNCT_2019 =if_else(YEAR == last_year_punct,
                                  lag(WEEK_DEP_PUNCT, 364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7),
                                  NA),
         WEEK_ARR_PUNCT_DIF_PY = WEEK_ARR_PUNCT - WEEK_ARR_PUNCT_PY,
         WEEK_DEP_PUNCT_DIF_PY = WEEK_DEP_PUNCT - WEEK_DEP_PUNCT_PY,
         WEEK_ARR_PUNCT_DIF_2019 = WEEK_ARR_PUNCT - WEEK_ARR_PUNCT_2019,
         WEEK_DEP_PUNCT_DIF_2019 = WEEK_DEP_PUNCT - WEEK_DEP_PUNCT_2019
         , test_date = if_else(YEAR == last_year_punct,
                               lag(DAY_DATE, 364 * (last_year_punct - 2019) + floor((last_year_punct - 2019) / 4) * 7),
                               NA)
  )

st_punct_d_w <- st_punct_data %>%
  filter (DAY_DATE == last_day_punct) %>%
  select(
    ISO_2LETTER,
    DAY_DATE,
    DAY_ARR_PUNCT,
    DAY_DEP_PUNCT,
    DAY_ARR_PUNCT_DIF_PY,
    DAY_DEP_PUNCT_DIF_PY,
    DAY_ARR_PUNCT_DIF_2019,
    DAY_DEP_PUNCT_DIF_2019,
    WEEK_ARR_PUNCT,
    WEEK_DEP_PUNCT,
    WEEK_ARR_PUNCT_DIF_PY,
    WEEK_DEP_PUNCT_DIF_PY,
    WEEK_ARR_PUNCT_DIF_2019,
    WEEK_DEP_PUNCT_DIF_2019
  ) %>%
  rename(
    FLIGHT_DATE = DAY_DATE,
    DY_ARR_PUN = DAY_ARR_PUNCT,
    DY_DEP_PUN = DAY_DEP_PUNCT,
    DY_ARR_PUN_DIF_PREV_YEAR = DAY_ARR_PUNCT_DIF_PY,
    DY_DEP_PUN_DIF_PREV_YEAR = DAY_DEP_PUNCT_DIF_PY,
    DY_ARR_PUN_DIF_2019 = DAY_ARR_PUNCT_DIF_2019,
    DY_DEP_PUN_DIF_2019 = DAY_DEP_PUNCT_DIF_2019,
    WK_ARR_PUN = WEEK_ARR_PUNCT,
    WK_DEP_PUN = WEEK_DEP_PUNCT,
    WK_ARR_PUN_DIF_PREV_YEAR = WEEK_ARR_PUNCT_DIF_PY,
    WK_DEP_PUN_DIF_PREV_YEAR = WEEK_DEP_PUNCT_DIF_PY,
    WK_ARR_PUN_DIF_2019 = WEEK_ARR_PUNCT_DIF_2019,
    WK_DEP_PUN_DIF_2019 = WEEK_DEP_PUNCT_DIF_2019
  )

st_punct_y2d <- st_punct_data_joined %>%
  arrange(ISO_2LETTER, DAY_DATE) %>%
  mutate(MONTH_DAY = as.numeric(format(DAY_DATE, format="%m%d"))) %>%
  filter(MONTH_DAY <= as.numeric(format(last_day_punct, format="%m%d"))) %>%
  group_by(ISO_2LETTER, YEAR) %>%
  summarise (
    Y2D_ARR_PUN = sum(ARR_PUNCTUAL_FLIGHTS, na.rm=TRUE) / sum(ARR_SCHEDULE_FLIGHT, na.rm=TRUE) * 100,
    Y2D_DEP_PUN = sum(DEP_PUNCTUAL_FLIGHTS, na.rm=TRUE) / sum(DEP_SCHEDULE_FLIGHT, na.rm=TRUE) * 100,
    .groups = "drop") %>%
  group_by(ISO_2LETTER) %>%
  mutate(Y2D_ARR_PUN_PY = lag(Y2D_ARR_PUN, 1),
         Y2D_DEP_PUN_PY = lag(Y2D_DEP_PUN, 1),
         Y2D_ARR_PUN_2019 = lag(Y2D_ARR_PUN, last_year_punct - 2019),
         Y2D_DEP_PUN_2019 = lag(Y2D_DEP_PUN, last_year_punct - 2019),
         Y2D_ARR_PUN_DIF_PREV_YEAR = Y2D_ARR_PUN - Y2D_ARR_PUN_PY,
         Y2D_DEP_PUN_DIF_PREV_YEAR = Y2D_DEP_PUN - Y2D_DEP_PUN_PY,
         Y2D_ARR_PUN_DIF_2019 = Y2D_ARR_PUN - Y2D_ARR_PUN_2019,
         Y2D_DEP_PUN_DIF_2019 = Y2D_DEP_PUN - Y2D_DEP_PUN_2019
  ) %>%
  filter(YEAR == last_year_punct) %>%
  ungroup() %>%
  select(ISO_2LETTER,
         Y2D_ARR_PUN,
         Y2D_DEP_PUN,
         Y2D_ARR_PUN_DIF_PREV_YEAR,
         Y2D_DEP_PUN_DIF_PREV_YEAR,
         Y2D_ARR_PUN_DIF_2019,
         Y2D_DEP_PUN_DIF_2019
  )


st_punct_for_json <- merge(st_punct_d_w, st_punct_y2d, by="ISO_2LETTER") %>%
  # Iceland exception
  mutate(
    DY_ARR_PUN_DIF_PREV_YEAR =  if_else(ISO_2LETTER == "IS" & year(FLIGHT_DATE) < 2025, NA, DY_ARR_PUN_DIF_PREV_YEAR),
    WK_ARR_PUN_DIF_PREV_YEAR =  if_else(ISO_2LETTER == "IS" & year(FLIGHT_DATE) < 2025, NA, WK_ARR_PUN_DIF_PREV_YEAR),
    Y2D_ARR_PUN_DIF_PREV_YEAR = if_else(ISO_2LETTER == "IS" & year(FLIGHT_DATE) < 2025, NA, Y2D_ARR_PUN_DIF_PREV_YEAR),

    DY_DEP_PUN_DIF_PREV_YEAR =  if_else(ISO_2LETTER == "IS" & year(FLIGHT_DATE) < 2025, NA, DY_DEP_PUN_DIF_PREV_YEAR),
    WK_DEP_PUN_DIF_PREV_YEAR =  if_else(ISO_2LETTER == "IS" & year(FLIGHT_DATE) < 2025, NA, WK_DEP_PUN_DIF_PREV_YEAR),
    Y2D_DEP_PUN_DIF_PREV_YEAR = if_else(ISO_2LETTER == "IS" & year(FLIGHT_DATE) < 2025, NA, Y2D_DEP_PUN_DIF_PREV_YEAR),

    DY_ARR_PUN_DIF_2019 =  if_else(ISO_2LETTER == "IS", NA, DY_ARR_PUN_DIF_2019),
    WK_ARR_PUN_DIF_2019 =  if_else(ISO_2LETTER == "IS", NA, WK_ARR_PUN_DIF_2019),
    Y2D_ARR_PUN_DIF_2019 = if_else(ISO_2LETTER == "IS", NA, Y2D_ARR_PUN_DIF_2019),

    DY_DEP_PUN_DIF_2019 =  if_else(ISO_2LETTER == "IS", NA, DY_DEP_PUN_DIF_2019),
    WK_DEP_PUN_DIF_2019 =  if_else(ISO_2LETTER == "IS", NA, WK_DEP_PUN_DIF_2019),
    Y2D_DEP_PUN_DIF_2019 = if_else(ISO_2LETTER == "IS", NA, Y2D_DEP_PUN_DIF_2019)
  ) %>%
  ### rank calculation
  mutate(
    DY_ARR_PUN_RANK = min_rank(desc(DY_ARR_PUN)),
    WK_ARR_PUN_RANK = min_rank(desc(WK_ARR_PUN)),
    Y2D_ARR_PUN_RANK = min_rank(desc(Y2D_ARR_PUN)),

    DY_DEP_PUN_RANK = min_rank(desc(DY_DEP_PUN)),
    WK_DEP_PUN_RANK = min_rank(desc(WK_DEP_PUN)),
    Y2D_DEP_PUN_RANK = min_rank(desc(Y2D_DEP_PUN)),

    PUN_RANK_TEXT = "*Top rank for highest."
  ) %>%
  rename(iso_2letter = ISO_2LETTER) %>%
  right_join(state_iso, by = "iso_2letter") %>%
  select (-state) %>%
  arrange(iso_2letter)

#### CO2 data ----
if (exists("co2_data_raw") == FALSE) {co2_data_raw <- get_co2_data()}

st_co2_data_filtered <- co2_data_raw %>%
  mutate(co2_state = STATE_NAME) %>%
  right_join(list_state_co2, by = "co2_state") %>%
  select(-c(STATE_NAME, STATE_CODE, co2_state) )

st_co2_data <- st_co2_data_filtered %>%
  select(iso_2letter,
         FLIGHT_MONTH,
         CO2_QTY_TONNES,
         LY_CO2_QTY_TONNES,
         TF,
         LY_TF,
         YEAR,
         MONTH) %>%
  group_by(iso_2letter, FLIGHT_MONTH, YEAR, MONTH) %>%
  summarise (MM_DEP = sum(TF, na.rm=TRUE) / 10^6,
             MM_DEP_PY = sum(LY_TF, na.rm=TRUE) / 10^6,
             MM_CO2 = sum(CO2_QTY_TONNES, na.rm=TRUE) / 10^6,
             MM_CO2_PY = sum(LY_CO2_QTY_TONNES, na.rm=TRUE) / 10^6,
             .groups = "drop"
  ) %>%
  mutate(
    CO2_DATE = FLIGHT_MONTH,
    MM_CO2_DEP = MM_CO2 / MM_DEP,
    MM_CO2_DEP_PY = MM_CO2_PY / MM_DEP_PY
  ) %>%
  arrange(iso_2letter, FLIGHT_MONTH) %>%
  mutate(FLIGHT_MONTH = ceiling_date(as_date(FLIGHT_MONTH), unit = 'month')-1)

st_co2_last_date <- min(max(st_co2_data$FLIGHT_MONTH, na.rm=TRUE),
                        floor_date(data_day_date, 'month') -1,
                        na.rm = TRUE)
st_co2_last_month <- format(st_co2_last_date,'%B')
st_co2_last_month_num <- as.numeric(format(st_co2_last_date,'%m'))
st_co2_last_year <- lubridate::year(st_co2_last_date)

#check last month number of flights
check_flights <- st_co2_data %>% ungroup() %>%
  filter (YEAR == st_co2_last_year) %>% filter(MONTH == st_co2_last_month_num) %>%
  summarise (TTF = sum(MM_DEP, na.rm=TRUE)*10^6) %>%
  select(TTF) %>% pull()

if (check_flights < 1000) {
  st_co2_data <- st_co2_data %>% filter (FLIGHT_MONTH < st_co2_last_date)
  st_co2_last_date <- max(st_co2_data$FLIGHT_MONTH, na.rm=TRUE)
}

st_co2_for_json <- st_co2_data %>%
  arrange(iso_2letter, FLIGHT_MONTH) %>%
  mutate(
    MONTH_TEXT = format(FLIGHT_MONTH,'%B'),
    MM_CO2_2019 = lag(MM_CO2, (as.numeric(st_co2_last_year) - 2019) * 12),
    MM_DEP_2019 = lag(MM_DEP, (as.numeric(st_co2_last_year) - 2019) * 12),
    MM_CO2_DEP_2019 = lag(MM_CO2_DEP, (as.numeric(st_co2_last_year) - 2019) * 12)
  ) %>%
  mutate(
    MM_CO2_DIF_PREV_YEAR = MM_CO2 / MM_CO2_PY - 1,
    MM_DEP_DIF_PREV_YEAR = MM_DEP / MM_DEP_PY - 1,
    MM_CO2_DEP_DIF_PREV_YEAR = MM_CO2_DEP / MM_CO2_DEP_PY - 1,
    MM_CO2_DIF_2019 = MM_CO2 / MM_CO2_2019 - 1,
    MM_DEP_DIF_2019 = MM_DEP / MM_DEP_2019 - 1,
    MM_CO2_DEP_DIF_2019 = MM_CO2_DEP / MM_CO2_DEP_2019 - 1
  ) %>%
  group_by(iso_2letter, YEAR) %>%
  mutate(
    Y2D_CO2 = cumsum(MM_CO2),
    Y2D_DEP = cumsum(MM_DEP),
    Y2D_CO2_DEP = cumsum(MM_CO2) / cumsum(MM_DEP)
  ) %>%
  ungroup() %>%
  mutate(
    Y2D_CO2_PY = lag(Y2D_CO2, 12),
    Y2D_DEP_PY = lag(Y2D_DEP, 12),
    Y2D_CO2_DEP_PY = lag(Y2D_CO2_DEP, 12),
    Y2D_CO2_2019 = lag(Y2D_CO2, (as.numeric(st_co2_last_year) - 2019) * 12),
    Y2D_DEP_2019 = lag(Y2D_DEP, (as.numeric(st_co2_last_year) - 2019) * 12),
    Y2D_CO2_DEP_2019 = lag(Y2D_CO2_DEP, (as.numeric(st_co2_last_year) - 2019) * 12)
  ) %>%
  mutate(
    Y2D_CO2_DIF_PREV_YEAR = Y2D_CO2 / Y2D_CO2_PY - 1,
    Y2D_DEP_DIF_PREV_YEAR = Y2D_DEP / Y2D_DEP_PY - 1,
    Y2D_CO2_DEP_DIF_PREV_YEAR = Y2D_CO2_DEP / Y2D_CO2_DEP_PY - 1,
    Y2D_CO2_DIF_2019 = Y2D_CO2 / Y2D_CO2_2019 - 1,
    Y2D_DEP_DIF_2019 = Y2D_DEP / Y2D_DEP_2019 - 1,
    Y2D_CO2_DEP_DIF_2019 = Y2D_CO2_DEP / Y2D_CO2_DEP_2019 - 1
  ) %>%
  select(
    iso_2letter,
    FLIGHT_MONTH,
    MONTH_TEXT,
    MM_CO2,
    MM_CO2_DIF_PREV_YEAR,
    MM_CO2_DIF_2019,
    MM_CO2_DEP,
    MM_CO2_DEP_DIF_PREV_YEAR,
    MM_CO2_DEP_DIF_2019,
    Y2D_CO2,
    Y2D_CO2_DIF_PREV_YEAR,
    Y2D_CO2_DIF_2019,
    Y2D_CO2_DEP,
    Y2D_CO2_DEP_DIF_PREV_YEAR,
    Y2D_CO2_DEP_DIF_2019
  ) %>%
  filter(FLIGHT_MONTH == st_co2_last_date) %>%
  ### rank calculation
  mutate(
    MM_CO2_RANK = rank(desc(MM_CO2), ties.method = "max"),
    Y2D_CO2_RANK = rank(desc(Y2D_CO2), ties.method = "max"),

    MM_CO2_DEP_RANK = rank(desc(MM_CO2_DEP), ties.method = "max"),
    Y2D_CO2_DEP_RANK = rank(desc(Y2D_CO2_DEP), ties.method = "max"),
    CO2_RANK_TEXT = "*Top rank for highest"
  ) %>%
  right_join(state_iso, by = "iso_2letter") %>%
  select(-state) %>%
  arrange(iso_2letter)

## join tables ----
st_json_app_j <- rel_iso_icao_country %>% select(iso_2letter, icao_code, state) %>% rename(icao_2letter=icao_code) %>% arrange(iso_2letter)
st_json_app_j$st_daio <- select(st_daio_for_json, -c(iso_2letter))
st_json_app_j$st_dai <- select(st_dai_for_json, -c(iso_2letter))
st_json_app_j$st_ovf <- select(st_overflight_for_json, -c(iso_2letter))
st_json_app_j$st_delay <- select(st_delay_for_json, -c(iso_2letter))
st_json_app_j$st_punct <- select(st_punct_for_json, -c(iso_2letter))
st_json_app_j$st_billed <- select(st_billed_for_json, -c(iso_2letter))
st_json_app_j$st_co2 <- select(st_co2_for_json, -c(iso_2letter))

update_day <- floor_date(lubridate::now(), unit = "days") %>%
  as_tibble() %>%
  rename(APP_UPDATE = 1)

st_json_app_j$st_update <- update_day

st_json_app_j <- st_json_app_j %>%   group_by(iso_2letter, state)

st_json_app <- st_json_app_j %>%
  toJSON(., pretty = TRUE)

save_json(st_json_app, "st_json_app")
print(paste(format(now(), "%H:%M:%S"), "st_json_app"))

# ____________________________________________________________________________________________
#
#    State ranking tables  -----
#
# ____________________________________________________________________________________________

## TRAFFIC ----
### Aircraft operators ----
mydataframe <-  "st_ao_agg"
stakeholder <- str_sub(mydataframe, 1, 2)

#### day ----
st_ao_data_day_int <- create_ranking(mydataframe, "DAY", FLIGHT)

st_ao_data_day <- st_ao_data_day_int %>%
  mutate(
    ST_RANK = paste0(tolower(STK_NAME), R_RANK)
  ) %>% 
  filter(R_RANK <11) %>% 
  arrange(STK_NAME, R_RANK) %>% 
  select(
    ST_RANK,
    DY_RANK_DIF_PREV_WEEK = RANK_DIF,
    DY_AO_GRP_NAME = NAME,
    DY_TO_DATE = TO_DATE,
    DY_FLT = CURRENT,
    DY_FLT_DIF_PREV_WEEK_PERC = DIF1_METRIC_PERC,
    DY_FLT_DIF_PREV_YEAR_PERC = DIF2_METRIC_PERC
  ) 

#### week ----
st_ao_data_wk_int <- create_ranking(mydataframe, "WEEK", FLIGHT)

st_ao_data_wk <- st_ao_data_wk_int %>%
  mutate(
    ST_RANK = paste0(tolower(STK_NAME), R_RANK)
  ) %>% 
  filter(R_RANK <11) %>% 
  arrange(STK_NAME, R_RANK) %>% 
  select(
    ST_RANK,
    WK_RANK_DIF_PREV_WEEK = RANK_DIF,
    WK_AO_GRP_NAME = NAME,
    WK_FROM_DATE = FROM_DATE,
    WK_TO_DATE = TO_DATE,
    WK_FLT_AVG = CURRENT,
    WK_FLT_DIF_PREV_WEEK_PERC = DIF1_METRIC_PERC,
    WK_FLT_DIF_PREV_YEAR_PERC = DIF2_METRIC_PERC
  ) 

#### y2d ----
st_ao_data_y2d_int <- create_ranking(mydataframe, "Y2D", FLIGHT)

st_ao_data_y2d <- st_ao_data_y2d_int %>%
  mutate(
    ST_RANK = paste0(tolower(STK_NAME), R_RANK)
  ) %>% 
  filter(R_RANK <11) %>% 
  arrange(STK_NAME, R_RANK) %>% 
  select(
    # STK_NAME,
    ST_RANK,
    Y2D_RANK_DIF_PREV_YEAR = RANK_DIF,
    Y2D_AO_GRP_NAME = NAME,
    Y2D_TO_DATE = TO_DATE,
    Y2D_FLT_AVG = CURRENT,
    Y2D_FLT_DIF_PREV_YEAR_PERC = DIF1_METRIC_PERC,
    Y2D_FLT_DIF_2019_PERC = DIF2_METRIC_PERC
  ) 

#### main card ----
st_ao_main_traffic <- create_main_card (st_ao_data_day_int) %>% 
  mutate(
    ST_RANK = paste0(tolower(STK_NAME), R_RANK)
  ) %>% 
  arrange(STK_NAME, R_RANK) %>% 
  select(ST_RANK,
         MAIN_TFC_AO_GRP_NAME = NAME, 
         MAIN_TFC_AO_GRP_CODE = CODE, 
         MAIN_TFC_AO_GRP_FLT = CURRENT)


st_ao_main_traffic_dif <- create_main_card_dif (st_ao_data_day_int) %>% 
  mutate(
    ST_RANK = paste0(tolower(STK_NAME), R_RANK)
  ) %>% 
  arrange(tolower(STK_NAME), R_RANK) %>% 
  select(ST_RANK,
         MAIN_TFC_DIF_AO_GRP_NAME = NAME,
         MAIN_TFC_DIF_AO_GRP_CODE = CODE,
         MAIN_TFC_DIF_AO_GRP_FLT_DIF = DIF1_METRIC
  )


#### join tables ----
# create list of state/rankings for left join
state_iso_ranking <- list()
i = 0
for (i in 1:10) {
  i = i + 1
  state_iso_ranking <- state_iso_ranking %>%
    bind_rows(state_iso, .)
}

state_iso_ranking <- state_iso_ranking %>%
  arrange(state) %>%
  group_by(state) %>%
  mutate(
    RANK = row_number(),
    ST_RANK = paste0(tolower(state), RANK)
         )

# join and reorder tables
st_ao_data <- state_iso_ranking %>%
  left_join(st_ao_main_traffic, by = "ST_RANK") %>%
  left_join(st_ao_main_traffic_dif, by = "ST_RANK") %>%
  left_join(st_ao_data_day, by = "ST_RANK") %>%
  left_join(st_ao_data_wk, by = "ST_RANK") %>%
  left_join(st_ao_data_y2d, by = "ST_RANK") %>%
  ungroup() %>%
  select(-ST_RANK, -state) %>%
  arrange (iso_2letter, RANK)

# covert to json and save in app data folder and archive
st_ao_data_j <- st_ao_data %>% toJSON(., pretty = TRUE)
save_json(st_ao_data_j, "st_ao_ranking_traffic")
print(paste(format(now(), "%H:%M:%S"), "st_ao_ranking_traffic"))

### Airports ----
mydataframe <-  "st_ap_agg"
stakeholder <- str_sub(mydataframe, 1, 2)

#### day ----
st_ap_data_day_int <- create_ranking(mydataframe, "DAY", DEP_ARR)

st_ap_data_day <- st_ap_data_day_int %>%
  mutate(
    ST_RANK = paste0(tolower(STK_NAME), R_RANK)
  ) %>% 
  filter(R_RANK <11) %>% 
  arrange(STK_NAME, R_RANK) %>% 
  select(
    ST_RANK,
    DY_RANK_DIF_PREV_WEEK = RANK_DIF,
    DY_APT_NAME = NAME,
    DY_TO_DATE = TO_DATE,
    DY_FLT = CURRENT,
    DY_FLT_DIF_PREV_WEEK_PERC = DIF1_METRIC_PERC,
    DY_FLT_DIF_PREV_YEAR_PERC = DIF2_METRIC_PERC
  ) 

#### week ----
st_ap_data_wk_int <- create_ranking(mydataframe, "WEEK", DEP_ARR)

st_ap_data_wk <- st_ap_data_wk_int %>%
  mutate(
    ST_RANK = paste0(tolower(STK_NAME), R_RANK)
  ) %>% 
  filter(R_RANK <11) %>% 
  #NOTE!! Temporary fix while the app is corrected
  mutate(
    FROM_DATE = FROM_DATE - days(6),
    TO_DATE = TO_DATE - days(6)
  ) %>% 
  arrange(STK_NAME, R_RANK) %>% 
  select(
    ST_RANK,
    WK_RANK_DIF_PREV_WEEK = RANK_DIF,
    WK_APT_NAME = NAME,
    WK_FROM_DATE = FROM_DATE,
    WK_TO_DATE = TO_DATE,
    WK_FLT_AVG = CURRENT,
    WK_FLT_DIF_PREV_WEEK_PERC = DIF1_METRIC_PERC,
    WK_FLT_DIF_PREV_YEAR_PERC = DIF2_METRIC_PERC
  ) 

#### y2d ----
st_ap_data_y2d_int <- create_ranking(mydataframe, "Y2D", DEP_ARR)

st_ap_data_y2d <- st_ap_data_y2d_int %>%
  mutate(
    ST_RANK = paste0(tolower(STK_NAME), R_RANK)
  ) %>% 
  filter(R_RANK <11) %>% 
  arrange(STK_NAME, R_RANK) %>% 
  select(
    ST_RANK,
    Y2D_RANK_DIF_PREV_YEAR = RANK_DIF,
    Y2D_APT_NAME = NAME,
    Y2D_TO_DATE = TO_DATE,
    Y2D_FLT_AVG = CURRENT,
    Y2D_FLT_DIF_PREV_YEAR_PERC = DIF1_METRIC_PERC,
    Y2D_FLT_DIF_2019_PERC = DIF2_METRIC_PERC
  ) 

#### main card ----
st_ap_main_traffic <- create_main_card (st_ap_data_day_int) %>% 
  mutate(
    ST_RANK = paste0(tolower(STK_NAME), R_RANK)
  ) %>% 
  arrange(STK_NAME, R_RANK) %>% 
  select(ST_RANK,
         MAIN_TFC_APT_NAME = NAME, 
         MAIN_TFC_APT_CODE = CODE,
         MAIN_TFC_APT_FLT = CURRENT)


st_ap_main_traffic_dif <- create_main_card_dif (st_ap_data_day_int) %>% 
  mutate(
    ST_RANK = paste0(tolower(STK_NAME), R_RANK)
  ) %>% 
  arrange(tolower(STK_NAME), R_RANK) %>% 
  select(ST_RANK,
         MAIN_TFC_DIF_APT_NAME = NAME,
         # MAIN_TFC_DIF_APT_CODE = CODE,
         MAIN_TFC_DIF_APT_FLT_DIF = DIF1_METRIC
  )

#### join tables ----
# create list of state/rankings for left join
state_iso_ranking <- list()
i = 0
for (i in 1:10) {
  i = i + 1
  state_iso_ranking <- state_iso_ranking %>%
    bind_rows(state_iso, .)
}

state_iso_ranking <- state_iso_ranking %>%
  arrange(state) %>%
  group_by(state) %>%
  mutate(
    RANK = row_number(),
    ST_RANK = paste0(tolower(state), RANK)
  )

# join and reorder tables
st_ap_data <- state_iso_ranking %>%
  left_join(st_ap_main_traffic, by = "ST_RANK") %>%
  left_join(st_ap_main_traffic_dif, by = "ST_RANK") %>%
  left_join(st_ap_data_day, by = "ST_RANK") %>%
  left_join(st_ap_data_wk, by = "ST_RANK") %>%
  left_join(st_ap_data_y2d, by = "ST_RANK") %>%
  select(-ST_RANK)

# covert to json and save in app data folder and archive
st_ap_data_j <- st_ap_data %>% toJSON(., pretty = TRUE)
save_json(st_ap_data_j, "st_apt_ranking_traffic")
print(paste(format(now(), "%H:%M:%S"), "st_apt_ranking_traffic"))


### State pair ----
mydataframe <-  "st_st_agg"
stakeholder <- str_sub(mydataframe, 1, 2)

#### day ----
st_st_data_day_int <- create_ranking(mydataframe, "DAY", FLIGHT)

st_st_data_day <- st_st_data_day_int %>%
  mutate(
    ST_RANK = paste0(tolower(STK_NAME), R_RANK)
  ) %>% 
  filter(R_RANK <11) %>% 
  arrange(STK_NAME, R_RANK) %>% 
  select(
    ST_RANK,
    DY_RANK_DIF_PREV_WEEK = RANK_DIF,
    DY_COUNTRY_NAME = NAME,
    DY_TO_DATE = TO_DATE,
    DY_CTRY_DAI = CURRENT,
    DY_DIF_PREV_WEEK_PERC = DIF1_METRIC_PERC,
    DY_DIF_PREV_YEAR_PERC = DIF2_METRIC_PERC
  ) 

#### week ----
st_st_data_wk_int <- create_ranking(mydataframe, "WEEK", FLIGHT)

st_st_data_wk <- st_st_data_wk_int %>%
  mutate(
    ST_RANK = paste0(tolower(STK_NAME), R_RANK)
  ) %>% 
  filter(R_RANK <11) %>% 
  arrange(STK_NAME, R_RANK) %>% 
  select(
    ST_RANK,
    WK_RANK_DIF_PREV_WEEK = RANK_DIF,
    WK_COUNTRY_NAME = NAME,
    WK_FROM_DATE = FROM_DATE,
    WK_TO_DATE = TO_DATE,
    WK_CTRY_DAI = CURRENT,
    WK_DIF_PREV_WEEK_PERC = DIF1_METRIC_PERC,
    WK_DIF_PREV_YEAR_PERC = DIF2_METRIC_PERC
  ) 

#### y2d ----
st_st_data_y2d_int <- create_ranking(mydataframe, "Y2D", FLIGHT)

st_st_data_y2d <- st_st_data_y2d_int %>%
  mutate(
    ST_RANK = paste0(tolower(STK_NAME), R_RANK)
  ) %>% 
  filter(R_RANK <11) %>% 
  arrange(STK_NAME, R_RANK) %>% 
  select(
    ST_RANK,
    Y2D_RANK_DIF_PREV_YEAR = RANK_DIF,
    Y2D_COUNTRY_NAME = NAME,
    Y2D_TO_DATE = TO_DATE,
    Y2D_CTRY_DAI = CURRENT,
    Y2D_CTRY_DAI_PREV_YEAR_PERC = DIF1_METRIC_PERC,
    Y2D_CTRY_DAI_2019_PERC = DIF2_METRIC_PERC
  ) 

#### main card ----
st_st_main_traffic <- create_main_card (st_st_data_day_int) %>% 
  mutate(
    ST_RANK = paste0(tolower(STK_NAME), R_RANK)
  ) %>% 
  arrange(STK_NAME, R_RANK) %>% 
  select(ST_RANK,
         MAIN_TFC_CTRY_NAME = NAME, 
         MAIN_TFC_CTRY_CODE = CODE,
         MAIN_TFC_CTRY_DAI = CURRENT)

st_st_main_traffic_dif <- create_main_card_dif (st_st_data_day_int) %>% 
  mutate(
    ST_RANK = paste0(tolower(STK_NAME), R_RANK)
  ) %>% 
  arrange(tolower(STK_NAME), R_RANK) %>% 
  select(ST_RANK,
         MAIN_TFC_DIF_CTRY_NAME = NAME,
         MAIN_TFC_DIF_CTRY_CODE = CODE,
         MAIN_TFC_CTRY_DIF = DIF1_METRIC
  )

#### join tables ----
# create list of state/rankings for left join
state_iso_ranking <- list()
i = 0
for (i in 1:10) {
  i = i + 1
  state_iso_ranking <- state_iso_ranking %>%
    bind_rows(state_iso, .)
}

state_iso_ranking <- state_iso_ranking %>%
  arrange(state) %>%
  group_by(state) %>%
  mutate(
    RANK = row_number(),
    ST_RANK = paste0(tolower(state), RANK)
  )

# join and reorder tables
st_st_data <- state_iso_ranking %>%
  left_join(st_st_main_traffic, by = "ST_RANK") %>%
  left_join(st_st_main_traffic_dif, by = "ST_RANK") %>%
  left_join(st_st_data_day, by = "ST_RANK") %>%
  left_join(st_st_data_wk, by = "ST_RANK") %>%
  left_join(st_st_data_y2d, by = "ST_RANK") %>%
  select(-ST_RANK)

# covert to json and save in app data folder and archive
st_st_data_j <- st_st_data %>% toJSON(., pretty = TRUE)

# name of json file in consistency with network
save_json(st_st_data_j, "st_ctry_ranking_traffic_DAI")
print(paste(format(now(), "%H:%M:%S"), "st_ctry_ranking_traffic_DAI"))


## DELAY ----
### ACC  ----
#### day ----
if(!exists("nw_acc_delay_day_raw")) {
  nw_acc_delay_day_raw <- export_query(query_nw_acc_delay_day_raw(format(data_day_date, "%Y-%m%-%d"))) 
}

if (max(nw_acc_delay_day_raw$ENTRY_DATE) != data_day_date) {
  nw_acc_delay_day_raw <- export_query(query_nw_acc_delay_day_raw(format(data_day_date, "%Y-%m%-%d"))) 
}

# process data
acc_delay_day_sorted <-  nw_acc_delay_day_raw %>%
  left_join(distinct(list_acc, NAME, ICAO_CODE), by = c("UNIT_CODE" = "ICAO_CODE")) %>%
  relocate(NAME, .before = everything()) %>% 

  arrange(desc(DLY_ER), NAME) %>%
  mutate(
    DY_RANK = rank(desc(DLY_ER), ties.method = "max"),
    ICAO_CODE = UNIT_CODE,
    DY_ACC_NAME = NAME,
    DY_ACC_ER_DLY = DLY_ER,
    DY_ACC_ER_DLY_FLT = if_else(FLIGHT == 0, 0, DLY_ER / FLIGHT),
    DY_RANK_ER_DLY_FLT = rank(desc(DY_ACC_ER_DLY_FLT), ties.method = "max"),
    DY_TO_DATE = ENTRY_DATE) %>%
  right_join(list_acc, by = "ICAO_CODE") %>%
  mutate( 
    #canarias case
    iso_2letter = if_else(substr(ICAO_CODE,1,2) == "GC", "IC", ISO_2LETTER)
  ) %>% 
  left_join(state_iso, by = "iso_2letter") %>%
  group_by(iso_2letter) %>%
  arrange(iso_2letter, desc(DY_ACC_ER_DLY), DY_ACC_NAME) %>%
  mutate (
    ST_RANK = paste0(tolower(state), row_number()),
  ) %>%
  ungroup()

  acc_delay_day <- acc_delay_day_sorted %>%
  select(
    ST_RANK,
    DY_RANK,
    DY_ACC_NAME,
    DY_TO_DATE,
    DY_ACC_ER_DLY,
    DY_ACC_ER_DLY_FLT
    )

#### week ----
if(!exists("nw_acc_delay_week_raw")) {
  nw_acc_delay_week_raw <- export_query(query_nw_acc_delay_week_raw(format(data_day_date, "%Y-%m%-%d"))) 
}

if (max(nw_acc_delay_week_raw$MAX_ENTRY_DATE) != data_day_date) {
  nw_acc_delay_week_raw <- export_query(query_nw_acc_delay_week_raw(format(data_day_date, "%Y-%m%-%d"))) 
}
  
# process data
acc_delay_week <- nw_acc_delay_week_raw %>%
  left_join(distinct(list_acc, NAME, ICAO_CODE), by = c("UNIT_CODE" = "ICAO_CODE")) %>%
  relocate(NAME, .before = everything()) %>% 
  arrange(desc(DAILY_DLY_ER), NAME) %>%
  mutate(
    WK_RANK = rank(desc(DAILY_DLY_ER), ties.method = "max"),
    ICAO_CODE = UNIT_CODE,
    WK_ACC_NAME = NAME,
    WK_ACC_ER_DLY = DAILY_DLY_ER,
    WK_ACC_ER_DLY_FLT = if_else(DAILY_FLIGHT == 0, 0, DAILY_DLY_ER / DAILY_FLIGHT),
    WK_FROM_DATE = MIN_ENTRY_DATE,
    WK_TO_DATE = MAX_ENTRY_DATE
    ) %>%
  right_join(list_acc, by = "ICAO_CODE") %>%
  mutate( 
    #canarias case
    iso_2letter = if_else(substr(ICAO_CODE,1,2) == "GC", "IC", ISO_2LETTER)
  ) %>% 
  left_join(state_iso, by = "iso_2letter") %>%
  group_by(iso_2letter) %>%
  arrange(iso_2letter, desc(WK_ACC_ER_DLY), WK_ACC_NAME) %>%
  mutate (
    ST_RANK = paste0(tolower(state), row_number()),
  ) %>%
  ungroup() %>%
  select(
    ST_RANK,
    WK_RANK,
    WK_ACC_NAME,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_ACC_ER_DLY,
    WK_ACC_ER_DLY_FLT
  )

#### y2d ----
if(!exists("nw_acc_delay_y2d_raw")) {
  nw_acc_delay_y2d_raw <- export_query(query_nw_acc_delay_y2d_raw(format(data_day_date, "%Y-%m%-%d"))) 
}

if (max(nw_acc_delay_y2d_raw$ENTRY_DATE) != data_day_date) {
  nw_acc_delay_y2d_raw <- export_query(query_nw_acc_delay_y2d_raw(format(data_day_date, "%Y-%m%-%d"))) 
}

# process data
acc_delay_y2d <- nw_acc_delay_y2d_raw %>%
  rename(Y2D_FROM_DATE = MIN_DATE) %>%
  left_join(distinct(list_acc, NAME, ICAO_CODE), by = c("UNIT_CODE" = "ICAO_CODE")) %>%
  relocate(NAME, .before = everything()) %>%

  arrange(desc(Y2D_AVG_DLY), NAME) %>%
  mutate(
    Y2D_RANK = rank(desc(Y2D_AVG_DLY), ties.method = "max"),
    ICAO_CODE = UNIT_CODE,
    Y2D_ACC_NAME = NAME,
    Y2D_ACC_ER_DLY = Y2D_AVG_DLY,
    Y2D_ACC_ER_DLY_FLT = if_else(Y2D_AVG_FLIGHT == 0, 0, Y2D_AVG_DLY / Y2D_AVG_FLIGHT),
    Y2D_TO_DATE = ENTRY_DATE
  ) %>%
  right_join(list_acc, by = "ICAO_CODE") %>%
  mutate( 
    #canarias case
    iso_2letter = if_else(substr(ICAO_CODE,1,2) == "GC", "IC", ISO_2LETTER)
  ) %>% 
  left_join(state_iso, by = "iso_2letter") %>%
  group_by(iso_2letter) %>%
  arrange(iso_2letter, desc(Y2D_ACC_ER_DLY), Y2D_ACC_NAME) %>%
  mutate (
    ST_RANK = paste0(tolower(state), row_number()),
  ) %>%
  ungroup() %>%
  select(
    ST_RANK,
    Y2D_RANK,
    Y2D_ACC_NAME,
    Y2D_FROM_DATE,
    Y2D_TO_DATE,
    Y2D_ACC_ER_DLY,
    Y2D_ACC_ER_DLY_FLT
  )

#### main card ----
st_acc_main_delay <- acc_delay_day_sorted %>%
  mutate(
    MAIN_DLY_ACC_RANK = DY_RANK,
    MAIN_DLY_ACC_NAME = DY_ACC_NAME,
    MAIN_DLY_ACC_DLY = DY_ACC_ER_DLY
    ) %>%
  select(ST_RANK, MAIN_DLY_ACC_RANK, MAIN_DLY_ACC_NAME, MAIN_DLY_ACC_DLY)

st_acc_main_delay_flt <- acc_delay_day_sorted %>%
  group_by(iso_2letter) %>%
  arrange(iso_2letter, DY_RANK_ER_DLY_FLT, DY_ACC_NAME) %>%
  mutate (
    ST_RANK = paste0(tolower(state), row_number()),
  ) %>%
  ungroup() %>%
  mutate(
    MAIN_DLY_FLT_ACC_RANK = DY_RANK_ER_DLY_FLT,
    MAIN_DLY_FLT_ACC_NAME = DY_ACC_NAME,
    MAIN_DLY_FLT_ACC_DLY_FLT = DY_ACC_ER_DLY_FLT
  ) %>%
  select(ST_RANK, MAIN_DLY_FLT_ACC_RANK, MAIN_DLY_FLT_ACC_NAME, MAIN_DLY_FLT_ACC_DLY_FLT)


#### join tables ----
# create list of state/rankings for left join
state_iso_ranking <- list()
i = 0
for (i in 1:10) {
  i = i + 1
  state_iso_ranking <- state_iso_ranking %>%
    bind_rows(state_iso, .)
}

state_iso_ranking <- state_iso_ranking %>%
  arrange(state) %>%
  group_by(state) %>%
  mutate(
    RANK = row_number(),
    ST_RANK = paste0(tolower(state), RANK)
  )

# join and reorder tables
st_acc_delay <- state_iso_ranking %>%
  left_join(acc_delay_day, by = "ST_RANK") %>%
  left_join(acc_delay_week, by = "ST_RANK") %>%
  left_join(acc_delay_y2d, by = "ST_RANK") %>%
  left_join(st_acc_main_delay, by = "ST_RANK") %>%
  left_join(st_acc_main_delay_flt, by = "ST_RANK") %>%
  select(-ST_RANK)

# covert to json and save in app data folder and archive
st_acc_delay_j <- st_acc_delay %>% toJSON(., pretty = TRUE)
save_json(st_acc_delay_j, "st_acc_ranking_delay")
print(paste(format(now(), "%H:%M:%S"), "st_acc_ranking_delay"))


### Airport ----
if (!exists("nw_ap_traffic_delay_data")) {
  mydatafile <- paste0("ap_traffic_delay_day.parquet")
  stakeholder <- substr(mydatafile, 1,2)
  
  nw_ap_traffic_delay_data <- read_parquet(here(archive_dir_raw, stakeholder, mydatafile))
  }

st_ap_delay_raw <- nw_ap_traffic_delay_data %>% 
  left_join(list_airport_extended_iso, by = c("STK_CODE" = "EC_AP_CODE")) %>% 
  #canarias case
  mutate(
    AIU_ISO_CT_CODE = if_else(substr(STK_CODE,1,2) == "GC", "IC", AIU_ISO_CT_CODE)
  ) %>% 
  left_join(list_iso_country, by = c("AIU_ISO_CT_CODE" = "ISO_COUNTRY_CODE"))

#### day ----
st_ap_delay_day_int <- st_ap_delay_raw %>%
  select(
    COUNTRY_NAME,
    STK_NAME, 
    FLIGHT_DATE, 
    DAY_DLY, 
    DAY_DLY_FLT
  ) %>% 
  filter(FLIGHT_DATE == data_day_date) %>%
  mutate(
    RANK = rank(desc(DAY_DLY), ties.method = "max"),
    RANK_DLY_FLT = rank(desc(DAY_DLY_FLT), ties.method = "max")
  ) %>% 
  group_by(COUNTRY_NAME) %>% 
  arrange(COUNTRY_NAME, desc(DAY_DLY), STK_NAME) %>% 
  mutate(
    ST_RANK = paste0(tolower(COUNTRY_NAME), row_number()),
    DLY_PER_FLT = round(DAY_DLY_FLT, 2)
  ) %>%
  ungroup()

st_ap_delay_day <- st_ap_delay_day_int %>% 
  select(
    ST_RANK, 
    DY_RANK = RANK,
    DY_APT_NAME = STK_NAME, 
    DY_TO_DATE = FLIGHT_DATE, 
    DY_APT_ARR_DLY = DAY_DLY, 
    DY_APT_ARR_DLY_FLT = DLY_PER_FLT
    ) 

#### week ----
st_ap_delay_week <- st_ap_delay_raw %>%  
  select(
    COUNTRY_NAME,
    STK_NAME, 
    FLIGHT_DATE, 
    RWK_AVG_DLY, 
    RWK_DLY_FLT
  ) %>% 
    filter(FLIGHT_DATE == data_day_date) %>%
    mutate(
      RANK = rank(desc(RWK_AVG_DLY), ties.method = "max")
    ) %>% 
    group_by(COUNTRY_NAME) %>% 
    arrange(COUNTRY_NAME, desc(RWK_AVG_DLY), STK_NAME) %>% 
    mutate(
      ST_RANK = paste0(tolower(COUNTRY_NAME), row_number()),
      DLY_PER_FLT = round(RWK_DLY_FLT, 2),
      WK_FROM_DATE = FLIGHT_DATE - days(6)
    ) %>%
    ungroup() %>% 
    select(
      ST_RANK, 
      WK_RANK = RANK,
      WK_APT_NAME = STK_NAME, 
      WK_FROM_DATE,
      WK_TO_DATE = FLIGHT_DATE, 
      WK_APT_ARR_DLY = RWK_AVG_DLY, 
      WK_APT_ARR_DLY_FLT = DLY_PER_FLT
    ) 


#### y2d ----
st_ap_delay_y2d <- st_ap_delay_raw %>%
  select(
    COUNTRY_NAME,
    STK_NAME, 
    FLIGHT_DATE, 
    Y2D_AVG_DLY, 
    Y2D_DLY_FLT
  ) %>% 
  filter(FLIGHT_DATE == data_day_date) %>%
  mutate(
    RANK = rank(desc(Y2D_AVG_DLY), ties.method = "max")
  ) %>% 
  group_by(COUNTRY_NAME) %>% 
  arrange(COUNTRY_NAME, desc(Y2D_AVG_DLY), STK_NAME) %>% 
  mutate(
    ST_RANK = paste0(tolower(COUNTRY_NAME), row_number()),
    DLY_PER_FLT = round(Y2D_DLY_FLT, 2)
  ) %>%
  ungroup() %>% 
  select(
    ST_RANK, 
    Y2D_RANK = RANK,
    Y2D_APT_NAME = STK_NAME, 
    Y2D_TO_DATE = FLIGHT_DATE, 
    Y2D_APT_ARR_DLY = Y2D_AVG_DLY, 
    Y2D_APT_ARR_DLY_FLT = DLY_PER_FLT
  ) 

#### main card ----
st_ap_main_delay <- st_ap_delay_day_int %>%
  group_by(COUNTRY_NAME) %>% 
  filter(row_number() < 6) %>%
  ungroup() %>%
  select(
    ST_RANK,
    MAIN_DLY_APT_RANK = RANK,
    MAIN_DLY_APT_NAME = STK_NAME,
    MAIN_DLY_APT_DLY = DAY_DLY
  )%>% 
  arrange(ST_RANK)

st_ap_main_delay_flt <- st_ap_delay_day_int %>%
  group_by(COUNTRY_NAME) %>%
  arrange(COUNTRY_NAME, RANK_DLY_FLT, STK_NAME) %>%
  mutate (
    ST_RANK = paste0(tolower(COUNTRY_NAME), row_number()),
    DAY_DLY_FLT = round(DAY_DLY_FLT,2)
  ) %>%
  filter(row_number() < 6) %>%
  ungroup() %>%
  select(
    ST_RANK,
    MAIN_DLY_FLT_APT_RANK = RANK_DLY_FLT,
    MAIN_DLY_FLT_APT_NAME = STK_NAME,
    MAIN_DLY_FLT_APT_DLY_FLT = DAY_DLY_FLT
  ) %>% 
  arrange(ST_RANK)

#### join tables ----
# create list of state/rankings for left join
state_iso_ranking <- list()
i = 0
for (i in 1:10) {
  i = i + 1
  state_iso_ranking <- state_iso_ranking %>%
    bind_rows(state_iso, .)
}

state_iso_ranking <- state_iso_ranking %>%
  arrange(state) %>%
  group_by(state) %>%
  mutate(
    RANK = row_number(),
    ST_RANK = paste0(tolower(state), RANK)
  )

# join and reorder tables
st_ap_delay <- state_iso_ranking %>%
  left_join(st_ap_delay_day, by = "ST_RANK") %>%
  left_join(st_ap_delay_week, by = "ST_RANK") %>%
  left_join(st_ap_delay_y2d, by = "ST_RANK") %>%
  left_join(st_ap_main_delay, by = "ST_RANK") %>%
  left_join(st_ap_main_delay_flt, by = "ST_RANK") %>%
  select(-ST_RANK)

# covert to json and save in app data folder and archive
st_ap_delay_j <- st_ap_delay %>% toJSON(., pretty = TRUE)
save_json(st_ap_delay_j, "st_apt_ranking_delay")
print(paste(format(now(), "%H:%M:%S"), "st_apt_ranking_delay"))


## PUNTCUALITY ----
### Airport ----
# raw data
if(exists("st_apt_punct_raw") == FALSE) {
  st_apt_punct_raw <- get_punct_data_apt()
}

# calc
st_apt_punct_calc <- st_apt_punct_raw %>%
  mutate(ISO_COUNTRY_CODE = if_else(substr(ARP_CODE, 1,2) == 'GC',
                                    'IC',
                                    ISO_COUNTRY_CODE ),
         EC_ISO_CT_NAME = case_when (
           substr(ARP_CODE, 1,2) == 'GC' ~ 'Spain Canaries',
           substr(ARP_CODE, 1,2) == 'LE' ~ 'Spain Continental',
           .default = EC_ISO_CT_NAME )
         ) %>%
  # select(DAY_DATE, APT_NAME, DAY_ARR_PUNCT, RANK)
  group_by(ARP_NAME) %>%
  arrange(DAY_DATE) %>%
  mutate(
    DY_APT_ARR_PUNCT = ARR_PUNCTUALITY_PERCENTAGE / 100,
    DY_APT_ARR_PUNCT_DIF_PREV_WEEK = (DY_APT_ARR_PUNCT - lag(DY_APT_ARR_PUNCT, 7)),
    DY_APT_ARR_PUNCT_DIF_PREV_YEAR = (DY_APT_ARR_PUNCT - lag(DY_APT_ARR_PUNCT, 364)),
    WK_APT_ARR_PUNCT = rollsum(ARR_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") / rollsum(ARR_SCHEDULE_FLIGHT, 7,
                                                                                          fill = NA, align = "right"),
    iso_2letter = ISO_COUNTRY_CODE,
    state = EC_ISO_CT_NAME
  )  %>%
  ungroup()


#### day ----
st_apt_punct_dy_all <- st_apt_punct_calc %>%
  group_by(iso_2letter, DAY_DATE) %>%
  arrange(iso_2letter, desc(DY_APT_ARR_PUNCT), ARP_NAME) %>%
  mutate(
    ST_RANK = row_number(),
    ST_RANK = paste0(tolower(state), ST_RANK)     #index for joining tables later
    ) %>%
  ungroup() %>%
  group_by(ARP_NAME) %>%
  arrange(ARP_NAME, DAY_DATE) %>%
  mutate(
         # DY_RANK_DIF_PREV_WEEK = lag(RANK, 7) - RANK,          #not used anymore
         DY_APT_NAME = ARP_NAME,
         DY_TO_DATE = round_date(DAY_DATE, "day")
         ) %>%
  ungroup() %>%
  filter(DAY_DATE == last_day_punct) %>%
  mutate(DY_RANK = rank(desc(DY_APT_ARR_PUNCT), ties.method = "max")) %>%
  group_by(iso_2letter) %>%
  arrange(iso_2letter, desc(DY_APT_ARR_PUNCT), DY_APT_NAME) %>%
  ungroup()  %>%
  # iceland exception
  mutate(
    DY_APT_ARR_PUNCT_DIF_PREV_YEAR = if_else(iso_2letter == "IS" & year(DY_TO_DATE) < 2025, NA, DY_APT_ARR_PUNCT_DIF_PREV_YEAR)
  )


st_apt_punct_dy <- st_apt_punct_dy_all %>%
  select(
    ST_RANK,
    DY_RANK,
    DY_APT_NAME,
    DY_TO_DATE,
    DY_APT_ARR_PUNCT,
    DY_APT_ARR_PUNCT_DIF_PREV_WEEK,
    DY_APT_ARR_PUNCT_DIF_PREV_YEAR
  )

#### week ----
st_apt_punct_wk <- st_apt_punct_calc %>%
  group_by(iso_2letter, DAY_DATE) %>%
  arrange(iso_2letter, desc(WK_APT_ARR_PUNCT), ARP_NAME) %>%
  mutate(
    ST_RANK = row_number(),
    ST_RANK = paste0(tolower(state), ST_RANK)     #index for joining tables later
  ) %>%
  ungroup() %>%
  group_by(ARP_NAME) %>%
  arrange(ARP_NAME, DAY_DATE) %>%
  mutate(
    # WK_RANK_DIF_PREV_WEEK = lag(RANK, 7) - RANK,            #not used anymore
    WK_APT_NAME = ARP_NAME,
    WK_TO_DATE = round_date(DAY_DATE, "day"),
    WK_FROM_DATE = round_date(DAY_DATE, "day") + days(-7),
    WK_APT_ARR_PUNCT_DIF_PREV_WEEK = (WK_APT_ARR_PUNCT - lag(WK_APT_ARR_PUNCT, 7)),
    WK_APT_ARR_PUNCT_DIF_PREV_YEAR = (WK_APT_ARR_PUNCT - lag(WK_APT_ARR_PUNCT, 364))
  ) %>%
  ungroup() %>%
  filter(DAY_DATE == last_day_punct) %>%
  mutate(WK_RANK = rank(desc(WK_APT_ARR_PUNCT), ties.method = "max")) %>%
  group_by(iso_2letter) %>%
  arrange(iso_2letter, desc(WK_APT_ARR_PUNCT), WK_APT_NAME) %>%
  ungroup() %>%
  # iceland exception
  mutate(
    WK_APT_ARR_PUNCT_DIF_PREV_YEAR = if_else(iso_2letter == "IS" & year(WK_TO_DATE) < 2025, NA, WK_APT_ARR_PUNCT_DIF_PREV_YEAR)
  ) %>%
  select(
    ST_RANK,
    WK_RANK,
    WK_APT_NAME,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_APT_ARR_PUNCT,
    WK_APT_ARR_PUNCT_DIF_PREV_WEEK,
    WK_APT_ARR_PUNCT_DIF_PREV_YEAR
  )

#### y2d ----
st_apt_punct_y2d <- st_apt_punct_calc %>%
  mutate(MONTH_DAY = as.numeric(format(DAY_DATE, format = "%m%d"))) %>%
  filter(MONTH_DAY <= as.numeric(format(last_day_punct, format = "%m%d"))) %>%
  mutate(YEAR = as.numeric(format(DAY_DATE, format="%Y"))) %>%
  group_by(state, ARP_NAME, ARP_CODE, YEAR) %>%
  summarise (
    Y2D_APT_ARR_PUNCT = sum(ARR_PUNCTUAL_FLIGHTS, na.rm=TRUE) / sum(ARR_SCHEDULE_FLIGHT, na.rm=TRUE),
    .groups = "drop"
  ) %>%
  group_by(state, YEAR) %>%
  arrange(state, YEAR, desc(Y2D_APT_ARR_PUNCT), ARP_NAME) %>%
  mutate(
    ST_RANK = row_number(),
    ST_RANK = paste0(tolower(state), ST_RANK)     #index for joining tables later
    ) %>%
  ungroup() %>%
  group_by(ARP_NAME) %>%
  arrange(ARP_NAME, YEAR) %>%
  mutate(
    Y2D_APT_ARR_PUNCT_DIF_PREV_YEAR = (Y2D_APT_ARR_PUNCT - lag(Y2D_APT_ARR_PUNCT, 1))
    , Y2D_APT_ARR_PUNCT_DIF_2019 = (Y2D_APT_ARR_PUNCT - lag(Y2D_APT_ARR_PUNCT, max(YEAR) - 2019))
  )  %>%
  ungroup() %>%
  filter(YEAR == max(YEAR)) %>%
  mutate(Y2D_RANK = rank(desc(Y2D_APT_ARR_PUNCT), ties.method = "max")) %>%
  mutate(Y2D_APT_NAME = ARP_NAME) %>%
  group_by(state) %>%
  arrange(state, Y2D_RANK, Y2D_APT_NAME) %>%
  mutate(
    NO_APTS = row_number(),
    Y2D_TO_DATE = lubridate::round_date(last_day_punct, unit = 'day')
    ) %>%
  filter(NO_APTS < 11) %>%
  ungroup() %>%
  # iceland exception
  mutate(
    Y2D_APT_ARR_PUNCT_DIF_PREV_YEAR = if_else(state == "Iceland" & year(Y2D_TO_DATE) < 2025, NA, Y2D_APT_ARR_PUNCT_DIF_PREV_YEAR),
    Y2D_APT_ARR_PUNCT_DIF_2019 = if_else(state == "Iceland", NA, Y2D_APT_ARR_PUNCT_DIF_2019)
  ) %>%
  select(
    ST_RANK,
    Y2D_RANK,
    Y2D_APT_NAME,
    Y2D_TO_DATE,
    Y2D_APT_ARR_PUNCT,
    Y2D_APT_ARR_PUNCT_DIF_PREV_YEAR,
    Y2D_APT_ARR_PUNCT_DIF_2019
  )

#### main card ----
st_apt_main_punct <- st_apt_punct_dy %>%
  mutate(
    MAIN_PUNCT_APT_RANK = DY_RANK,
    MAIN_PUNCT_APT_NAME = DY_APT_NAME,
    MAIN_PUNCT_APT_ARR_PUNCT = DY_APT_ARR_PUNCT
  ) %>%
  select(ST_RANK, MAIN_PUNCT_APT_RANK, MAIN_PUNCT_APT_NAME, MAIN_PUNCT_APT_ARR_PUNCT)

st_apt_main_punct_dif <- st_apt_punct_dy_all %>%
  mutate(
    MAIN_PUNCT_DIF_APT_RANK = rank(desc(DY_APT_ARR_PUNCT_DIF_PREV_WEEK), ties.method = "max"),
    MAIN_PUNCT_DIF_APT_NAME = DY_APT_NAME,
    MAIN_PUNCT_DIF_APT_ARR_PUNCT_DIF = DY_APT_ARR_PUNCT_DIF_PREV_WEEK
  ) %>%
  group_by(iso_2letter) %>%
  arrange(iso_2letter, MAIN_PUNCT_DIF_APT_RANK, MAIN_PUNCT_DIF_APT_NAME) %>%
  mutate (
    ST_RANK = paste0(tolower(state), row_number()),
  ) %>%
  ungroup() %>%
  select(ST_RANK, MAIN_PUNCT_DIF_APT_RANK, MAIN_PUNCT_DIF_APT_NAME, MAIN_PUNCT_DIF_APT_ARR_PUNCT_DIF)


#### join tables ----
# create list of state/rankings for left join
state_iso_ranking <- list()
i = 0
for (i in 1:10) {
  i = i + 1
  state_iso_ranking <- state_iso_ranking %>%
    bind_rows(state_iso, .)
}

state_iso_ranking <- state_iso_ranking %>%
  arrange(state) %>%
  group_by(state) %>%
  mutate(
    RANK = row_number(),
    ST_RANK = paste0(tolower(state), RANK)
  )

# join and reorder tables
st_apt_punctuality <- state_iso_ranking %>%
  left_join(st_apt_punct_dy, by = "ST_RANK") %>%
  left_join(st_apt_punct_wk, by = "ST_RANK") %>%
  left_join(st_apt_punct_y2d, by = "ST_RANK") %>%
  left_join(st_apt_main_punct, by = "ST_RANK") %>%
  left_join(st_apt_main_punct_dif, by = "ST_RANK") %>%
  select(-ST_RANK)

# convert to json and save in app data folder and archive
st_apt_punctuality_j <- st_apt_punctuality %>% toJSON(., pretty = TRUE)

save_json(st_apt_punctuality_j, "st_apt_ranking_punctuality")
print(paste(format(now(), "%H:%M:%S"), "st_apt_ranking_punctuality"))


# ____________________________________________________________________________________________
#
#    State graphs  -----
#
# ____________________________________________________________________________________________

## TRAFFIC ----
### 7-day DAIO avg ----
st_daio_evo_app <- st_daio_delay_data %>%
  mutate(WK_AVG_TFC = if_else(FLIGHT_DATE > min(data_day_date,
                                                max(DATA_DAY, na.rm = TRUE),na.rm = TRUE), NA, WK_AVG_TFC)
  ) %>%
  right_join(rel_iso_country_daio_zone, by = "daio_zone_lc", relationship = "many-to-many") %>% 
  select(
    iso_2letter,
    daio_zone,
    FLIGHT_DATE,
    WK_AVG_TFC,
    WK_AVG_TFC_PREV_YEAR,
    WK_AVG_TFC_2020,
    WK_AVG_TFC_2019
  ) %>% 
  arrange(iso_2letter, FLIGHT_DATE)

column_names <- c('iso_2letter', 'daio_zone', 'FLIGHT_DATE', data_day_year, data_day_year-1, 2020, 2019)
colnames(st_daio_evo_app) <- column_names


### nest data
st_daio_evo_app_long <- st_daio_evo_app %>%
  pivot_longer(-c(iso_2letter, daio_zone, FLIGHT_DATE), names_to = 'year', values_to = 'daio') %>%
  group_by(iso_2letter, daio_zone, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

st_daio_evo_app_j <- st_daio_evo_app_long %>% toJSON(., pretty = TRUE)

save_json(st_daio_evo_app_j, "st_daio_evo_chart_daily")
print(paste(format(now(), "%H:%M:%S"), "st_daio_evo_chart_daily"))


### 7-day DAI avg ----
st_dai_evo_app <- st_dai_data %>%
  mutate(WK_AVG_TFC = if_else(FLIGHT_DATE > min(data_day_date,
                                                max(DATA_DAY, na.rm = TRUE),na.rm = TRUE), NA, WK_AVG_TFC)
  ) %>%
  right_join(rel_iso_country_daio_zone, by = "daio_zone_lc", relationship = "many-to-many") %>% 
  select(
    iso_2letter,
    daio_zone,
    FLIGHT_DATE,
    WK_AVG_TFC,
    WK_AVG_TFC_PREV_YEAR,
    WK_AVG_TFC_2020,
    WK_AVG_TFC_2019
  ) %>% 
  arrange(iso_2letter, FLIGHT_DATE)

column_names <- c('iso_2letter', 'daio_zone', 'FLIGHT_DATE', data_day_year, data_day_year-1, 2020, 2019)
colnames(st_dai_evo_app) <- column_names


### nest data
st_dai_evo_app_long <- st_dai_evo_app %>%
  pivot_longer(-c(iso_2letter, daio_zone, FLIGHT_DATE), names_to = 'year', values_to = 'dai') %>%
  group_by(iso_2letter, daio_zone, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

st_dai_evo_app_j <- st_dai_evo_app_long %>% toJSON(., pretty = TRUE)

save_json(st_dai_evo_app_j, "st_dai_evo_chart_daily")
print(paste(format(now(), "%H:%M:%S"), "st_dai_evo_chart_daily"))


### 7-day OVF avg ----
st_ovf_evo_app <- st_overflight_data %>%
  mutate(WK_AVG_OVF = if_else(FLIGHT_DATE > min(data_day_date,
                                                max(LAST_DATA_DAY, na.rm = TRUE),na.rm = TRUE), NA, WK_AVG_OVF)
  ) %>%
  right_join(rel_iso_country_daio_zone, by = "daio_zone_lc", relationship = "many-to-many") %>% 
  select(
    iso_2letter,
    daio_zone,
    FLIGHT_DATE,
    WK_AVG_OVF,
    WK_AVG_OVF_PREV_YEAR,
    WK_AVG_OVF_2020,
    WK_AVG_OVF_2019
  ) %>% 
  arrange(iso_2letter, FLIGHT_DATE)

column_names <- c('iso_2letter', 'daio_zone', 'FLIGHT_DATE', data_day_year, data_day_year-1, 2020, 2019)
colnames(st_ovf_evo_app) <- column_names

### nest data
st_ovf_evo_app_long <- st_ovf_evo_app %>%
  pivot_longer(-c(iso_2letter, daio_zone, FLIGHT_DATE), names_to = 'year', values_to = 'ovf') %>%
  group_by(iso_2letter, daio_zone, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

st_ovf_evo_app_j <- st_ovf_evo_app_long %>% toJSON(., pretty = TRUE)

save_json(st_ovf_evo_app_j, "st_ovf_evo_chart_daily")
print(paste(format(now(), "%H:%M:%S"), "st_ovf_evo_chart_daily"))


## PUNCTUALITY ----
### 7-day punctuality avg ----
st_punct_evo_app <- st_punct_data_joined %>%
  filter(DAY_DATE >= as.Date(paste0("01-01-", data_day_year-2), format = "%d-%m-%Y")) %>%
  arrange(ISO_2LETTER, DAY_DATE) %>%
  mutate(
    DEP_PUN_WK = rollsum(DEP_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") /
           rollsum(DEP_SCHEDULE_FLIGHT,7, fill = NA, align = "right") * 100,
    ARR_PUN_WK = rollsum(ARR_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right") /
           rollsum(ARR_SCHEDULE_FLIGHT,7, fill = NA, align = "right") * 100,
    OP_FLT_WK = 100 - rollsum(MISSING_SCHED_FLIGHTS, 7, fill = NA, align = "right") /
           rollsum((MISSING_SCHED_FLIGHTS+DEP_FLIGHTS_NO_OVERFLIGHTS),7, fill = NA, align = "right")*100
         ) %>%
  filter(DAY_DATE >= as.Date(paste0("01-01-", data_day_year-1), format = "%d-%m-%Y"),
         DAY_DATE <= last_day_punct) %>%
  mutate(iso_2letter = ISO_2LETTER) %>%
  right_join(state_iso, by ="iso_2letter") %>%
  select(
    iso_2letter,
    state,
    DAY_DATE,
    DEP_PUN_WK,
    ARR_PUN_WK,
    OP_FLT_WK
  ) %>%   # iceland exception
  mutate(
    DEP_PUN_WK = if_else(iso_2letter == "IS" & year(DAY_DATE) < 2024, NA, DEP_PUN_WK),
    ARR_PUN_WK = if_else(iso_2letter == "IS" & year(DAY_DATE) < 2024, NA, ARR_PUN_WK),
    OP_FLT_WK = if_else(iso_2letter == "IS" & year(DAY_DATE) < 2024, NA, OP_FLT_WK)
  )


column_names <- c('iso_2letter',
                  'state',
                  'FLIGHT_DATE',
                  "Departure punct.",
                  "Arrival punct.",
                  "Operated schedules")

colnames(st_punct_evo_app) <- column_names

### nest data
st_punct_evo_app_long <- st_punct_evo_app %>%
  pivot_longer(-c(iso_2letter, state, FLIGHT_DATE), names_to = 'metric', values_to = 'value') %>%
  group_by(iso_2letter, state, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

st_punct_evo_app_j <- st_punct_evo_app_long %>% toJSON(., pretty = TRUE)

save_json(st_punct_evo_app_j, "st_punct_evo_chart")
print(paste(format(now(), "%H:%M:%S"), "st_punct_evo_chart"))


## DELAY ----
st_daio_delay_data_iso <- st_daio_delay_data %>% 
  mutate(
    DY_SHARE_DLY_ERT_FLT = if_else(DY_DLY_FLT == 0, 0, DY_DLY_ERT_FLT/DY_DLY_FLT),
    DY_SHARE_DLY_ARP_FLT = if_else(DY_DLY_FLT == 0, 0, DY_DLY_ARP_FLT/DY_DLY_FLT),
    
    WK_SHARE_DLY_ERT_FLT = if_else(WK_DLY_FLT == 0, 0, WK_DLY_ERT_FLT/WK_DLY_FLT),
    WK_SHARE_DLY_ARP_FLT = if_else(WK_DLY_FLT == 0, 0, WK_DLY_ARP_FLT/WK_DLY_FLT),
    
    Y2D_SHARE_DLY_ERT_FLT = if_else(Y2D_DLY_FLT == 0, 0, Y2D_DLY_ERT_FLT/Y2D_DLY_FLT),
    Y2D_SHARE_DLY_ARP_FLT = if_else(Y2D_DLY_FLT == 0, 0, Y2D_DLY_ARP_FLT/Y2D_DLY_FLT),
    
    DY_SHARE_DLY_G = if_else(DY_DLY == 0, 0, DY_DLY_G / DY_DLY),
    DY_SHARE_DLY_CS = if_else(DY_DLY == 0, 0, DY_DLY_CS / DY_DLY),
    DY_SHARE_DLY_IT = if_else(DY_DLY == 0, 0, DY_DLY_IT / DY_DLY),
    DY_SHARE_DLY_WD = if_else(DY_DLY == 0, 0, DY_DLY_WD / DY_DLY),
    DY_SHARE_DLY_OTHER = if_else(DY_DLY == 0, 0, DY_DLY_OTHER / DY_DLY)
    
  ) %>% 
  right_join(rel_iso_country_daio_zone, by = "daio_zone_lc", relationship = "many-to-many") %>%
  arrange(iso_2letter, FLIGHT_DATE)

### Delay category ----
#### day ----
st_delay_cause_day <- st_daio_delay_data_iso %>%
  filter(FLIGHT_DATE == min(max(FLIGHT_DATE),
                            data_day_date,
                            na.rm = TRUE)
  )%>%
  select(iso_2letter,
         daio_zone,
         FLIGHT_DATE,
         DY_DLY_G,
         DY_DLY_CS,
         DY_DLY_IT,
         DY_DLY_WD,
         DY_DLY_OTHER,
         DY_DLY_PREV_YEAR,
         DY_SHARE_DLY_G,
         DY_SHARE_DLY_CS,
         DY_SHARE_DLY_IT,
         DY_SHARE_DLY_WD,
         DY_SHARE_DLY_OTHER
  ) %>%
  arrange(iso_2letter)

column_names <- c(
  "iso_2letter",
  "daio_zone",
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

colnames(st_delay_cause_day) <- column_names

### nest data
st_delay_value_day_long <- st_delay_cause_day %>%
  select(-c(share_aerodrome_capacity,
            share_capacity_staffing_atc,
            share_disruptions_atc,
            share_weather,
            share_other)
  ) %>%
  pivot_longer(-c(iso_2letter, daio_zone, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

st_delay_share_day_long <- st_delay_cause_day %>%
  select(-c("Aerodrome capacity",
            "Capacity/Staffing (ATC)",
            "Disruptions (ATC)",
            "Weather",
            "Other",
            paste0("Total delay ", data_day_year - 1)
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(iso_2letter, daio_zone, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

st_delay_cause_day_long <- cbind(st_delay_value_day_long, st_delay_share_day_long) %>%
  select(-name) %>%
  group_by(iso_2letter, daio_zone, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

# for consistency with v1 we use the word category in the name files... should have been cause
st_delay_cause_evo_dy_j <- st_delay_cause_day_long %>% toJSON(., pretty = TRUE)

save_json(st_delay_cause_evo_dy_j, "st_delay_category_evo_chart_dy")
print(paste(format(now(), "%H:%M:%S"), "st_delay_category_evo_chart_dy"))


#### week ----
st_delay_cause_wk <- st_daio_delay_data_iso %>%
  filter(FLIGHT_DATE >= min(max(FLIGHT_DATE), data_day_date, na.rm  = TRUE) + lubridate::days(-6),
         FLIGHT_DATE <= min(max(FLIGHT_DATE), data_day_date, na.rm  = TRUE)
  ) %>%
  group_by(iso_2letter) %>% 
  mutate(
    WK_SHARE_DLY_G = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_G) / sum(DY_DLY)),
    WK_SHARE_DLY_CS = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_CS) / sum(DY_DLY)),
    WK_SHARE_DLY_IT = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_IT) / sum(DY_DLY)),
    WK_SHARE_DLY_WD = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_WD) / sum(DY_DLY)),
    WK_SHARE_DLY_OTHER = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_OTHER) / sum(DY_DLY))
  ) %>%
  ungroup() %>% 
  select(
    iso_2letter,
    daio_zone,
    FLIGHT_DATE,
    DY_DLY_G,
    DY_DLY_CS,
    DY_DLY_IT,
    DY_DLY_WD,
    DY_DLY_OTHER,
    DY_DLY_PREV_YEAR,
    WK_SHARE_DLY_G,
    WK_SHARE_DLY_CS,
    WK_SHARE_DLY_IT,
    WK_SHARE_DLY_WD,
    WK_SHARE_DLY_OTHER
  ) %>% 
  arrange(iso_2letter)

colnames(st_delay_cause_wk) <- column_names

### nest data
st_delay_value_wk_long <- st_delay_cause_wk %>%
  select(-c(share_aerodrome_capacity,
            share_capacity_staffing_atc,
            share_disruptions_atc,
            share_weather,
            share_other)
  ) %>%
  pivot_longer(-c(iso_2letter, daio_zone, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

st_delay_share_wk_long <- st_delay_cause_wk %>%
  select(-c("Aerodrome capacity",
            "Capacity/Staffing (ATC)",
            "Disruptions (ATC)",
            "Weather",
            "Other",
            paste0("Total delay ", data_day_year - 1)
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(iso_2letter, daio_zone, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

st_delay_cause_wk_long <- cbind(st_delay_value_wk_long, st_delay_share_wk_long) %>%
  select(-name) %>%
group_by(iso_2letter, daio_zone, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")


# for consistency with v1 we use the word category in the name files... should have been cause
st_delay_cause_evo_wk_j <- st_delay_cause_wk_long %>% toJSON(., pretty = TRUE)

save_json(st_delay_cause_evo_wk_j, "st_delay_category_evo_chart_wk")
print(paste(format(now(), "%H:%M:%S"), "st_delay_category_evo_chart_wk"))


#### y2d ----
st_delay_cause_y2d <- st_daio_delay_data_iso %>%
  filter(FLIGHT_DATE <= data_day_date & YEAR == data_day_year) %>%
  group_by(iso_2letter) %>% 
  mutate(
    SHARE_TDM_G = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_G) / sum(DY_DLY)),
    SHARE_TDM_CS = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_CS) / sum(DY_DLY)),
    SHARE_TDM_IT = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_IT) / sum(DY_DLY)),
    SHARE_TDM_WD = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_WD) / sum(DY_DLY)),
    SHARE_TDM_NOCSGITWD = if_else(sum(DY_DLY) == 0, 0, sum(DY_DLY_OTHER) / sum(DY_DLY))
  ) %>%
  ungroup() %>% 
  select(
    iso_2letter,
    daio_zone,
    FLIGHT_DATE,
         WK_AVG_DLY_G,
         WK_AVG_DLY_CS,
         WK_AVG_DLY_IT,
         WK_AVG_DLY_WD,
         WK_AVG_DLY_OTHER,
         WK_AVG_DLY_PREV_YEAR,
         SHARE_TDM_G,
         SHARE_TDM_CS,
         SHARE_TDM_IT,
         SHARE_TDM_WD,
         SHARE_TDM_NOCSGITWD
  ) %>% 
arrange(iso_2letter)

colnames(st_delay_cause_y2d) <- column_names


### nest data
st_delay_value_y2d_long <- st_delay_cause_y2d %>%
  select(-c(share_aerodrome_capacity,
            share_capacity_staffing_atc,
            share_disruptions_atc,
            share_weather,
            share_other)
         ) %>%
  pivot_longer(-c(iso_2letter, daio_zone, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

st_delay_share_y2d_long <- st_delay_cause_y2d %>%
   select(-c("Aerodrome capacity",
            "Capacity/Staffing (ATC)",
            "Disruptions (ATC)",
            "Weather",
            "Other",
            paste0("Total delay ", data_day_year - 1)
            )
  )  %>%
   mutate(share_delay_prev_year = NA) %>%
   pivot_longer(-c(iso_2letter, daio_zone, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
   select(name, share)

 st_delay_cause_y2d_long <- cbind(st_delay_value_y2d_long, st_delay_share_y2d_long) %>%
   select(-name) %>%
   group_by(iso_2letter, daio_zone, FLIGHT_DATE) %>%
   nest_legacy(.key = "statistics")


# for consistency with v1 we use the word category in the name files... should have been cause
st_delay_cause_evo_y2d_j <- st_delay_cause_y2d_long %>% toJSON(., pretty = TRUE)

save_json(st_delay_cause_evo_y2d_j, "st_delay_category_evo_chart_y2d")
print(paste(format(now(), "%H:%M:%S"), "st_delay_category_evo_chart_y2d"))

### Delay type ----
#### day ----
st_delay_type_day <- st_daio_delay_data_iso %>% 
  filter(FLIGHT_DATE == min(max(FLIGHT_DATE),
                            data_day_date,
                            na.rm = TRUE)) %>%
  select(iso_2letter,
         daio_zone,
         FLIGHT_DATE,
         DY_DLY_ERT_FLT,
         DY_DLY_ARP_FLT,
         DY_DLY_FLT_PREV_YEAR,
         DY_SHARE_DLY_ERT_FLT,
         DY_SHARE_DLY_ARP_FLT
  )   %>%
  arrange(iso_2letter, FLIGHT_DATE)

column_names <- c(
  "iso_2letter",
  "daio_zone",
  "FLIGHT_DATE",
  "En-route ATFM delay/flight",
  "Airport ATFM delay/flight",
  paste0("Total ATFM delay/flight ", data_day_year - 1),
  "share_en_route",
  "share_airport"
)

colnames(st_delay_type_day) <- column_names

### nest data
st_delay_type_value_day_long <- st_delay_type_day %>%
  select(-c(share_en_route,
            share_airport)
  ) %>%
  pivot_longer(-c(iso_2letter, daio_zone, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

st_delay_type_share_day_long <- st_delay_type_day %>%
  select(-c("En-route ATFM delay/flight",
            "Airport ATFM delay/flight",
            paste0("Total ATFM delay/flight ", data_day_year - 1)
            )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(iso_2letter, daio_zone, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

st_delay_type_day_long <- cbind(st_delay_type_value_day_long, st_delay_type_share_day_long) %>%
  select(-name) %>%
  group_by(iso_2letter, daio_zone, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

st_delay_type_evo_dy_j <- st_delay_type_day_long %>% toJSON(., pretty = TRUE)

save_json(st_delay_type_evo_dy_j, "st_delay_flt_type_evo_chart_dy")
print(paste(format(now(), "%H:%M:%S"), "st_delay_flt_type_evo_chart_dy"))


#### week ----
st_delay_type_wk <- st_daio_delay_data_iso %>% 
  filter(FLIGHT_DATE >= min(max(FLIGHT_DATE), data_day_date, na.rm  = TRUE) + lubridate::days(-6),
         FLIGHT_DATE <= min(max(FLIGHT_DATE), data_day_date, na.rm  = TRUE)
  ) %>%
  select(iso_2letter,
         daio_zone,
         FLIGHT_DATE,
         DY_DLY_ERT_FLT,
         DY_DLY_ARP_FLT,
         DY_DLY_FLT_PREV_YEAR,
         
         WK_SHARE_DLY_ERT_FLT,
         WK_SHARE_DLY_ARP_FLT
  ) %>% 
  group_by(iso_2letter, daio_zone) %>%
  mutate(
    across(
      c(WK_SHARE_DLY_ERT_FLT, WK_SHARE_DLY_ARP_FLT),
      ~ .[FLIGHT_DATE == max(FLIGHT_DATE, na.rm = TRUE)][1]
    )
  ) %>%
  ungroup()

colnames(st_delay_type_wk) <- column_names

### nest data
st_delay_type_value_wk_long <- st_delay_type_wk %>%
  select(-c(share_en_route,
            share_airport)
  ) %>%
  pivot_longer(-c(iso_2letter, daio_zone, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

st_delay_type_share_wk_long <- st_delay_type_wk %>%
  select(-c("En-route ATFM delay/flight",
            "Airport ATFM delay/flight",
            paste0("Total ATFM delay/flight ", data_day_year - 1)
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(iso_2letter, daio_zone, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

st_delay_type_wk_long <- cbind(st_delay_type_value_wk_long, st_delay_type_share_wk_long) %>%
  select(-name) %>%
  group_by(iso_2letter, daio_zone, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

st_delay_type_evo_wk_j <- st_delay_type_wk_long %>% toJSON(., pretty = TRUE)

save_json(st_delay_type_evo_wk_j, "st_delay_flt_type_evo_chart_wk")
print(paste(format(now(), "%H:%M:%S"), "st_delay_flt_type_evo_chart_wk"))

#### y2d ----
st_delay_type_y2d <- st_daio_delay_data_iso %>% 
  filter(FLIGHT_DATE <= data_day_date) %>%
  select(iso_2letter,
         daio_zone,
         FLIGHT_DATE,
         WK_DLY_ERT_FLT,
         WK_DLY_ARP_FLT,
         WK_DLY_FLT_PREV_YEAR,
         
         Y2D_SHARE_DLY_ERT_FLT,
         Y2D_SHARE_DLY_ARP_FLT
  ) %>% 
  group_by(iso_2letter, daio_zone) %>%
  mutate(
    across(
      c(Y2D_SHARE_DLY_ERT_FLT, Y2D_SHARE_DLY_ARP_FLT),
      ~ .[FLIGHT_DATE == max(FLIGHT_DATE, na.rm = TRUE)][1]
    )
  ) %>%
  ungroup()

colnames(st_delay_type_y2d) <- column_names

### nest data
st_delay_type_value_y2d_long <- st_delay_type_y2d %>%
  select(-c(share_en_route,
            share_airport)
  ) %>%
  pivot_longer(-c(iso_2letter, daio_zone, FLIGHT_DATE), names_to = 'metric', values_to = 'value')

st_delay_type_share_y2d_long <- st_delay_type_y2d %>%
  select(-c("En-route ATFM delay/flight",
            "Airport ATFM delay/flight",
            paste0("Total ATFM delay/flight ", data_day_year - 1)
  )
  )  %>%
  mutate(share_delay_prev_year = NA) %>%
  pivot_longer(-c(iso_2letter, daio_zone, FLIGHT_DATE), names_to = 'name', values_to = 'share') %>%
  select(name, share)

st_delay_type_y2d_long <- cbind(st_delay_type_value_y2d_long, st_delay_type_share_y2d_long) %>%
  select(-name) %>%
  group_by(iso_2letter, daio_zone, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")

st_delay_type_evo_y2d_j <- st_delay_type_y2d_long %>% toJSON(., pretty = TRUE)

save_json(st_delay_type_evo_y2d_j, "st_delay_flt_type_evo_chart_y2d")
print(paste(format(now(), "%H:%M:%S"), "st_delay_flt_type_evo_chart_y2d"))

### Delay breakdown----
#### En-route ----
st_delay_ERT_flt_evo <- st_daio_delay_data_iso %>% 
  filter(FLIGHT_DATE <= data_day_date) %>%
  select(
    iso_2letter,
    daio_zone,
    FLIGHT_DATE,
    WK_DLY_ERT_FLT,
    WK_DLY_ERT_FLT_PREV_YEAR
  ) 

y2d_delay_ERT_flt <- st_daio_delay_data_iso %>%
  filter(FLIGHT_DATE == min(data_day_date,
                            max(DATA_DAY, na.rm = TRUE),
                            na.rm = TRUE)
  ) %>% 
  select(daio_zone,
         Y2D_DLY_ERT_FLT,
         Y2D_DLY_ERT_FLT_PREV_YEAR
  ) 


column_names <- c(
  "iso_2letter",
  "daio_zone",
  "FLIGHT_DATE",
  paste0("En-route ATFM delay/flight ", data_day_year),
  paste0("En-route ATFM delay/flight ", data_day_year - 1)
)


colnames(st_delay_ERT_flt_evo) <- column_names

### nest data
st_delay_ERT_flt_evo_long <- st_delay_ERT_flt_evo %>%
  pivot_longer(-c(iso_2letter, daio_zone, FLIGHT_DATE), names_to = 'year', values_to = 'daio') %>%
  left_join(y2d_delay_ERT_flt, by = "daio_zone") %>%
  mutate(
    year = if_else(str_detect(year, as.character(data_day_year)),
                   paste0(year, " (", format(round(Y2D_DLY_ERT_FLT,2), nsmall=2),"')"),
                   paste0(year, " (", format(round(Y2D_DLY_ERT_FLT_PREV_YEAR,2), nsmall=2),"')"))
  ) %>%
  select(-Y2D_DLY_ERT_FLT, -Y2D_DLY_ERT_FLT_PREV_YEAR) %>%
  group_by(iso_2letter, daio_zone, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")


st_delay_ERT_flt_evo_long_j <- st_delay_ERT_flt_evo_long %>% toJSON(., pretty = TRUE)

save_json(st_delay_ERT_flt_evo_long_j, "st_delay_ert_per_flight_evo_chart")
print(paste(format(now(), "%H:%M:%S"), "st_delay_ert_per_flight_evo_chart"))


#### Airport ----
st_delay_APT_flt_evo <- st_daio_delay_data_iso %>% 
  filter(FLIGHT_DATE <= data_day_date) %>%
  select(
    iso_2letter,
    daio_zone,
    FLIGHT_DATE,
    WK_DLY_ARP_FLT,
    WK_DLY_ARP_FLT_PREV_YEAR
  ) 

y2d_delay_APT_flt <- st_daio_delay_data_iso %>%
  filter(FLIGHT_DATE == min(data_day_date,
                            max(DATA_DAY, na.rm = TRUE),
                            na.rm = TRUE)
  ) %>% 
  select(daio_zone,
         Y2D_DLY_APT_FLT = Y2D_DLY_ARP_FLT,
         Y2D_DLY_APT_FLT_PREV_YEAR = Y2D_DLY_ARP_FLT_PREV_YEAR
  ) 

column_names <- c(
  "iso_2letter",
  "daio_zone",
  "FLIGHT_DATE",
  paste0("En-route ATFM delay/flight ", data_day_year),
  paste0("En-route ATFM delay/flight ", data_day_year - 1)
)


colnames(st_delay_APT_flt_evo) <- column_names

### nest data
st_delay_APT_flt_evo_long <- st_delay_APT_flt_evo %>%
  pivot_longer(-c(iso_2letter, daio_zone, FLIGHT_DATE), names_to = 'year', values_to = 'daio') %>%
  left_join(y2d_delay_APT_flt, by = "daio_zone") %>%
  mutate(
    year = if_else(str_detect(year, as.character(data_day_year)),
                   paste0(year, " (", format(round(Y2D_DLY_APT_FLT,2), nsmall=2),"')"),
                   paste0(year, " (", format(round(Y2D_DLY_APT_FLT_PREV_YEAR,2), nsmall=2),"')"))
  ) %>%
  select(-Y2D_DLY_APT_FLT, -Y2D_DLY_APT_FLT_PREV_YEAR) %>%
  group_by(iso_2letter, daio_zone, FLIGHT_DATE) %>%
  nest_legacy(.key = "statistics")




st_delay_APT_flt_evo_long_j <- st_delay_APT_flt_evo_long %>% toJSON(., pretty = TRUE)

save_json(st_delay_APT_flt_evo_long_j, "st_delay_apt_per_flight_evo_chart")
print(paste(format(now(), "%H:%M:%S"), "st_delay_apt_per_flight_evo_chart"))

## BILLING ----
st_billing_evo <- st_billing %>%
  arrange(iso_2letter, year, month) %>%
  mutate(
    total_billing = total_billing/10^6,
    total_billing_py = lag(total_billing, 12),
    total_billing_dif_mm_perc = total_billing / total_billing_py -1
  ) %>%
  group_by(iso_2letter, corrected_cz, year) %>%
  mutate(
    total_billing_y2d = cumsum(total_billing)
  ) %>%
  ungroup() %>%
  mutate(
    total_billing_y2d_py = lag(total_billing_y2d, 12),
    total_billing_dif_y2d_perc = total_billing_y2d / total_billing_y2d_py -1
  ) %>%
  filter(year == last_billing_year,
         month <= last_billing_month
         ) %>%
  select(
    iso_2letter,
    cz_proper,
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
  'iso_2letter',
  'charging_zone',
  "month",
  last_billing_year,
  last_billing_year - 1,
  paste0("Monthly variation vs ", last_billing_year - 1),
  paste0 ("Year-to-date variation vs ", last_billing_year - 1),
  "min_right_axis",
  "max_right_axis"
)

colnames(st_billing_evo) <- column_names

### nest data
st_billing_evo_long <- st_billing_evo %>%
  pivot_longer(-c(iso_2letter, charging_zone, month), names_to = 'metric', values_to = 'value') %>%
  group_by(iso_2letter, charging_zone, month) %>%
  nest_legacy(.key = "statistics")


st_billing_evo_j <- st_billing_evo_long %>% toJSON(., pretty = TRUE)

save_json(st_billing_evo_j, "st_billing_evo")
print(paste(format(now(), "%H:%M:%S"), "st_billing_evo"))


## CO2 ----
st_co2_data_filtered <- co2_data_raw %>%
  mutate(co2_state = STATE_NAME) %>%
  right_join(list_state_co2, by = "co2_state") %>%
  left_join(state_iso, by = "iso_2letter") %>%
  select(-c(STATE_NAME, STATE_CODE, co2_state))

st_co2_evo <- st_co2_data_filtered %>%
  filter(YEAR >= 2019,
         YEAR <= st_co2_last_year,
         MONTH <= st_co2_last_month_num
         ) %>%
  group_by(iso_2letter, state, FLIGHT_MONTH)%>%
  summarise(TTF = sum(TF), TCO2 = sum(CO2_QTY_TONNES), .groups = "drop") %>%
  group_by(iso_2letter, state)%>%
  mutate(
    YEAR = as.numeric(format(FLIGHT_MONTH,'%Y')),
    MONTH = as.numeric(format(FLIGHT_MONTH,'%m'))
  )%>%
  arrange(iso_2letter, FLIGHT_MONTH) %>%
  mutate(
    DEP_IDX = TTF / first(TTF) * 100,
    CO2_IDX = TCO2 / first(TCO2) * 100,
    FLIGHT_MONTH = ceiling_date(as_date(FLIGHT_MONTH), unit = 'month') - 1
  ) %>%
  select(
    iso_2letter, state,
    FLIGHT_MONTH,
    CO2_IDX,
    DEP_IDX
  ) 

column_names <- c(
  "iso_2letter",
  "state",
  "month",
  "CO2 index",
  "Departures index"
)

colnames(st_co2_evo) <- column_names

### nest data
st_co2_evo_long <- st_co2_evo %>%
  pivot_longer(-c(iso_2letter, state, month), names_to = 'metric', values_to = 'value') %>%
  group_by(iso_2letter, state, month) %>%
  nest_legacy(.key = "statistics")

st_co2_evo_j <- st_co2_evo_long %>% toJSON(., pretty = TRUE)

save_json(st_co2_evo_j, "st_co2_evo")
print(paste(format(now(), "%H:%M:%S"), "st_co2_evo"))

## TRAFFIC FORECAST ----
### input data
if (!exists("forecast_raw")) {
  forecast_raw <-  read_csv(here("..", "mobile-app", "data", forecast_file_name), show_col_types = FALSE)
}

### process data
forecast_max_actual_year <- forecast_raw %>% 
  filter(scenario == "High") %>% 
  summarise(min(year)) %>% pull()

forecast_graph <- forecast_raw %>% 
  right_join(list_statfor_states, by = c("tz_name" = "statfor_tz"), relationship = "many-to-many") %>% 
  right_join(state_iso, by ="iso_2letter", relationship = "many-to-many") %>%
  # select(-state) %>%
  arrange(iso_2letter) %>% 
  mutate(
    forecast_name = forecast_name_value
    # , forecast_date = forecast_name_date
  ) 

### DAIO----
forecast_graph_daio <- forecast_graph %>% 
  filter(daio == "T") %>% 
  # group_by(forecast_name, iso_2letter, tz_name, scenario, year) %>% 
  # summarise(flights = sum(flights, na.rm = TRUE)) %>% 
  group_by(iso_2letter, scenario) %>% 
  arrange(iso_2letter, scenario, year) %>% 
  mutate(
    FLIGHT_DATE = lubridate::ymd(paste0(year,'01','01')), #represents the year - requested by ewasoft for mapping
    yoy = flights / lag(flights,1) -1,
    label_flights = if_else(year == forecast_max_actual_year & scenario != "Actual", 
                            NA_character_, 
                            paste0(round(flights/1000, 0), "k")),
    label_yoy = if_else(year == forecast_max_actual_year & scenario != "Actual", 
                        NA_character_,
                        paste0(if_else(yoy >= 0, "+", ""),round(yoy*100, 1), "%")),
    
    label_tooltip = if_else(year == forecast_max_actual_year & scenario != "Actual", 
                            NA_character_,
                            paste0(label_flights, " (", label_yoy, ")")),
    label_flights = if_else(scenario == "High" | scenario == "Low", 
                            NA_character_, 
                            label_flights),
    label_yoy = if_else(scenario == "High" | scenario == "Low",
                        NA_character_, 
                        label_yoy),
    
  )%>% 
  select(-yoy, -daio, -state) %>% 
  filter(year >= forecast_min_year_graph) %>% 
  filter((year <= forecast_max_actual_year & scenario == "Actual") | year >= forecast_max_actual_year) %>% 
  select(-year) %>% 
  ungroup()


### nest and save data
forecast_graph_daio_nest <- forecast_graph_daio %>%
  group_by(forecast_name, iso_2letter, tz_name, FLIGHT_DATE) %>% 
  nest_legacy(.key = "statistics")

forecast_graph_daio_nest_j <- forecast_graph_daio_nest %>% toJSON(., pretty = TRUE)

save_json(forecast_graph_daio_nest_j, "st_daio_forecast_chart")
print(paste(format(now(), "%H:%M:%S"), "st_daio_forecast_chart"))

### DAI----
forecast_graph_dai <- forecast_graph %>% 
  filter(daio == "AD" | daio == "I") %>% 
  group_by(forecast_name, iso_2letter, tz_name, scenario, year) %>% 
  summarise(flights = sum(flights, na.rm = TRUE), .groups = "drop") %>% 
  group_by(iso_2letter, scenario) %>% 
  arrange(iso_2letter, scenario, year) %>% 
  mutate(
    FLIGHT_DATE = lubridate::ymd(paste0(year,'01','01')), #represents the year - requested by ewasoft for mapping
    yoy = flights / lag(flights,1) -1,
    label_flights = if_else(year == forecast_max_actual_year & scenario != "Actual", 
                            NA_character_, 
                            paste0(round(flights/1000, 0), "k")),
    label_yoy = if_else(year == forecast_max_actual_year & scenario != "Actual", 
                        NA_character_,
                        paste0(if_else(yoy >= 0, "+", ""),round(yoy*100, 1), "%")),
    
    label_tooltip = if_else(year == forecast_max_actual_year & scenario != "Actual", 
                            NA_character_,
                            paste0(label_flights, " (", label_yoy, ")")),
    label_flights = if_else(scenario == "High" | scenario == "Low", 
                            NA_character_, 
                            label_flights),
    label_yoy = if_else(scenario == "High" | scenario == "Low",
                        NA_character_, 
                        label_yoy),
    
  )%>% 
  select(-yoy) %>% 
  filter(year >= forecast_min_year_graph) %>% 
  filter((year <= forecast_max_actual_year & scenario == "Actual") | year >= forecast_max_actual_year) %>% 
  select(-year) %>% 
  ungroup()

### nest and save data
forecast_graph_dai_nest <- forecast_graph_dai %>%
  group_by(forecast_name, iso_2letter, tz_name, FLIGHT_DATE) %>% 
  nest_legacy(.key = "statistics")

forecast_graph_dai_nest_j <- forecast_graph_dai_nest %>% toJSON(., pretty = TRUE)

save_json(forecast_graph_dai_nest_j, "st_dai_forecast_chart")
print(paste(format(now(), "%H:%M:%S"), "st_dai_forecast_chart"))

### Oveflights----
forecast_graph_over <- forecast_graph %>% 
  filter(daio == "O") %>% 
  # group_by(forecast_name, iso_2letter, tz_name, scenario, year) %>% 
  # summarise(flights = sum(flights, na.rm = TRUE)) %>% 
  group_by(iso_2letter, scenario) %>% 
  arrange(iso_2letter, scenario, year) %>% 
  mutate(
    FLIGHT_DATE = lubridate::ymd(paste0(year,'01','01')), #represents the year - requested by ewasoft for mapping
    yoy = flights / lag(flights,1) -1,
    label_flights = if_else(year == forecast_max_actual_year & scenario != "Actual", 
                            NA_character_, 
                            paste0(round(flights/1000, 0), "k")),
    label_yoy = if_else(year == forecast_max_actual_year & scenario != "Actual", 
                        NA_character_,
                        paste0(if_else(yoy >= 0, "+", ""),round(yoy*100, 1), "%")),
    
    label_tooltip = if_else(year == forecast_max_actual_year & scenario != "Actual", 
                            NA_character_,
                            paste0(label_flights, " (", label_yoy, ")")),
    label_flights = if_else(scenario == "High" | scenario == "Low", 
                            NA_character_, 
                            label_flights),
    label_yoy = if_else(scenario == "High" | scenario == "Low",
                        NA_character_, 
                        label_yoy),
    
  )%>% 
  select(-yoy, -daio, -state) %>% 
  filter(year >= forecast_min_year_graph) %>% 
  filter((year <= forecast_max_actual_year & scenario == "Actual") | year >= forecast_max_actual_year) %>% 
  select(-year) %>% 
  ungroup()


### nest and save data
forecast_graph_over_nest <- forecast_graph_over %>%
  group_by(forecast_name, iso_2letter, tz_name, FLIGHT_DATE) %>% 
  nest_legacy(.key = "statistics")

forecast_graph_over_nest_j <- forecast_graph_over_nest %>% toJSON(., pretty = TRUE)

save_json(forecast_graph_over_nest_j, "st_ovf_forecast_chart")
print(paste(format(now(), "%H:%M:%S"), "st_ovf_forecast_chart"))

print(" ")

