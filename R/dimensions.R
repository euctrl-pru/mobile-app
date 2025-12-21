library(dplyr)
library(lubridate)
library(here)
library(RODBC)
library(readxl)

source(here::here("..", "mobile-app", "R", "helpers.R"))
source(here::here("..", "mobile-app", "R", "params.R"))
source(here::here("..", "mobile-app", "R", "dimension_queries.R"))

# parameters ----
if (!exists("data_day_date")) {current_day <- today() - days(1)} else {current_day <- data_day_date}

# DIMENSIONS ----
## ao group ----  
# same as v_covid_dim_ao but adding ao_id and removing old aos

dim_ao_group <- export_query(dim_ao_grp_query) 

dim_ao_group1 <- dim_ao_group %>% select(AO_GRP_CODE, AO_GRP_NAME, TIL) %>% 
  group_by(AO_GRP_CODE, AO_GRP_NAME) %>% 
  summarise(TIL = max(TIL)) %>% 
  group_by(AO_GRP_CODE, AO_GRP_NAME, TIL) %>% 
  ungroup()

dim_ao_group1_last <- dim_ao_group1 %>% 
  group_by(AO_GRP_NAME) %>% 
  filter(TIL == max(TIL)) %>% 
  ungroup()

list_ao <- export_query(list_ao_query) %>% 
  left_join(dim_ao_group, by = c("AO_ID", "AO_CODE", "AO_NAME")) %>% 
  select (AO_ID, AO_CODE, AO_NAME, AO_GRP_CODE, AO_GRP_NAME)

list_ao_new <- export_query(list_ao_query_new)

list_ao_group <- list_ao %>% group_by(AO_GRP_CODE, AO_GRP_NAME) %>% 
  summarise(AO_GRP_CODE =  max(AO_GRP_CODE), AO_GRP_NAME =  max(AO_GRP_NAME)) %>%
  arrange(AO_GRP_CODE) %>% ungroup()

list_ao_group_new <- export_query(list_ao_grp_query)


## market segment ---- 
dim_marktet_segment <- export_query(dim_ms_query) 

dim_marktet_segment_new <- export_query(dim_ms_query) %>% rename(MS_NAME = MARKET_SEGMENT)

list_marktet_segment_app_new <- dim_marktet_segment_new %>% 
  filter(MS_ID %in% c(2,3,4, 6, 7,8)) %>% 
  add_row(MS_ID = 9, MS_NAME = "Other Types")
  

## airport ----
dim_airport <- export_query(dim_ap_query) 

dim_airport_new <- export_query(dim_ap_query_new) %>% 
  filter(!is.na(CFMU_AP_CODE)) %>% 
  filter(VALID_TO >= ymd(20190101)) %>% 
  group_by(BK_AP_ID, EC_AP_CODE, EC_AP_NAME, LATITUDE, LONGITUDE) %>% 
  summarise(VALID_TO = max(VALID_TO, na.rm = TRUE), .groups = "drop") %>% 
  distinct(BK_AP_ID, EC_AP_CODE, EC_AP_NAME, LATITUDE, LONGITUDE, VALID_TO)

# dim_airport_new %>% group_by(BK_AP_ID) %>% summarise(myrows = n()) %>% filter(myrows>1)
# 
# dim_airport_new %>% filter(BK_AP_ID == 52)

list_airport_new <- export_query(list_ap_query_new) 
list_airport_extended_new <- export_query(list_ap_ext_query_new)
list_airport_extended_iso_new <- export_query(list_ap_ext_iso_query_new)

## country ----
### iso ----
dim_iso_country <- export_query(dim_iso_st_query) 

dim_iso_country_spain <- export_query(dim_iso_st_query) %>% 
  mutate(
    COUNTRY_NAME = if_else(ISO_COUNTRY_CODE == "ES",
                           "Spain Continental",
                           COUNTRY_NAME)
  ) %>% 
  add_row(ISO_COUNTRY_CODE = "IC", COUNTRY_NAME = "Spain Canaries")

list_iso_country <- export_query(list_iso_st_query)

rel_iso_country_daio_zone <- read_xlsx(
  here("stakeholder_lists.xlsx"),
  sheet = "state_lists",
  range = cell_limits(c(2, 11), c(NA, 13))) %>%
  as_tibble()

### icao ----
list_icao_country <- export_query(list_icao_st_query) 

list_icao_country_spain <- list_icao_country %>% 
  add_row(COUNTRY_CODE = 'LEGC', COUNTRY_NAME = 'Spain')

### statfor ----
list_statfor_states <- read_xlsx(
  here("stakeholder_lists.xlsx"),
  sheet = "state_lists",
  range = cell_limits(c(2, 20), c(NA, 21))) %>%
  as_tibble()

### crco  ----
list_state_crco <-  read_xlsx(
  here("stakeholder_lists.xlsx"),
  sheet = "state_lists",
  range = cell_limits(c(2, 6), c(NA, 8))) %>%
  as_tibble()

### co2 ----
list_state_co2 <-  read_xlsx(
  here("stakeholder_lists.xlsx"),
  sheet = "state_lists",
  range = cell_limits(c(2, 16), c(NA, 17))) %>%
  as_tibble()

## ansp ----  
dim_ansp <- export_query(dim_ansp_query) 

list_ansp <-  read_xlsx(
  here("stakeholder_lists.xlsx"),
  sheet = "ansp_lists",
  range = cell_limits(c(1, 1), c(NA, 3))) %>%
  select(-ANSP_NAME) %>% 
  as_tibble() %>% 
  left_join(dim_ansp, by = "ANSP_ID") %>% 
  select(ANSP_ID, ANSP_NAME, ANSP_CODE)

## ACC ----
list_acc <-  read_xlsx(
  here("stakeholder_lists.xlsx"),
  sheet = "acc_lists",
  range = cell_limits(c(1, 1), c(NA, NA))) %>%
  as_tibble()

colnames(list_acc) <- toupper(colnames(list_acc))
