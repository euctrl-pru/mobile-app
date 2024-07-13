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

source(here::here("..", "mobile-app", "R", "helpers.R"))

# Parameters ----
data_folder <- here::here("..", "mobile-app", "data", "v3")
# base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/'
base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Oscar/Develop/'
base_file <- '099b_app_ao_dataset.xlsx'
nw_base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/LastVersion/'
nw_base_file <- '099_Traffic_Landing_Page_dataset_new.xlsx'
# archive_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/web_daily_json_files/app/'
archive_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Oscar/old/'
today <- (lubridate::now() +  days(-1)) %>% format("%Y%m%d")
last_day <-  trunc((lubridate::now() +  days(-1)), "day")
last_year <- as.numeric(format(last_day,'%Y'))
st_json_app <-""
# DB params
usr <- Sys.getenv("PRU_DEV_USR")
pwd <- Sys.getenv("PRU_DEV_PWD")
dbn <- Sys.getenv("PRU_DEV_DBNAME")

# ____________________________________________________________________________________________
#
#    AO landing page -----
#
# ____________________________________________________________________________________________

#### Traffic data ----
ao_traffic_delay_data <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(base_file),
    start = base_dir),
  sheet = "ao_traffic_delay",
  range = cell_limits(c(1, 1), c(NA, NA))) %>%
  as_tibble() %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

ao_traffic_delay_last_day <- ao_traffic_delay_data %>%
  filter(FLIGHT_DATE == last_day) %>%
  arrange(AO_GRP_NAME, FLIGHT_DATE)

ao_traffic_for_json <- ao_traffic_delay_last_day %>%
  select(
    AO_GRP_NAME,
    FLIGHT_DATE,
    DAY_TFC,
    DAY_TFC_DIF_PREV_YEAR_PERC,
    DAY_TFC_DIF_2019_PERC,
    RWK_AVG_TFC,
    RWK_TFC_DIF_PREV_YEAR_PERC,
    RWK_TFC_DIF_2019_PERC,
    Y2D_TFC_YEAR,
    Y2D_AVG_TFC_YEAR,
    Y2D_TFC_DIF_PREV_YEAR_PERC,
    Y2D_TFC_DIF_2019_PERC
  ) %>%
  rename(
    DY_TFC = DAY_TFC,
    DY_TFC_DIF_PREV_YEAR_PERC = DAY_TFC_DIF_PREV_YEAR_PERC,
    DY_TFC_DIF_2019_PERC = DAY_TFC_DIF_2019_PERC,
    WK_TFC_AVG_ROLLING = RWK_AVG_TFC,
    WK_TFC_DIF_PREV_YEAR_PERC = RWK_TFC_DIF_PREV_YEAR_PERC,
    WK_TFC_DIF_2019_PERC = RWK_TFC_DIF_2019_PERC,
    Y2D_TFC = Y2D_TFC_YEAR,
    Y2D_TFC_AVG = Y2D_AVG_TFC_YEAR
  )

#### Delay data ----
ao_delay_for_json <- ao_traffic_delay_last_day %>%
  select(
    AO_GRP_NAME,
    FLIGHT_DATE,
    DAY_DLY,
    DAY_DLY_DIF_PREV_YEAR_PERC,
    DAY_DLY_DIF_2019_PERC,
    DAY_DLY_FLT,
    DAY_DLY_FLT_DIF_PREV_YEAR_PERC,
    DAY_DLY_FLT_DIF_2019_PERC,

    RWK_AVG_DLY,
    RWK_DLY_DIF_PREV_YEAR_PERC,
    RWK_DLY_DIF_2019_PERC,
    RWK_DLY_FLT,
    RWK_DLY_FLT_DIF_PREV_YEAR_PERC,
    RWK_DLY_FLT_DIF_2019_PERC,

    Y2D_AVG_DLY_YEAR,
    Y2D_DLY_DIF_PREV_YEAR_PERC,
    Y2D_DLY_DIF_2019_PERC,
    Y2D_DLY_FLT_YEAR,
    Y2D_DLY_FLT_DIF_PREV_YEAR_PERC,
    Y2D_DLY_FLT_DIF_2019_PERC
  ) %>%
  rename(
    DY_DLY = DAY_DLY,
    DY_DLY_DIF_PREV_YEAR_PERC = DAY_DLY_DIF_PREV_YEAR_PERC,
    DY_DLY_DIF_2019_PERC = DAY_DLY_DIF_2019_PERC,
    DY_DLY_FLT = DAY_DLY_FLT,
    DY_DLY_FLT_DIF_PREV_YEAR_PERC = DAY_DLY_FLT_DIF_PREV_YEAR_PERC,
    DY_DLY_FLT_DIF_2019_PERC = DAY_DLY_FLT_DIF_2019_PERC,

    WK_DLY_AVG_ROLLING = RWK_AVG_DLY,
    WK_DLY_DIF_PREV_YEAR_PERC = RWK_DLY_DIF_PREV_YEAR_PERC,
    WK_DLY_DIF_2019_PERC = RWK_DLY_DIF_2019_PERC,
    WK_DLY_FLT = RWK_DLY_FLT,
    WK_DLY_FLT_DIF_PREV_YEAR_PERC = RWK_DLY_FLT_DIF_PREV_YEAR_PERC,
    WK_DLY_FLT_DIF_2019_PERC = RWK_DLY_FLT_DIF_2019_PERC,

    Y2D_DLY_AVG = Y2D_AVG_DLY_YEAR
  )

#### Punctuality data ----
# it won't be published initially, but we prepare the data
query <- "
      WITH

      AO_LIST AS (
      SELECT ao_code,ao_name,ao_grp_code,ao_grp_name
      FROM prudev.v_covid_dim_ao
      WHERE  LIST_DSH = 'Y'
       )
      , AO_DAY AS (
      SELECT
              a.ao_code,
              a.ao_name,
              a.ao_grp_code,
              a.ao_grp_name,
              t.year,
              t.month,
              t.week,
              t.week_nb_year,
              t.day_type,
              t.day_of_week_nb AS day_of_week,
              t.day_date
      FROM AO_LIST a, pru_time_references t
      WHERE
         t.day_date >= to_date('24-12-2018','DD-MM-YYYY')
         AND t.day_date < trunc(sysdate)
      )

      SELECT a.*, b.*

      FROM AO_DAY a
      left join LDW_VDM.VIEW_FAC_PUNCTUALITY_AO_DAY b on a.ao_code = b.ICAO_CODE and a.day_date = b.DATA_DATE
   "

ao_punct_raw <- export_query(query) %>%
  as_tibble() %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

last_day_punct <-  max(ao_punct_raw$DAY_DATE)
last_year_punct <- as.numeric(format(last_day_punct,'%Y'))

ao_punct_data <- ao_punct_raw %>%
  group_by(AO_GRP_NAME, DAY_DATE) %>%
  summarise(
    ARR_PUNCTUAL_FLIGHTS = sum(ARR_PUNCTUAL_FLIGHTS, na.rm = TRUE),
    ARR_SCHEDULE_FLIGHT = sum(ARR_SCHEDULE_FLIGHT, na.rm = TRUE),
    DEP_PUNCTUAL_FLIGHTS = sum(DEP_PUNCTUAL_FLIGHTS, na.rm = TRUE),
    DEP_SCHEDULE_FLIGHT = sum(DEP_SCHEDULE_FLIGHT, na.rm = TRUE)
    ) %>%
  arrange(AO_GRP_NAME, DAY_DATE) %>%
  mutate(
    YEAR = lubridate::year(DAY_DATE),
    DAY_ARR_PUNCT = if_else(ARR_SCHEDULE_FLIGHT == 0, 0, ARR_PUNCTUAL_FLIGHTS/ARR_SCHEDULE_FLIGHT * 100),
    DAY_DEP_PUNCT = if_else(DEP_SCHEDULE_FLIGHT == 0, 0, DEP_PUNCTUAL_FLIGHTS/DEP_SCHEDULE_FLIGHT * 100),

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
  )

ao_punct_d_w <- ao_punct_data %>%
  filter (DAY_DATE == last_day_punct) %>%
  select(
    AO_GRP_NAME,
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

ao_punct_y2d <- ao_punct_raw %>%
  arrange(AO_GRP_NAME, DAY_DATE) %>%
  mutate(MONTH_DAY = as.numeric(format(DAY_DATE, format="%m%d"))) %>%
  filter(MONTH_DAY <= as.numeric(format(last_day_punct, format="%m%d"))) %>%
  group_by(AO_GRP_NAME, YEAR) %>%
  summarise (Y2D_ARR_PUN = sum(ARR_PUNCTUAL_FLIGHTS, na.rm=TRUE) / sum(ARR_SCHEDULE_FLIGHT, na.rm=TRUE) * 100,
             Y2D_DEP_PUN = sum(DEP_PUNCTUAL_FLIGHTS, na.rm=TRUE) / sum(DEP_SCHEDULE_FLIGHT, na.rm=TRUE) * 100) %>%
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
  select(AO_GRP_NAME,
         Y2D_ARR_PUN,
         Y2D_DEP_PUN,
         Y2D_ARR_PUN_DIF_PREV_YEAR,
         Y2D_DEP_PUN_DIF_PREV_YEAR,
         Y2D_ARR_PUN_DIF_2019,
         Y2D_DEP_PUN_DIF_2019
  )

ao_punct_for_json <- merge(ao_punct_d_w, ao_punct_y2d, by="AO_GRP_NAME")

#### CO2 data ----
# it won't be published initially, but we prepare the data
query <- "
with

LIST_AO_MAX_TIL as (
select a.AO_NAME,
    max(a.TIL) as TIL
FROM LDW_ACC.AO_GROUPS_ASSOCIATION a
group by a.AO_NAME
)

, LIST_AO_LASTCODE as (
select a.AO_NAME, b.AO_CODE
FROM LIST_AO_MAX_TIL a
left join LDW_ACC.AO_GROUPS_ASSOCIATION b on a.TIL = b.TIL and a.AO_NAME=b.AO_NAME
group by a.AO_NAME, b.AO_CODE

)
, LIST_AO as (
select distinct a.AO_CRCO_ID, a.AO_NAME ,b.AO_CODE
FROM LDW_ACC.AO_GROUPS_ASSOCIATION a
left join LIST_AO_LASTCODE b on a.AO_NAME=b.AO_NAME
where AO_CRCO_ID is not NULL
group by a.AO_CRCO_ID, a.AO_NAME ,b.AO_CODE
)
, LIST_GRP1 as (
select distinct AO_CRCO_ID, AO_NAME, AO_GRP_NAME, AO_GRP_CODE, AO_GRP_LEVEL
FROM LDW_ACC.AO_GROUPS_ASSOCIATION
where AO_CRCO_ID is not NULL
      and AO_GRP_LEVEL='GROUP1'
)
,DIM_AO as (
    select a.AO_CRCO_ID,
          coalesce(b.AO_GRP_NAME,a.AO_NAME) AO_GRP_NAME,
          coalesce(b.AO_GRP_CODE,a.AO_CODE) AO_GRP_CODE,
          coalesce(b.AO_GRP_LEVEL,'OTHER') AO_GRP_LEVEL
from LIST_AO a left join LIST_GRP1 b
    on a.AO_NAME=b.AO_NAME
    Group by  a.AO_CRCO_ID,
--          a.AO_NAME,
         coalesce(b.AO_GRP_NAME,a.AO_NAME),
         coalesce(b.AO_GRP_CODE,a.AO_CODE),
          coalesce(b.AO_GRP_LEVEL,'OTHER')

)

, max_flight_month as (
    select
        max(FLIGHT_MONTH) as max_date
    from emma_pub.MV_FLIGHT_EMIS_M_OP
)

, check_flight as (
    select
        sum(TF) as flt_last_month
     from emma_pub.MV_FLIGHT_EMIS_M_OP, max_flight_month
     where flight_month = max_date
)

, max_month as (
    select
        case
            when flt_last_month < 1000 then extract (month from max_date) - 1
            else extract (month from max_date)
        end max_month,
        case
            when flt_last_month < 1000 then extract (year from max_date-15)
            else extract (year from max_date)
        end max_year
    from  max_flight_month, check_flight
)

, data_group as (
SELECT  --a.USER_NUMBER,
        a.FLIGHT_MONTH,
        extract(month from a.FLIGHT_MONTH) as month,
        extract(year from a.FLIGHT_MONTH) as year,
--        a.ETS_PERIOD,
        sum(coalesce(a.TF,0)) TF,
        sum(coalesce(a.co2_qty,0)) co2_qty,
--        a.src_info, a.dep_cov_src_info,
--        a.des_cov_src_info,a.route,
        coalesce(b.AO_GRP_NAME,c.LEGAL_NAME) AO_GRP_NAME,
        b.AO_GRP_CODE
      FROM emma_pub.MV_FLIGHT_EMIS_M_OP a
         left join DIM_AO b on b.AO_CRCO_ID = a.USER_NUMBER
         left join emma_pub.operator c on c.ETS_ID = a.USER_NUMBER
     where a.DEP_COV_SRC_INFO = 'C'
     group by b.AO_GRP_NAME, b.AO_GRP_CODE, c.LEGAL_NAME, a.FLIGHT_MONTH, a.ETS_PERIOD, a.src_info, a.dep_cov_src_info, a.des_cov_src_info,a.route
)

, data_y2d as (
    select
        AO_GRP_NAME,
        sum(co2_qty) as co2_qty_y2d_curr
    from data_group, max_month
    where year = max_year
    group by year, AO_GRP_NAME
)

, data_y2d_rank as (
    select
        row_number () over (order by co2_qty_y2d_curr desc, AO_GRP_NAME asc) as rank_current,
        AO_GRP_NAME,
        co2_qty_y2d_curr
    from data_y2d
)

, data_group_filtered as (
        select a.*
    from data_group a
    inner join data_y2d_rank b on a.AO_GRP_NAME = b.AO_GRP_NAME
    where rank_current <= 200
)

select
        a.*,
        case
            when a.month <= b.max_month then 'Y'
            else 'N'
        end flag_ytd
from data_group_filtered a, max_month b
   "

ao_co2_raw <- export_query(query) %>%
  as_tibble() %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

ao_co2_data <- ao_co2_raw %>%
  select(AO_GRP_NAME,
         FLIGHT_MONTH,
         CO2_QTY,
         TF,
         YEAR,
         MONTH) %>%
  group_by(AO_GRP_NAME, FLIGHT_MONTH, YEAR, MONTH) %>%
  summarise (MM_DEP = sum(TF, na.rm=TRUE) / 10^6,
             MM_CO2 = sum(CO2_QTY_TONNES, na.rm=TRUE) / 10^6
             # MM_CO2_PY = sum(LY_CO2_QTY_TONNES, na.rm=TRUE) / 10^6,
             # MM_DEP_PY = sum(LY_TF, na.rm=TRUE) / 10^6,

  ) %>%
  ungroup() %>%
  mutate(
    CO2_DATE = FLIGHT_MONTH,
    MM_CO2_DEP = MM_CO2 / MM_DEP,
    MM_CO2_DEP_PY = MM_CO2_PY / MM_DEP_PY
  ) %>%
  arrange(iso_2letter, FLIGHT_MONTH) %>%
  mutate(FLIGHT_MONTH = ceiling_date(as_date(FLIGHT_MONTH), unit = 'month')-1)

st_co2_last_date <- max(st_co2_data$FLIGHT_MONTH, na.rm=TRUE)
st_co2_last_month <- format(st_co2_last_date,'%B')
st_co2_last_month_num <- as.numeric(format(st_co2_last_date,'%m'))
st_co2_last_year <- max(st_co2_data$YEAR, na.rm=TRUE)

#check last month number of flights
check_flights <- st_co2_data %>%
  filter (YEAR == st_co2_last_year) %>% filter(MONTH == st_co2_last_month_num) %>%
  summarise (TTF = sum(MM_DEP, na.rm=TRUE)) %>%
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
  right_join(state_iso, by = "iso_2letter") %>%
  select(-state) %>%
  arrange(iso_2letter)
