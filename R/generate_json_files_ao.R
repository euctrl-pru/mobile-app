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

# Dimension tables ----
ao_grp_icao <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(base_file),
    start = base_dir),
  sheet = "lists",
  range = cell_limits(c(1, 1), c(NA, NA))) %>%
  as_tibble()


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
    AO_GRP_CODE,
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
    AO_GRP_CODE,
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
  group_by(AO_GRP_NAME, AO_GRP_CODE, DAY_DATE) %>%
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
    AO_GRP_CODE,
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
  group_by(AO_GRP_NAME, AO_GRP_CODE, YEAR) %>%
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
         AO_GRP_CODE,
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
         AO_GRP_CODE,
         FLIGHT_MONTH,
         CO2_QTY,
         TF,
         YEAR,
         MONTH) %>%
  group_by(AO_GRP_NAME, AO_GRP_CODE, FLIGHT_MONTH, YEAR, MONTH) %>%
  summarise (MM_DEP = sum(TF, na.rm=TRUE) / 10^6,
             MM_CO2 = sum(CO2_QTY, na.rm=TRUE) / 10^6
             # MM_CO2_PY = sum(LY_CO2_QTY_TONNES, na.rm=TRUE) / 10^6,
             # MM_DEP_PY = sum(LY_TF, na.rm=TRUE) / 10^6,

  ) %>%
  ungroup() |>
  group_by(AO_GRP_NAME, AO_GRP_CODE) %>%
  mutate(MM_DEP_PY = lag(MM_DEP, 12),
         MM_CO2_PY = lag(MM_CO2, 12)) |>
  mutate(
    CO2_DATE = FLIGHT_MONTH,
    MM_CO2_DEP = MM_CO2 / MM_DEP,
    MM_CO2_DEP_PY = MM_CO2_PY / MM_DEP_PY
  ) |>
  mutate(FLIGHT_MONTH = ceiling_date(as_date(FLIGHT_MONTH), unit = 'month')-1)

ao_co2_last_date <- max(ao_co2_data$FLIGHT_MONTH, na.rm=TRUE)
ao_co2_last_month <- format(ao_co2_last_date,'%B')
ao_co2_last_month_num <- as.numeric(format(ao_co2_last_date,'%m'))
ao_co2_last_year <- max(ao_co2_data$YEAR, na.rm=TRUE)

#check last month number of flights
check_flights <- ao_co2_data %>% ungroup() |>
  filter (YEAR == ao_co2_last_year) %>% filter(MONTH == ao_co2_last_month_num) %>%
  summarise (TTF = sum(MM_DEP*10^6, na.rm=TRUE)) %>%
  select(TTF) %>% pull()

if (check_flights < 1000) {
  ao_co2_data <- ao_co2_data %>% filter (FLIGHT_MONTH < ao_co2_last_date)
  ao_co2_last_date <- max(ao_co2_data$FLIGHT_MONTH, na.rm=TRUE)
}

ao_co2_for_json <- ao_co2_data %>%
  group_by(AO_GRP_NAME, AO_GRP_CODE) |>
  arrange(AO_GRP_NAME, FLIGHT_MONTH) %>%
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
  group_by(AO_GRP_NAME, AO_GRP_CODE, YEAR) %>%
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
    AO_GRP_NAME,
    AO_GRP_CODE,
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
  ) |>
  filter(FLIGHT_MONTH == ao_co2_last_date) |>
  right_join(ao_grp_icao, by = 'AO_GRP_CODE')

#### Join strings and save  ----
ao_json_app_j <- ao_grp_icao %>% arrange(AO_GRP_NAME)
ao_json_app_j$ao_traffic <- select(arrange(ao_traffic_for_json, AO_GRP_NAME), -c(AO_GRP_CODE, AO_GRP_NAME))
ao_json_app_j$ao_delay <- select(arrange(ao_delay_for_json, AO_GRP_NAME), -c(AO_GRP_CODE, AO_GRP_NAME))

update_day <- floor_date(lubridate::now(), unit = "days") %>%
  as_tibble() %>%
  rename(APP_UPDATE = 1)

ao_json_app_j$ao_update <- update_day

ao_json_app_j <- ao_json_app_j %>%   group_by(AO_GRP_CODE, AO_GRP_NAME)

ao_json_app <- ao_json_app_j %>%
  toJSON(., pretty = TRUE)

write(ao_json_app, here(data_folder,"ao_json_app.json"))
write(ao_json_app, paste0(archive_dir, "ao_json_app.json"))
write(ao_json_app, paste0(archive_dir, today, "_ao_json_app.json"))

# ____________________________________________________________________________________________
#
#    AO ranking tables  -----
#
# ____________________________________________________________________________________________

## TRAFFIC ----
### Destination country ----
#### day ----
ao_st_des_data_day_raw <- read_xlsx(
  path  = fs::path_abs(
    str_glue(base_file),
    start = base_dir),
  sheet = "ao_state_des_day",
  range = cell_limits(c(1, 1), c(NA, NA))) %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

ao_st_des_data_day_int <- ao_st_des_data_day_raw %>%
  mutate(TO_DATE = max(TO_DATE)) %>%
  spread(., key = FLAG_DAY, value = FLIGHT) %>%
  arrange(AO_GRP_NAME, R_RANK) %>%
  mutate(
    DY_RANK_DIF_PREV_WEEK = case_when(
      is.na(RANK_PREV_WEEK) ~ RANK,
      .default = RANK_PREV_WEEK - RANK
    ),
    DY_FLT_DIF_PREV_WEEK_PERC =   case_when(
      DAY_PREV_WEEK == 0 | is.na(DAY_PREV_WEEK) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_WEEK - 1
    ),
    DY_FLT_DIF_PREV_YEAR_PERC = case_when(
      DAY_PREV_YEAR == 0 | is.na(DAY_PREV_YEAR) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_YEAR - 1
    ),
    AO_GRP_RANK = paste0(tolower(AO_GRP_NAME), R_RANK),
    AO_GRP_TFC_ST_DES_DIF = CURRENT_DAY - DAY_PREV_WEEK
  )

ao_st_des_data_day <- ao_st_des_data_day_int %>%
  rename(
    DY_ST_DES_NAME = ISO_CT_NAME_ARR,
    DY_TO_DATE = TO_DATE,
    DY_FLT = CURRENT_DAY
  ) %>%
  select(
    AO_GRP_RANK,
    DY_RANK_DIF_PREV_WEEK,
    DY_ST_DES_NAME,
    DY_TO_DATE,
    DY_FLT,
    DY_FLT_DIF_PREV_WEEK_PERC,
    DY_FLT_DIF_PREV_YEAR_PERC
  )

#### week ----
ao_st_des_data_wk_raw <- read_xlsx(
  path  = fs::path_abs(
    str_glue(base_file),
    start = base_dir),
  sheet = "ao_state_des_week",
  range = cell_limits(c(1, 1), c(NA, NA))) %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

ao_st_des_data_wk <- ao_st_des_data_wk_raw %>%
  mutate(FLIGHT = FLIGHT / 7) %>%
  spread(., key = FLAG_ROLLING_WEEK, value = FLIGHT) %>%
  arrange(AO_GRP_NAME, R_RANK) %>%
  mutate(
    WK_RANK_DIF_PREV_WEEK = case_when(
      is.na(RANK_PREV_WEEK) ~ RANK,
      .default = RANK_PREV_WEEK - RANK
    ),
    WK_FLT_DIF_PREV_WEEK_PERC =   case_when(
      PREV_ROLLING_WEEK == 0 | is.na(PREV_ROLLING_WEEK) ~ NA,
      .default = CURRENT_ROLLING_WEEK / PREV_ROLLING_WEEK - 1
    ),
    WK_FLT_DIF_PREV_YEAR_PERC = case_when(
      ROLLING_WEEK_PREV_YEAR == 0 | is.na(ROLLING_WEEK_PREV_YEAR) ~ NA,
      .default = CURRENT_ROLLING_WEEK / ROLLING_WEEK_PREV_YEAR - 1
    ),
    AO_GRP_RANK = paste0(tolower(AO_GRP_NAME), R_RANK)
  ) %>%
  rename(
    WK_ST_DES_NAME = ISO_CT_NAME_ARR,
    WK_FROM_DATE = FROM_DATE,
    WK_TO_DATE = TO_DATE,
    WK_FLT_AVG = CURRENT_ROLLING_WEEK
  ) %>%
  select(
    AO_GRP_RANK,
    WK_RANK_DIF_PREV_WEEK,
    WK_ST_DES_NAME,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_FLT_AVG,
    WK_FLT_DIF_PREV_WEEK_PERC,
    WK_FLT_DIF_PREV_YEAR_PERC
  )

#### y2d ----
ao_st_des_data_y2d_raw <- read_xlsx(
  path  = fs::path_abs(
    str_glue(base_file),
    start = base_dir),
  sheet = "ao_state_des_y2d",
  range = cell_limits(c(1, 1), c(NA, NA))) %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

ao_st_des_data_y2d <- ao_st_des_data_y2d_raw %>%
  mutate(
    FROM_DATE = max(FROM_DATE),
    TO_DATE = max(TO_DATE),
    PERIOD =   case_when(
      YEAR == max(YEAR) ~ 'CURRENT_YEAR',
      YEAR == max(YEAR) - 1 ~ 'PREV_YEAR',
      .default = paste0('PERIOD_', YEAR)
    )
  ) %>%
  select(-FLIGHT, -YEAR, -NO_DAYS) %>%
  spread(., key = PERIOD, value = AVG_FLIGHT) %>%
  arrange(AO_GRP_NAME, R_RANK) |>
  mutate(
    Y2D_RANK_DIF_PREV_YEAR = case_when(
      is.na(RANK_PREV_YEAR) ~ RANK,
      .default = RANK_PREV_YEAR - RANK
    ),
    Y2D_FLT_DIF_PREV_YEAR_PERC =   case_when(
      PREV_YEAR == 0 | is.na(PREV_YEAR) ~ NA,
      .default = CURRENT_YEAR / PREV_YEAR - 1
    ),
    Y2D_FLT_DIF_2019_PERC  = case_when(
      PERIOD_2019 == 0 | is.na(PERIOD_2019) ~ NA,
      .default = CURRENT_YEAR / PERIOD_2019 - 1
    ),
    AO_GRP_RANK = paste0(tolower(AO_GRP_NAME), R_RANK)
  ) %>%
  rename(
    Y2D_ST_DES_NAME = ISO_CT_NAME_ARR,
    Y2D_TO_DATE = TO_DATE,
    Y2D_FLT_AVG = CURRENT_YEAR
  ) %>%
  select(
    AO_GRP_RANK,
    Y2D_RANK_DIF_PREV_YEAR,
    Y2D_ST_DES_NAME,
    Y2D_TO_DATE,
    Y2D_FLT_AVG,
    Y2D_FLT_DIF_PREV_YEAR_PERC,
    Y2D_FLT_DIF_2019_PERC
  )

#### main card ----
ao_st_des_main_traffic <- ao_st_des_data_day_int %>%
  mutate(
    MAIN_TFC_ST_DES_NAME = if_else(
      R_RANK <= 4,
      ISO_CT_NAME_ARR,
      NA
    ),
    MAIN_TFC_ST_DES_FLT = if_else(
      R_RANK <= 4,
      CURRENT_DAY,
      NA
    ),
    ############ we need to add the iso code so the flag can be shown
    AO_GRP_RANK = paste0(tolower(AO_GRP_NAME), R_RANK)
  ) %>%
  select(AO_GRP_RANK, MAIN_TFC_ST_DES_NAME, MAIN_TFC_ST_DES_FLT)

ao_st_des_main_traffic_dif <- ao_st_des_data_day_int %>%
  arrange(AO_GRP_NAME, desc(abs(AO_GRP_TFC_ST_DES_DIF)), R_RANK) %>%
  group_by(AO_GRP_NAME) %>%
  mutate(RANK_DIF_ST_DES_TFC = row_number()) %>%
  ungroup() %>%
  arrange(AO_GRP_NAME, R_RANK) %>%
  mutate(
    MAIN_TFC_DIF_ST_DES_NAME = if_else(
      RANK_DIF_ST_DES_TFC <= 4,
      ISO_CT_NAME_ARR,
      NA
    ),
    MAIN_TFC_DIF_ST_DES_FLT_DIF = if_else(
      RANK_DIF_ST_DES_TFC <= 4,
      AO_GRP_TFC_ST_DES_DIF,
      NA
    )
  ) %>%
  arrange(AO_GRP_NAME, desc(MAIN_TFC_DIF_ST_DES_FLT_DIF)) %>%
  group_by(AO_GRP_NAME) %>%
  mutate(
    RANK_MAIN_DIF = row_number(),
    AO_GRP_RANK = paste0(tolower(AO_GRP_NAME), RANK_MAIN_DIF)
  ) %>%
  ungroup() %>%
  select(AO_GRP_RANK, MAIN_TFC_DIF_ST_DES_NAME, MAIN_TFC_DIF_ST_DES_FLT_DIF)

#### join tables ----
# create list of ao_grp/rankings for left join
ao_grp_icao_ranking <- list()
i = 0
for (i in 1:10) {
  i = i + 1
  ao_grp_icao_ranking <- ao_grp_icao_ranking %>%
    bind_rows(ao_grp_icao, .)
}

ao_grp_icao_ranking <- ao_grp_icao_ranking %>%
  arrange(AO_GRP_NAME) %>%
  group_by(AO_GRP_NAME) %>%
  mutate(
    RANK = row_number(),
    AO_GRP_RANK = paste0(tolower(AO_GRP_NAME), RANK)
  )

# join and reorder tables
ao_st_des_data <- ao_grp_icao_ranking %>%
  left_join(ao_st_des_main_traffic, by = "AO_GRP_RANK") %>%
  left_join(ao_st_des_main_traffic_dif, by = "AO_GRP_RANK") %>%
  left_join(ao_st_des_data_day, by = "AO_GRP_RANK") %>%
  left_join(ao_st_des_data_wk, by = "AO_GRP_RANK") %>%
  left_join(ao_st_des_data_y2d, by = "AO_GRP_RANK") %>%
  ungroup() %>%
  select(-AO_GRP_RANK) %>%
  arrange (AO_GRP_CODE, RANK)

# covert to json and save in app data folder and archive
ao_st_des_data_j <- ao_st_des_data %>% toJSON(., pretty = TRUE)
write(ao_st_des_data_j, here(data_folder,"ao_st_des_ranking_traffic.json"))
write(ao_st_des_data_j, paste0(archive_dir, today, "_ao_st_des_ranking_traffic.json"))
write(ao_st_des_data_j, paste0(archive_dir, "ao_st_des_ranking_traffic.json"))

### Departure airport ----
#### day ----
ao_apt_dep_data_day_raw <- read_xlsx(
  path  = fs::path_abs(
    str_glue(base_file),
    start = base_dir),
  sheet = "ao_apt_dep_day",
  range = cell_limits(c(1, 1), c(NA, NA))) %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

ao_apt_dep_data_day_int <- ao_apt_dep_data_day_raw %>%
  mutate(TO_DATE = max(TO_DATE)) %>%
  spread(., key = FLAG_DAY, value = FLIGHT) %>%
  arrange(AO_GRP_NAME, R_RANK) %>%
  mutate(
    DY_RANK_DIF_PREV_WEEK = case_when(
      is.na(RANK_PREV_WEEK) ~ RANK,
      .default = RANK_PREV_WEEK - RANK
    ),
    DY_FLT_DIF_PREV_WEEK_PERC =   case_when(
      DAY_PREV_WEEK == 0 | is.na(DAY_PREV_WEEK) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_WEEK - 1
    ),
    DY_FLT_DIF_PREV_YEAR_PERC = case_when(
      DAY_PREV_YEAR == 0 | is.na(DAY_PREV_YEAR) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_YEAR - 1
    ),
    AO_GRP_RANK = paste0(tolower(AO_GRP_NAME), R_RANK),
    AO_GRP_TFC_APT_DEP_DIF = CURRENT_DAY - DAY_PREV_WEEK
  )

ao_apt_dep_data_day <- ao_apt_dep_data_day_int %>%
  rename(
    DY_APT_DEP_NAME = ADEP_NAME,
    DY_TO_DATE = TO_DATE,
    DY_FLT = CURRENT_DAY
  ) %>%
  select(
    AO_GRP_RANK,
    DY_RANK_DIF_PREV_WEEK,
    DY_APT_DEP_NAME,
    DY_TO_DATE,
    DY_FLT,
    DY_FLT_DIF_PREV_WEEK_PERC,
    DY_FLT_DIF_PREV_YEAR_PERC
  )

#### week ----
ao_apt_dep_data_wk_raw <- read_xlsx(
  path  = fs::path_abs(
    str_glue(base_file),
    start = base_dir),
  sheet = "ao_apt_dep_week",
  range = cell_limits(c(1, 1), c(NA, NA))) %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

ao_apt_dep_data_wk <- ao_apt_dep_data_wk_raw %>%
  mutate(FLIGHT = FLIGHT / 7) %>%
  spread(., key = FLAG_ROLLING_WEEK, value = FLIGHT) %>%
  arrange(AO_GRP_NAME, R_RANK) %>%
  mutate(
    WK_RANK_DIF_PREV_WEEK = case_when(
      is.na(RANK_PREV_WEEK) ~ RANK,
      .default = RANK_PREV_WEEK - RANK
    ),
    WK_FLT_DIF_PREV_WEEK_PERC =   case_when(
      PREV_ROLLING_WEEK == 0 | is.na(PREV_ROLLING_WEEK) ~ NA,
      .default = CURRENT_ROLLING_WEEK / PREV_ROLLING_WEEK - 1
    ),
    WK_FLT_DIF_PREV_YEAR_PERC = case_when(
      ROLLING_WEEK_PREV_YEAR == 0 | is.na(ROLLING_WEEK_PREV_YEAR) ~ NA,
      .default = CURRENT_ROLLING_WEEK / ROLLING_WEEK_PREV_YEAR - 1
    ),
    AO_GRP_RANK = paste0(tolower(AO_GRP_NAME), R_RANK)
  ) %>%
  rename(
    WK_APT_DEP_NAME = ADEP_NAME,
    WK_FROM_DATE = FROM_DATE,
    WK_TO_DATE = TO_DATE,
    WK_FLT_AVG = CURRENT_ROLLING_WEEK
  ) %>%
  select(
    AO_GRP_RANK,
    WK_RANK_DIF_PREV_WEEK,
    WK_APT_DEP_NAME,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_FLT_AVG,
    WK_FLT_DIF_PREV_WEEK_PERC,
    WK_FLT_DIF_PREV_YEAR_PERC
  )

#### y2d ----
ao_apt_dep_data_y2d_raw <- read_xlsx(
  path  = fs::path_abs(
    str_glue(base_file),
    start = base_dir),
  sheet = "ao_apt_dep_y2d",
  range = cell_limits(c(1, 1), c(NA, NA))) %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

ao_apt_dep_data_y2d <- ao_apt_dep_data_y2d_raw %>%
  mutate(
    FROM_DATE = max(FROM_DATE),
    TO_DATE = max(TO_DATE),
    PERIOD =   case_when(
      YEAR == max(YEAR) ~ 'CURRENT_YEAR',
      YEAR == max(YEAR) - 1 ~ 'PREV_YEAR',
      .default = paste0('PERIOD_', YEAR)
    )
  ) %>%
  select(-FLIGHT, -YEAR, -NO_DAYS) %>%
  spread(., key = PERIOD, value = AVG_FLIGHT) %>%
  arrange(AO_GRP_NAME, R_RANK) |>
  mutate(
    Y2D_RANK_DIF_PREV_YEAR = case_when(
      is.na(RANK_PREV_YEAR) ~ RANK,
      .default = RANK_PREV_YEAR - RANK
    ),
    Y2D_FLT_DIF_PREV_YEAR_PERC =   case_when(
      PREV_YEAR == 0 | is.na(PREV_YEAR) ~ NA,
      .default = CURRENT_YEAR / PREV_YEAR - 1
    ),
    Y2D_FLT_DIF_2019_PERC  = case_when(
      PERIOD_2019 == 0 | is.na(PERIOD_2019) ~ NA,
      .default = CURRENT_YEAR / PERIOD_2019 - 1
    ),
    AO_GRP_RANK = paste0(tolower(AO_GRP_NAME), R_RANK)
  ) %>%
  rename(
    Y2D_APT_DEP_NAME = ADEP_NAME,
    Y2D_TO_DATE = TO_DATE,
    Y2D_FLT_AVG = CURRENT_YEAR
  ) %>%
  select(
    AO_GRP_RANK,
    Y2D_RANK_DIF_PREV_YEAR,
    Y2D_APT_DEP_NAME,
    Y2D_TO_DATE,
    Y2D_FLT_AVG,
    Y2D_FLT_DIF_PREV_YEAR_PERC,
    Y2D_FLT_DIF_2019_PERC
  )

#### main card ----
ao_apt_dep_main_traffic <- ao_apt_dep_data_day_int %>%
  mutate(
    MAIN_TFC_APT_DEP_NAME = if_else(
      R_RANK <= 4,
      ADEP_NAME,
      NA
    ),
    MAIN_TFC_APT_DEP_FLT = if_else(
      R_RANK <= 4,
      CURRENT_DAY,
      NA
    ),
    AO_GRP_RANK = paste0(tolower(AO_GRP_NAME), R_RANK)
  ) %>%
  select(AO_GRP_RANK, MAIN_TFC_APT_DEP_NAME, MAIN_TFC_APT_DEP_FLT)

ao_apt_dep_main_traffic_dif <- ao_apt_dep_data_day_int %>%
  arrange(AO_GRP_NAME, desc(abs(AO_GRP_TFC_APT_DEP_DIF)), R_RANK) %>%
  group_by(AO_GRP_NAME) %>%
  mutate(RANK_DIF_APT_DEP_TFC = row_number()) %>%
  ungroup() %>%
  arrange(AO_GRP_NAME, R_RANK) %>%
  mutate(
    MAIN_TFC_DIF_APT_DEP_NAME = if_else(
      RANK_DIF_APT_DEP_TFC <= 4,
      ADEP_NAME,
      NA
    ),
    MAIN_TFC_DIF_APT_DEP_FLT_DIF = if_else(
      RANK_DIF_APT_DEP_TFC <= 4,
      AO_GRP_TFC_APT_DEP_DIF,
      NA
    )
  ) %>%
  arrange(AO_GRP_NAME, desc(MAIN_TFC_DIF_APT_DEP_FLT_DIF)) %>%
  group_by(AO_GRP_NAME) %>%
  mutate(
    RANK_MAIN_DIF = row_number(),
    AO_GRP_RANK = paste0(tolower(AO_GRP_NAME), RANK_MAIN_DIF)
  ) %>%
  ungroup() %>%
  select(AO_GRP_RANK, MAIN_TFC_DIF_APT_DEP_NAME, MAIN_TFC_DIF_APT_DEP_FLT_DIF)

#### join tables ----
# create list of ao_grp/rankings for left join
ao_grp_icao_ranking <- list()
i = 0
for (i in 1:10) {
  i = i + 1
  ao_grp_icao_ranking <- ao_grp_icao_ranking %>%
    bind_rows(ao_grp_icao, .)
}

ao_grp_icao_ranking <- ao_grp_icao_ranking %>%
  arrange(AO_GRP_NAME) %>%
  group_by(AO_GRP_NAME) %>%
  mutate(
    RANK = row_number(),
    AO_GRP_RANK = paste0(tolower(AO_GRP_NAME), RANK)
  )

# join and reorder tables
ao_apt_dep_data <- ao_grp_icao_ranking %>%
  left_join(ao_apt_dep_main_traffic, by = "AO_GRP_RANK") %>%
  left_join(ao_apt_dep_main_traffic_dif, by = "AO_GRP_RANK") %>%
  left_join(ao_apt_dep_data_day, by = "AO_GRP_RANK") %>%
  left_join(ao_apt_dep_data_wk, by = "AO_GRP_RANK") %>%
  left_join(ao_apt_dep_data_y2d, by = "AO_GRP_RANK") %>%
  ungroup() %>%
  select(-AO_GRP_RANK) %>%
  arrange (AO_GRP_CODE, RANK)

# covert to json and save in app data folder and archive
ao_apt_dep_data_j <- ao_apt_dep_data %>% toJSON(., pretty = TRUE)
write(ao_apt_dep_data_j, here(data_folder,"ao_apt_dep_ranking_traffic.json"))
write(ao_apt_dep_data_j, paste0(archive_dir, today, "_ao_apt_dep_ranking_traffic.json"))
write(ao_apt_dep_data_j, paste0(archive_dir, "ao_apt_dep_ranking_traffic.json"))

### Airport pair ----
#### day ----
ao_apt_pair_data_day_raw <- read_xlsx(
  path  = fs::path_abs(
    str_glue(base_file),
    start = base_dir),
  sheet = "ao_apt_pair_day",
  range = cell_limits(c(1, 1), c(NA, NA))) %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

ao_apt_pair_data_day_int <- ao_apt_pair_data_day_raw %>%
  mutate(TO_DATE = max(TO_DATE)) %>%
  spread(., key = FLAG_DAY, value = FLIGHT) %>%
  arrange(AO_GRP_NAME, R_RANK) %>%
  mutate(
    DY_RANK_DIF_PREV_WEEK = case_when(
      is.na(RANK_PREV_WEEK) ~ RANK,
      .default = RANK_PREV_WEEK - RANK
    ),
    DY_FLT_DIF_PREV_WEEK_PERC =   case_when(
      DAY_PREV_WEEK == 0 | is.na(DAY_PREV_WEEK) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_WEEK - 1
    ),
    DY_FLT_DIF_PREV_YEAR_PERC = case_when(
      DAY_PREV_YEAR == 0 | is.na(DAY_PREV_YEAR) ~ NA,
      .default = CURRENT_DAY / DAY_PREV_YEAR - 1
    ),
    AO_GRP_RANK = paste0(tolower(AO_GRP_NAME), R_RANK),
    AO_GRP_TFC_APT_PAIR_DIF = CURRENT_DAY - DAY_PREV_WEEK
  )

ao_apt_pair_data_day <- ao_apt_pair_data_day_int %>%
  rename(
    DY_APT_PAIR_NAME = AIRPORT_PAIR,
    DY_TO_DATE = TO_DATE,
    DY_FLT = CURRENT_DAY
  ) %>%
  select(
    AO_GRP_RANK,
    DY_RANK_DIF_PREV_WEEK,
    DY_APT_PAIR_NAME,
    DY_TO_DATE,
    DY_FLT,
    DY_FLT_DIF_PREV_WEEK_PERC,
    DY_FLT_DIF_PREV_YEAR_PERC
  )

#### week ----
ao_apt_pair_data_wk_raw <- read_xlsx(
  path  = fs::path_abs(
    str_glue(base_file),
    start = base_dir),
  sheet = "ao_apt_pair_week",
  range = cell_limits(c(1, 1), c(NA, NA))) %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

ao_apt_pair_data_wk <- ao_apt_pair_data_wk_raw %>%
  mutate(FLIGHT = FLIGHT / 7) %>%
  spread(., key = FLAG_ROLLING_WEEK, value = FLIGHT) %>%
  arrange(AO_GRP_NAME, R_RANK) %>%
  mutate(
    WK_RANK_DIF_PREV_WEEK = case_when(
      is.na(RANK_PREV_WEEK) ~ RANK,
      .default = RANK_PREV_WEEK - RANK
    ),
    WK_FLT_DIF_PREV_WEEK_PERC =   case_when(
      PREV_ROLLING_WEEK == 0 | is.na(PREV_ROLLING_WEEK) ~ NA,
      .default = CURRENT_ROLLING_WEEK / PREV_ROLLING_WEEK - 1
    ),
    WK_FLT_DIF_PREV_YEAR_PERC = case_when(
      ROLLING_WEEK_PREV_YEAR == 0 | is.na(ROLLING_WEEK_PREV_YEAR) ~ NA,
      .default = CURRENT_ROLLING_WEEK / ROLLING_WEEK_PREV_YEAR - 1
    ),
    AO_GRP_RANK = paste0(tolower(AO_GRP_NAME), R_RANK)
  ) %>%
  rename(
    WK_APT_PAIR_NAME = AIRPORT_PAIR,
    WK_FROM_DATE = FROM_DATE,
    WK_TO_DATE = TO_DATE,
    WK_FLT_AVG = CURRENT_ROLLING_WEEK
  ) %>%
  select(
    AO_GRP_RANK,
    WK_RANK_DIF_PREV_WEEK,
    WK_APT_PAIR_NAME,
    WK_FROM_DATE,
    WK_TO_DATE,
    WK_FLT_AVG,
    WK_FLT_DIF_PREV_WEEK_PERC,
    WK_FLT_DIF_PREV_YEAR_PERC
  )

#### y2d ----
ao_apt_pair_data_y2d_raw <- read_xlsx(
  path  = fs::path_abs(
    str_glue(base_file),
    start = base_dir),
  sheet = "ao_apt_pair_y2d",
  range = cell_limits(c(1, 1), c(NA, NA))) %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

ao_apt_pair_data_y2d <- ao_apt_pair_data_y2d_raw %>%
  mutate(
    FROM_DATE = max(FROM_DATE),
    TO_DATE = max(TO_DATE),
    PERIOD =   case_when(
      YEAR == max(YEAR) ~ 'CURRENT_YEAR',
      YEAR == max(YEAR) - 1 ~ 'PREV_YEAR',
      .default = paste0('PERIOD_', YEAR)
    )
  ) %>%
  select(-FLIGHT, -YEAR, -NO_DAYS) %>%
  spread(., key = PERIOD, value = AVG_FLIGHT) %>%
  arrange(AO_GRP_NAME, R_RANK) |>
  mutate(
    Y2D_RANK_DIF_PREV_YEAR = case_when(
      is.na(RANK_PREV_YEAR) ~ RANK,
      .default = RANK_PREV_YEAR - RANK
    ),
    Y2D_FLT_DIF_PREV_YEAR_PERC =   case_when(
      PREV_YEAR == 0 | is.na(PREV_YEAR) ~ NA,
      .default = CURRENT_YEAR / PREV_YEAR - 1
    ),
    Y2D_FLT_DIF_2019_PERC  = case_when(
      PERIOD_2019 == 0 | is.na(PERIOD_2019) ~ NA,
      .default = CURRENT_YEAR / PERIOD_2019 - 1
    ),
    AO_GRP_RANK = paste0(tolower(AO_GRP_NAME), R_RANK)
  ) %>%
  rename(
    Y2D_APT_PAIR_NAME = AIRPORT_PAIR,
    Y2D_TO_DATE = TO_DATE,
    Y2D_FLT_AVG = CURRENT_YEAR
  ) %>%
  select(
    AO_GRP_RANK,
    Y2D_RANK_DIF_PREV_YEAR,
    Y2D_APT_PAIR_NAME,
    Y2D_TO_DATE,
    Y2D_FLT_AVG,
    Y2D_FLT_DIF_PREV_YEAR_PERC,
    Y2D_FLT_DIF_2019_PERC
  )

#### main card ----
ao_apt_pair_main_traffic <- ao_apt_pair_data_day_int %>%
  mutate(
    MAIN_TFC_APT_PAIR_NAME = if_else(
      R_RANK <= 4,
      AIRPORT_PAIR,
      NA
    ),
    MAIN_TFC_APT_PAIR_FLT = if_else(
      R_RANK <= 4,
      CURRENT_DAY,
      NA
    ),
    AO_GRP_RANK = paste0(tolower(AO_GRP_NAME), R_RANK)
  ) %>%
  select(AO_GRP_RANK, MAIN_TFC_APT_PAIR_NAME, MAIN_TFC_APT_PAIR_FLT)

ao_apt_pair_main_traffic_dif <- ao_apt_pair_data_day_int %>%
  arrange(AO_GRP_NAME, desc(abs(AO_GRP_TFC_APT_PAIR_DIF)), R_RANK) %>%
  group_by(AO_GRP_NAME) %>%
  mutate(RANK_DIF_APT_PAIR_TFC = row_number()) %>%
  ungroup() %>%
  arrange(AO_GRP_NAME, R_RANK) %>%
  mutate(
    MAIN_TFC_DIF_APT_PAIR_NAME = if_else(
      RANK_DIF_APT_PAIR_TFC <= 4,
      AIRPORT_PAIR,
      NA
    ),
    MAIN_TFC_DIF_APT_PAIR_FLT_DIF = if_else(
      RANK_DIF_APT_PAIR_TFC <= 4,
      AO_GRP_TFC_APT_PAIR_DIF,
      NA
    )
  ) %>%
  arrange(AO_GRP_NAME, desc(MAIN_TFC_DIF_APT_PAIR_FLT_DIF)) %>%
  group_by(AO_GRP_NAME) %>%
  mutate(
    RANK_MAIN_DIF = row_number(),
    AO_GRP_RANK = paste0(tolower(AO_GRP_NAME), RANK_MAIN_DIF)
  ) %>%
  ungroup() %>%
  select(AO_GRP_RANK, MAIN_TFC_DIF_APT_PAIR_NAME, MAIN_TFC_DIF_APT_PAIR_FLT_DIF)

#### join tables ----
# create list of ao_grp/rankings for left join
ao_grp_icao_ranking <- list()
i = 0
for (i in 1:10) {
  i = i + 1
  ao_grp_icao_ranking <- ao_grp_icao_ranking %>%
    bind_rows(ao_grp_icao, .)
}

ao_grp_icao_ranking <- ao_grp_icao_ranking %>%
  arrange(AO_GRP_NAME) %>%
  group_by(AO_GRP_NAME) %>%
  mutate(
    RANK = row_number(),
    AO_GRP_RANK = paste0(tolower(AO_GRP_NAME), RANK)
  )

# join and reorder tables
ao_apt_pair_data <- ao_grp_icao_ranking %>%
  left_join(ao_apt_pair_main_traffic, by = "AO_GRP_RANK") %>%
  left_join(ao_apt_pair_main_traffic_dif, by = "AO_GRP_RANK") %>%
  left_join(ao_apt_pair_data_day, by = "AO_GRP_RANK") %>%
  left_join(ao_apt_pair_data_wk, by = "AO_GRP_RANK") %>%
  left_join(ao_apt_pair_data_y2d, by = "AO_GRP_RANK") %>%
  ungroup() %>%
  select(-AO_GRP_RANK) %>%
  arrange (AO_GRP_CODE, RANK) |>
  mutate_all( ~str_replace_all(., "<->", " ⟷ "))

# covert to json and save in app data folder and archive
ao_apt_pair_data_j <- ao_apt_pair_data %>% toJSON(., pretty = TRUE)
write(ao_apt_pair_data_j, here(data_folder,"ao_apt_pair_ranking_traffic.json"))
write(ao_apt_pair_data_j, paste0(archive_dir, today, "_ao_apt_pair_ranking_traffic.json"))
write(ao_apt_pair_data_j, paste0(archive_dir, "ao_apt_pair_ranking_traffic.json"))
