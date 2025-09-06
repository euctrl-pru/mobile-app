library(arrow)
library(duckdb)
library(duckplyr)
library(lubridate)
library(here)
library(RODBC)

source(here::here("..", "mobile-app", "R", "helpers.R"))
source(here::here("..", "mobile-app", "R", "params.R"))
source(here::here("..", "mobile-app", "R", "dimension_queries.R"))
source(here::here("..", "mobile-app", "R", "duckdb_functions.R"))

# parameters ----
if (!exists("data_day_date")) {current_day <- today() - days(1)} else {current_day <- data_day_date}

# DIMENSIONS ----
## ao group ----  
dim_ao_group <- export_query(dim_ao_grp_query) 

## airport ----
dim_airport <- export_query(dim_ap_query) 

## iso country ----
dim_iso_country <- export_query(dim_iso_st_query) %>% 
  mutate(
    COUNTRY_NAME = if_else(ISO_COUNTRY_CODE == "ES",
                           "Spain Continental",
                           COUNTRY_NAME)
  ) %>% 
  add_row(ISO_COUNTRY_CODE = "IC", COUNTRY_NAME = "Spain Canaries")

list_iso_country <- export_query(list_iso_st_query)

## icao country ----
list_icao_country <- export_query(list_icao_st_query)

# prep data functions ----
import_dataframe <- function(dfname, con = con) {
  # import data 
  # dfname <- "st_dai"
  mydataframe <- dfname
  myparquetfile <- paste0(mydataframe, "_day_base.parquet")
  
  # con <- duck_open()
  df_base <- duck_ingest_parquet(con, here::here(archive_dir_raw, myparquetfile))  # now eager by default
 
  # df_base <- read_parquet_duckdb(here(archive_dir_raw, 
  #                                     myparquetfile)
  # ) 
  
  # filter to keep days and airports needed
  if ("ISO_CT_CODE" %in% names(df_base)) {
    df_alldays <- df_base %>% 
      filter(ISO_CT_CODE %in% list_iso_country$ISO_COUNTRY_CODE)  
  } else if ("ISO_CT_CODE1" %in% names(df_base)) {
    df_alldays <- df_base %>% 
      filter(ISO_CT_CODE1 %in% list_iso_country$ISO_COUNTRY_CODE)  
    
  } else if ("COUNTRY_CODE" %in% names(df_base)) {
    df_alldays <- df_base %>% 
      filter(COUNTRY_CODE %in% list_icao_country$COUNTRY_CODE)  
    
  }

  # pre-process data
  if (dfname == "st_ao") {
    df_app <- df_alldays %>% 
      filter(AO_ID != 1777) %>%  # undefined
      compute(prudence = "lavish") %>%
      left_join(dim_ao_group, by = c("AO_ID", "AO_CODE")) %>% 
      summarise(
        FLIGHT = sum(FLIGHT, na.rm = TRUE),
        .by = c(ENTRY_DATE, ISO_CT_CODE, AO_GRP_CODE, AO_GRP_NAME)
      )
    
  } else if (dfname == "st_ap") {
    df_app <- df_alldays %>% 
      filter(ARP_ICAO_CODE != "ZZZZ") %>%  
      left_join(dim_airport, by = c("ARP_PRU_ID" = "APT_ID", "ARP_ICAO_CODE" = "APT_ICAO_CODE")) %>% 
      summarise(
        DEP_ARR = sum(MVT, na.rm = TRUE),
        .by = c(FLIGHT_DATE, ISO_CT_CODE, ARP_ICAO_CODE, ARP_PRU_ID, APT_NAME)
      )
    
  } else {
    df_app <- df_alldays
  }

  return(df_app) 
}

# st daio ----
st_daio <- function() {
  mydataframe <-  "st_daio"
  mydataquery <- paste0(mydataframe, "_day_query")
  mydatafile <- paste0(mydataframe, "_day_raw.parquet")
  stakeholder <- str_sub(mydataframe, 1, 2)

  
  df_day <- export_query(st_daio_day_query)
  df_day %>% write_parquet(here(archive_dir_raw, stakeholder, mydatafile))
  
  print(paste(format(now(), "%H:%M:%S"), mydataframe))

}

# st dai ----
st_dai <- function() {
  mydataframe <-  "st_dai"
  mydatafile <- paste0(mydataframe, "_day_raw.parquet")
  stakeholder <- str_sub(mydataframe, 1, 2)
  
  con <- duck_open()
  df_app <- import_dataframe(mydataframe, con = con)
  
  mydate <- max(df_app$FLIGHT_DATE, na.rm = TRUE)
  current_year = year(mydate)
  
  #### create date sequence
  year_from <- paste0(2018, "-12-24")
  year_til <- paste0(current_year, "-12-31")
  days_sequence <- seq(ymd(year_from), ymd(year_til), by = "1 day") %>%
    as_tibble() %>%
    select(FLIGHT_DATE = value) 
  
  #### combine with ansp list to get full sequence
  days_icao_st <- crossing(days_sequence, list_icao_country) %>% 
    arrange(COUNTRY_CODE, FLIGHT_DATE)%>% 
    mutate(YEAR = year(FLIGHT_DATE))
  
  df_day_year <- days_icao_st %>%
    left_join(df_app, by = c("YEAR", "FLIGHT_DATE", "COUNTRY_CODE")) %>% 
    arrange(COUNTRY_CODE, FLIGHT_DATE) %>%
    group_by(COUNTRY_CODE) %>% 
    mutate(
      #rolling week
      RWK_AVG_TFC = rollsum(DAY_TFC, 7, fill = NA, align = "right") / 7
    )  %>% 
    group_by(COUNTRY_CODE, YEAR) %>% 
    # arrange(AO_GRP_CODE, FLIGHT_DATE) %>% 
    mutate(
      # year to date
      Y2D_TFC_YEAR = cumsum(coalesce(DAY_TFC, 0)),
      Y2D_AVG_TFC_YEAR = cumsum(coalesce(DAY_TFC, 0)) / row_number()
    ) %>% 
    ungroup ()
  
  ## split table ----
  df_day <- df_day_year %>% 
    mutate(
      FLIGHT_DATE_2019 = FLIGHT_DATE - days((YEAR-2019)*364+ floor((YEAR - 2019) / 4) * 7),
      FLIGHT_DATE_2020 = FLIGHT_DATE - days((YEAR-2020)*364+ floor((YEAR - 2020) / 4) * 7),
      FLIGHT_DATE_2019_SD = FLIGHT_DATE %m-% years(YEAR-2019),
      FLIGHT_DATE_PREV_YEAR_SD = FLIGHT_DATE %m-% years(1) 
    ) %>% 
    left_join(select(df_day_year, COUNTRY_CODE, YEAR, FLIGHT_DATE, starts_with(c("Y2D"))), by = c("COUNTRY_CODE", "FLIGHT_DATE_PREV_YEAR_SD" = "FLIGHT_DATE"), suffix = c("","_PREV_YEAR")) %>% 
    left_join(select(df_day_year, COUNTRY_CODE, YEAR, FLIGHT_DATE, starts_with(c("Y2D"))), by = c("COUNTRY_CODE", "FLIGHT_DATE_2019_SD" = "FLIGHT_DATE"), suffix = c("","_2019")) %>% 
    left_join(select(df_day_year, COUNTRY_CODE, YEAR, FLIGHT_DATE, starts_with(c("DAY", "RWK"))), by = c("COUNTRY_CODE", "FLIGHT_DATE_2019" = "FLIGHT_DATE"), suffix = c("","_2019")) %>% 
    left_join(select(df_day_year, COUNTRY_CODE, YEAR, FLIGHT_DATE, RWK_AVG_TFC), by = c("COUNTRY_CODE", "FLIGHT_DATE_2020" = "FLIGHT_DATE"), suffix = c("","_2020")) %>% 
    arrange(COUNTRY_NAME, FLIGHT_DATE) %>% 
    group_by(COUNTRY_NAME) %>% 
    mutate(
      # prev week
      FLIGHT_DATE_PREV_WEEK = lag(FLIGHT_DATE, 7),
      DAY_TFC_PREV_WEEK = lag(DAY_TFC , 7),
      RWK_AVG_TFC_PREV_WEEK = lag(RWK_AVG_TFC, 7),
      
      # dif prev week
      DAY_TFC_DIF_PREV_WEEK = coalesce(DAY_TFC,0) - coalesce(DAY_TFC_PREV_WEEK, 0),
      DAY_TFC_DIF_PREV_WEEK_PERC = if_else(DAY_TFC_PREV_WEEK == 0, NA, DAY_TFC/ DAY_TFC_PREV_WEEK) -1,
      
      RWK_TFC_DIF_PREV_WEEK_PERC = if_else(RWK_AVG_TFC_PREV_WEEK == 0, NA, RWK_AVG_TFC/ RWK_AVG_TFC_PREV_WEEK)-1,
      
      # prev year
      FLIGHT_DATE_PREV_YEAR = lag(FLIGHT_DATE, 364),
      DAY_TFC_PREV_YEAR = lag(DAY_TFC , 364),
      RWK_AVG_TFC_PREV_YEAR = lag(RWK_AVG_TFC, 364),
      Y2D_TFC_PREV_YEAR = Y2D_TFC_YEAR_PREV_YEAR,
      Y2D_AVG_TFC_PREV_YEAR = Y2D_AVG_TFC_YEAR_PREV_YEAR,

      # dif prev year
      DAY_TFC_DIF_PREV_YEAR = coalesce(DAY_TFC, 0) - coalesce(DAY_TFC_PREV_YEAR, 0),
      DAY_TFC_DIF_PREV_YEAR_PERC = if_else(DAY_TFC_PREV_YEAR == 0, NA, DAY_TFC/ DAY_TFC_PREV_YEAR)-1,
      RWK_TFC_DIF_PREV_YEAR_PERC = if_else(RWK_AVG_TFC_PREV_YEAR == 0, NA, RWK_AVG_TFC/ RWK_AVG_TFC_PREV_YEAR)-1,
      Y2D_TFC_DIF_PREV_YEAR_PERC = if_else(Y2D_AVG_TFC_PREV_YEAR == 0, NA, Y2D_AVG_TFC_YEAR/ Y2D_AVG_TFC_PREV_YEAR)-1,
      
      # 2020
      FLIGHT_DATE_2020 = FLIGHT_DATE_2020,
      RWK_AVG_TFC_2020 = RWK_AVG_TFC_2020,
      
      # 2019
      FLIGHT_DATE_2019 = FLIGHT_DATE_2019,
      
      DAY_TFC_2019 = DAY_TFC_2019,
      RWK_AVG_TFC_2019 = RWK_AVG_TFC_2019,
      Y2D_TFC_2019 = Y2D_TFC_YEAR_2019,
      Y2D_AVG_TFC_2019 = Y2D_AVG_TFC_YEAR_2019,
      
      # dif 2019
      DAY_TFC_DIF_2019 = coalesce(DAY_TFC, 0) - coalesce(DAY_TFC_2019, 0),
      DAY_TFC_DIF_2019_PERC = if_else(DAY_TFC_2019 == 0, NA, DAY_TFC/ DAY_TFC_2019)-1,
      RWK_TFC_DIF_2019_PERC = if_else(RWK_AVG_TFC_2019 == 0, NA, RWK_AVG_TFC/ RWK_AVG_TFC_2019)-1,
      Y2D_TFC_DIF_2019_PERC = if_else(Y2D_AVG_TFC_2019 == 0, NA, Y2D_AVG_TFC_YEAR/ Y2D_AVG_TFC_2019)-1,
      
      LAST_DATA_DAY = mydate
    ) %>% 
    # iceland case
    mutate(across(
        c(
          DAY_TFC_2019, 
          DAY_TFC_DIF_2019, 
          DAY_TFC_DIF_2019_PERC,
          
          RWK_AVG_TFC_2019, 
          RWK_AVG_TFC_2020,
          RWK_TFC_DIF_2019_PERC,
          
          Y2D_TFC_2019,
          Y2D_AVG_TFC_2019,
          Y2D_TFC_DIF_2019_PERC,
        ),
        ~ if_else(tolower(COUNTRY_NAME) == "iceland", NA, .x)
      )
    ) %>% 
    mutate(across(
      c(
        DAY_TFC_PREV_YEAR,
        DAY_TFC_DIF_PREV_YEAR,
        DAY_TFC_DIF_PREV_YEAR_PERC,

        RWK_AVG_TFC_PREV_YEAR,
        RWK_TFC_DIF_PREV_YEAR_PERC,

        Y2D_TFC_PREV_YEAR,
        Y2D_AVG_TFC_PREV_YEAR,
        Y2D_TFC_DIF_PREV_YEAR_PERC,
      ),
      ~ if_else(tolower(COUNTRY_NAME) == "iceland" & YEAR < 2025, NA, .x)
    )
    ) %>% 
    select(
      COUNTRY_NAME,
      YEAR,
      MONTH,
      WEEK,
      WEEK_NB_YEAR,
      DAY_TYPE,
      DAY_OF_WEEK,

      FLIGHT_DATE,
      FLIGHT_DATE_PREV_WEEK,
      FLIGHT_DATE_PREV_YEAR,
      FLIGHT_DATE_2020,
      FLIGHT_DATE_2019,
      
      DAY_TFC,
      DAY_TFC_PREV_WEEK,
      DAY_TFC_PREV_YEAR,
      DAY_TFC_2019,
      DAY_TFC_DIFF_PREV_WEEK = DAY_TFC_DIF_PREV_WEEK,
      DAY_TFC_DIFF_PREV_YEAR = DAY_TFC_DIF_PREV_YEAR,
      DAY_TFC_DIFF_2019 = DAY_TFC_DIF_2019,
      DAY_TFC_PREV_WEEK_PERC = DAY_TFC_DIF_PREV_WEEK_PERC,
      DAY_DIFF_PREV_YEAR_PERC = DAY_TFC_DIF_PREV_YEAR_PERC,
      DAY_TFC_DIFF_2019_PERC = DAY_TFC_DIF_2019_PERC,
      
      AVG_ROLLING_WEEK = RWK_AVG_TFC,
      AVG_ROLLING_PREV_WEEK = RWK_AVG_TFC_PREV_WEEK,
      AVG_ROLLING_WEEK_PREV_YEAR = RWK_AVG_TFC_PREV_YEAR,
      AVG_ROLLING_WEEK_2020 = RWK_AVG_TFC_2020,
      AVG_ROLLING_WEEK_2019 = RWK_AVG_TFC_2019,
      DIF_WEEK_PREV_YEAR_PERC = RWK_TFC_DIF_PREV_YEAR_PERC,
      DIF_ROLLING_WEEK_2019_PERC = RWK_TFC_DIF_2019_PERC,
      
      Y2D_TFC_YEAR,
      Y2D_TFC_PREV_YEAR,
      Y2D_TFC_2019,
      Y2D_AVG_TFC_YEAR,
      Y2D_AVG_TFC_PREV_YEAR,
      Y2D_AVG_TFC_2019,
      Y2D_DIFF_PREV_YEAR_PERC = Y2D_TFC_DIF_PREV_YEAR_PERC,
      Y2D_DIFF_2019_PERC = Y2D_TFC_DIF_2019_PERC,
      
      LAST_DATA_DAY
    ) %>% 
    ungroup() %>% 
    arrange(COUNTRY_NAME, FLIGHT_DATE)
  
  df_day %>% write_parquet(here(archive_dir_raw, stakeholder, mydatafile))
  
  print(paste(format(now(), "%H:%M:%S"), mydataframe, mydate))
  duck_close(con, clean = TRUE)
  
  # test <- df_day %>% filter(YEAR == 2024)
    # filter(FLIGHT_DATE >= ymd(20240106)) # %>%
    # filter(AO_GRP_CODE == 'AEA') %>%
    # select(AO_GRP_CODE, FLIGHT_DATE,FLIGHT_DATE_PREV_YEAR, FLIGHT_DATE_2019, Y2D_TFC_YEAR,	Y2D_TFC_PREV_YEAR,	Y2D_TFC_2019)


  # test %>% write_csv(here(archive_dir_raw,"test.csv"))
  
  # # dcheck
  # dfc <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(st_base_file),
  #     start = st_base_dir),
  #   sheet = "state_dai",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  # dfc <- read_csv(here(archive_dir_raw, stakeholder, "20241231_state_dai_raw.csv"), show_col_types = FALSE)
  # 
  # df <- dfc %>% arrange(COUNTRY_NAME, FLIGHT_DATE)
    # filter(YEAR == 2025) %>%
    # filter(FLIGHT_DATE >= ymd(20240106))
    # filter(AO_GRP_CODE == 'AEA') %>%
    # select(AO_GRP_CODE, FLIGHT_DATE,FLIGHT_DATE_PREV_YEAR, FLIGHT_DATE_2019, Y2D_TFC_YEAR,	Y2D_TFC_PREV_YEAR,	Y2D_TFC_2019)
  
  # colnames(df)
# for (i in 1:nrow(list_icao_country)) {
#   df1 <- df %>% filter(tolower(COUNTRY_NAME) == tolower(list_icao_country$COUNTRY_NAME[i])) 
#   # %>% filter(FLIGHT_DATE <= ymd(20250905) & FLIGHT_DATE != ymd(20250228))
# 
# 
#   df_day1 <- test %>% filter(COUNTRY_NAME == list_icao_country$COUNTRY_NAME[i]) 
#   # %>% filter(FLIGHT_DATE <= ymd(20250905) & FLIGHT_DATE != ymd(20250228))
# 
#   # print(paste(list_ao_group$AO_GRP_CODE[i], all.equal(df1, df_day1)))
#   # nrow(df1)
#   for (j in 1:nrow(df1)) {
#     for (t in 13:37){
#       if (((coalesce(df1[[j,t]],0) - coalesce(df_day1[[j,t]],0))<10^-8) == FALSE) {
#         print(paste(t,df1[[j,1]], df1[[j,8]], (df1[[j,t]] - df_day1[[j,t]])<10^-8))
#         break}
#       # print(paste(t,df1[[j,1]], df1[[j,8]], (df1[[j,t]] - df_day1[[j,t]])<10^-8))
#     }
#   }
#   print(paste(t,df1[[j,1]], df1[[j,8]], (df1[[j,t]] - df_day1[[j,t]])<10^-8))
# 
# }

}

# st ao grp ----
st_ao <- function(mydate =  current_day) {
  mydataframe <-  "st_ao"
  
  con <- duck_open()
  df_app <- import_dataframe(mydataframe, con = con)

    # mydate <- current_day
  data_day_text <- mydate %>% format("%Y%m%d")
  day_prev_week <- mydate + days(-7)
  day_prev_year <- mydate + days(-364)
  day_2019 <- mydate - days(364 * (year(mydate) - 2019) + floor((year(mydate) - 2019) / 4) * 7)
  current_year = year(mydate)
  
  stakeholder <- str_sub(mydataframe, 1, 2)
  
  ## day ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_day_raw.csv")

  df_day <- df_app %>%
    filter(ENTRY_DATE %in% c(mydate, day_prev_week, day_2019, day_prev_year)) %>% 
    left_join(list_iso_country, by = c("ISO_CT_CODE" = "ISO_COUNTRY_CODE")) %>% 
    group_by(ISO_CT_CODE, ENTRY_DATE) %>%
    arrange(ISO_CT_CODE, ENTRY_DATE, desc(FLIGHT), AO_GRP_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        ENTRY_DATE == mydate ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        ENTRY_DATE == mydate ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        ENTRY_DATE == day_prev_week ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      FLAG_DAY = case_when( 
        ENTRY_DATE == mydate ~ "CURRENT_DAY",
        ENTRY_DATE == day_prev_week ~ "DAY_PREV_WEEK",
        ENTRY_DATE == day_2019 ~ "DAY_2019",
        ENTRY_DATE == day_prev_year ~ "DAY_PREV_YEAR",
      )
    ) %>% 
    ungroup() %>% 
    group_by(COUNTRY_NAME, AO_GRP_NAME) %>% 
    arrange(COUNTRY_NAME, AO_GRP_NAME, desc(ENTRY_DATE)) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    # Iceland case
    mutate(
      AO_GRP_CODE = if_else(year(ENTRY_DATE)<2024 & tolower(COUNTRY_NAME) == 'iceland', NA,AO_GRP_CODE),
      AO_GRP_NAME = if_else(year(ENTRY_DATE)<2024 & tolower(COUNTRY_NAME) == 'iceland', NA,AO_GRP_NAME),
      FLIGHT = if_else(year(ENTRY_DATE)<2024 & tolower(COUNTRY_NAME) == 'iceland', NA,FLIGHT)
    ) %>% 
    # mutate(LAST_DATA_DAY = max(ENTRY_DATE)) %>% 
    select(
      COUNTRY_NAME,
      FLAG_DAY,
      AO_GRP_CODE,
      AO_GRP_NAME,
      FLIGHT_WITHOUT_OVERFLIGHT = FLIGHT,
      R_RANK,
      RANK,
      RANK_PREV_WEEK = RANK_PREV,
      TO_DATE = ENTRY_DATE
      
    ) %>% 
    arrange(COUNTRY_NAME, FLAG_DAY, R_RANK, AO_GRP_NAME)
  
  df_day %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))

  # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(st_base_file),
  #     start = st_base_dir),
  #   sheet = "state_ao_day",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(COUNTRY_NAME, FLAG_DAY, R_RANK, AO_GRP_NAME)
  # 
  # 
  # # nrow(list_iso_country)
  # for (i in 1:nrow(list_iso_country)) {
  #   df1 <- df %>% 
  #     mutate(COUNTRY_NAME = tolower(COUNTRY_NAME)) %>% 
  #     filter(COUNTRY_NAME == tolower(list_iso_country$COUNTRY_NAME[i]))
  # 
  #   df_day1 <- df_day %>% 
  #     mutate(COUNTRY_NAME = tolower(COUNTRY_NAME)) %>% 
  #     filter(COUNTRY_NAME == tolower(list_iso_country$COUNTRY_NAME[i]))
  # 
  #   print(paste(list_iso_country$COUNTRY_NAME[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   #
  #   #   print(paste(j, df1[[j,4]] == df_day1[[j,4]]))
  #   #
  #   # }
  # }

## week ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_week_raw.csv")

  df_week <- df_app %>%
    filter(
      ENTRY_DATE %in% c(seq.Date(mydate-6, mydate)) |
        ENTRY_DATE %in% c(seq.Date(day_prev_week-6, day_prev_week))|
        ENTRY_DATE %in% c(seq.Date(day_2019-6, day_2019)) |
        ENTRY_DATE %in% c(seq.Date(day_prev_year-6, day_prev_year))
    ) %>% 
    compute(prudence = "lavish") %>%
    mutate(
      PERIOD_TYPE = case_when( 
        (ENTRY_DATE >= mydate - days(6) & ENTRY_DATE <= mydate) ~ "CURRENT_ROLLING_WEEK",
        (ENTRY_DATE >= day_prev_week - days(6) & ENTRY_DATE <= day_prev_week) ~ "PREV_ROLLING_WEEK",
        (ENTRY_DATE >= day_2019 - days(6) & ENTRY_DATE <= day_2019) ~ "ROLLING_WEEK_2019",
        (ENTRY_DATE >= day_prev_year - days(6) & ENTRY_DATE <= day_prev_year) ~ "ROLLING_WEEK_PREV_YEAR"
      )
    ) %>% 
    summarise(
      FLIGHT = sum(FLIGHT, na.rm = TRUE),
      # TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      # FROM_DATE = min(ENTRY_DATE, na.rm = TRUE),
      .by = c(PERIOD_TYPE, ISO_CT_CODE, AO_GRP_NAME, AO_GRP_CODE)
    ) %>% 
    left_join(list_iso_country, by = c("ISO_CT_CODE" = "ISO_COUNTRY_CODE")) %>% 
    ungroup() %>% 
    group_by(ISO_CT_CODE, PERIOD_TYPE) %>%
    arrange(ISO_CT_CODE, PERIOD_TYPE, desc(FLIGHT), AO_GRP_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        PERIOD_TYPE == "CURRENT_ROLLING_WEEK" ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        PERIOD_TYPE == "CURRENT_ROLLING_WEEK" ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        PERIOD_TYPE == "PREV_ROLLING_WEEK" ~ min_rank(desc(FLIGHT)),
        .default = NA
      )
    ) %>% 
    ungroup() %>% 
    group_by(COUNTRY_NAME, AO_GRP_NAME) %>% 
    arrange(COUNTRY_NAME, AO_GRP_NAME, PERIOD_TYPE) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(
      FROM_DATE = mydate - days(6),
      TO_DATE = mydate
      ) %>% 
    # Iceland case
    mutate(
      AO_GRP_CODE = if_else((PERIOD_TYPE == "ROLLING_WEEK_2019" | 
                               (PERIOD_TYPE == "ROLLING_WEEK_PREV_YEAR" & year(TO_DATE) < 2025)) & 
                              tolower(COUNTRY_NAME) == 'iceland', 
                            NA,
                            AO_GRP_CODE),
      AO_GRP_NAME = if_else((PERIOD_TYPE == "ROLLING_WEEK_2019" | 
                               (PERIOD_TYPE == "ROLLING_WEEK_PREV_YEAR" & year(TO_DATE) < 2025)) & 
                              tolower(COUNTRY_NAME) == 'iceland', 
                            NA,
                            AO_GRP_NAME),

      FLIGHT = if_else((PERIOD_TYPE == "ROLLING_WEEK_2019" | 
                               (PERIOD_TYPE == "ROLLING_WEEK_PREV_YEAR" & year(TO_DATE) < 2025)) & 
                              tolower(COUNTRY_NAME) == 'iceland', 
                            NA,
                       FLIGHT)
      ) %>% 
    select(
      COUNTRY_NAME,
      FLAG_ROLLING_WEEK = PERIOD_TYPE,
      AO_GRP_CODE,
      AO_GRP_NAME,
      FLIGHT_WITHOUT_OVERFLIGHT = FLIGHT,
      R_RANK,
      RANK,
      RANK_PREV_WEEK = RANK_PREV,
      FROM_DATE,
      TO_DATE
      
    ) %>% 
    arrange(COUNTRY_NAME, FLAG_ROLLING_WEEK, R_RANK, AO_GRP_NAME)
  
  df_week %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # test2 <- df_week %>% filter(COUNTRY_NAME == 'Iceland')
  
  # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(st_base_file),
  #     start = st_base_dir),
  #   sheet = "state_ao_week",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(COUNTRY_NAME, FLAG_ROLLING_WEEK, R_RANK, AO_GRP_NAME)
  # 
  # 
  # # nrow(list_iso_country)
  # for (i in 1:nrow(list_iso_country)) {
  #   df1 <- df %>%
  #     mutate(COUNTRY_NAME = tolower(COUNTRY_NAME)) %>%
  #     filter(COUNTRY_NAME == tolower(list_iso_country$COUNTRY_NAME[i]))
  # 
  #   df_day1 <- df_week %>%
  #     mutate(COUNTRY_NAME = tolower(COUNTRY_NAME)) %>%
  #     filter(COUNTRY_NAME == tolower(list_iso_country$COUNTRY_NAME[i]))
  # 
  #   print(paste(list_iso_country$COUNTRY_NAME[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   #
  #   #   print(paste(j, df1[[j,4]] == df_day1[[j,4]]))
  #   #
  #   # }
  # }
  
  ## year ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_y2d_raw.csv")

  y2d_dates <- seq.Date(ymd(paste0(2019,"01","01")),
                        mydate) %>% as_tibble() %>% 
    filter(year(value) %in% c(2019, current_year-1, current_year)) %>%
    filter(format(value, "%m-%d") <= format(mydate, "%m-%d")) %>% 
    pull()
  
  df_y2d <- df_app %>% 
    compute(prudence = "lavish") %>%
    mutate(
      YEAR = year(ENTRY_DATE)
    ) %>% 
    filter(YEAR %in% c(current_year, current_year-1, 2019)) %>% 
    filter(ENTRY_DATE %in% y2d_dates) %>% 
    # filter(DEP_ARP_PRU_ID == 4467) %>% 
    group_by(YEAR) %>% 
    mutate(
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE)
    ) %>% 
    ungroup() %>% 
    summarise(
      FLIGHT = sum(FLIGHT, na.rm = TRUE),
      .by = c(YEAR, ISO_CT_CODE, AO_GRP_NAME, AO_GRP_CODE, TO_DATE, FROM_DATE)
    ) %>% 
    left_join(list_iso_country, by = c("ISO_CT_CODE" = "ISO_COUNTRY_CODE")) %>% 
    group_by(ISO_CT_CODE, YEAR) %>%
    arrange(ISO_CT_CODE, YEAR, desc(FLIGHT), AO_GRP_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        YEAR == current_year ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        YEAR == current_year ~ min_rank(desc(FLIGHT)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        YEAR == current_year-1 ~ min_rank(desc(FLIGHT)),
        .default = NA
      )
    ) %>% 
    ungroup() %>% 
    group_by(COUNTRY_NAME, AO_GRP_NAME) %>% 
    arrange(COUNTRY_NAME, AO_GRP_NAME, YEAR) %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "up") %>% 
    
    fill(RANK, .direction = "down") %>% 
    fill(RANK, .direction = "up") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    # Iceland case
    mutate(
      AO_GRP_CODE = if_else(year(TO_DATE)<2024 & tolower(COUNTRY_NAME) == 'iceland', NA,AO_GRP_CODE),
      AO_GRP_NAME = if_else(year(TO_DATE)<2024 & tolower(COUNTRY_NAME) == 'iceland', NA,AO_GRP_NAME),
      FLIGHT = if_else(year(TO_DATE)<2024 & tolower(COUNTRY_NAME) == 'iceland', NA,FLIGHT)
    ) %>% 
    mutate(
      NO_DAYS = as.numeric(TO_DATE - FROM_DATE) +1,
      AVG_FLT = FLIGHT/NO_DAYS
    ) %>%
    select(
      COUNTRY_NAME,
      YEAR,
      AO_GRP_CODE,
      AO_GRP_NAME,
      FLIGHT_WITHOUT_OVERFLIGHT = FLIGHT,	
      AVG_FLT,
      R_RANK,
      RANK_CURRENT = RANK,
      RANK_PREV_YEAR = RANK_PREV,
      FROM_DATE,
      TO_DATE
      
    ) %>% 
    arrange(COUNTRY_NAME, desc(YEAR), R_RANK, AO_GRP_NAME)
  
  
  df_y2d %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(st_base_file),
  #     start = st_base_dir),
  #   sheet = "state_ao_y2d",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(COUNTRY_NAME, desc(YEAR), R_RANK, AO_GRP_NAME)
  # 
  # 
  # # nrow(list_iso_country)
  # for (i in 1:nrow(list_iso_country)) {
  #   df1 <- df %>%
  #     mutate(COUNTRY_NAME = tolower(COUNTRY_NAME)) %>%
  #     filter(COUNTRY_NAME == tolower(list_iso_country$COUNTRY_NAME[i]))
  # 
  #   df_day1 <- df_y2d %>%
  #     mutate(COUNTRY_NAME = tolower(COUNTRY_NAME)) %>%
  #     filter(COUNTRY_NAME == tolower(list_iso_country$COUNTRY_NAME[i]))
  # 
  #   print(paste(list_iso_country$COUNTRY_NAME[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   #
  #   #   print(paste(j, df1[[j,4]] == df_day1[[j,4]]))
  #   #
  #   # }
  # }

  print(paste(now(), mydataframe, mydate))
  
  duck_close(con, clean = TRUE)
}

# st st ----
st_st <- function(mydate =  current_day) {
  mydataframe <-  "st_st"
  
  con <- duck_open()
  df_app <- import_dataframe(mydataframe)
  
  # mydate <- current_day
  data_day_text <- mydate %>% format("%Y%m%d")
  day_prev_week <- mydate + days(-7)
  day_prev_year <- mydate + days(-364)
  day_2019 <- mydate - days(364 * (year(mydate) - 2019) + floor((year(mydate) - 2019) / 4) * 7)
  current_year = year(mydate)
  
  stakeholder <- str_sub(mydataframe, 1, 2)
  
  ## day ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_day_raw.csv")

  df_day <- df_app %>%
    filter(ISO_CT_CODE2 != "##") %>%  
    filter(ENTRY_DATE %in% c(mydate, day_prev_week, day_2019, day_prev_year)) %>% 
    left_join(list_iso_country, by = c("ISO_CT_CODE1" = "ISO_COUNTRY_CODE")) %>% 
    left_join(dim_iso_country, by = c("ISO_CT_CODE2" = "ISO_COUNTRY_CODE"),
              suffix = c("","_TO")) %>% 
    group_by(ISO_CT_CODE1, ENTRY_DATE) %>%
    arrange(ISO_CT_CODE1, ENTRY_DATE, desc(TOT_MVT), ISO_CT_CODE2) %>% 
    mutate(
      R_RANK = case_when( 
        ENTRY_DATE == mydate ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        ENTRY_DATE == mydate ~ min_rank(desc(TOT_MVT)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        ENTRY_DATE == day_prev_week ~ min_rank(desc(TOT_MVT)),
        .default = NA
      ),
      FLAG_PERIOD = case_when( 
        ENTRY_DATE == mydate ~ "CURRENT_DAY",
        ENTRY_DATE == day_prev_week ~ "DAY_PREV_WEEK",
        ENTRY_DATE == day_2019 ~ "DAY_2019",
        ENTRY_DATE == day_prev_year ~ "DAY_PREV_YEAR",
      )
    ) %>% 
    ungroup() %>% 
    group_by(COUNTRY_NAME, COUNTRY_NAME_TO) %>% 
    arrange(COUNTRY_NAME, COUNTRY_NAME_TO, desc(ENTRY_DATE)) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(TO_DATE = max(ENTRY_DATE)) %>% 
    # Iceland case
    mutate(
      COUNTRY_NAME_TO = if_else((FLAG_PERIOD == "DAY_2019" | 
                                 (FLAG_PERIOD == "DAY_PREV_YEAR" & year(TO_DATE)<2025)) &
                                tolower(COUNTRY_NAME) == 'iceland', 
                              NA,
                              COUNTRY_NAME_TO),
      ISO_CT_CODE2 = if_else((FLAG_PERIOD == "DAY_2019" | 
                            (FLAG_PERIOD == "DAY_PREV_YEAR" & year(TO_DATE)<2025)) &
                           tolower(COUNTRY_NAME) == 'iceland', 
                         NA,
                         ISO_CT_CODE2),
      TOT_MVT = if_else((FLAG_PERIOD == "DAY_2019" | 
                           (FLAG_PERIOD == "DAY_PREV_YEAR" & year(TO_DATE)<2025)) &
                          tolower(COUNTRY_NAME) == 'iceland', 
                        NA,
                        TOT_MVT)
    ) %>% 
    select(
      COUNTRY_NAME,
      FLAG_DAY = FLAG_PERIOD,
      FROM_TO_ISO_CT_CODE = ISO_CT_CODE2,
      FROM_TO_COUNTRY_NAME = COUNTRY_NAME_TO,
      TOT_MVT,
      R_RANK,
      RANK,
      RANK_PREV_WEEK = RANK_PREV,
      TO_DATE
    ) %>% 
    arrange(COUNTRY_NAME, FLAG_DAY, R_RANK, FROM_TO_COUNTRY_NAME)
  
  df_day %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # test2 <- df_day %>% filter(COUNTRY_NAME == "Armenia")
  
  # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(st_base_file),
  #     start = st_base_dir),
  #   sheet = "state_st_day",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(COUNTRY_NAME, FLAG_DAY, R_RANK, FROM_TO_COUNTRY_NAME)
  # 
  # 
  # # nrow(list_iso_country)
  # for (i in 1:nrow(list_iso_country)) {
  #   df1 <- df %>%
  #     mutate(COUNTRY_NAME = tolower(COUNTRY_NAME)) %>%
  #     filter(COUNTRY_NAME == tolower(list_iso_country$COUNTRY_NAME[i]))
  # 
  #   df_day1 <- df_day %>%
  #     mutate(COUNTRY_NAME = tolower(COUNTRY_NAME)) %>%
  #     filter(COUNTRY_NAME == tolower(list_iso_country$COUNTRY_NAME[i]))
  # 
  #   print(paste(list_iso_country$COUNTRY_NAME[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   #
  #   #   print(paste(j, df1[[j,4]] == df_day1[[j,4]]))
  #   #
  #   # }
  # }

## week ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_week_raw.csv")

  df_week <- df_app %>%
    filter(ISO_CT_CODE2 != "##") %>%  
    filter(
      ENTRY_DATE %in% c(seq.Date(mydate-6, mydate)) |
        ENTRY_DATE %in% c(seq.Date(day_prev_week-6, day_prev_week))|
        ENTRY_DATE %in% c(seq.Date(day_2019-6, day_2019)) |
        ENTRY_DATE %in% c(seq.Date(day_prev_year-6, day_prev_year))
    ) %>% 
    compute(prudence = "lavish") %>%
    mutate(
      FLAG_PERIOD = case_when( 
        (ENTRY_DATE >= mydate - days(6) & ENTRY_DATE <= mydate) ~ "CURRENT_ROLLING_WEEK",
        (ENTRY_DATE >= day_prev_week - days(6) & ENTRY_DATE <= day_prev_week) ~ "PREV_ROLLING_WEEK",
        (ENTRY_DATE >= day_2019 - days(6) & ENTRY_DATE <= day_2019) ~ "ROLLING_WEEK_2019",
        (ENTRY_DATE >= day_prev_year - days(6) & ENTRY_DATE <= day_prev_year) ~ "ROLLING_WEEK_PREV_YEAR"
      )
    ) %>% 
    summarise(
      TOT_MVT = sum(TOT_MVT, na.rm = TRUE),
      .by = c(FLAG_PERIOD, ISO_CT_CODE1, ISO_CT_CODE2)
    ) %>% 
    ungroup() %>% 
    left_join(list_iso_country, by = c("ISO_CT_CODE1" = "ISO_COUNTRY_CODE")) %>% 
    left_join(dim_iso_country, by = c("ISO_CT_CODE2" = "ISO_COUNTRY_CODE"),
              suffix = c("","_TO")) %>% 
    group_by(ISO_CT_CODE1, FLAG_PERIOD) %>%
    arrange(ISO_CT_CODE1, FLAG_PERIOD, desc(TOT_MVT), ISO_CT_CODE2) %>% 
    mutate(
      R_RANK = case_when( 
        FLAG_PERIOD == "CURRENT_ROLLING_WEEK" ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        FLAG_PERIOD == "CURRENT_ROLLING_WEEK" ~ min_rank(desc(TOT_MVT)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        FLAG_PERIOD == "PREV_ROLLING_WEEK" ~ min_rank(desc(TOT_MVT)),
        .default = NA
      )
    ) %>% 
    ungroup() %>% 
    group_by(COUNTRY_NAME, COUNTRY_NAME_TO) %>% 
    arrange(COUNTRY_NAME, COUNTRY_NAME_TO, FLAG_PERIOD) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(
      FROM_DATE = mydate - days(6),
      TO_DATE = mydate
    ) %>% 
    # Iceland case
    mutate(
      ISO_CT_CODE2 = if_else((FLAG_PERIOD == "ROLLING_WEEK_2019" | 
                                 (FLAG_PERIOD == "ROLLING_WEEK_PREV_YEAR" & year(TO_DATE) < 2025)) & 
                                tolower(COUNTRY_NAME) == 'iceland', 
                              NA,
                              ISO_CT_CODE2),
      COUNTRY_NAME_TO = if_else((FLAG_PERIOD == "ROLLING_WEEK_2019" | 
                            (FLAG_PERIOD == "ROLLING_WEEK_PREV_YEAR" & year(TO_DATE) < 2025)) & 
                           tolower(COUNTRY_NAME) == 'iceland', 
                         NA,
                         COUNTRY_NAME_TO),
      
      TOT_MVT = if_else((FLAG_PERIOD == "ROLLING_WEEK_2019" | 
                           (FLAG_PERIOD == "ROLLING_WEEK_PREV_YEAR" & year(TO_DATE) < 2025)) & 
                          tolower(COUNTRY_NAME) == 'iceland', 
                        NA,
                        TOT_MVT)
    ) %>% 
    select(
      COUNTRY_NAME,
      FLAG_ROLLING_WEEK = FLAG_PERIOD,
      FROM_TO_ISO_CT_CODE = ISO_CT_CODE2,
      FROM_TO_COUNTRY_NAME	= COUNTRY_NAME_TO,
      TOT_MVT,
      R_RANK,
      RANK,
      RANK_PREV_WEEK = RANK_PREV,
      FROM_DATE,
      TO_DATE
    ) %>% 
    arrange(COUNTRY_NAME, FLAG_ROLLING_WEEK, R_RANK, FROM_TO_ISO_CT_CODE)
  
  df_week %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(st_base_file),
  #     start = st_base_dir),
  #   sheet = "state_st_week",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% 
  #   arrange(COUNTRY_NAME, FLAG_ROLLING_WEEK, R_RANK, FROM_TO_ISO_CT_CODE)
  # 
  # 
  # # nrow(list_iso_country)
  # for (i in 1:nrow(list_iso_country)) {
  #   df1 <- df %>%
  #     mutate(COUNTRY_NAME = tolower(COUNTRY_NAME)) %>%
  #     filter(COUNTRY_NAME == tolower(list_iso_country$COUNTRY_NAME[i]))
  # 
  #   df_day1 <- df_week %>%
  #     mutate(COUNTRY_NAME = tolower(COUNTRY_NAME)) %>%
  #     filter(COUNTRY_NAME == tolower(list_iso_country$COUNTRY_NAME[i]))
  # 
  #   print(paste(list_iso_country$COUNTRY_NAME[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   #
  #   #   print(paste(j, df1[[j,4]] == df_day1[[j,4]]))
  #   #
  #   # }
  # }
  
  
  ## year ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_y2d_raw.csv")

  y2d_dates <- seq.Date(ymd(paste0(2019,"01","01")),
                        mydate) %>% as_tibble() %>%
    filter(year(value) %in% c(2019, current_year-1, current_year)) %>%
    filter(format(value, "%m-%d") <= format(mydate, "%m-%d")) %>%
    pull()

  df_y2d <- df_app %>%
    filter(ISO_CT_CODE2 != "##") %>%  
    compute(prudence = "lavish") %>%
    mutate(
      YEAR = year(ENTRY_DATE)
    ) %>%
    filter(YEAR %in% c(current_year, current_year-1, 2019)) %>%
    filter(ENTRY_DATE %in% y2d_dates) %>%
    group_by(YEAR) %>% 
    mutate(
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE)
    ) %>% 
    ungroup() %>% 
    summarise(
      TOT_MVT = sum(TOT_MVT, na.rm = TRUE),
      .by = c(YEAR, ISO_CT_CODE1, ISO_CT_CODE2, TO_DATE, FROM_DATE)
    ) %>%
    ungroup() %>%
    left_join(list_iso_country, by = c("ISO_CT_CODE1" = "ISO_COUNTRY_CODE")) %>% 
    left_join(dim_iso_country, by = c("ISO_CT_CODE2" = "ISO_COUNTRY_CODE"),
              suffix = c("","_TO")) %>% 
    group_by(ISO_CT_CODE1, YEAR) %>%
    arrange(ISO_CT_CODE1, YEAR, desc(TOT_MVT), ISO_CT_CODE2) %>%
    mutate(
      R_RANK = case_when(
        YEAR == current_year ~ row_number(),
        .default = NA
      ),
      RANK = case_when(
        YEAR == current_year ~ min_rank(desc(TOT_MVT)),
        .default = NA
      ),
      RANK_PREV = case_when(
        YEAR == current_year-1 ~ min_rank(desc(TOT_MVT)),
        .default = NA
      )
    ) %>%
    ungroup() %>%
    group_by(COUNTRY_NAME, COUNTRY_NAME_TO) %>%
    arrange(COUNTRY_NAME, COUNTRY_NAME_TO, YEAR) %>%
    fill(R_RANK, .direction = "down") %>%
    fill(R_RANK, .direction = "up") %>%
    fill(RANK, .direction = "down") %>%
    fill(RANK, .direction = "up") %>%
    fill(RANK_PREV, .direction = "down") %>%
    fill(RANK_PREV, .direction = "up") %>%
    ungroup() %>%
    filter(R_RANK < 11) %>%
    # Iceland case
    mutate(
      ISO_CT_CODE2 = if_else(year(TO_DATE)<2024 & tolower(COUNTRY_NAME) == 'iceland', NA,ISO_CT_CODE2),
      COUNTRY_NAME_TO = if_else(year(TO_DATE)<2024 & tolower(COUNTRY_NAME) == 'iceland', NA,COUNTRY_NAME_TO),
      TOT_MVT = if_else(year(TO_DATE)<2024 & tolower(COUNTRY_NAME) == 'iceland', NA,TOT_MVT)
    ) %>% 
    mutate(
      NO_DAYS = as.numeric(TO_DATE - FROM_DATE) +1,
      AVG_MVT = TOT_MVT/NO_DAYS
    ) %>%
    select(
      COUNTRY_NAME,
      YEAR,
      FROM_TO_ISO_CT_CODE = ISO_CT_CODE2,
      FROM_TO_COUNTRY_NAME = COUNTRY_NAME_TO,
      TOT_MVT,
      AVG_MVT,
      R_RANK,
      RANK_CURRENT = RANK,
      RANK_PREV_YEAR	= RANK_PREV,
      FROM_DATE,
      TO_DATE
    ) %>%
    arrange(COUNTRY_NAME, desc(YEAR), R_RANK, FROM_TO_ISO_CT_CODE)


  df_y2d %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck 
  # df <- read_xlsx(
  # path  = fs::path_abs(
  #     str_glue(st_base_file),
  #     start = st_base_dir),
  #   sheet = "state_st_y2d",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>%
  #   arrange(COUNTRY_NAME, desc(YEAR), R_RANK, FROM_TO_ISO_CT_CODE)
  # 
  # 
  # # nrow(list_iso_country)
  # for (i in 1:nrow(list_iso_country)) {
  #   df1 <- df %>%
  #     mutate(COUNTRY_NAME = tolower(COUNTRY_NAME)) %>%
  #     filter(COUNTRY_NAME == tolower(list_iso_country$COUNTRY_NAME[i]))
  # 
  #   df_day1 <- df_y2d %>%
  #     mutate(COUNTRY_NAME = tolower(COUNTRY_NAME)) %>%
  #     filter(COUNTRY_NAME == tolower(list_iso_country$COUNTRY_NAME[i]))
  # 
  #   print(paste(list_iso_country$COUNTRY_NAME[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   #
  #   #   print(paste(j, df1[[j,4]] == df_day1[[j,4]]))
  #   #
  #   # }
  # }
  print(paste(now(), mydataframe, mydate))
  
  duck_close(con, clean = TRUE)
}
  
# st ap ----
st_ap <- function(mydate =  current_day) {
    
  mydataframe <-  "st_ap"
  con <- duck_open()
  df_app <- import_dataframe(mydataframe, con = con)
  
  # mydate <- current_day
  data_day_text <- mydate %>% format("%Y%m%d")
  day_prev_week <- mydate + days(-7)
  day_prev_year <- mydate + days(-364)
  day_2019 <- mydate - days(364 * (year(mydate) - 2019) + floor((year(mydate) - 2019) / 4) * 7)
  current_year = year(mydate)
  
  stakeholder <- str_sub(mydataframe, 1, 2)
  
  ## day ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_day_raw.csv")

  df_day <- df_app %>%
    filter(FLIGHT_DATE %in% c(mydate, day_prev_week, day_2019, day_prev_year)) %>% 
    left_join(list_iso_country, by = c("ISO_CT_CODE" = "ISO_COUNTRY_CODE")) %>% 
    group_by(ISO_CT_CODE, FLIGHT_DATE) %>%
    arrange(ISO_CT_CODE, FLIGHT_DATE, desc(DEP_ARR), APT_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        FLIGHT_DATE == mydate ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        FLIGHT_DATE == mydate ~ min_rank(desc(DEP_ARR)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        FLIGHT_DATE == day_prev_week ~ min_rank(desc(DEP_ARR)),
        .default = NA
      ),
      FLAG_PERIOD = case_when( 
        FLIGHT_DATE == mydate ~ "CURRENT_DAY",
        FLIGHT_DATE == day_prev_week ~ "DAY_PREV_WEEK",
        FLIGHT_DATE == day_2019 ~ "DAY_2019",
        FLIGHT_DATE == day_prev_year ~ "DAY_PREV_YEAR",
      )
    ) %>% 
    ungroup() %>% 
    group_by(COUNTRY_NAME, APT_NAME) %>% 
    arrange(COUNTRY_NAME, APT_NAME, desc(FLIGHT_DATE)) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(TO_DATE = max(FLIGHT_DATE)) %>% 
    # Iceland case
    mutate(
      ARP_ICAO_CODE = if_else((FLAG_PERIOD == "DAY_2019" | 
                               (FLAG_PERIOD == "DAY_PREV_YEAR" & year(TO_DATE)<2025)) &
                              tolower(COUNTRY_NAME) == 'iceland', 
                            NA,
                            ARP_ICAO_CODE),
      APT_NAME = if_else((FLAG_PERIOD == "DAY_2019" | 
                                 (FLAG_PERIOD == "DAY_PREV_YEAR" & year(TO_DATE)<2025)) &
                                tolower(COUNTRY_NAME) == 'iceland', 
                              NA,
                         APT_NAME),
      DEP_ARR = if_else((FLAG_PERIOD == "DAY_2019" | 
                            (FLAG_PERIOD == "DAY_PREV_YEAR" & year(TO_DATE)<2025)) &
                           tolower(COUNTRY_NAME) == 'iceland', 
                         NA,
                        DEP_ARR)
      ) %>% 
    select(
      COUNTRY_NAME,
      FLAG_DAY = FLAG_PERIOD,
      AIRPORT_CODE =  ARP_ICAO_CODE,
      AIRPORT_NAME = APT_NAME,
      DEP_ARR,
      R_RANK,
      RANK,
      RANK_PREV_WEEK = RANK_PREV,
      TO_DATE
      
    ) %>% 
    arrange(COUNTRY_NAME, FLAG_DAY, R_RANK, DEP_ARR)
  
  df_day %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(st_base_file),
  #     start = st_base_dir),
  #   sheet = "state_apt_day",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(COUNTRY_NAME, FLAG_DAY, R_RANK, DEP_ARR)
  # 
  # 
  # # nrow(list_iso_country)
  # for (i in 1:nrow(list_iso_country)) {
  #   df1 <- df %>%
  #     mutate(COUNTRY_NAME = tolower(COUNTRY_NAME)) %>%
  #     filter(COUNTRY_NAME == tolower(list_iso_country$COUNTRY_NAME[i]))
  # 
  #   df_day1 <- df_day %>%
  #     mutate(COUNTRY_NAME = tolower(COUNTRY_NAME)) %>%
  #     filter(COUNTRY_NAME == tolower(list_iso_country$COUNTRY_NAME[i]))
  # 
  #   print(paste(list_iso_country$COUNTRY_NAME[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   #
  #   #   print(paste(j, df1[[j,4]] == df_day1[[j,4]]))
  #   #
  #   # }
  # }
  
  ## week ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_week_raw.csv")

  df_week <- df_app %>%
    filter(
      FLIGHT_DATE %in% c(seq.Date(mydate-6, mydate)) |
        FLIGHT_DATE %in% c(seq.Date(day_prev_week-6, day_prev_week))|
        FLIGHT_DATE %in% c(seq.Date(day_2019-6, day_2019)) |
        FLIGHT_DATE %in% c(seq.Date(day_prev_year-6, day_prev_year))
    ) %>% 
    compute(prudence = "lavish") %>%
    mutate(
      PERIOD_TYPE = case_when( 
        (FLIGHT_DATE >= mydate - days(6) & FLIGHT_DATE <= mydate) ~ "CURRENT_ROLLING_WEEK",
        (FLIGHT_DATE >= day_prev_week - days(6) & FLIGHT_DATE <= day_prev_week) ~ "PREV_ROLLING_WEEK",
        (FLIGHT_DATE >= day_2019 - days(6) & FLIGHT_DATE <= day_2019) ~ "ROLLING_WEEK_2019",
        (FLIGHT_DATE >= day_prev_year - days(6) & FLIGHT_DATE <= day_prev_year) ~ "ROLLING_WEEK_PREV_YEAR"
      )
    ) %>% 
    summarise(
      DEP_ARR = sum(DEP_ARR, na.rm = TRUE),
      .by = c(PERIOD_TYPE, ISO_CT_CODE, ARP_ICAO_CODE, APT_NAME)
    ) %>% 
    left_join(list_iso_country, by = c("ISO_CT_CODE" = "ISO_COUNTRY_CODE")) %>% 
    group_by(ISO_CT_CODE, PERIOD_TYPE) %>%
    arrange(ISO_CT_CODE, PERIOD_TYPE, desc(DEP_ARR), APT_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        PERIOD_TYPE == "CURRENT_ROLLING_WEEK" ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        PERIOD_TYPE == "CURRENT_ROLLING_WEEK" ~ min_rank(desc(DEP_ARR)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        PERIOD_TYPE == "PREV_ROLLING_WEEK" ~ min_rank(desc(DEP_ARR)),
        .default = NA
      )
    ) %>% 
    ungroup() %>% 
    group_by(COUNTRY_NAME, APT_NAME) %>% 
    arrange(COUNTRY_NAME, APT_NAME, PERIOD_TYPE) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(
      FROM_DATE = mydate - days(6),
      TO_DATE = mydate
    ) %>% 
    # Iceland case
    mutate(
      ARP_ICAO_CODE = if_else((PERIOD_TYPE == "ROLLING_WEEK_2019" | 
                               (PERIOD_TYPE == "ROLLING_WEEK_PREV_YEAR" & year(TO_DATE) < 2025)) & 
                              tolower(COUNTRY_NAME) == 'iceland', 
                            NA,
                            ARP_ICAO_CODE),
      APT_NAME = if_else((PERIOD_TYPE == "ROLLING_WEEK_2019" | 
                               (PERIOD_TYPE == "ROLLING_WEEK_PREV_YEAR" & year(TO_DATE) < 2025)) & 
                              tolower(COUNTRY_NAME) == 'iceland', 
                            NA,
                         APT_NAME),
      
      DEP_ARR = if_else((PERIOD_TYPE == "ROLLING_WEEK_2019" | 
                          (PERIOD_TYPE == "ROLLING_WEEK_PREV_YEAR" & year(TO_DATE) < 2025)) & 
                         tolower(COUNTRY_NAME) == 'iceland', 
                       NA,
                       DEP_ARR)
    ) %>% 
    select(
      COUNTRY_NAME,
      FLAG_ROLLING_WEEK = PERIOD_TYPE,
      AIRPORT_CODE = ARP_ICAO_CODE,
      AIRPORT_NAME = APT_NAME,
      DEP_ARR,
      R_RANK,
      RANK,
      RANK_PREV_WEEK = RANK_PREV,
      FROM_DATE,
      TO_DATE
      
    ) %>% 
    arrange(COUNTRY_NAME, FLAG_ROLLING_WEEK, R_RANK, AIRPORT_NAME)
  
  df_week %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # test2 <- df_week %>% filter(COUNTRY_NAME == 'Iceland')
  
  # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(st_base_file),
  #     start = st_base_dir),
  #   sheet = "state_apt_week",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(COUNTRY_NAME, FLAG_ROLLING_WEEK, R_RANK, AIRPORT_NAME)
  # 
  # 
  # # nrow(list_iso_country)
  # for (i in 1:nrow(list_iso_country)) {
  #   df1 <- df %>%
  #     mutate(COUNTRY_NAME = tolower(COUNTRY_NAME)) %>%
  #     filter(COUNTRY_NAME == tolower(list_iso_country$COUNTRY_NAME[i]))
  # 
  #   df_day1 <- df_week %>%
  #     mutate(COUNTRY_NAME = tolower(COUNTRY_NAME)) %>%
  #     filter(COUNTRY_NAME == tolower(list_iso_country$COUNTRY_NAME[i]))
  # 
  #   print(paste(list_iso_country$COUNTRY_NAME[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   #
  #   #   print(paste(j, df1[[j,4]] == df_day1[[j,4]]))
  #   #
  #   # }
  # }
  
  ## year ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_y2d_raw.csv")

  y2d_dates <- seq.Date(ymd(paste0(2019,"01","01")),
           mydate) %>% as_tibble() %>% 
    filter(year(value) %in% c(2019, current_year-1, current_year)) %>%
    filter(format(value, "%m-%d") <= format(mydate, "%m-%d")) %>% 
    pull()
  
  df_y2d <- df_app %>% 
    compute(prudence = "lavish") %>%
    mutate(
      YEAR = year(FLIGHT_DATE)
    ) %>% 
    filter(YEAR %in% c(current_year, current_year-1, 2019)) %>% 
    filter(FLIGHT_DATE %in% y2d_dates) %>% 
    group_by(YEAR) %>% 
    mutate(
      TO_DATE = max(FLIGHT_DATE, na.rm = TRUE),
      FROM_DATE = min(FLIGHT_DATE, na.rm = TRUE)
    ) %>% 
    ungroup() %>% 
    summarise(
      DEP_ARR = sum(DEP_ARR, na.rm = TRUE),
      .by = c(YEAR, ISO_CT_CODE, ARP_ICAO_CODE, APT_NAME, TO_DATE, FROM_DATE)
    ) %>% 
    left_join(list_iso_country, by = c("ISO_CT_CODE" = "ISO_COUNTRY_CODE")) %>% 
    group_by(ISO_CT_CODE, YEAR) %>%
    arrange(ISO_CT_CODE, YEAR, desc(DEP_ARR), APT_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        YEAR == current_year ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        YEAR == current_year ~ min_rank(desc(DEP_ARR)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        YEAR == current_year-1 ~ min_rank(desc(DEP_ARR)),
        .default = NA
      )
    ) %>% 
    ungroup() %>% 
    group_by(COUNTRY_NAME, APT_NAME) %>% 
    arrange(COUNTRY_NAME, APT_NAME, YEAR) %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "up") %>% 
    
    fill(RANK, .direction = "down") %>% 
    fill(RANK, .direction = "up") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    # Iceland case
    mutate(
      ARP_ICAO_CODE = if_else(year(TO_DATE)<2024 & tolower(COUNTRY_NAME) == 'iceland', NA,ARP_ICAO_CODE),
      APT_NAME = if_else(year(TO_DATE)<2024 & tolower(COUNTRY_NAME) == 'iceland', NA,APT_NAME),
      DEP_ARR = if_else(year(TO_DATE)<2024 & tolower(COUNTRY_NAME) == 'iceland', NA,DEP_ARR)
    ) %>% 
    mutate(
      NO_DAYS = as.numeric(TO_DATE - FROM_DATE) +1,
      AVG_DEP_ARR = DEP_ARR/NO_DAYS
    ) %>%
    select(
      COUNTRY_NAME,
      YEAR,
      AIRPORT_CODE = ARP_ICAO_CODE,
      AIRPORT_NAME = APT_NAME,
      DEP_ARR,
      AVG_DEP_ARR,
      R_RANK,
      RANK_CURRENT = RANK,
      RANK_PREV_YEAR = RANK_PREV,
      FROM_DATE,
      TO_DATE
    ) %>% 
    arrange(COUNTRY_NAME, desc(YEAR), R_RANK, AIRPORT_NAME)
  
  
  df_y2d %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # test2 <- df_y2d %>% filter(COUNTRY_NAME == "Serbia")
  
  # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(st_base_file),
  #     start = st_base_dir),
  #   sheet = "state_apt_y2d",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(COUNTRY_NAME, desc(YEAR), R_RANK, AIRPORT_NAME)
  # 
  # 
  # # nrow(list_iso_country)
  # for (i in 1:nrow(list_iso_country)) {
  #   df1 <- df %>%
  #     mutate(COUNTRY_NAME = tolower(COUNTRY_NAME)) %>%
  #     filter(COUNTRY_NAME == tolower(list_iso_country$COUNTRY_NAME[i]))
  # 
  #   df_day1 <- df_y2d %>%
  #     mutate(COUNTRY_NAME = tolower(COUNTRY_NAME)) %>%
  #     filter(COUNTRY_NAME == tolower(list_iso_country$COUNTRY_NAME[i]))
  # 
  #   print(paste(list_iso_country$COUNTRY_NAME[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df1)) {
  #   #
  #   #   print(paste(j, df1[[j,4]] == df_day1[[j,4]]))
  #   #
  #   # }
  # }
  
  print(paste(now(), mydataframe, mydate))
  
  duck_close(con, clean = TRUE)
  
}

# execute functions ----
# wef <- "2025-04-09"  #included in output
# til <- "2025-04-09"  #included in output
# current_day <- seq(ymd(til), ymd(wef), by = "-1 day")

purrr::walk(current_day, st_ao)
purrr::walk(current_day, st_st)
purrr::walk(current_day, st_ap)

