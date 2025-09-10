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
list_airport <- export_query(list_ap_query) 
list_airport_extended <- export_query(list_ap_ext_query)

## iso country ----
dim_iso_country <- export_query(dim_iso_st_query) 


## market segment ---- 
dim_marktet_segment <- export_query(dim_ms_query) 


# prep data functions ----
import_dataframe <- function(dfname) {
  # import data 
  # dfname <- "ap_traffic_delay"
  mydataframe <- dfname
  myparquetfile <- paste0(mydataframe, "_day_base.parquet")
  
  # con <- duck_open()
  # df_base <- duck_ingest_parquet(con, here::here(archive_dir_raw, myparquetfile))  # now eager by default
  
  df_base <- read_parquet_duckdb(here(archive_dir_raw,
                                      myparquetfile)
  )
  
  # filter to keep days and airports needed
  if ("ARP_PRU_ID" %in% names(df_base)) {
    df_alldays <- df_base %>% 
      filter(ARP_PRU_ID %in% list_airport$APT_ID)  
  } else if ("DEP_ARP_PRU_ID" %in% names(df_base)) {
    df_alldays <- df_base %>% 
      filter(DEP_ARP_PRU_ID %in% list_airport$APT_ID)  
    
  } else if ("ARP_ID" %in% names(df_base)) {
    df_alldays <- df_base %>% 
      compute(prudence = "lavish") %>% 
      filter(ARP_ID %in% list_airport_extended$APT_ID)  
    
  }
  
  # pre-process data
  if (dfname == "ap_ao") {
    df_app <- df_alldays %>% 
      filter(AO_ID != 1777) %>%  # undefined
      compute(prudence = "lavish") %>%
      left_join(dim_ao_group, by = c("AO_ID", "AO_CODE")) %>% 
      summarise(
        DEP_ARR = sum(DEP_ARR, na.rm = TRUE),
        .by = c(ENTRY_DATE, ARP_PRU_ID, AO_GRP_CODE, AO_GRP_NAME)
      )
    
  } else if (dfname == "ap_ms") {
    df_app <- df_alldays %>% compute(prudence = "lavish") %>% 
      mutate(MS_ID = case_when(
        MS_ID == -1 | MS_ID ==  1 | MS_ID == 5 ~ 0,
        .default = MS_ID)
      ) %>% 
      group_by(ENTRY_DATE, ARP_PRU_ID, MS_ID) %>%
      summarise(DEP_ARR = sum(DEP_ARR, na.rm = TRUE)) %>% 
      ungroup() %>% 
      left_join(dim_marktet_segment, by = "MS_ID") %>% 
      left_join(dim_airport, by = c("ARP_PRU_ID" = "APT_ID"))
    
  } else {
    df_app <- df_alldays %>% compute(prudence = "lavish") 

  }

  return(df_app) 
}

# ap traffic delay ----
ap_traffic_delay <- function() {
  mydataframe <-  "ap_traffic_delay"
  stakeholder <- str_sub(mydataframe, 1, 2)
  
  df_app <- import_dataframe(mydataframe)

  mydate <- df_app %>% summarise(max(FLIGHT_DATE, na.rm = TRUE)) %>% pull()
  current_year = year(mydate)
  
  #### create date sequence
  year_from <- paste0(2018, "-12-24")
  year_til <- paste0(current_year, "-12-31")
  days_sequence <- seq(ymd(year_from), ymd(year_til), by = "1 day") %>%
    as_tibble() %>%
    select(FLIGHT_DATE = value) 
  
  #### combine with ap list to get full sequence
  days_ap <- crossing(days_sequence, list_airport_extended) %>% 
    arrange(APT_ID, FLIGHT_DATE)%>% 
    mutate(YEAR = year(FLIGHT_DATE))
  
  df_day_year <- days_ap %>%
    left_join(df_app, by = c("FLIGHT_DATE", "APT_ID" = "ARP_ID")) %>% 
    arrange(APT_ID, FLIGHT_DATE) %>% 
    group_by(APT_ID) %>% 
    mutate(
      ARP_CODE = APT_ICAO_CODE,
      ARP_NAME = APT_NAME,
      ICAO2LETTER = substr(ARP_CODE, 1,2),
      YEAR =  year(FLIGHT_DATE),
      
      # traffic
      DAY_ARR = coalesce(ARR,0),
      DAY_DEP = coalesce(DEP,0),
      DAY_DEP_ARR = coalesce(DEP_ARR,0),
      
      RWK_AVG_DEP = rollsum(DAY_DEP, 7, fill = NA, align = "right") / 7,
      RWK_AVG_ARR = rollsum(DAY_ARR, 7, fill = NA, align = "right") / 7,
      RWK_AVG_DEP_ARR = rollsum(DAY_DEP_ARR, 7, fill = NA, align = "right") / 7,
      
      # delay
      TDM_ARP_ARR = coalesce(TDM_ARP_ARR, 0),
      TDF_ARP_ARR = coalesce(TDF_ARP_ARR, 0),
      TDF_15_ARP_ARR = coalesce(TDF_15_ARP_ARR, 0),
      TDM_ARP_ARR_G = coalesce(TDM_ARP_ARR_G, 0),
      TDM_ARP_ARR_CS = coalesce(TDM_ARP_ARR_C, 0) + coalesce(TDM_ARP_ARR_S, 0),
      TDM_ARP_ARR_WD = coalesce(TDM_ARP_ARR_W, 0) + coalesce(TDM_ARP_ARR_D, 0),
      TDM_ARP_ARR_IT = coalesce(TDM_ARP_ARR_I, 0) + coalesce(TDM_ARP_ARR_T, 0)
    )  %>% 
    group_by(APT_ID, YEAR) %>% 
    mutate(
      # year to date
      Y2D_DEP_YEAR = cumsum(coalesce(DEP, 0)),
      Y2D_AVG_DEP_YEAR = cumsum(coalesce(DEP, 0)) / row_number(),
      
      Y2D_ARR_YEAR = cumsum(coalesce(ARR, 0)),
      Y2D_AVG_ARR_YEAR = cumsum(coalesce(ARR, 0)) / row_number(),
      
      Y2D_DEP_ARR_YEAR = cumsum(coalesce(DEP_ARR, 0)),
      Y2D_AVG_DEP_ARR_YEAR = cumsum(coalesce(DEP_ARR, 0)) / row_number()
      
    ) %>% 
    ungroup ()
  
  # test2<- df_day_year %>% filter(ARP_CODE == "LFPO")
  
  ## split table ----
  df_day <- df_day_year %>% 
    mutate(
      FLIGHT_DATE_2019 = FLIGHT_DATE - days((YEAR-2019)*364+ floor((YEAR - 2019) / 4) * 7),
      FLIGHT_DATE_2020 = FLIGHT_DATE - days((YEAR-2020)*364+ floor((YEAR - 2020) / 4) * 7),
      FLIGHT_DATE_2019_SD = FLIGHT_DATE %m-% years(YEAR-2019),
      FLIGHT_DATE_PREV_YEAR_SD = FLIGHT_DATE %m-% years(1) 
    ) %>% 
    left_join(select(df_day_year, APT_ID, YEAR, FLIGHT_DATE, starts_with(c("Y2D"))), by = c("APT_ID", "FLIGHT_DATE_PREV_YEAR_SD" = "FLIGHT_DATE"), suffix = c("","_PREV_YEAR")) %>% 
    left_join(select(df_day_year, APT_ID, YEAR, FLIGHT_DATE, starts_with(c("Y2D"))), by = c("APT_ID", "FLIGHT_DATE_2019_SD" = "FLIGHT_DATE"), suffix = c("","_2019")) %>% 
    left_join(select(df_day_year, APT_ID, YEAR, FLIGHT_DATE, starts_with(c("DAY", "RWK"))), by = c("APT_ID", "FLIGHT_DATE_2019" = "FLIGHT_DATE"), suffix = c("","_2019")) %>% 
    left_join(select(df_day_year, APT_ID, YEAR, FLIGHT_DATE, RWK_AVG_DEP, RWK_AVG_ARR, RWK_AVG_DEP_ARR), by = c("APT_ID", "FLIGHT_DATE_2020" = "FLIGHT_DATE"), suffix = c("","_2020")) %>% 
    arrange(APT_ID, FLIGHT_DATE) %>% 
    group_by(APT_ID) %>% 
    mutate(
      # prev week
      FLIGHT_DATE_PREV_WEEK = lag(FLIGHT_DATE, 7),
      
      DAY_DEP_PREV_WEEK = lag(DAY_DEP , 7),
      DAY_ARR_PREV_WEEK = lag(DAY_ARR , 7),
      DAY_DEP_ARR_PREV_WEEK = lag(DAY_DEP_ARR , 7),
      
      RWK_AVG_DEP_PREV_WEEK = lag(RWK_AVG_DEP, 7),
      RWK_AVG_ARR_PREV_WEEK = lag(RWK_AVG_ARR, 7),
      RWK_AVG_DEP_ARR_PREV_WEEK = lag(RWK_AVG_DEP_ARR, 7),
      
      # dif prev week
      DAY_DEP_ARR_DIF_PREV_WEEK = coalesce(DAY_DEP_ARR,0) - coalesce(DAY_DEP_ARR_PREV_WEEK, 0),
      DAY_DEP_ARR_DIF_PREV_WEEK_PERC = if_else(DAY_DEP_ARR_PREV_WEEK == 0, NA, DAY_DEP_ARR/ DAY_DEP_ARR_PREV_WEEK) -1,
      
      RWK_DEP_ARR_DIF_PREV_WEEK_PERC = if_else(RWK_AVG_DEP_ARR_PREV_WEEK == 0, NA, RWK_AVG_DEP_ARR/ RWK_AVG_DEP_ARR_PREV_WEEK)-1,
      
      # prev year
      FLIGHT_DATE_PREV_YEAR = lag(FLIGHT_DATE, 364),
      
      DAY_DEP_PREV_YEAR = lag(DAY_DEP , 364),
      DAY_ARR_PREV_YEAR = lag(DAY_ARR , 364),
      DAY_DEP_ARR_PREV_YEAR = lag(DAY_DEP_ARR , 364),
      
      RWK_AVG_DEP_PREV_YEAR = lag(RWK_AVG_DEP, 364),
      RWK_AVG_ARR_PREV_YEAR = lag(RWK_AVG_ARR, 364),
      RWK_AVG_DEP_ARR_PREV_YEAR = lag(RWK_AVG_DEP_ARR, 364),
      
      Y2D_DEP_PREV_YEAR = Y2D_DEP_YEAR_PREV_YEAR,
      Y2D_ARR_PREV_YEAR = Y2D_ARR_YEAR_PREV_YEAR,
      Y2D_DEP_ARR_PREV_YEAR = Y2D_DEP_ARR_YEAR_PREV_YEAR,
      
      Y2D_AVG_DEP_PREV_YEAR = Y2D_AVG_DEP_YEAR_PREV_YEAR,
      Y2D_AVG_ARR_PREV_YEAR = Y2D_AVG_ARR_YEAR_PREV_YEAR,
      Y2D_AVG_DEP_ARR_PREV_YEAR = Y2D_AVG_DEP_ARR_YEAR_PREV_YEAR,
      
      # dif prev year
      DAY_DEP_ARR_DIF_PREV_YEAR = coalesce(DAY_DEP_ARR, 0) - coalesce(DAY_DEP_ARR_PREV_YEAR, 0),
      DAY_DEP_ARR_DIF_PREV_YEAR_PERC = if_else(DAY_DEP_ARR_PREV_YEAR == 0, NA, DAY_DEP_ARR/ DAY_DEP_ARR_PREV_YEAR)-1,
      RWK_DEP_ARR_DIF_PREV_YEAR_PERC = if_else(RWK_AVG_DEP_ARR_PREV_YEAR == 0, NA, RWK_AVG_DEP_ARR/ RWK_AVG_DEP_ARR_PREV_YEAR)-1,
      Y2D_DEP_ARR_DIF_PREV_YEAR_PERC = if_else(Y2D_AVG_DEP_ARR_PREV_YEAR == 0, NA, Y2D_AVG_DEP_ARR_YEAR/ Y2D_AVG_DEP_ARR_PREV_YEAR)-1,
      
      # 2020
      FLIGHT_DATE_2020 = FLIGHT_DATE_2020,
      
      RWK_AVG_DEP_2020 = RWK_AVG_DEP_2020,
      RWK_AVG_ARR_2020 = RWK_AVG_ARR_2020,
      RWK_AVG_DEP_ARR_2020 = RWK_AVG_DEP_ARR_2020,
      
      # 2019
      FLIGHT_DATE_2019 = FLIGHT_DATE_2019,
      
      DAY_DEP_2019 = DAY_DEP_2019,
      DAY_ARR_2019 = DAY_ARR_2019,
      DAY_DEP_ARR_2019 = DAY_DEP_ARR_2019,

      RWK_AVG_DEP_2019 = RWK_AVG_DEP_2019,
      RWK_AVG_ARR_2019 = RWK_AVG_ARR_2019,
      RWK_AVG_DEP_ARR_2019 = RWK_AVG_DEP_ARR_2019,
      
      Y2D_DEP_2019 = Y2D_DEP_YEAR_2019,
      Y2D_ARR_2019 = Y2D_ARR_YEAR_2019,
      Y2D_DEP_ARR_2019 = Y2D_DEP_ARR_YEAR_2019,
      
      Y2D_AVG_DEP_2019 = Y2D_AVG_DEP_YEAR_2019,
      Y2D_AVG_ARR_2019 = Y2D_AVG_ARR_YEAR_2019,
      Y2D_AVG_DEP_ARR_2019 = Y2D_AVG_DEP_ARR_YEAR_2019,
      
      # dif 2019
      DAY_DEP_ARR_DIF_2019 = coalesce(DAY_DEP_ARR, 0) - coalesce(DAY_DEP_ARR_2019, 0),
      DAY_DEP_ARR_DIF_2019_PERC = if_else(DAY_DEP_ARR_2019 == 0, NA, DAY_DEP_ARR/ DAY_DEP_ARR_2019)-1,
      RWK_DEP_ARR_DIF_2019_PERC = if_else(RWK_AVG_DEP_ARR_2019 == 0, NA, RWK_AVG_DEP_ARR/ RWK_AVG_DEP_ARR_2019)-1,
      Y2D_DEP_ARR_DIF_2019_PERC = if_else(Y2D_AVG_DEP_ARR_2019 == 0, NA, Y2D_AVG_DEP_ARR_YEAR/ Y2D_AVG_DEP_ARR_2019)-1,
      
      LAST_DATA_DAY = mydate
    ) %>%  
    ungroup() %>% 
    select(
      ARP_CODE = APT_ICAO_CODE,
      ARP_NAME = APT_NAME,
      ICAO2LETTER,
      
      YEAR,
      FLIGHT_DATE,
      FLIGHT_DATE_PREV_WEEK,
      FLIGHT_DATE_PREV_YEAR,
      FLIGHT_DATE_2019,
      
      DAY_DEP,
      DAY_DEP_PREV_WEEK,
      DAY_DEP_PREV_YEAR,
      DAY_DEP_2019,
      
      DAY_ARR,
      DAY_ARR_PREV_WEEK,
      DAY_ARR_PREV_YEAR,
      DAY_ARR_2019,
      
      DAY_DEP_ARR,
      DAY_DEP_ARR_PREV_WEEK,
      DAY_DEP_ARR_PREV_YEAR,
      DAY_DEP_ARR_2019,
      DAY_DEP_ARR_DIF_PREV_WEEK,
      DAY_DEP_ARR_DIF_PREV_YEAR,
      DAY_DEP_ARR_DIF_2019,
      DAY_DEP_ARR_DIF_PREV_WEEK_PERC,
      DAY_DEP_ARR_DIF_PREV_YEAR_PERC,
      DAY_DEP_ARR_DIF_2019_PERC,
      
      RWK_AVG_DEP,
      RWK_AVG_DEP_PREV_WEEK,
      RWK_AVG_DEP_PREV_YEAR,
      RWK_AVG_DEP_2020,
      RWK_AVG_DEP_2019,
      
      RWK_AVG_ARR,
      RWK_AVG_ARR_PREV_WEEK,
      RWK_AVG_ARR_PREV_YEAR,
      RWK_AVG_ARR_2020,
      RWK_AVG_ARR_2019,
      
      RWK_AVG_DEP_ARR,
      RWK_AVG_DEP_ARR_PREV_WEEK,
      RWK_AVG_DEP_ARR_PREV_YEAR,
      RWK_AVG_DEP_ARR_2020,
      RWK_AVG_DEP_ARR_2019,
      RWK_DEP_ARR_DIF_PREV_YEAR_PERC,
      RWK_DEP_ARR_DIF_2019_PERC	,
      
      Y2D_DEP_YEAR,
      Y2D_AVG_DEP_YEAR,
      Y2D_DEP_PREV_YEAR,
      Y2D_AVG_DEP_PREV_YEAR,
      Y2D_DEP_2019,
      Y2D_AVG_DEP_2019,
      Y2D_ARR_YEAR,
      Y2D_AVG_ARR_YEAR,
      Y2D_ARR_PREV_YEAR,
      Y2D_AVG_ARR_PREV_YEAR,
      Y2D_ARR_2019,
      Y2D_AVG_ARR_2019,
      Y2D_DEP_ARR_YEAR,
      Y2D_AVG_DEP_ARR_YEAR,
      Y2D_DEP_ARR_PREV_YEAR,
      Y2D_AVG_DEP_ARR_PREV_YEAR,
      Y2D_DEP_ARR_2019,
      Y2D_AVG_DEP_ARR_2019,
      Y2D_DEP_ARR_DIF_PREV_YEAR_PERC,
      Y2D_DEP_ARR_DIF_2019_PERC,
      
      LAST_DATA_DAY
      
    ) %>% 
    arrange(ARP_NAME, FLIGHT_DATE)
  
  
  mydatafile <- paste0("ap_delay_day_raw.parquet")
  df_day_year %>% write_parquet(here(archive_dir_raw, stakeholder, mydatafile))
  
  mydatafile <- paste0("ap_traffic_day_raw.parquet")
  df_day %>% write_parquet(here(archive_dir_raw, stakeholder, mydatafile))
  
  # df_day %>% filter(ARP_CODE == "EBCI") %>% filter(FLIGHT_DATE == current_day) %>% select(DAY_ARR)
  
  print(paste(format(now(), "%H:%M:%S"), mydataframe, mydate))

 # test <- df_day %>% filter(YEAR == 2025) %>% 
 #  filter(FLIGHT_DATE > ymd(20250106)& FLIGHT_DATE != ymd(20250228))#  %>%
  # filter(ARP_CODE == 'LFPO') #%>%
  # select(AO_GRP_CODE, FLIGHT_DATE,FLIGHT_DATE_PREV_YEAR, FLIGHT_DATE_2019, Y2D_TFC_YEAR,	Y2D_TFC_PREV_YEAR,	Y2D_TFC_2019)
  
  
  # test %>% write_csv(here(archive_dir_raw,"test.csv"))
  
  # # dcheck
#   dfc <- read_xlsx(
#     path  = fs::path_abs(
#       str_glue(ap_base_file),
#       start = "G:/HQ/dgof-pru/Project/DDP/AIU app/data_archive/z_excel_files"),
#     sheet = "apt_traffic",
#     range = cell_limits(c(2, 1), c(NA, NA))) |>
#     mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
#   
#   # dfc <- read_csv(here(archive_dir_raw, stakeholder, "20241231_apt__raw.csv"), show_col_types = FALSE)
#   # 
#   df <- dfc %>% arrange(ARP_NAME, FLIGHT_DATE) %>% 
#   filter(YEAR == 2025) %>%
#   filter(FLIGHT_DATE > ymd(20250106) & FLIGHT_DATE != ymd(20250228))
#   # filter(ARP_CODE == 'EBBR') #%>%
# #  select(AO_GRP_CODE, FLIGHT_DATE,FLIGHT_DATE_PREV_YEAR, FLIGHT_DATE_2019, Y2D_TFC_YEAR,	Y2D_TFC_PREV_YEAR,	Y2D_TFC_2019)
#   
#   # colnames(df)
#   for (i in 1:nrow(list_airport)) {
#     df1 <- df %>% filter(tolower(ARP_NAME) == tolower(list_airport$APT_NAME[i]))
#     # %>% filter(FLIGHT_DATE <= ymd(20250905) & FLIGHT_DATE != ymd(20250228))
# 
# 
#     df_day1 <- test %>% filter(ARP_NAME == list_airport$APT_NAME[i])
#     # %>% filter(FLIGHT_DATE <= ymd(20250905) & FLIGHT_DATE != ymd(20250228))
# 
#     # print(paste(list_ao_group$AO_GRP_CODE[i], all.equal(df1, df_day1)))
#     # nrow(df1)
#     for (j in 1:nrow(df1)) {
#       for (t in 15:69){
#         if (((coalesce(df1[[j,t]],0) - coalesce(df_day1[[j,t-5]],0))<10^-8) == FALSE) {
#           print(paste(t,df1[[j,1]], df1[[j,11]], (df1[[j,t]] - df_day1[[j,t-5]])<10^-8))
#           break}
#         # print(paste(t,df1[[j,1]], df1[[j,8]], (df1[[j,t]] - df_day1[[j,t]])<10^-8))
#       }
#     }
#     print(paste(t,df1[[j,1]], df1[[j,11]], (df1[[j,t]] - df_day1[[j,t-5]])<10^-8))
# 
#   }

}


# ap ao grp ----
ap_ao <- function(mydate =  current_day) {
  mydataframe <-  "ap_ao"
  
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
    filter(ENTRY_DATE %in% c(mydate, day_prev_week, day_2019, day_prev_year)) %>% 
    left_join(dim_airport, by = c("ARP_PRU_ID" = "APT_ID")) %>% 
    group_by(ARP_PRU_ID, ENTRY_DATE) %>%
    arrange(APT_ICAO_CODE, ENTRY_DATE, desc(DEP_ARR), AO_GRP_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        ENTRY_DATE == mydate ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        ENTRY_DATE == mydate ~ min_rank(desc(DEP_ARR)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        ENTRY_DATE == day_prev_week ~ min_rank(desc(DEP_ARR)),
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
    group_by(APT_NAME, AO_GRP_NAME) %>% 
    arrange(APT_NAME, AO_GRP_NAME, desc(ENTRY_DATE)) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(LAST_DATA_DAY = max(ENTRY_DATE)) %>% 
    select(
      ARP_CODE = APT_ICAO_CODE,
      ARP_NAME = APT_NAME,
      FLAG_DAY,
      TO_DATE = ENTRY_DATE,
      LAST_DATA_DAY,
      AO_GRP_CODE,
      AO_GRP_NAME,
      DEP_ARR,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>% 
    arrange(ARP_CODE, FLAG_DAY, R_RANK, AO_GRP_NAME)
  
  df_day %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  # test <- df_day %>% 
  #   filter(ARP_CODE == "EFHK") 
  
  # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_ao_day",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE, FLAG_DAY, R_RANK, AO_GRP_NAME)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   df_day1 <- df_day %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, df_day1)))
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
    group_by(PERIOD_TYPE) %>% 
    mutate(
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE)
    ) %>% 
    ungroup() %>% 
    summarise(
      DEP_ARR = sum(DEP_ARR, na.rm = TRUE),
      .by = c(PERIOD_TYPE, ARP_PRU_ID, AO_GRP_NAME, AO_GRP_CODE, FROM_DATE, TO_DATE)
    ) %>% 
    left_join(dim_airport, by = c("ARP_PRU_ID" = "APT_ID")) %>% 
    ungroup() %>% 
    group_by(ARP_PRU_ID, PERIOD_TYPE) %>%
    arrange(APT_ICAO_CODE, PERIOD_TYPE, desc(DEP_ARR), AO_GRP_NAME) %>% 
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
    group_by(APT_NAME, AO_GRP_NAME) %>% 
    arrange(APT_NAME, AO_GRP_NAME, PERIOD_TYPE) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(LAST_DATA_DAY = max(TO_DATE)) %>% 
    select(
      ARP_CODE = APT_ICAO_CODE,
      ARP_NAME = APT_NAME,
      PERIOD_TYPE,
      FROM_DATE,
      TO_DATE,
      LAST_DATA_DAY,
      AO_GRP_CODE,
      AO_GRP_NAME,
      DEP_ARR,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>% 
    arrange(ARP_CODE, PERIOD_TYPE, R_RANK, AO_GRP_NAME)
  
  df_week %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))

  # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_ao_week",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE, PERIOD_TYPE, R_RANK, AO_GRP_NAME)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   df_day1 <- df_week %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, df_day1)))
  # 
  #   # for (j in 1:nrow(df)) {
  #   #
  #   #   print(paste(j, df1[[j,1]] == df_day1[[j,1]]))
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
    group_by(YEAR) %>% 
    mutate(
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE)
    ) %>% 
    ungroup() %>%
    summarise(
      DEP_ARR = sum(DEP_ARR, na.rm = TRUE),
      .by = c(YEAR, ARP_PRU_ID, AO_GRP_NAME, AO_GRP_CODE, FROM_DATE, TO_DATE)
    ) %>% 
    ungroup() %>% 
    left_join(dim_airport, by = c("ARP_PRU_ID" = "APT_ID")) %>% 
    group_by(ARP_PRU_ID, YEAR) %>%
    arrange(ARP_PRU_ID, YEAR, desc(DEP_ARR), AO_GRP_NAME) %>% 
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
    group_by(APT_NAME, AO_GRP_NAME) %>% 
    arrange(APT_NAME, AO_GRP_NAME, YEAR) %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "up") %>% 
    
    fill(RANK, .direction = "down") %>% 
    fill(RANK, .direction = "up") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(LAST_DATA_DAY = max(TO_DATE)) %>% 
    select(
      ARP_CODE = APT_ICAO_CODE,
      ARP_NAME = APT_NAME,
      YEAR,
      AO_GRP_CODE,
      AO_GRP_NAME,
      DEP_ARR,
      FROM_DATE,
      TO_DATE,
      LAST_DATA_DATE = LAST_DATA_DAY,
      R_RANK,
      RANK,
      RANK_PY = RANK_PREV
    ) %>% 
    arrange(ARP_CODE, desc(YEAR), R_RANK, AO_GRP_NAME)
  
  
  df_y2d %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
# dcheck
#   df <- read_xlsx(
#     path  = fs::path_abs(
#       str_glue(ap_base_file),
#       start = ap_base_dir),
#     sheet = "apt_ao_y2d",
#     range = cell_limits(c(1, 1), c(NA, NA))) |>
#     mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
# 
#   df <- df %>% arrange(ARP_CODE, desc(YEAR), R_RANK, AO_GRP_NAME)
# 
#   for (i in 1:nrow(list_airport)) {
#     df1 <- df %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
#     df_y2d1 <- df_y2d %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
# 
#     print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, df_y2d1)))
# 
# #
# #       for (j in 1:1) {
# #
# #         print(paste(j, df1[[j,6]], df_y2d1[[j,6]]))
# #
# #       }
# 
#   }
  
  print(paste(format(now(), "%H:%M:%S"), mydataframe, mydate))
}

# ap st des ----
ap_st_des <- function(mydate =  current_day) {
  mydataframe <-  "ap_st_des"
  
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
    filter(ENTRY_DATE %in% c(mydate, day_prev_week, day_2019, day_prev_year)) %>% 
    left_join(dim_airport, by = c("DEP_ARP_PRU_ID" = "APT_ID")) %>% 
    left_join(dim_iso_country, by = c("ARR_ISO_CTY_CODE" = "ISO_COUNTRY_CODE")) %>% 
    group_by(DEP_ARP_PRU_ID, ENTRY_DATE) %>%
    arrange(APT_ICAO_CODE, ENTRY_DATE, desc(DEP), COUNTRY_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        ENTRY_DATE == mydate ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        ENTRY_DATE == mydate ~ min_rank(desc(DEP)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        ENTRY_DATE == day_prev_week ~ min_rank(desc(DEP)),
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
    group_by(APT_NAME, COUNTRY_NAME) %>% 
    arrange(APT_NAME, COUNTRY_NAME, desc(ENTRY_DATE)) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(LAST_DATA_DAY = max(ENTRY_DATE)) %>% 
    select(
      FLAG_PERIOD,
      ARP_CODE = APT_ICAO_CODE,
      ARP_NAME = APT_NAME,
      ISO_CT_NAME_ARR = COUNTRY_NAME,
      ISO_CT_CODE_ARR = ARR_ISO_CTY_CODE,
      DEP,
      TO_DATE = ENTRY_DATE,
      LAST_DATA_DAY,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>% 
    arrange(ARP_CODE, FLAG_PERIOD, R_RANK, ISO_CT_NAME_ARR)
  
  df_day %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_state_des_day",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE, FLAG_PERIOD, R_RANK, ISO_CT_NAME_ARR)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   df_day1 <- df_day %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, df_day1)))
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
      FLAG_PERIOD = case_when( 
        (ENTRY_DATE >= mydate - days(6) & ENTRY_DATE <= mydate) ~ "CURRENT_ROLLING_WEEK",
        (ENTRY_DATE >= day_prev_week - days(6) & ENTRY_DATE <= day_prev_week) ~ "PREV_ROLLING_WEEK",
        (ENTRY_DATE >= day_2019 - days(6) & ENTRY_DATE <= day_2019) ~ "ROLLING_WEEK_2019",
        (ENTRY_DATE >= day_prev_year - days(6) & ENTRY_DATE <= day_prev_year) ~ "ROLLING_WEEK_PREV_YEAR"
      )
    ) %>% 
    group_by(FLAG_PERIOD) %>% 
    mutate(
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE)
    ) %>% 
    ungroup() %>% 
    summarise(
      DEP = sum(DEP, na.rm = TRUE),
      .by = c(FLAG_PERIOD, DEP_ARP_PRU_ID, ARR_ISO_CTY_CODE, FROM_DATE, TO_DATE)
    ) %>% 
    ungroup() %>% 
    left_join(dim_airport, by = c("DEP_ARP_PRU_ID" = "APT_ID")) %>% 
    left_join(dim_iso_country, by = c("ARR_ISO_CTY_CODE" = "ISO_COUNTRY_CODE")) %>% 
    group_by(DEP_ARP_PRU_ID, FLAG_PERIOD) %>%
    arrange(APT_ICAO_CODE, FLAG_PERIOD, desc(DEP), COUNTRY_NAME) %>% 
    mutate(
      R_RANK = case_when( 
        FLAG_PERIOD == "CURRENT_ROLLING_WEEK" ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        FLAG_PERIOD == "CURRENT_ROLLING_WEEK" ~ min_rank(desc(DEP)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        FLAG_PERIOD == "PREV_ROLLING_WEEK" ~ min_rank(desc(DEP)),
        .default = NA
      )
    ) %>% 
    ungroup() %>% 
    group_by(APT_NAME, COUNTRY_NAME) %>% 
    arrange(APT_NAME, COUNTRY_NAME, FLAG_PERIOD) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(LAST_DATA_DAY = max(TO_DATE)) %>% 
    select(
      FLAG_PERIOD,
      ARP_CODE = APT_ICAO_CODE,
      ARP_NAME = APT_NAME,
      ISO_CT_NAME_ARR = COUNTRY_NAME,
      ISO_CT_CODE_ARR = ARR_ISO_CTY_CODE,
      DEP,
      FROM_DATE,
      TO_DATE,
      LAST_DATA_DAY,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>% 
    arrange(ARP_CODE, FLAG_PERIOD, R_RANK, ISO_CT_NAME_ARR)
  
  df_week %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_state_des_week",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE, FLAG_PERIOD, R_RANK, ISO_CT_NAME_ARR)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   df_day1 <- df_week %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, df_day1)))
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
    group_by(YEAR) %>% 
    mutate(
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE)
    ) %>% 
    ungroup() %>%
    summarise(
      DEP = sum(DEP, na.rm = TRUE),
      .by = c(YEAR, DEP_ARP_PRU_ID, ARR_ISO_CTY_CODE, FROM_DATE, TO_DATE)
    ) %>% 
    ungroup() %>% 
    left_join(dim_airport, by = c("DEP_ARP_PRU_ID" = "APT_ID")) %>%
    left_join(dim_iso_country, by = c("ARR_ISO_CTY_CODE" = "ISO_COUNTRY_CODE")) %>%
    group_by(DEP_ARP_PRU_ID, YEAR) %>%
    arrange(DEP_ARP_PRU_ID, YEAR, desc(DEP), COUNTRY_NAME) %>%
    mutate(
      R_RANK = case_when(
        YEAR == current_year ~ row_number(),
        .default = NA
      ),
      RANK = case_when(
        YEAR == current_year ~ min_rank(desc(DEP)),
        .default = NA
      ),
      RANK_PREV = case_when(
        YEAR == current_year-1 ~ min_rank(desc(DEP)),
        .default = NA
      )
    ) %>%
    ungroup() %>%
    group_by(APT_NAME, COUNTRY_NAME) %>%
    arrange(APT_NAME, COUNTRY_NAME, YEAR) %>%
    fill(R_RANK, .direction = "down") %>%
    fill(R_RANK, .direction = "up") %>%

    fill(RANK, .direction = "down") %>%
    fill(RANK, .direction = "up") %>%
    fill(RANK_PREV, .direction = "down") %>%
    fill(RANK_PREV, .direction = "up") %>%
    ungroup() %>%
    filter(R_RANK < 11) %>%
    mutate(LAST_DATA_DAY = max(TO_DATE)) %>%
    select(
      YEAR,
      ARP_CODE = APT_ICAO_CODE,
      ARP_NAME = APT_NAME,
      ISO_CT_NAME_ARR = COUNTRY_NAME,
      ISO_CT_CODE_ARR = ARR_ISO_CTY_CODE,
      DEP,
      FROM_DATE,
      TO_DATE,
      LAST_DATA_DAY,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>%
    arrange(ARP_CODE, desc(YEAR), R_RANK, ISO_CT_NAME_ARR)


  df_y2d %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck 
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_state_des_y2d",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE, desc(YEAR), R_RANK, ISO_CT_NAME_ARR)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  #   ap_ms_app_day1 <- df_y2d %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, ap_ms_app_day1)))
  # }
  
  print(paste(format(now(), "%H:%M:%S"), mydataframe, mydate))
}
  
# ap ap des ----
ap_ap_des <- function(mydate =  current_day) {
  mydataframe <-  "ap_ap_des"
  
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
    filter(ENTRY_DATE %in% c(mydate, day_prev_week, day_2019, day_prev_year)) %>% 
    left_join(dim_airport, by = c("DEP_ARP_PRU_ID" = "APT_ID")) %>% 
    left_join(dim_airport, by = c("ARR_ARP_PRU_ID" = "APT_ID"),
              suffix = c("_DEP", "_ARR")) %>% 
    group_by(DEP_ARP_PRU_ID, ENTRY_DATE) %>%
    arrange(APT_ICAO_CODE_DEP, ENTRY_DATE, desc(DEP), APT_NAME_ARR) %>% 
    mutate(
      R_RANK = case_when( 
        ENTRY_DATE == mydate ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        ENTRY_DATE == mydate ~ min_rank(desc(DEP)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        ENTRY_DATE == day_prev_week ~ min_rank(desc(DEP)),
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
    group_by(APT_NAME_DEP, APT_NAME_ARR) %>% 
    arrange(APT_NAME_DEP, APT_NAME_ARR, desc(ENTRY_DATE)) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(LAST_DATA_DAY = max(ENTRY_DATE)) %>% 
    select(
      FLAG_PERIOD,
      ARP_CODE_DEP = APT_ICAO_CODE_DEP,
      ARP_NAME_DEP = APT_NAME_DEP,
      ARP_CODE_ARR = APT_ICAO_CODE_ARR,
      ARP_NAME_ARR = APT_NAME_ARR,
      DEP,
      TO_DATE = ENTRY_DATE,
      LAST_DATA_DAY,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>% 
    arrange(ARP_CODE_DEP, FLAG_PERIOD, R_RANK, ARP_NAME_ARR)
  
  df_day %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_apt_des_day",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE_DEP, FLAG_PERIOD, R_RANK, ARP_NAME_ARR)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE_DEP == list_airport$APT_ICAO_CODE[i])
  # 
  #   df_day1 <- df_day %>% filter(ARP_CODE_DEP == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, df_day1)))
  #   #
  #   # for (j in 1:nrow(df)) {
  #   #
  #   #   print(paste(j, df1[[j,1]] == df_day1[[j,1]]))
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
      FLAG_PERIOD = case_when( 
        (ENTRY_DATE >= mydate - days(6) & ENTRY_DATE <= mydate) ~ "CURRENT_ROLLING_WEEK",
        (ENTRY_DATE >= day_prev_week - days(6) & ENTRY_DATE <= day_prev_week) ~ "PREV_ROLLING_WEEK",
        (ENTRY_DATE >= day_2019 - days(6) & ENTRY_DATE <= day_2019) ~ "ROLLING_WEEK_2019",
        (ENTRY_DATE >= day_prev_year - days(6) & ENTRY_DATE <= day_prev_year) ~ "ROLLING_WEEK_PREV_YEAR"
      )
      ) %>% 
    group_by(FLAG_PERIOD) %>% 
    mutate(
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE)
    ) %>% 
    ungroup() %>% 
    summarise(
      DEP = sum(DEP, na.rm = TRUE),
      .by = c(FLAG_PERIOD, DEP_ARP_PRU_ID, ARR_ARP_PRU_ID, FROM_DATE, TO_DATE)
    ) %>% 
    ungroup() %>% 
    left_join(dim_airport, by = c("DEP_ARP_PRU_ID" = "APT_ID")) %>% 
    left_join(dim_airport, by = c("ARR_ARP_PRU_ID" = "APT_ID"),
              suffix = c("_DEP", "_ARR")) %>% 
    group_by(DEP_ARP_PRU_ID, FLAG_PERIOD) %>%
    arrange(APT_ICAO_CODE_DEP, FLAG_PERIOD, desc(DEP), APT_NAME_ARR) %>% 
    mutate(
      R_RANK = case_when( 
        FLAG_PERIOD == "CURRENT_ROLLING_WEEK" ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        FLAG_PERIOD == "CURRENT_ROLLING_WEEK" ~ min_rank(desc(DEP)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        FLAG_PERIOD == "PREV_ROLLING_WEEK" ~ min_rank(desc(DEP)),
        .default = NA
      )
    ) %>% 
    ungroup() %>% 
    group_by(APT_NAME_DEP, APT_NAME_ARR) %>% 
    arrange(APT_NAME_DEP, APT_NAME_ARR, FLAG_PERIOD) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(LAST_DATA_DAY = max(TO_DATE)) %>% 
    select(
      FLAG_PERIOD,
      ARP_CODE_DEP = APT_ICAO_CODE_DEP,
      ARP_NAME_DEP = APT_NAME_DEP,
      ARP_CODE_ARR = APT_ICAO_CODE_ARR,
      ARP_NAME_ARR = APT_NAME_ARR,
      DEP,
      FROM_DATE,
      TO_DATE,
      LAST_DATA_DAY,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>% 
    arrange(ARP_CODE_DEP, FLAG_PERIOD, R_RANK, ARP_NAME_ARR)
  
  df_week %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_apt_des_week",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE_DEP, FLAG_PERIOD, R_RANK, ARP_NAME_ARR)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE_DEP == list_airport$APT_ICAO_CODE[i])
  # 
  #   df_day1 <- df_week %>% filter(ARP_CODE_DEP == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, df_day1)))
  #   #
  #   # for (j in 1:nrow(df)) {
  #   #
  #   #   print(paste(j, df1[[j,1]] == df_day1[[j,1]]))
  #   #
  #   # }
  # }
  # 
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
    group_by(YEAR) %>% 
    mutate(
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE)
    ) %>% 
    ungroup() %>%
    summarise(
      DEP = sum(DEP, na.rm = TRUE),
      .by = c(YEAR, DEP_ARP_PRU_ID, ARR_ARP_PRU_ID, FROM_DATE, TO_DATE)
    ) %>% 
    ungroup() %>% 
    left_join(dim_airport, by = c("DEP_ARP_PRU_ID" = "APT_ID")) %>% 
    left_join(dim_airport, by = c("ARR_ARP_PRU_ID" = "APT_ID"),
              suffix = c("_DEP", "_ARR")) %>% 
    group_by(DEP_ARP_PRU_ID, YEAR) %>%
    arrange(DEP_ARP_PRU_ID, YEAR, desc(DEP), APT_NAME_ARR) %>% 
    mutate(
      R_RANK = case_when( 
        YEAR == current_year ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        YEAR == current_year ~ min_rank(desc(DEP)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        YEAR == current_year-1 ~ min_rank(desc(DEP)),
        .default = NA
      )
    ) %>% 
    ungroup() %>% 
    group_by(APT_NAME_DEP, APT_NAME_ARR) %>% 
    arrange(APT_NAME_DEP, APT_NAME_ARR, YEAR) %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "up") %>% 
  
    fill(RANK, .direction = "down") %>% 
    fill(RANK, .direction = "up") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    filter(R_RANK < 11) %>% 
    mutate(LAST_DATA_DAY = max(TO_DATE)) %>% 
    select(
      YEAR,
      ARP_CODE_DEP = APT_ICAO_CODE_DEP,
      ARP_NAME_DEP = APT_NAME_DEP,
      ARP_CODE_ARR = APT_ICAO_CODE_ARR,
      ARP_NAME_ARR = APT_NAME_ARR,
      DEP,
      FROM_DATE,
      TO_DATE,
      LAST_DATA_DAY,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>% 
    arrange(ARP_CODE_DEP, desc(YEAR), R_RANK, ARP_NAME_ARR)
  
  
  df_y2d %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck 
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_apt_des_y2d",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE_DEP, desc(YEAR), R_RANK, ARP_NAME_ARR)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE_DEP == list_airport$APT_ICAO_CODE[i])
  #   ap_ms_app_day1 <- df_y2d %>% filter(ARP_CODE_DEP == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, ap_ms_app_day1)))
  # }

  
  print(paste(format(now(), "%H:%M:%S"), mydataframe, mydate))
  
}

# ap ms ----
ap_ms <- function(mydate =  current_day) {
  mydataframe <-  "ap_ms"

  df_app <- import_dataframe(mydataframe)
  
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
    group_by(APT_ICAO_CODE, ENTRY_DATE) %>% 
    arrange(APT_ICAO_CODE, ENTRY_DATE, desc(DEP_ARR), MARKET_SEGMENT) %>% 
    mutate(
      R_RANK = case_when( 
        ENTRY_DATE == mydate ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        ENTRY_DATE == mydate ~ min_rank(desc(DEP_ARR)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        ENTRY_DATE == day_prev_week ~ min_rank(desc(DEP_ARR)),
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
    group_by(APT_NAME, MARKET_SEGMENT) %>% 
    arrange(APT_NAME, MARKET_SEGMENT, desc(ENTRY_DATE)) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    mutate(LAST_DATA_DAY = max(ENTRY_DATE)) %>% 
    select(
      ARP_CODE = APT_ICAO_CODE,
      ARP_NAME = APT_NAME,
      FLAG_PERIOD,
      TO_DATE = ENTRY_DATE,
      LAST_DATA_DAY,
      MARKET_SEGMENT,
      DEP_ARR,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>% 
    arrange(ARP_CODE, FLAG_PERIOD, R_RANK, MARKET_SEGMENT)
  
  df_day %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  
  # # dcheck
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_ms_day",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE, FLAG_PERIOD, R_RANK, MARKET_SEGMENT)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   ap_ms_app_day1 <- df_day %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, ap_ms_app_day1)))
  # }

  
  ## week ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_week_raw.csv")

  df_week <- df_app %>% 
    filter(
      (ENTRY_DATE >= mydate - days(6) & ENTRY_DATE <= mydate) | 
        (ENTRY_DATE >= day_prev_week - days(6) & ENTRY_DATE <= day_prev_week) | 
        (ENTRY_DATE >= day_prev_year - days(6) & ENTRY_DATE <= day_prev_year) | 
        (ENTRY_DATE >= day_2019 - days(6) & ENTRY_DATE <= day_2019)  
    ) %>% 
    mutate(
      FLAG_PERIOD = case_when( 
        (ENTRY_DATE >= mydate - days(6) & ENTRY_DATE <= mydate) ~ "CURRENT_ROLLING_WEEK",
        (ENTRY_DATE >= day_prev_week - days(6) & ENTRY_DATE <= day_prev_week) ~ "PREV_ROLLING_WEEK",
        (ENTRY_DATE >= day_2019 - days(6) & ENTRY_DATE <= day_2019) ~ "ROLLING_WEEK_2019",
        (ENTRY_DATE >= day_prev_year - days(6) & ENTRY_DATE <= day_prev_year) ~ "ROLLING_WEEK_PREV_YEAR"
      )
    ) %>% 
    group_by(FLAG_PERIOD) %>% 
    mutate(
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE)
    ) %>% 
    ungroup() %>%
    group_by(APT_ICAO_CODE, APT_NAME, MARKET_SEGMENT, FLAG_PERIOD, FROM_DATE, TO_DATE) %>%
    summarise(
      DEP_ARR = sum(DEP_ARR, na.rm = TRUE),
    ) %>% 
    ungroup() %>% 
    group_by(APT_ICAO_CODE, APT_NAME, FLAG_PERIOD) %>%
    arrange(APT_ICAO_CODE, FLAG_PERIOD, desc(DEP_ARR)) %>% 
    mutate(
      R_RANK = case_when( 
        FLAG_PERIOD == "CURRENT_ROLLING_WEEK" ~ row_number(),
        .default = NA
      ),
      RANK = case_when( 
        FLAG_PERIOD == "CURRENT_ROLLING_WEEK" ~ min_rank(desc(DEP_ARR)),
        .default = NA
      ),
      RANK_PREV = case_when( 
        FLAG_PERIOD == "PREV_ROLLING_WEEK" ~ min_rank(desc(DEP_ARR)),
        .default = NA
      )
    ) %>% 
    ungroup() %>% 
    group_by(APT_NAME, MARKET_SEGMENT) %>% 
    arrange(APT_NAME, MARKET_SEGMENT, FLAG_PERIOD) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(RANK, .direction = "up") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "up") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    mutate(LAST_DATA_DAY = max(TO_DATE)) %>% 
    select(
      ARP_CODE = APT_ICAO_CODE,
      ARP_NAME = APT_NAME,
      FLAG_PERIOD,
      FROM_DATE,
      TO_DATE,
      LAST_DATA_DAY,
      MARKET_SEGMENT,
      DEP_ARR,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>% 
    arrange(ARP_CODE, FLAG_PERIOD, R_RANK, MARKET_SEGMENT)
  
  df_week %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck 
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_ms_week",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE, FLAG_PERIOD, R_RANK, MARKET_SEGMENT)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  #   ap_ms_app_day1 <- df_week %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, ap_ms_app_day1)))
  # }
  
  ## year ----
  mycsvfile <- paste0(data_day_text, "_", mydataframe, "_data_y2d_raw.csv")
  
  df_y2d <- df_app %>% 
    filter(format(ENTRY_DATE, "%m-%d") <= format(mydate, "%m-%d")) %>%
    filter(year(ENTRY_DATE) %in% c(current_year, current_year-1, 2019)) %>%
    mutate(
      YEAR = year(ENTRY_DATE)
    ) %>% 
    group_by(YEAR) %>% 
    mutate(
      TO_DATE = max(ENTRY_DATE, na.rm = TRUE),
      FROM_DATE = min(ENTRY_DATE, na.rm = TRUE)
    ) %>% 
    ungroup() %>%
    group_by(APT_ICAO_CODE, APT_NAME, MARKET_SEGMENT, YEAR, FROM_DATE, TO_DATE) %>%
    summarise(
      DEP_ARR = sum(DEP_ARR, na.rm = TRUE)
    ) %>% 
    ungroup() %>% 
    group_by(APT_ICAO_CODE, APT_NAME, YEAR) %>%
    arrange(APT_ICAO_CODE, YEAR, desc(DEP_ARR)) %>% 
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
    group_by(APT_NAME, MARKET_SEGMENT) %>% 
    arrange(APT_NAME, MARKET_SEGMENT, YEAR) %>% 
    fill(RANK, .direction = "down") %>% 
    fill(RANK, .direction = "up") %>% 
    fill(R_RANK, .direction = "down") %>% 
    fill(R_RANK, .direction = "up") %>% 
    fill(RANK_PREV, .direction = "down") %>% 
    fill(RANK_PREV, .direction = "up") %>% 
    ungroup() %>% 
    mutate(LAST_DATA_DAY = max(TO_DATE)) %>% 
    select(
      ARP_CODE = APT_ICAO_CODE,
      ARP_NAME = APT_NAME,
      YEAR,
      FROM_DATE,
      TO_DATE,
      LAST_DATA_DAY,
      MARKET_SEGMENT,
      DEP_ARR,
      R_RANK,
      RANK,
      RANK_PREV
    ) %>% 
    arrange(ARP_CODE, desc(YEAR), R_RANK, MARKET_SEGMENT)
  
  df_y2d %>% write_csv(here(archive_dir_raw, stakeholder, mycsvfile))
  
  # # dcheck 
  # df <- read_xlsx(
  #   path  = fs::path_abs(
  #     str_glue(ap_base_file),
  #     start = ap_base_dir),
  #   sheet = "apt_ms_y2d",
  #   range = cell_limits(c(1, 1), c(NA, NA))) |>
  #   mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
  # 
  # df <- df %>% arrange(ARP_CODE, desc(YEAR), R_RANK, MARKET_SEGMENT)
  # 
  # for (i in 1:nrow(list_airport)) {
  #   df1 <- df %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  #   ap_ms_app_day1 <- df_y2d %>% filter(ARP_CODE == list_airport$APT_ICAO_CODE[i])
  # 
  #   print(paste(list_airport$APT_ICAO_CODE[i], all.equal(df1, ap_ms_app_day1)))
  # }
  
  print(paste(format(now(), "%H:%M:%S"), mydataframe, mydate))
}

# execute functions ----
# wef <- "2024-01-01"  #included in output
# til <- "2025-09-07"  #included in output
# current_day <- seq(ymd(til), ymd(wef), by = "-1 day")

ap_traffic_delay()
purrr::walk(current_day, ap_ao)
purrr::walk(current_day, ap_st_des)
purrr::walk(current_day, ap_ap_des)
purrr::walk(current_day, ap_ms)

