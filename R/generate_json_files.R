## libraries
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


# json files for mobile web
  data_folder <- here::here("data")
  base_dir <- '//ihx-vdm05/LIVE_var_www_Economics$/Download'
  today <- (lubridate::now() +  days(-1)) %>% format("%Y%m%d")
  nw_json_app <-""

  # traffic data
  nw_traffic_data <-  read_xlsx(
    path  = fs::path_abs(
      str_glue("Network_Traffic.xlsx"),
      start = base_dir),
    sheet = "Data",
    range = cell_limits(c(1, 1), c(NA, 39))) %>%
    as_tibble()

  nw_traffic_json <- nw_traffic_data %>%
    filter(FLIGHT_DATE == max(LAST_DATA_DAY))%>%
    toJSON() %>%
    substr(., 1, nchar(.)-1) %>%
    substr(., 2, nchar(.))


  # delay data
  base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/'

  nw_delay_data <-  read_xlsx(
    path  = fs::path_abs(
      str_glue("99_Traffic_Landing_Page_dataset_{today}.xlsx"),
      start = base_dir),
    sheet = "NM_Daily_Delay_All",
    range = cell_limits(c(2, 1), c(NA, 39))) %>%
    as_tibble()

  nw_delay_json  <- nw_delay_data %>%
    mutate(FLIGHT_DATE = as.Date(FLIGHT_DATE)) %>%
    filter(FLIGHT_DATE == max(LAST_DATA_DAY)) %>%
    toJSON() %>%
    substr(., 1, nchar(.)-1) %>%
    substr(., 2, nchar(.))

  # punctuality data

  nw_punct_data_raw <-  read_xlsx(
    path  = fs::path_abs(
      str_glue("98_PUNCTUALITY_{today}.xlsx"),
      start = base_dir),
    sheet = "NETWORK",
    range = cell_limits(c(1, 1), c(NA, NA))) %>%
    as_tibble()%>%
    mutate(DATE = as.Date(DATE, format = "%d-%m-%Y"))

  last_day <-  max(nw_punct_data_raw$DATE)
  last_year <- as.numeric(format(last_day,'%Y'))

  nw_punct_data_d_w <- nw_punct_data_raw%>%
    arrange(DATE)%>%
    mutate(YEAR_FLIGHT = as.numeric(format(DATE,'%Y')))%>%
    mutate(ARR_PUN_PREV_YEAR = lag(ARR_PUNCTUALITY_PERCENTAGE, 364),
           DEP_PUN_PREV_YEAR = lag(DEP_PUNCTUALITY_PERCENTAGE, 364),
           ARR_PUN_2019 =if_else(YEAR_FLIGHT == last_year,
                                 lag(ARR_PUNCTUALITY_PERCENTAGE, 364*(last_year-2019)+floor((last_year-2019)/4)*7),
                                 1),
           DEP_PUN_2019 =if_else(YEAR_FLIGHT == last_year,
                                 lag(DEP_PUNCTUALITY_PERCENTAGE, 364*(last_year-2019)+floor((last_year-2019)/4)*7),
                                 1),
           DAY_2019 = if_else(YEAR_FLIGHT == last_year,
                              lag(DATE, 364*(last_year-2019)+floor((last_year-2019)/4)*7)
                              ,last_day)
    ) %>%
    mutate(DEP_PUN_WK = rollsum((DEP_PUNCTUAL_FLIGHTS), 7, fill = NA, align = "right")/rollsum(DEP_SCHEDULE_FLIGHT,7, fill = NA, align = "right")*100,
           ARR_PUN_WK = rollsum((ARR_PUNCTUAL_FLIGHTS), 7, fill = NA, align = "right")/rollsum(ARR_SCHEDULE_FLIGHT,7, fill = NA, align = "right")*100)%>%
    mutate(ARR_PUN_WK_PREV_YEAR = lag(ARR_PUN_WK, 364),
           DEP_PUN_WK_PREV_YEAR = lag(DEP_PUN_WK, 364),
           ARR_PUN_WK_2019 =if_else(YEAR_FLIGHT == last_year,
                                    lag(ARR_PUN_WK, 364*(last_year-2019)+floor((last_year-2019)/4)*7),
                                    1),
           DEP_PUN_WK_2019 =if_else(YEAR_FLIGHT == last_year,
                                    lag(DEP_PUN_WK, 364*(last_year-2019)+floor((last_year-2019)/4)*7),
                                    1)
    ) %>%
    filter (DATE==last_day) %>%
    mutate(INDEX = 1)

  nw_punct_data_y2d <- nw_punct_data_raw%>%
    arrange(DATE)%>%
    mutate(YEAR_FLIGHT = as.numeric(format(DATE,'%Y')))%>%
    mutate(MONTH_DAY = as.numeric(format(DATE, format="%m%d")))%>%
    filter(MONTH_DAY<=as.numeric(format(last_day, format="%m%d")))%>%
    mutate(YEAR = as.numeric(format(DATE, format="%Y")))%>%
    group_by(YEAR)%>%
    summarise (ARR_PUN_Y2D = sum(ARR_PUNCTUAL_FLIGHTS, na.rm=TRUE)/sum(ARR_SCHEDULE_FLIGHT, na.rm=TRUE)*100,
               DEP_PUN_Y2D = sum(DEP_PUNCTUAL_FLIGHTS, na.rm=TRUE)/sum(DEP_SCHEDULE_FLIGHT, na.rm=TRUE)*100)%>%
    mutate(Y2D_ARR_PUN_PREV_YEAR = lag(ARR_PUN_Y2D, 1),
           Y2D_DEP_PUN_PREV_YEAR = lag(DEP_PUN_Y2D, 1),
           Y2D_ARR_PUN_2019 = lag(ARR_PUN_Y2D, last_year-2019),
           Y2D_DEP_PUN_2019 = lag(DEP_PUN_Y2D, last_year-2019)
    ) %>%
    filter(YEAR == as.numeric(format(last_day, format="%Y"))) %>%
    select(ARR_PUN_Y2D, DEP_PUN_Y2D, Y2D_ARR_PUN_PREV_YEAR, Y2D_DEP_PUN_PREV_YEAR, Y2D_ARR_PUN_2019, Y2D_DEP_PUN_2019) %>%
    mutate(INDEX = 1)


  nw_punct_json <- merge(nw_punct_data_d_w, nw_punct_data_y2d, by="INDEX") %>% select(-INDEX) %>%
    toJSON() %>%
    substr(., 1, nchar(.)-1) %>%
    substr(., 2, nchar(.))

  ####CO2 json
  
  export_query <- function(schema, query) {
    USR <- Sys.getenv(paste0(schema, "_USR"))
    PWD <- Sys.getenv(paste0(schema, "_PWD"))
    DBN <- Sys.getenv(paste0(schema, "_DBNAME"))
    withr::local_envvar(c("TZ" = "UTC", "ORA_SDTZ" = "UTC"))
    withr::local_namespace("ROracle")
    con <- withr::local_db_connection(
      DBI::dbConnect(DBI::dbDriver("Oracle"),
                     USR, PWD, dbname = DBN,
                     timezone = "UTC"))
    con %>%
      dbSendQuery(query) %>%
      fetch(n = -1)
  }
  
  query <- str_glue("
    SELECT *
      FROM TABLE (emma_pub.api_aiu_stats.MM_AIU_STATE_DEP ())
      where year >= 2019 and STATE_NAME not in ('LIECHTENSTEIN')
    ORDER BY 2, 3, 4
   ")
  
  
  co2_data_raw <- export_query("PRU_DEV", query = query) %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

  co2_data_evo_nw <- co2_data_raw %>%
    select(FLIGHT_MONTH, CO2_QTY_TONNES, TF, YEAR, MONTH) %>%
    group_by(FLIGHT_MONTH) %>%
    summarise(TTF = sum(TF), TCO2 = sum(CO2_QTY_TONNES)) %>%
    mutate(YEAR = as.numeric(format(FLIGHT_MONTH,'%Y')), MONTH = as.numeric(format(FLIGHT_MONTH,'%m'))) %>%
    arrange(FLIGHT_MONTH) %>% 
    mutate(FLIGHT_MONTH = ceiling_date(as_date(FLIGHT_MONTH), unit='month')-1)

  co2_last_date <- max(co2_data_evo_nw$FLIGHT_MONTH, na.rm=TRUE)
  co2_last_month <- format(co2_last_date,'%B')
  CO2_last_month_num <- as.numeric(format(co2_last_date,'%m'))
  co2_last_year <- max(co2_data_evo_nw$YEAR)
    
  co2_for_json <- co2_data_evo_nw %>%
    mutate(MONTH_TEXT = format(FLIGHT_MONTH,'%B'),
           TCO2_PREV_YEAR = lag(TCO2, 12),
           TTF_PREV_YEAR = lag(TTF, 12),
           TCO2_2019 = lag(TCO2, (as.numeric(co2_last_year)-2019)*12),
           TTF_2019 = lag(TTF, (as.numeric(co2_last_year)-2019)*12)) %>%
    mutate(DIF_CO2_MONTH_PREV_YEAR = TCO2 / TCO2_PREV_YEAR -1,
           DIF_TTF_MONTH_PREV_YEAR = TTF / TTF_PREV_YEAR -1,
           DIF_CO2_MONTH_2019 = TCO2 / TCO2_2019 -1,
           DIF_TTF_MONTH_2019 = TTF / TTF_2019 -1,
    ) %>% 
    group_by(YEAR) %>%
    mutate(YTD_TCO2 = cumsum(TCO2),
           YTD_TTF = cumsum(TTF))%>%
    ungroup() %>% 
    mutate(YTD_TCO2_PREV_YEAR = lag(YTD_TCO2, 12),
           YTD_TTF_PREV_YEAR = lag(YTD_TTF, 12),
           YTD_TCO2_2019 = lag(YTD_TCO2, (as.numeric(co2_last_year)-2019)*12),
           YTD_TTF_2019 = lag(YTD_TTF, (as.numeric(co2_last_year)-2019)*12)) %>%
    mutate(YTD_DIF_CO2_MONTH_PREV_YEAR = YTD_TCO2 / YTD_TCO2_PREV_YEAR -1,
           YTD_DIF_TTF_MONTH_PREV_YEAR = YTD_TTF / YTD_TTF_PREV_YEAR -1,
           YTD_DIF_CO2_MONTH_2019 = YTD_TCO2 / YTD_TCO2_2019 -1,
           YTD_DIF_TTF_MONTH_2019 = YTD_TTF / YTD_TTF_2019 -1,
    ) %>% 
    filter(FLIGHT_MONTH == co2_last_date)
  
  nw_co2_json <- co2_for_json %>% 
    toJSON() %>%
    substr(., 1, nchar(.)-1) %>%
    substr(., 2, nchar(.))
  
  
  # join data strings and save
  nw_json_app <- paste0("{",
                        '"nw_traffic":', nw_traffic_json,
                        ', "nw_delay":', nw_delay_json, 
                        ', "nw_punct":', nw_punct_json,
                        ', "nw_co2":', nw_co2_json,
                        "}")
  write(nw_json_app, here(data_folder,"nw_json_app.json"))
  

# -----------------------------------------------------------------------------------------------------------------------------------------
  ####CSVs for mobile app graphs
  ### traffic

    nw_traffic_evo_app <- nw_traffic_data %>%
    select(FLIGHT_DATE, AVG_ROLLING_WEEK, AVG_ROLLING_WEEK_PREV_YEAR,
           AVG_ROLLING_WEEK_2020, AVG_ROLLING_WEEK_2019)

  column_names <- c('FLIGHT_DATE', last_year, last_year-1, 2020, 2019)
  colnames(nw_traffic_evo_app) <- column_names

  write.csv(nw_traffic_evo_app,
            file = here(data_folder,"nw_traffic_evo_app.csv"),
            row.names = FALSE)

    # json
  # nw_traffic_evo_app_j <- nw_traffic_evo_app %>% toJSON()
  # write(nw_traffic_evo_app_j, paste0(data_folder, "nw_traffic_evo_app.json"))

  ### delay
  nw_delay_raw <-  read_xlsx(
    path  = fs::path_abs(
      str_glue("99_Traffic_Landing_Page_dataset_{today}.xlsx"),
      start = base_dir),
    sheet = "NM_Delay_for_graph",
    range = cell_limits(c(2, 1), c(NA, 29))) %>%
    as_tibble()

  nw_delay_evo_app <- nw_delay_raw %>%
    filter(FLIGHT_DATE>=paste0(last_year-1,'-06-01'))%>%
    # mutate(FLIGHT_YEAR = as.character(format(FLIGHT_DATE,'%Y')))%>%
    mutate(FLIGHT_DATE = as.Date(FLIGHT_DATE))%>%
    mutate(
      # ROLL_WK_PER_DLY_ERT= ifelse(ROLL_WK_AVG_DLY == 0, 0, round(ROLL_WK_AVG_DLY_ERT/ROLL_WK_AVG_DLY,2)),
      # ROLL_WK_PER_DLY_APT= ifelse(ROLL_WK_AVG_DLY == 0, 0, round(ROLL_WK_AVG_DLY_APT/ROLL_WK_AVG_DLY,2)),
      ROLL_WK_PER_DLY_CAP_STAF= ifelse(ROLL_WK_AVG_DLY == 0,
                                       0,
                                       round(ROLL_WK_AVG_DLY_CAP_STAF/ROLL_WK_AVG_DLY,2)),
      ROLL_WK_PER_DLY_DISR= ifelse(ROLL_WK_AVG_DLY == 0,
                                   0,
                                   round(ROLL_WK_AVG_DLY_DISR/ROLL_WK_AVG_DLY,2)),
      ROLL_WK_PER_DLY_WTH= ifelse(ROLL_WK_AVG_DLY == 0,
                                  0,
                                  round(ROLL_WK_AVG_DLY_WTH/ROLL_WK_AVG_DLY,2)),
      ROLL_WK_PER_DLY_OTH= ifelse(ROLL_WK_AVG_DLY == 0,
                                  0,
                                  round(ROLL_WK_AVG_DLY_OTH/ROLL_WK_AVG_DLY,2))
    ) %>%
    select(FLIGHT_DATE,
           ROLL_WK_AVG_DLY_CAP_STAF, ROLL_WK_AVG_DLY_DISR,
           ROLL_WK_AVG_DLY_WTH, ROLL_WK_AVG_DLY_OTH)

  column_names <- c('FLIGHT_DATE',
                    "Capacity/Staffing",
                    "Disruptions (ATC)",
                    "Weather",
                    "Other")
  colnames(nw_delay_evo_app) <- column_names

  write.csv(nw_delay_evo_app,
            file = here(data_folder,"nw_delay_evo_app.csv"),
            row.names = FALSE)

  #punctuality
  nw_punct_evo_app <- nw_punct_data_raw %>%
    filter(DATE >= as.Date(paste0("01-01-", last_year-2), format = "%d-%m-%Y")) %>%
    arrange(DATE) %>%
    mutate(DEP_PUN = DEP_PUNCTUALITY_PERCENTAGE, ARR_PUN = ARR_PUNCTUALITY_PERCENTAGE, OPERATED = 100-MISSING_SCHEDULES_PERCENTAGE)%>%
    mutate(DEP_PUN_WK = rollsum(DEP_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right")/rollsum(DEP_SCHEDULE_FLIGHT,7, fill = NA, align = "right")*100,
           ARR_PUN_WK = rollsum(ARR_PUNCTUAL_FLIGHTS, 7, fill = NA, align = "right")/rollsum(ARR_SCHEDULE_FLIGHT,7, fill = NA, align = "right")*100,
           OP_FLT_WK = 100-rollsum(MISSING_SCHED_FLIGHTS, 7, fill = NA, align = "right")/rollsum((MISSING_SCHED_FLIGHTS+DEP_FLIGHTS_NO_OVERFLIGHTS),7, fill = NA, align = "right")*100) %>%
    select(DATE, DEP_PUN_WK, ARR_PUN_WK, OP_FLT_WK) %>%
    filter(DATE >= as.Date(paste0("01-01-", last_year-1), format = "%d-%m-%Y"))


  column_names <- c('FLIGHT_DATE',
                    "Departure punct.",
                    "Arrival punct.",
                    "Operated schedules")
  colnames(nw_punct_evo_app) <- column_names

  write.csv(nw_punct_evo_app,
            file = here(data_folder,"nw_punct_evo_app.csv"),
            row.names = FALSE)

  
  # billing
  dir_billing <- "//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Crco - Billing/output/Oscar_playground"
  
  nw_billing_data <-  read_xlsx(
    path  = fs::path_abs(
      str_glue("AIU monthly reports_automatised_for AIU Portal.xlsx"),
      start = dir_billing),
    sheet = "AIU_PORTAL",
    range = cell_limits(c(7, 2), c(NA, 4))) %>%
    as_tibble() 
  
  nw_billing_evo_app <- nw_billing_data %>% 
    arrange(Year, Month) %>% 
    mutate(Total = Total/10^6,
          "PREV_YEAR" = lag(Total,12 )) %>% 
    filter(Year >= current_year-1) %>% 
    select(Month, Year, Total) %>% 
    spread(Year, Total) %>% 
    mutate(Month = as.Date(paste0("01",Month,current_year), "%d%m%Y"))  
    

    # (as.numeric(co2_last_year)-2019)*12)
    
  # column_names <- c('FLIGHT_DATE',
  #                   "Departure punct.",
  #                   "Arrival punct.",
  #                   "Operated schedules")
  # colnames(nw_punct_evo_app) <- column_names
  
  write.csv(nw_billing_evo_app,
            file = here(data_folder,"nw_bill_evo_app.csv"),
            row.names = FALSE)  
    
# -----------------------------------------------------------------------------------------------------------------------------------------
# #json files for denis dailytrafficvariation
#   ########## APT
#     base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive'
#   # base_dir <- 'Z:/Data/DataProcessing/Covid19/Oscar/Develop'
# 
# 
#   apt_data <- read_xlsx(
#     path  = fs::path_abs(
#       str_glue("0_Top_Airport_dep+arr_traffic (Synthesis)_{today}.xlsx"),
#       # '0_Top_100_Airport_dep+arr_traffic(Synthesis)_test.xlsx',
#           start = base_dir),
#     sheet = "DATA",
#     range = cell_limits(c(11, 15), c(NA, 26))) %>%
#     mutate(across(.cols = where(is.instant), ~ as.Date(.x)))
# 
#   apt_data_type <- sapply(apt_data, class) %>%
#     str_replace(., 'Date', 'date') %>%
#     str_replace(., 'numeric', 'number') %>%
#     str_replace(., 'integer', 'number') %>%
#     str_replace(., 'character', 'string') %>%
#     toJSON()
# 
# 
#   apt_data_headers <- names(apt_data) %>% toJSON()
# 
#   apt_data_matrix <- as.matrix(apt_data)
#   apt_data_json <-   jsonlite::toJSON(apt_data_matrix)%>%
#     str_sub(.,2,-2)
# 
#   apt_data_json <- paste0("[", apt_data_headers, ",",apt_data_type, ",", apt_data_json,"]")
# 
#   write(apt_data_json, "//ihx-vdm05/LIVE_var_www_Economics$/Oscar/Old/apt_data.json")
