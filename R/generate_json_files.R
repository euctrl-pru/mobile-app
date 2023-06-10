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
  base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/'
  base_file <- '99_Traffic_Landing_Page_dataset_{today}.xlsx'
  today <- (lubridate::now() +  days(-1)) %>% format("%Y%m%d")
  nw_json_app <-""

  # traffic data
  nw_traffic_data <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(base_file),
      start = base_dir),
    sheet = "NM_Daily_Traffic_All",
    range = cell_limits(c(2, 1), c(NA, 39))) %>%
    as_tibble()


  nw_traffic_last_day <- nw_traffic_data %>%
    filter(FLIGHT_DATE == max(LAST_DATA_DAY))

  nw_traffic_json <- nw_traffic_last_day %>%
    filter(FLIGHT_DATE == max(LAST_DATA_DAY)) %>%
    select(
      FLIGHT_DATE,
      DAY_TFC,
      DAY_DIFF_PREV_YEAR_PERC,
      DAY_TFC_DIFF_2019_PERC,
      AVG_ROLLING_WEEK,
      DIF_WEEK_PREV_YEAR_PERC,
      DIF_ROLLING_WEEK_2019_PERC,
      Y2D_TFC_YEAR,
      Y2D_AVG_TFC_YEAR,
      Y2D_DIFF_PREV_YEAR_PERC,
      Y2D_DIFF_2019_PERC
    )  %>%
    toJSON() %>%
    substr(., 1, nchar(.)-1) %>%
    substr(., 2, nchar(.))


  # delay data

  nw_delay_data <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(base_file),
      start = base_dir),
    sheet = "NM_Daily_Delay_All",
    range = cell_limits(c(2, 1), c(NA, 39))) %>%
    as_tibble()

  nw_delay_for_json  <- nw_delay_data %>%
    mutate(FLIGHT_DATE = as.Date(FLIGHT_DATE)) %>%
    filter(FLIGHT_DATE == max(LAST_DATA_DAY)) %>%
    mutate(
      DAY_DLY_FLT = DAY_DLY / nw_traffic_last_day$DAY_TFC,
      DAY_DLY_FLT_PY = DAY_DLY_PREV_YEAR / nw_traffic_last_day$DAY_TFC_PREV_YEAR,
      DAY_DLY_FLT_2019 = DAY_DLY_2019 / nw_traffic_last_day$DAY_TFC_2019,
      DAY_DLY_FLT_DIF_PY_PERC = if_else(
        DAY_DLY_FLT_PY == 0, NA , DAY_DLY_FLT / DAY_DLY_FLT_PY -1
      ),
      DAY_DLY_FLT_DIF_2019_PERC = if_else(
        DAY_DLY_FLT_2019 == 0, NA , DAY_DLY_FLT / DAY_DLY_FLT_2019 -1
      ),

      RWEEK_DLY_FLT = TOTAL_ROLLING_WEEK / nw_traffic_last_day$TOTAL_ROLLING_WEEK,
      RWEEK_DLY_FLT_PY = AVG_ROLLING_WEEK_PREV_YEAR / nw_traffic_last_day$AVG_ROLLING_WEEK_PREV_YEAR,
      RWEEK_DLY_FLT_2019 = AVG_ROLLING_WEEK_2019 / nw_traffic_last_day$AVG_ROLLING_WEEK_2019,
      RWEEK_DLY_FLT_DIF_PY_PERC = if_else(
        RWEEK_DLY_FLT_PY == 0, NA , RWEEK_DLY_FLT / RWEEK_DLY_FLT_PY -1
      ),
      RWEEK_DLY_FLT_DIF_2019_PERC = if_else(
        RWEEK_DLY_FLT_2019 == 0, NA , RWEEK_DLY_FLT / RWEEK_DLY_FLT_2019 -1
      ),

      Y2D_DLY_FLT = Y2D_DLY_YEAR / nw_traffic_last_day$Y2D_TFC_YEAR,
      Y2D_DLY_FLT_PY = Y2D_AVG_DLY_PREV_YEAR / nw_traffic_last_day$Y2D_AVG_TFC_PREV_YEAR,
      Y2D_DLY_FLT_2019 = Y2D_AVG_DLY_2019 / nw_traffic_last_day$Y2D_AVG_TFC_2019,
      Y2D_DLY_FLT_DIF_PY_PERC = if_else(
        Y2D_DLY_FLT_PY == 0, NA , Y2D_DLY_FLT / Y2D_DLY_FLT_PY -1
      ),
      Y2D_DLY_FLT_DIF_2019_PERC = if_else(
        Y2D_DLY_FLT_2019 == 0, NA , Y2D_DLY_FLT / Y2D_DLY_FLT_2019 -1
      )

    ) %>%
    select(
      FLIGHT_DATE,
      DAY_DLY,
      DAY_DIFF_PREV_YEAR_PERC,
      DAY_DLY_DIFF_2019_PERC,
      DAY_DLY_FLT,
      DAY_DLY_FLT_DIF_PY_PERC,
      DAY_DLY_FLT_DIF_2019_PERC,

      AVG_ROLLING_WEEK,
      DIF_WEEK_PREV_YEAR_PERC,
      DIF_ROLLING_WEEK_2019_PERC,
      RWEEK_DLY_FLT,
      RWEEK_DLY_FLT_DIF_PY_PERC,
      RWEEK_DLY_FLT_DIF_2019_PERC,

      Y2D_AVG_DLY_YEAR,
      Y2D_DIFF_PREV_YEAR_PERC,
      Y2D_DIFF_2019_PERC,
      Y2D_DLY_FLT,
      Y2D_DLY_FLT_DIF_PY_PERC,
      Y2D_DLY_FLT_DIF_2019_PERC
    )

  nw_delay_json <- nw_delay_for_json %>%
    toJSON() %>%
    substr(., 1, nchar(.)-1) %>%
    substr(., 2, nchar(.))

  # punctuality data
  ### select * from LDW_VDM.VIEW_FAC_PUNCTUALITY_NW_DAY

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
    select(ARR_PUNCTUALITY_PERCENTAGE, DEP_PUNCTUALITY_PERCENTAGE,
           ARR_PUN_PREV_YEAR, DEP_PUN_PREV_YEAR,
           ARR_PUN_2019, DEP_PUN_2019,
           ARR_PUN_WK, DEP_PUN_WK, ARR_PUN_WK_PREV_YEAR, DEP_PUN_WK_PREV_YEAR, ARR_PUN_WK_2019, DEP_PUN_WK_2019) %>%
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

  #check last month number of flights
  check_flights <- co2_data_evo_nw %>%
    filter (YEAR == max(YEAR)) %>% filter(MONTH == max(MONTH)) %>%  select(TTF) %>% pull()

  if (check_flights < 1000) {
    co2_data_raw <-  co2_data_raw %>% filter (FLIGHT_MONTH < max(FLIGHT_MONTH))
    co2_data_evo_nw <- co2_data_evo_nw %>% filter (FLIGHT_MONTH < max(FLIGHT_MONTH))
    co2_last_date <- max(co2_data_evo_nw$FLIGHT_MONTH, na.rm=TRUE)
  }

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


  ####billing json
  dir_billing <- "G:/HQ/dgof-pru/Data/DataProcessing/Covid19/Oscar/Billing"

  nw_billed_data_raw <-  read_xlsx(
    path  = fs::path_abs(
      str_glue("Billing_tables.xlsx"),
      start = dir_billing),
    sheet = "network",
    range = cell_limits(c(5, 2), c(NA, NA))) %>%
    as_tibble()%>%
    mutate(DATE = as.Date(Billing_period_start_date, format = "%d-%m-%Y"))

  last_billing_date <- max(nw_billed_data_raw$DATE)

  nw_billed_json <- nw_billed_data_raw %>%
    arrange(Year, DATE) %>%
    mutate(
      MONTH_F = format(DATE,'%B'),
      BILL_MONTH_PY = lag(Total, 12),
      DIF_BILL_MONTH_PY = Total/BILL_MONTH_PY -1,
      BILLED = round(Total/1000000,0)
    ) %>%
    filter(DATE == last_billing_date) %>%
    select(MONTH_F, BILLED, DIF_BILL_MONTH_PY) %>%
    toJSON() %>%
    substr(., 1, nchar(.)-1) %>%
    substr(., 2, nchar(.))


  # join data strings and save
  nw_json_app <- paste0("{",
                        '"nw_traffic":', nw_traffic_json,
                        ', "nw_delay":', nw_delay_json,
                        ', "nw_punct":', nw_punct_json,
                        ', "nw_co2":', nw_co2_json,
                        ', "nw_billed":', nw_billed_json,
                        "}")
  write(nw_json_app, here(data_folder,"nw_json_app.json"))


# -----------------------------------------------------------------------------------------------------------------------------------------
  ####json for mobile app graphs
  ### traffic

   # 7-day average daily

    nw_traffic_evo_app <- nw_traffic_data %>%
    select(FLIGHT_DATE, AVG_ROLLING_WEEK, AVG_ROLLING_WEEK_PREV_YEAR,
           AVG_ROLLING_WEEK_2020, AVG_ROLLING_WEEK_2019)

  column_names <- c('FLIGHT_DATE', last_year, last_year-1, 2020, 2019)
  colnames(nw_traffic_evo_app) <- column_names

  # write.csv(nw_traffic_evo_app,
  #           file = here(data_folder,"nw_traffic_evo_app.csv"),
  #           row.names = FALSE)

  nw_traffic_evo_app_j <- nw_traffic_evo_app %>% toJSON()
  write(nw_traffic_evo_app_j, here(data_folder,"nw_traffic_evo_chart_daily.json"))

    # monthly

  base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/'
  base_file <- '99_Traffic_Landing_Page_dataset_{today}.xlsx'

  nw_traffic_month_data <-  read_xlsx(
    path  = fs::path_abs(
      str_glue(base_file),
      start = base_dir),
    sheet = "NM_Monthly_traffic",
    range = cell_limits(c(5, 10), c(NA, NA))) %>%
    as_tibble()

  nw_traffic_month_data_j <- nw_traffic_month_data %>% toJSON()
  write(nw_traffic_month_data_j, here(data_folder,"nw_traffic_evo_chart_monthly.json"))


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
    filter(Year >= last_year-1) %>%
    select(Month, Year, Total) %>%
    spread(Year, Total) %>%
    mutate(Month = as.Date(paste0("01",Month,last_year), "%d%m%Y"))


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
  ####json for mobile app ranking tables
  ######### Aircraft operators traffic

  ao_data_dy <- read_xlsx(
    path  = fs::path_abs(
      str_glue("5_AOs_infographic_{today}.xlsx"),
      start = base_dir),
    sheet = "Table_For_Traffic_LP_Portal",
    range = cell_limits(c(3, 2), c(NA, 10))) %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
    filter(WK_R_RANK_BY_DAY <= 10) %>%
    select (-DY_FLIGHT_DIFF_2019_PERC, -DY_R_RANK_BY_DAY)


  ao_data_wk <- read_xlsx(
    path  = fs::path_abs(
      str_glue("5_AOs_infographic_Week_{today}.xlsx"),
      start = base_dir),
    sheet = "Table_For_Traffic_LP_Portal",
    range = cell_limits(c(3, 2), c(NA, 9))) %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
    filter(WK_R_RANK_BY_DAY <= 10) %>%
    select (-WK_FLIGHT_DIFF_2019_PERC)


  ao_data_y2d <- read_xlsx(
    path  = fs::path_abs(
      str_glue(base_file),
      start = base_dir),
    sheet = "TOP40_AO_ALL",
    range = cell_limits(c(5, 2), c(NA, 8))) %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
    filter(WK_R_RANK_BY_DAY <= 10)

  ao_data = merge(x = ao_data_wk, y = ao_data_dy, by= "WK_R_RANK_BY_DAY")
  ao_data = merge(x = ao_data, y = ao_data_y2d, by = "WK_R_RANK_BY_DAY")

  ao_data <- ao_data %>%
    mutate(WK_MIN_ENTRY_DATE = WK_MAX_ENTRY_DATE-6)

  ao_data <- ao_data %>%
    relocate(c(RANK = WK_R_RANK_BY_DAY,
               DY_RANK_DIF_PREV_WEEK = DY_RANK_DIFF_7DAY,
               DY_AO_GRP_NAME,
               DY_TO_DATE = DY_ENTRY_DATE,
               DY_FLIGHT,
               DY_DIF_PREV_WEEK_PERC = DY_FLIGHT_DIFF_7DAY_PERC,
               DY_DIF_PREV_YEAR_PERC = DY_FLIGHT_DIFF_PERC,
               WK_RANK_DIF_PREV_WEEK = WK_RANK_DIFF_7DAY,
               WK_AO_GRP_NAME,
               WK_FROM_DATE = WK_MIN_ENTRY_DATE,
               WK_TO_DATE = WK_MAX_ENTRY_DATE,
               WK_DAILY_FLIGHT,
               WK_DIF_PREV_WEEK_PERC = WK_FLIGHT_DIFF_7DAY_PERC,
               WK_DIF_PREV_YEAR_PERC = WK_FLIGHT_DIFF_PERC,
               Y2D_RANK_DIF_PREV_YEAR,
               Y2D_AO_GRP_NAME,
               Y2D_TO_DATE = last_data_day,
               Y2D_DAILY_FLIGHT = "1_Y2D_CURRENT_YEAR",
               Y2D_DIF_PREV_YEAR_PERC = Dif_prev_year,
               Y2D_DIF_2019_PERC = Dif_2019))

  ao_data_j <- ao_data %>% toJSON()
  write(ao_data_j, here(data_folder,"ao_ranking_traffic.json"))

  ######### Airport traffic

  apt_data_dy <- read_xlsx(
    path  = fs::path_abs(
      str_glue("5_Airport_infographic_{today}.xlsx"),
      start = base_dir),
    sheet = "Table_For_Traffic_LP_Portal",
    range = cell_limits(c(3, 2), c(NA, 10))) %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
    filter(DY_R_RANK_BY_DAY <= 10) %>%
    select(-DY_AIRPORT_CODE, -DY_DEP_ARR_2019_PERC)

    apt_data_wk <- read_xlsx(
    path  = fs::path_abs(
      str_glue("5_Airport_infographic_week_{today}.xlsx"),
      start = base_dir),
    sheet = "Table_For_Traffic_LP_Portal",
    range = cell_limits(c(3, 2), c(NA, 10))) %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
      filter(DY_R_RANK_BY_DAY <= 10) %>%
      select(-WK_AIRPORT_CODE, -WK_2019_DIFF_PERC)

  apt_data_y2d <- read_xlsx(
    path  = fs::path_abs(
      str_glue(base_file),
      start = base_dir),
    sheet = "TOP40_APT_ALL",
    range = cell_limits(c(5, 2), c(NA, 11))) %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
    filter(DY_R_RANK_BY_DAY <= 10) %>%
    select(-Y2D_ARP_CODE, -Y2D_DEP_ARR_2019, -Y2D_DEP_ARR_PREV_YEAR)

  apt_data = merge(x = apt_data_dy, y = apt_data_wk, by = "DY_R_RANK_BY_DAY")
  apt_data = merge(x = apt_data, y = apt_data_y2d, by = "DY_R_RANK_BY_DAY")

  apt_data <- apt_data %>%
    mutate(WK_TO_DATE = CURRENT_WEEK_FIRST_DAY + 6) %>%
    relocate(c(RANK = DY_R_RANK_BY_DAY,
               DY_RANK_DIF_PREV_WEEK,
               DY_AIRPORT_NAME,
               DY_TO_DATE = DY_ENTRY_DATE,
               DY_DEP_ARR,
               DY_DIF_PREV_WEEK_PERC = DY_DEP_ARR_7DAY_PERC,
               DY_DIF_PREV_YEAR_PERC = DY_DEP_ARR_PREV_YEAR_PERC,
               WK_RANK_DIF_PREV_WEEK,
               WK_AIRPORT_NAME,
               WK_FROM_DATE = CURRENT_WEEK_FIRST_DAY,
               WK_TO_DATE,
               WK_DAILY_DEP_ARR,
               WK_DIF_PREV_WEEK_PERC = WK_PREV_WEEK_DIFF_PERC,
               WK_DIF_PREV_YEAR_PERC = WK_PREV_YEAR_DIFF_PERC,
               Y2D_RANK_DIF_PREV_YEAR,
               Y2D_ARP_NAME,
               Y2D_TO_DATE,
               Y2D_DEP_ARR = Y2D_DEP_ARR_CURRENT_YEAR,
               Y2D_DIF_PREV_YEAR_PERC,
               Y2D_DIF_2019_PERC))

  apt_data_j <- apt_data %>% toJSON()
  write(apt_data_j, here(data_folder,"apt_ranking_traffic.json"))

  ######### Country traffic DAI

  ct_dai_data_dy <- read_xlsx(
    path  = fs::path_abs(
      str_glue(base_file),
      start = base_dir),
    sheet = "CTRY_DAI_DAY",
    range = cell_limits(c(3, 2), c(NA, 8))) %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
    filter(DY_R_RANK_BY_DAY <= 10)

  ct_dai_data_wk <- read_xlsx(
    path  = fs::path_abs(
      str_glue(base_file),
      start = base_dir),
    sheet = "CTRY_DAI_WK",
    range = cell_limits(c(3, 2), c(NA, 9))) %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
    filter(DY_R_RANK_BY_DAY <= 10)

  ct_dai_data_y2d <- read_xlsx(
    path  = fs::path_abs(
      str_glue(base_file),
      start = base_dir),
    sheet = "CTRY_DAI_Y2D",
    range = cell_limits(c(3, 2), c(NA, 8))) %>%
    mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
    filter(DY_R_RANK_BY_DAY <= 10)

  ct_dai_data = merge(x = ct_dai_data_dy, y = ct_dai_data_wk, by = "DY_R_RANK_BY_DAY")
  ct_dai_data = merge(x = ct_dai_data, y = ct_dai_data_y2d, by = "DY_R_RANK_BY_DAY")

  ct_dai_data <- ct_dai_data %>%
    relocate(c(RANK = DY_R_RANK_BY_DAY,
               DY_RANK_DIF_PREV_WEEK,
               DY_COUNTRY_NAME,
               DY_TO_DATE=DY_ENTRY_DATE,
               DY_CTRY_DAI,
               DY_DIF_PREV_WEEK_PERC = DY_CTRY_DAI_7DAY_PERC,
               DY_DIF_PREV_YEAR_PERC = DY_CTRY_DAI_PREV_YEAR_PERC,
               WK_RANK_DIF_PREV_WEEK,
               WK_COUNTRY_NAME,
               WK_FROM_DATE,
               WK_TO_DATE,
               WK_CTRY_DAI,
               WK_DIF_PREV_WEEK_PERC = WK_CTRY_DAI_7DAY_PERC,
               WK_DIF_PREV_YEAR_PERC = WK_CTRY_DAI_PREV_YEAR_PERC,
               Y2D_RANK_DIF_PREV_YEAR,
               Y2D_COUNTRY_NAME,
               Y2D_TO_DATE,
               Y2D_CTRY_DAI,
               Y2D_CTRY_DAI_PREV_YEAR_PERC,
               Y2D_RANK_DIF_PREV_YEAR))

  ct_dai_data_j <- ct_dai_data %>% toJSON()
  write(ct_dai_data_j, here(data_folder,"ctry_ranking_traffic_DAI.json"))

