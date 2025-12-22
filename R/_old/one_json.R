one_json <- function() {
# copy in the function all the code necessary to generate the json you need
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
    query <- "SELECT
  arp_code AS apt_icao_code,
  arp_name AS apt_name
  FROM pruread.v_aiu_app_list_airport"

    apt_icao <- export_query(query) %>%
      janitor::clean_names()
  }

  # archive mode for past dates
  if (exists("archive_mode") == FALSE) {archive_mode <- FALSE}
  if (exists("data_day_date") == FALSE) {
    data_day_date <- lubridate::today(tzone = "") +  days(-1)
  }

  data_day_text <- data_day_date %>% format("%Y%m%d")
  data_day_year <- as.numeric(format(data_day_date,'%Y'))


  apt_json_app <-""


  ## TRAFFIC ----
  ### Aircraft operator ----

  #### day ----
  mydataframe <- "ap_ao_data_day_raw"
  myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
  stakeholder <- str_sub(mydataframe, 1, 2)

  if (archive_mode) {
    df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

  } else {
    df <- read_xlsx(
      path  = fs::path_abs(
        str_glue(ap_base_file),
        start = ap_base_dir),
      sheet = "apt_ao_day",
      range = cell_limits(c(1, 1), c(NA, NA))) |>
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    # save pre-processed file in archive for generation of past json files
    write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
  }

  # process data
  apt_ao_data_day_int <- assign(mydataframe, df) |>
    select(-TO_DATE) |>
    spread(key = FLAG_DAY, value = DEP_ARR) |>
    arrange(ARP_CODE, R_RANK) |>
    mutate(
      DY_RANK_DIF_PREV_WEEK = case_when(
        is.na(RANK_PREV) ~ RANK,
        .default = RANK_PREV - RANK
      ),
      DY_FLT_DIF_PREV_WEEK_PERC =   case_when(
        DAY_PREV_WEEK == 0 | is.na(DAY_PREV_WEEK) ~ NA,
        .default = CURRENT_DAY / DAY_PREV_WEEK - 1
      ),
      DY_FLT_DIF_PREV_YEAR_PERC = case_when(
        DAY_PREV_YEAR == 0 | is.na(DAY_PREV_YEAR) ~ NA,
        .default = CURRENT_DAY / DAY_PREV_YEAR - 1
      ),
      APT_TFC_AO_GRP_DIF = CURRENT_DAY - DAY_PREV_WEEK
    )

  apt_ao_data_day <- apt_ao_data_day_int |>
    select(
      APT_CODE = ARP_CODE,
      APT_NAME = ARP_NAME,
      RANK = R_RANK,
      DY_RANK_DIF_PREV_WEEK,
      DY_AO_GRP_NAME = AO_GRP_NAME,
      DY_TO_DATE = LAST_DATA_DAY,
      DY_FLT = CURRENT_DAY,
      DY_FLT_DIF_PREV_WEEK_PERC,
      DY_FLT_DIF_PREV_YEAR_PERC
    )


  #### week ----
  mydataframe <- "ap_ao_data_week_raw"
  myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
  stakeholder <- str_sub(mydataframe, 1, 2)

  if (archive_mode) {
    df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

  } else {
    df <- read_xlsx(
      path  = fs::path_abs(
        str_glue(ap_base_file),
        start = ap_base_dir),
      sheet = "apt_ao_week",
      range = cell_limits(c(1, 1), c(NA, NA))) |>
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    # save pre-processed file in archive for generation of past json files
    write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
  }

  # process data
  apt_ao_data_week <- assign(mydataframe, df) |>
    mutate(
      WK_TO_DATE = max(TO_DATE),
      WK_FROM_DATE = WK_TO_DATE + days(-6),
    ) |>
    select(-FROM_DATE, -TO_DATE, -LAST_DATA_DAY) |>
    spread(key = PERIOD_TYPE, value = DEP_ARR) |>
    arrange(ARP_CODE, R_RANK) |>
    mutate(
      WK_RANK_DIF_PREV_WEEK =  case_when(
        is.na(RANK_PREV) ~ RANK,
        .default = RANK_PREV - RANK
      ),
      WK_FLT_DIF_PREV_WEEK_PERC =   case_when(
        PREV_ROLLING_WEEK == 0 | is.na(PREV_ROLLING_WEEK) ~ NA,
        .default = round((CURRENT_ROLLING_WEEK / PREV_ROLLING_WEEK - 1), 3)
      ),
      WK_FLT_DIF_PREV_YEAR_PERC = case_when(
        ROLLING_WEEK_PREV_YEAR == 0 | is.na(ROLLING_WEEK_PREV_YEAR) ~ NA,
        .default = round((CURRENT_ROLLING_WEEK / ROLLING_WEEK_PREV_YEAR - 1), 3)
      ),
      WK_FLT_AVG = round((CURRENT_ROLLING_WEEK/7), 2)
    ) |>
    select(
      APT_CODE = ARP_CODE,
      APT_NAME = ARP_NAME,
      RANK = R_RANK,
      WK_RANK_DIF_PREV_WEEK,
      WK_AO_GRP_NAME = AO_GRP_NAME,
      WK_FROM_DATE,
      WK_TO_DATE,
      WK_FLT_AVG,
      WK_FLT_DIF_PREV_WEEK_PERC,
      WK_FLT_DIF_PREV_YEAR_PERC
    )

  #### y2d ----
  mydataframe <- "ap_ao_data_y2d_raw"
  myarchivefile <- paste0(data_day_text, "_", mydataframe, ".csv")
  stakeholder <- str_sub(mydataframe, 1, 2)

  if (archive_mode) {
    df <-  read_csv(here(archive_dir_raw, stakeholder, myarchivefile), show_col_types = FALSE)

  } else {
    df <- read_xlsx(
      path  = fs::path_abs(
        str_glue(ap_base_file),
        start = ap_base_dir),
      sheet = "apt_ao_y2d",
      range = cell_limits(c(1, 1), c(NA, NA))) |>
      mutate(across(.cols = where(is.instant), ~ as.Date(.x)))

    # save pre-processed file in archive for generation of past json files
    write_csv(df, here(archive_dir_raw, stakeholder, myarchivefile))
  }

  apt_ao_y2d <- assign(mydataframe, df)

  # get max year from dataset
  apt_ao_y2d_max_year <-  max(apt_ao_y2d$YEAR, na.rm = TRUE)

  # process data
  apt_ao_data_year <- apt_ao_y2d |>
    # calculate number of days to date
    group_by(YEAR) |>
    mutate(Y2D_DAYS = as.numeric(max(TO_DATE, na.rm = TRUE) - min(FROM_DATE, na.rm = TRUE) +1)) |>
    ungroup() |>
    arrange(ARP_CODE, AO_GRP_CODE, YEAR) |>
    mutate(
      Y2D_RANK_DIF_PREV_YEAR =  RANK_PY - RANK,
      # Y2D_FLT_DIF_PREV_YEAR_PERC = ifelse(YEAR == "2024",
      #                                     round((DEP_ARR/lag(DEP_ARR)-1), 3), NA),
      # Y2D_FLT_DIF_2019_PERC = ifelse(YEAR == "2024",
      #                                round((DEP_ARR/lag(DEP_ARR, 5)-1), 3), NA)
      Y2D_FLT_AVG = DEP_ARR / Y2D_DAYS,
      Y2D_FLT_DIF_PREV_YEAR_PERC = ifelse(YEAR == apt_ao_y2d_max_year,
                                          Y2D_FLT_AVG / lag(Y2D_FLT_AVG)-1, NA),
      Y2D_FLT_DIF_2019_PERC = ifelse(YEAR == apt_ao_y2d_max_year,
                                     Y2D_FLT_AVG / lag(Y2D_FLT_AVG, apt_ao_y2d_max_year - 2019)-1, NA)

    ) |>
    filter(YEAR == apt_ao_y2d_max_year) |>
    mutate(TO_DATE = max(TO_DATE)) |>
    arrange(ARP_CODE, ARP_NAME, R_RANK) |>
    select(
      APT_CODE = ARP_CODE,
      APT_NAME = ARP_NAME,
      RANK = R_RANK,
      Y2D_RANK_DIF_PREV_YEAR,
      Y2D_AO_GRP_NAME = AO_GRP_NAME,
      Y2D_TO_DATE = TO_DATE,
      Y2D_FLT_AVG,
      Y2D_FLT_DIF_PREV_YEAR_PERC,
      Y2D_FLT_DIF_2019_PERC
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
      MAIN_TFC_AO_GRP_FLT = if_else(
        R_RANK <= 4,
        CURRENT_DAY,
        NA
      )
    ) |>
    select(APT_CODE = ARP_CODE, APT_NAME = ARP_NAME, RANK = R_RANK,
           MAIN_TFC_AO_GRP_NAME, MAIN_TFC_AO_GRP_CODE, MAIN_TFC_AO_GRP_FLT)

  apt_ao_main_traffic_dif <- apt_ao_data_day_int |>
    arrange(ARP_CODE, desc(abs(APT_TFC_AO_GRP_DIF))) |>
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
      MAIN_TFC_DIF_AO_GRP_FLT_DIF = if_else(
        RANK_DIF_AO_TFC <= 4,
        APT_TFC_AO_GRP_DIF,
        NA
      )
    ) |>
    arrange(ARP_CODE, RANK_DIF_AO_TFC) |>
    select(APT_CODE = ARP_CODE, APT_NAME = ARP_NAME, RANK = RANK_DIF_AO_TFC,
           MAIN_TFC_DIF_AO_GRP_NAME, MAIN_TFC_DIF_AO_GRP_CODE,
           MAIN_TFC_DIF_AO_GRP_FLT_DIF)


  #### join tables ----
  apt_ao_ranking_traffic <- apt_ao_main_traffic |>
    left_join(apt_ao_main_traffic_dif, by = c("RANK", "APT_CODE", "APT_NAME"),
              relationship = "many-to-many") |>
    left_join(apt_ao_data_day, by = c("RANK", "APT_CODE", "APT_NAME"),
              relationship = "many-to-many") |>
    left_join(apt_ao_data_week, by = c("RANK", "APT_CODE", "APT_NAME"),
              relationship = "many-to-many") |>
    left_join(apt_ao_data_year, by = c("RANK", "APT_CODE", "APT_NAME"),
              relationship = "many-to-many") |>
    arrange(APT_CODE, APT_NAME, RANK) |>
    distinct(RANK, APT_CODE, APT_NAME, .keep_all = TRUE)

  # convert to json and save in app data folder
  apt_ao_ranking_traffic_j <- apt_ao_ranking_traffic |> toJSON(pretty = TRUE)

  save_json(apt_ao_ranking_traffic_j, "apt_ao_ranking_traffic", archive_file = FALSE)
}
