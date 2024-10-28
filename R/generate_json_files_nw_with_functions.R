# functions
source(here("..", "mobile-app", "R", "helpers.R")) # so it can be launched from the checkupdates script in grounded aircraft

# parameters ----
source(here("..", "mobile-app", "R", "params.R")) # so it can be launched from the checkupdates script in grounded aircraft

# archive mode for past dates
if (exists("archive_mode") == FALSE) {archive_mode <- FALSE}
if (exists("data_day_date") == FALSE) {
  data_day_date <- lubridate::today(tzone = "") +  days(-1)
  }

data_day_text <- data_day_date %>% format("%Y%m%d")
data_day_year <- as.numeric(format(data_day_date,'%Y'))

nw_json_app <- ""

# json functions ----
source(here("..", "mobile-app", "R", "functions_json_files_nw.R")) # so it can be launched from the checkupdates script in grounded aircraft

# json for main page ----
nw_json_app(data_day_date)

# jsons for graphs -------
## traffic -----
nw_traffic_evo_chart_daily(data_day_date)

## delay ----
nw_delay_category_evo_charts(data_day_date)

## punctuality ----
nw_punct_evo_chart(data_day_date)

## billing ----
nw_billing_evo_chart(data_day_date)

## co2 emissions ----
nw_co2_evo_chart(data_day_date)

# jsons for ranking tables ----

## Aircraft operators traffic ----
nw_ao_ranking_traffic(data_day_date)

## Airport traffic ----
nw_apt_ranking_traffic(data_day_date)

## Country traffic DAI ----
nw_ctry_ranking_traffic_DAI(data_day_date)

## Airport delay -----
nw_apt_ranking_delay(data_day_date)

## ACC delay ----
nw_acc_ranking_delay(data_day_date)

## Country delay ----
nw_ctry_ranking_delay(data_day_date)


## Airport punctuality ----
nw_apt_ranking_punctuality(data_day_date)

## Country punctuality ----
nw_ctry_ranking_punctuality(data_day_date)
