# to be re done when stable
# date parameters
if (exists("data_day_date") == FALSE) {
  data_day_date <- lubridate::today(tzone = "") +  days(-1)
}
data_day_text <- data_day_date %>% format("%Y%m%d")
data_day_year <- as.numeric(format(data_day_date,'%Y'))


# json for main page ----
nw_json_app <- function(data_day_date){

}
