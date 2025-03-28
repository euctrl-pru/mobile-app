
<!-- README.md is generated from README.Rmd. Please edit that file -->

# mobile-app

<!-- badges: start -->
<!-- badges: end -->

this repo has scripts to prepare the data for the EUROCONTROL Data
mobile app.

## Current live production procedure (json files on netork folder)

The scripts prepare a set of json files saved in the network folder:
`//ihx-vdm05/LIVE_var_www_performance$/briefing`

The **main scripts** are:

- `copy_to_production`: Main script executing all the other necessary
  ones. It’s the one automated to be run every morning.
- `generate_json_files_nw/ao/states/ap`: Retrieve the raw data, generate
  the json files and saves them locally in the repo’s data folder.

Support scripts:

- `helpers`: contains the functions used in other scripts.
- `params`: sets the necessary parameters.
- `get_common_data`: retrieves some datasets used for other scripts to
  avoid loading them multiple times, a.o. the dimension tables.

Generation of archive csvs:

- `generate_archive_data`: Generates source csv files necessary to
  generate json files for past dates. Only needs to be run if further
  dates are needed.
- `nw/ao/state_queries`: Contains SQL queries with the date as a
  parameter used by the previous script to generate past csv viles.

<!-- ## PocketHost API (currently not used for the app) -->
<!-- We have 2 applications hosted at [PocketHost][https:://pockethost.io] -->
<!-- 1. https://eurocontrol-data.pockethost.io -->
<!-- 1. https://eurocontrol-data-test.pockethost.io -->
<!-- The first as "production" the other as testing instance. -->
<!-- These applications have various collections (behind the scenes they are SQLite tables) -->
<!-- that are exposed as REST API. -->
<!-- The package `pockethostr` is (partially) wrapping the REST API to allow CRUD -->
<!-- operations as detailed below. -->
<!-- The API covers -->
<!-- * (public) List/Search: listing of (subset of) collection records -->
<!-- * (public) View: viewing of all details of one record of the collection -->
<!-- * (admin only) Create: create a new record -->
<!-- * (admin only) Update: update an existing record -->
<!-- * (admin only) Delete: delete an existing record -->
<!-- Admin-only operations need authentication. -->
<!-- In the following sections we provide examples for the above operations for the -->
<!-- network data collection, `nw_traffic`. -->
<!-- We start with loading some helper packages... -->
<!-- ```{r setup, eval=TRUE, warning=FALSE, message=FALSE} -->
<!-- library(pockethostr) -->
<!-- library(lubridate) -->
<!-- library(dplyr) -->
<!-- library(purrr) -->
<!-- library(readxl) -->
<!-- library(stringr) -->
<!-- library(magrittr) -->
<!-- app_main <- "eurocontrol-data" -->
<!-- app_test <- "eurocontrol-data-test" -->
<!-- collection <- "nw_traffic" -->
<!-- ``` -->
<!-- ### List/Search -->
<!-- This example shows how to retrieve the second page (5 per page instead of the -->
<!-- default 30) entries. -->
<!-- Records are returned in reverse order with respect to the `FLIGHT_DATE` property. -->
<!-- ```{r list-search, eval=TRUE} -->
<!-- ph_list_records( -->
<!--   app = app_test, -->
<!--   api = "/api/collections/", -->
<!--   collection = collection, -->
<!--   # fields = "FLIGHT_DATE,DAY_TFC,Y2D_TFC_YEAR", -->
<!--   perPage = 5, -->
<!--   page = 2, -->
<!--   sort = "-FLIGHT_DATE") -->
<!-- ``` -->
<!-- Eventually you can add a (server side) filter and pick only some columns: -->
<!-- ```{r list-search-filter, eval=TRUE} -->
<!-- ph_list_records( -->
<!--   app = app_test, -->
<!--   api = "/api/collections/", -->
<!--   collection = collection, -->
<!--   fields = "id,FLIGHT_DATE,DAY_TFC,Y2D_TFC_YEAR", -->
<!--   filter ="('2024-01-05'<=FLIGHT_DATE&&FLIGHT_DATE<'2024-01-15')",  -->
<!--   sort = "-FLIGHT_DATE") |>  -->
<!--   dplyr::relocate(id, .before = DAY_TFC) -->
<!-- ``` -->
<!-- ### View -->
<!-- You can retrieve one particular entry with eventually a subset of columns via the -->
<!-- `field` option. -->
<!-- ```{r view} -->
<!-- ph_view_record( -->
<!--   app = app_test, -->
<!--   api = "/api/collections/", -->
<!--   collection = collection, -->
<!--   id = "r4a9ag9dpvz62gv", -->
<!--   fields = "id,FLIGHT_DATE,DAY_TFC") -->
<!-- ``` -->
<!-- ### Authentication -->
<!-- For admin only operations you need to pass a token, hence you need to be authenticated. -->
<!-- ```{r authenticate, eval=FALSE} -->
<!-- username <- "performancereviewunit@gmail.com" -->
<!-- password <- "you know it" -->
<!-- # authenticate over test app -->
<!-- adm_test <- ph_authenticate_admin_username_password( -->
<!--   app_test, -->
<!--   "/api/admins/auth-with-password", -->
<!--   username, -->
<!--   password) -->
<!-- # authenticate over main app -->
<!-- adm_main <- ph_authenticate_admin_username_password( -->
<!--   app_main, -->
<!--   "/api/admins/auth-with-password", -->
<!--   username, -->
<!--   password) -->
<!-- ``` -->
<!-- ### Create record(s) -->
<!-- Let's get some internal data -->
<!-- ```{r examples, eval=FALSE} -->
<!-- today <- (lubridate::now() +  days(-1)) |> format("%Y%m%d") -->
<!-- base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/' -->
<!-- base_file <- '99_Traffic_Landing_Page_dataset_new_{today}.xlsx' -->
<!-- last_day <-  (lubridate::now() +  days(-1)) -->
<!-- last_year <- as.numeric(format(last_day,'%Y')) -->
<!-- nw_traffic_data <-  read_xlsx( -->
<!--   path  = fs::path_abs( -->
<!--     str_glue(base_file), -->
<!--     start = base_dir), -->
<!--   sheet = "NM_Daily_Traffic_All", -->
<!--   range = cell_limits(c(2, 1), c(NA, 39))) |>  -->
<!--   as_tibble() -->
<!-- nw_traffic_last_day <- nw_traffic_data %>% -->
<!--   filter(FLIGHT_DATE == max(LAST_DATA_DAY)) -->
<!-- nw_traffic_latest <- nw_traffic_last_day %>% -->
<!--   select( -->
<!--     FLIGHT_DATE, -->
<!--     DAY_TFC, -->
<!--     DAY_DIFF_PREV_YEAR_PERC, -->
<!--     DAY_TFC_DIFF_2019_PERC, -->
<!--     AVG_ROLLING_WEEK, -->
<!--     DIF_WEEK_PREV_YEAR_PERC, -->
<!--     DIF_ROLLING_WEEK_2019_PERC, -->
<!--     Y2D_TFC_YEAR, -->
<!--     Y2D_AVG_TFC_YEAR, -->
<!--     Y2D_DIFF_PREV_YEAR_PERC, -->
<!--     Y2D_DIFF_2019_PERC -->
<!--   ) |>  -->
<!--   as.list() |>  -->
<!--   purrr::list_transpose() |>  -->
<!--   magrittr::extract2(1) -->
<!-- ``` -->
<!-- Then we create a new record in the two apps. -->
<!-- ```{r create-record, eval=FALSE} -->
<!-- # create latest record on test -->
<!-- ph_create_record( -->
<!--   app = app_test, -->
<!--   api = "/api/collections", -->
<!--   collection = collection, -->
<!--   token = adm_main$token, -->
<!--   body = nw_traffic_latest) -->
<!-- # create latest record on main -->
<!-- ph_create_record( -->
<!--   app = app_main, -->
<!--   api = "/api/collections", -->
<!--   collection = collection, -->
<!--   token = adm_main$token, -->
<!--   body = nw_traffic_latest) -->
<!-- ``` -->
<!-- Here just a sort of batch -->
<!-- ```{r create-many, eval=FALSE} -->
<!-- # data for 2024 -->
<!-- lol <- nw_traffic_data |>  -->
<!--   filter(FLIGHT_DATE < today()) |>  -->
<!--   select( -->
<!--     FLIGHT_DATE, -->
<!--     DAY_TFC, -->
<!--     DAY_DIFF_PREV_YEAR_PERC, -->
<!--     DAY_TFC_DIFF_2019_PERC, -->
<!--     AVG_ROLLING_WEEK, -->
<!--     DIF_WEEK_PREV_YEAR_PERC, -->
<!--     DIF_ROLLING_WEEK_2019_PERC, -->
<!--     Y2D_TFC_YEAR, -->
<!--     Y2D_AVG_TFC_YEAR, -->
<!--     Y2D_DIFF_PREV_YEAR_PERC, -->
<!--     Y2D_DIFF_2019_PERC -->
<!--   ) |>  -->
<!--   as.list() |>  -->
<!--   purrr::list_transpose() -->
<!-- #------------- TEST ------------ -->
<!-- for (r in lol) { -->
<!--   # print(r) -->
<!--   ph_create_record( -->
<!--     app = app_test, -->
<!--     api = "/api/collections", -->
<!--     collection = collection, -->
<!--     token = adm_test$token, -->
<!--     body = r) -->
<!--   Sys.sleep(0.1) -->
<!-- } -->
<!-- # delete all records in test -->
<!-- lor <- ph_list_records( -->
<!--   app = app_test, -->
<!--   api = "/api/collections/", -->
<!--   collection = collection, -->
<!--   sort = "-FLIGHT_DATE") -->
<!-- ids <- lor |> pull(id) -->
<!-- for (i in ids) { -->
<!--   ph_delete_record( -->
<!--     app = app_test, -->
<!--     api = "/api/collections", -->
<!--     collection = collection, -->
<!--     token = adm_test$token, -->
<!--     id = i) -->
<!--   Sys.sleep(0.3) -->
<!-- } -->
<!-- ``` -->
<!-- ### DELETE all -->
<!-- ```{r delete-all, eval=FALSE} -->
<!-- #------------- TEST ------------ -->
<!-- # delete all records in test (just one page) -->
<!-- lor <- ph_list_records( -->
<!--   app = app_test, -->
<!--   api = "/api/collections/", -->
<!--   collection = collection, -->
<!--   sort = "-FLIGHT_DATE") -->
<!-- ids <- lor |> pull(id) -->
<!-- for (i in ids) { -->
<!--   ph_delete_record( -->
<!--     app = app_test, -->
<!--     api = "/api/collections", -->
<!--     collection = collection, -->
<!--     token = adm_test$token, -->
<!--     id = i) -->
<!--   Sys.sleep(0.1) -->
<!-- } -->
<!-- ``` -->
