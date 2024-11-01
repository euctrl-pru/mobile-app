
<!-- README.md is generated from README.Rmd. Please edit that file -->

# mobile-app

<!-- badges: start -->
<!-- badges: end -->

this repo has scripts to prepare the data for the EUROCONTROL Data
mobile app.

## PocketHost API

We have 2 applications hosted at
\[PocketHost\]\[<https:://pockethost.io>\]

1.  <https://eurocontrol-data.pockethost.io>
2.  <https://eurocontrol-data-test.pockethost.io>

The first as “production” the other as testing instance.

These applications have various collections (behind the scenes they are
SQLite tables) that are exposed as REST API. The package `pockethostr`
is (partially) wrapping the REST API to allow CRUD operations as
detailed below.

The API covers \* (public) List/Search: listing of (subset of)
collection records \* (public) View: viewing of all details of one
record of the collection \* (admin only) Create: create a new record \*
(admin only) Update: update an existing record \* (admin only) Delete:
delete an existing record

Admin-only operations need authentication.

In the following sections we provide examples for the above operations
for the network data collection, `nw_traffic`.

We start with loading some helper packages…

``` r
library(pockethostr)
library(lubridate)
library(dplyr)
library(purrr)
library(readxl)
library(stringr)
library(magrittr)

app_main <- "eurocontrol-data"
app_test <- "eurocontrol-data-test"
collection <- "nw_traffic"
```

### List/Search

This example shows how to retrieve the second page (5 per page instead
of the default 30) entries. Records are returned in reverse order with
respect to the `FLIGHT_DATE` property.

``` r
ph_list_records(
  app = app_test,
  api = "/api/collections/",
  collection = collection,
  # fields = "FLIGHT_DATE,DAY_TFC,Y2D_TFC_YEAR",
  perPage = 5,
  page = 2,
  sort = "-FLIGHT_DATE")
#> # A tibble: 5 × 20
#>   AVG_ROLLING_WEEK DAY_DIFF_PREV_YEAR_PERC DAY_TFC DAY_TFC_DIFF_2019_PERC
#>              <dbl>                   <dbl>   <int>                  <dbl>
#> 1           22077.                  0.0589   20767                -0.0443
#> 2           22066.                  0.0353   24946                -0.120 
#> 3           22059.                  0.0229   22046                -0.171 
#> 4           22191.                 -0.0377   19926                -0.229 
#> 5           22482.                  0.0185   20560                -0.158 
#> # ℹ 16 more variables: DIFF_ROLLING_WEEK_2019_PERC <int>,
#> #   DIFF_WEEK_PREV_YEAR_PERC <int>, FLIGHT_DATE <chr>, Y2D_AVG_TFC_YEAR <dbl>,
#> #   Y2D_DIFF_2019_PERC <dbl>, Y2D_DIFF_PREV_YEAR_PERC <dbl>,
#> #   Y2D_TFC_YEAR <int>, collectionId <chr>, collectionName <chr>,
#> #   created <chr>, id <chr>, updated <chr>, page <int>, perPage <int>,
#> #   totalItems <int>, totalPages <int>
```

Eventually you can add a (server side) filter and pick only some
columns:

``` r
ph_list_records(
  app = app_test,
  api = "/api/collections/",
  collection = collection,
  fields = "id,FLIGHT_DATE,DAY_TFC,Y2D_TFC_YEAR",
  filter ="('2024-01-05'<=FLIGHT_DATE&&FLIGHT_DATE<'2024-01-15')", 
  sort = "-FLIGHT_DATE") |> 
  dplyr::relocate(id, .before = DAY_TFC)
#> # A tibble: 10 × 8
#>    id       DAY_TFC FLIGHT_DATE Y2D_TFC_YEAR  page perPage totalItems totalPages
#>    <chr>      <int> <chr>              <int> <int>   <int>      <int>      <int>
#>  1 5z275jv…   23022 2024-01-14…       329527     1      30         10          1
#>  2 612vfdz…   20687 2024-01-13…       306505     1      30         10          1
#>  3 ff2eius…   24900 2024-01-12…       285818     1      30         10          1
#>  4 zh6054u…   22967 2024-01-11…       260918     1      30         10          1
#>  5 ene3278…   21963 2024-01-10…       237951     1      30         10          1
#>  6 yy3507s…   21647 2024-01-09…       215988     1      30         10          1
#>  7 nscastr…   24869 2024-01-08…       194341     1      30         10          1
#>  8 8k2q8x4…   24980 2024-01-07…       169472     1      30         10          1
#>  9 b5cz6bz…   23222 2024-01-06…       144492     1      30         10          1
#> 10 uefez6k…   26114 2024-01-05…       121270     1      30         10          1
```

### View

You can retrieve one particular entry with eventually a subset of
columns via the `field` option.

``` r
ph_view_record(
  app = app_test,
  api = "/api/collections/",
  collection = collection,
  id = "r4a9ag9dpvz62gv",
  fields = "id,FLIGHT_DATE,DAY_TFC")
#> $DAY_TFC
#> [1] 21082
#> 
#> $FLIGHT_DATE
#> [1] "2024-01-23 00:00:00.000Z"
#> 
#> $id
#> [1] "r4a9ag9dpvz62gv"
```

### Authentication

For admin only operations you need to pass a token, hence you need to be
authenticated.

``` r
username <- "performancereviewunit@gmail.com"
password <- "you know it"

# authenticate over test app
adm_test <- ph_authenticate_admin_username_password(
  app_test,
  "/api/admins/auth-with-password",
  username,
  password)

# authenticate over main app
adm_main <- ph_authenticate_admin_username_password(
  app_main,
  "/api/admins/auth-with-password",
  username,
  password)
```

### Create record(s)

Let’s get some internal data

``` r

today <- (lubridate::now() +  days(-1)) |> format("%Y%m%d")
base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/'
base_file <- '99_Traffic_Landing_Page_dataset_new_{today}.xlsx'

last_day <-  (lubridate::now() +  days(-1))
last_year <- as.numeric(format(last_day,'%Y'))

nw_traffic_data <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(base_file),
    start = base_dir),
  sheet = "NM_Daily_Traffic_All",
  range = cell_limits(c(2, 1), c(NA, 39))) |> 
  as_tibble()

nw_traffic_last_day <- nw_traffic_data %>%
  filter(FLIGHT_DATE == max(LAST_DATA_DAY))

nw_traffic_latest <- nw_traffic_last_day %>%
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
  ) |> 
  as.list() |> 
  purrr::list_transpose() |> 
  magrittr::extract2(1)
```

Then we create a new record in the two apps.

``` r

# create latest record on test
ph_create_record(
  app = app_test,
  api = "/api/collections",
  collection = collection,
  token = adm_main$token,
  body = nw_traffic_latest)

# create latest record on main
ph_create_record(
  app = app_main,
  api = "/api/collections",
  collection = collection,
  token = adm_main$token,
  body = nw_traffic_latest)
```

Here just a sort of batch

``` r
# data for 2024
lol <- nw_traffic_data |> 
  filter(FLIGHT_DATE < today()) |> 
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
  ) |> 
  as.list() |> 
  purrr::list_transpose()


#------------- TEST ------------
for (r in lol) {
  # print(r)
  ph_create_record(
    app = app_test,
    api = "/api/collections",
    collection = collection,
    token = adm_test$token,
    body = r)
  Sys.sleep(0.1)
}


# delete all records in test
lor <- ph_list_records(
  app = app_test,
  api = "/api/collections/",
  collection = collection,
  sort = "-FLIGHT_DATE")

ids <- lor |> pull(id)
for (i in ids) {
  ph_delete_record(
    app = app_test,
    api = "/api/collections",
    collection = collection,
    token = adm_test$token,
    id = i)
  Sys.sleep(0.3)
}
```

### DELETE all

``` r
#------------- TEST ------------
# delete all records in test (just one page)
lor <- ph_list_records(
  app = app_test,
  api = "/api/collections/",
  collection = collection,
  sort = "-FLIGHT_DATE")

ids <- lor |> pull(id)
for (i in ids) {
  ph_delete_record(
    app = app_test,
    api = "/api/collections",
    collection = collection,
    token = adm_test$token,
    id = i)
  Sys.sleep(0.1)
}
```
