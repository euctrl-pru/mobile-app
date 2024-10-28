# parameters ----
nw_local_data_folder_prod <- here::here("..", "mobile-app", "data", "prod", "nw")
st_local_data_folder_prod <- here::here("..", "mobile-app", "data", "prod", "st")

nw_local_data_folder_dev <- here::here("..", "mobile-app", "data", "dev", "nw")
st_local_data_folder_dev <- here::here("..", "mobile-app", "data", "dev", "st")
ao_local_data_folder_dev <- here::here("..", "mobile-app", "data", "dev", "ao")

nw_base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/LastVersion/'
nw_base_file <- "099_Traffic_Landing_Page_dataset_new.xlsx"

st_base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/LastVersion/'
st_base_file <- '099a_app_state_dataset.xlsx'

ao_base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/LastVersion/'
ao_base_file <- '099b_app_ao_dataset.xlsx'

archive_dir <- "//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/web_daily_json_files/app/"
archive_dir_raw <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Project/DDP/AIU app/data_archive'

# dimensions
state_iso <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(st_base_file),
    start = st_base_dir),
  sheet = "lists",
  range = cell_limits(c(2, 2), c(NA, 3))) %>%
  as_tibble()

state_crco <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(st_base_file),
    start = st_base_dir),
  sheet = "lists",
  range = cell_limits(c(2, 6), c(NA, 8))) %>%
  as_tibble()

state_daio <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(st_base_file),
    start = st_base_dir),
  sheet = "lists",
  range = cell_limits(c(2, 11), c(NA, 13))) %>%
  as_tibble()

state_co2 <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(st_base_file),
    start = st_base_dir),
  sheet = "lists",
  range = cell_limits(c(2, 16), c(NA, 17))) %>%
  as_tibble()

acc <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(nw_base_file),
    start = nw_base_dir),
  sheet = "ACC_names",
  range = cell_limits(c(2, 3), c(NA, NA))) %>%
  as_tibble()

query <- "select * from PRU_AIRPORT"

airport <- export_query(query) %>%
  as_tibble() %>%
  mutate(across(.cols = where(is.instant), ~ as.Date(.x))) %>%
  select(ICAO_CODE, ISO_COUNTRY_CODE) %>%
  #in case we need to separate spain from canarias
  # mutate(ISO_COUNTRY_CODE = if_else(substr(ICAO_CODE,1,2) == "GC",
  #                                   "IC", ISO_COUNTRY_CODE)) %>%
  rename(iso_2letter = ISO_COUNTRY_CODE)

ao_grp_icao <-  read_xlsx(
  path  = fs::path_abs(
    str_glue(ao_base_file),
    start = ao_base_dir),
  sheet = "lists",
  range = cell_limits(c(1, 1), c(NA, NA))) %>%
  as_tibble()


# DB params
usr <- Sys.getenv("PRU_DEV_USR")
pwd <- Sys.getenv("PRU_DEV_PWD")
dbn <- Sys.getenv("PRU_DEV_DBNAME")
