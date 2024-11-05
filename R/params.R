# parameters ----
# this will set in which folder data is saved locally in the repo
nw_status <- "prod"
st_status <- "prod"
ao_status <- "dev"
ap_status <- "dev"

nw_local_data_folder_prod <- here::here("..", "mobile-app", "data", "prod", "nw")
st_local_data_folder_prod <- here::here("..", "mobile-app", "data", "prod", "st")
ao_local_data_folder_prod <- here::here("..", "mobile-app", "data", "prod", "ao")
ap_local_data_folder_prod <- here::here("..", "mobile-app", "data", "prod", "ap")

nw_local_data_folder_dev <- here::here("..", "mobile-app", "data", "dev", "nw")
st_local_data_folder_dev <- here::here("..", "mobile-app", "data", "dev", "st")
ao_local_data_folder_dev <- here::here("..", "mobile-app", "data", "dev", "ao")
ap_local_data_folder_dev <- here::here("..", "mobile-app", "data", "dev", "ap")

# nw hardcoded as this is what we want to archive
nw_local_data_folder <- here::here("..", "mobile-app", "data", "dev", "nw")
st_local_data_folder <- here::here("..", "mobile-app", "data", st_status, "st")
ao_local_data_folder <- here::here("..", "mobile-app", "data", ao_status, "ao")
ap_local_data_folder <- here::here("..", "mobile-app", "data", ao_status, "ap")

nw_base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/LastVersion/'
nw_base_file <- "099_Traffic_Landing_Page_dataset_new.xlsx"

st_base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/LastVersion/'
st_base_file <- '099a_app_state_dataset.xlsx'

ao_base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/LastVersion/'
ao_base_file <- '099b_app_ao_dataset.xlsx'

ap_base_dir <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Project/DDP/AIU app/data_archive'
ap_base_file <- '099c_app_apt_dataset.xlsx'

archive_dir <- "//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/app/json/"
archive_dir_raw <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Project/DDP/AIU app/data_archive'
archive_dir_raw_backup <- '//sky.corp.eurocontrol.int/DFSRoot/Groups/HQ/dgof-pru/Data/DataProcessing/Covid19/Archive/app/csv'

# DB params
usr <- Sys.getenv("PRU_DEV_USR")
pwd <- Sys.getenv("PRU_DEV_PWD")
dbn <- Sys.getenv("PRU_DEV_DBNAME")
