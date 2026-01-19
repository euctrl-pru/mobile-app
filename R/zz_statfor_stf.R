library(statfor)
library(tidyverse)
library(tsibble)
library(plotly)

# parameters 
source(here("..", "mobile-app", "R", "params.R")) 

# set up data file name
# update the list in the params script, if necessary
forecast_data_file <- forecast_list %>% filter(id == statfor_id) %>% select(name) %>% pull()


## Step 1.
# From SAS Audit project.
list_sas_mtf_tt <- c(
  3716,
  3731,
  3910,
  3950)

# Define FCSets:
mtfs <- list_fc_set_table() %>%
  filter(id %in% list_sas_mtf_tt) %>%
  mutate(mtf_tt = gsub( " .*$", "", name)) %>%
  select(id, mtf_tt)

# Step 2. Obtain needed TTs
## Loop through the list of FCSets to obtain the method (table) with the final AP2 with HST effects before constraining by AP capacity
# Create vector of TTs:
list_of_tts <- mtfs$id
df_method <- data.frame(
  method_tt = NULL,
  mtf_id = NULL
)

method_id <- 313

# Loop
for (i in list_of_tts) {
  method_tt <- find_fc_method_in_fc_set(i , method_id, warn = T)
  df_result <- data.frame(method_tt)
  df_result$id <- i
  df_method <- as_tibble(rbind(df_method, df_result))
}
df_method

# Join data to have all the info in one table.
df_mtf_method <- left_join(mtfs, df_method, "id")

# Step 3. Obtain needed methods from the TTs.
## Loop through tt_method and fetch the MTFs from 2014.
df_out <- data.frame()

for (i in unique(df_mtf_method$method_tt)) {
  result <- unpack_fc_pts_to_dataset(i) 
  result$method <- i
  df_out <- as_tibble(bind_rows(df_out, result))
}
df_out

# Step 4.
## Clean the data: add labels to geoX columns, DAIO, Rank/Scenario. Finally join the name of the MTF on mtf_id
df_out_clean <- df_out %>%
  
  mutate(tz_id = geo1) |> 
  # Apply format and label variables.
  apply_format(list_formats = list(geo1 = .fid_tz_s)) %>%
  mutate(tz_name = as.character(labelled::to_factor(geo1))) %>%
  
  # Add labels for DAIO.
  mutate(flow = case_when(
    geo2 == 2 ~ "I",
    geo2 == 4 ~ "AD",
    geo2 == 6 ~ "O",
    geo2 == 9 ~ "T",
    geo2 == 21 ~ "ADI"),
    
    pi = case_when(
      rank == 100 ~ "PI100",
      rank == 110 ~ "PI10",
      rank == 113 ~ "PI90",
      rank == 114 ~ "PI25",
      rank == 115 ~ "PI75"
    ),
    date_month = as.Date(datetime),
    ymonth = yearmonth(datetime)
    
  ) %>%
  
  left_join(. , df_mtf_method, c("method" = "method_tt")) %>%
  
  # Group and aggregate flights.
  group_by(ymonth, date_month, tz_id, tz_name, flow, mtf_tt, id, method, pi) %>%
  summarise(flights = sum(value, na.rm = T)) %>%
  ungroup() %>% 
  pivot_wider(names_from = pi, values_from = flights) |> 
  rename(flights_forecast = PI100)

df_out_clean_ecac <- df_out_clean %>% 
  filter(tz_name == "ECAC") %>% 
  left_join(forecast_list, "id" ) %>% 
  mutate(
    mtf_name = name
    # mtf_name = stringr::str_replace_all(name, " Forecast","")
  ) %>% 
  select(-name, -publication_date) %>% 
  arrange(id, flow, ymonth)


# save csv file
df_out_clean_ecac |> write_csv(here("G:/HQ/dgof-pru/Data/DataProcessing/Covid19/Oscar", "stf_data.csv")) 

