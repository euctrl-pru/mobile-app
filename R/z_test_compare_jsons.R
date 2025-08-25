# test if jsons are identical
local_dir_test <- 'C:/Users/oaolive\repos/mobile-app/data/prod/ap'
network_dir_test <- '//ihx-vdm05/LIVE_var_www_performance$/briefing/data/v4/2025-08-24'

files_to_check <- list.files(local_dir_test)
checkfiles <- ''

# files_to_check[49]


for (i in 1:length(files_to_check)) {
local_text <- read_file(here(local_dir_test, files_to_check[i]))
network_text <- read_file(here(network_dir_test, files_to_check[i]))

checkfiles[i] <- local_text == network_text

# Reduce(setdiff, strsplit(c(local_text, network_text), split = ""))
#
# # Split strings into characters
# chars1 <- strsplit(local_text, "")[[1]]
# chars2 <- strsplit(network_text, "")[[1]]
#
# # Determine the minimum length
# min_length <- min(length(chars1), length(chars2))
#
# # Truncate both strings to the same length
# chars1 <- chars1[1:min_length]
# chars2 <- chars2[1:min_length]
#
# # Find differing positions
# diff_positions <- which(chars1 != chars2)
#
# # Output the differing positions
# diff_positions

# check<- local_text == network_text
}

checkfiles
