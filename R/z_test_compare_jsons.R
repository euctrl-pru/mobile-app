# test if jsons are identical
local_dir_test <- '//ihx-vdm05/LIVE_var_www_performance$/briefing/data/v5/2026-01-24'
network_dir_test <- '//ihx-vdm05/LIVE_var_www_performance$/briefing/data/v5/2026-01-24 - Copy'
# network_dir_test <- 'C:/Users/oaolive/repos/mobile-app/data/prod/ap - Copy'

files_to_check <- list.files(local_dir_test)
checkfiles <- ''

# files_to_check[49]

# library(diffobj)

# diffPrint(local_text, network_text)

for (i in 1:length(files_to_check)) {
local_text <- read_file(here(local_dir_test, files_to_check[i]))
  # local_text <- strsplit(readr::read_file(here(local_dir_test, files_to_check[i])), "\n", fixed = TRUE)[[1]]
network_text <- read_file(here(network_dir_test, files_to_check[i]))
  # network_text <- strsplit(readr::read_file(here(network_dir_test, files_to_check[i])), "\n", fixed = TRUE)[[1]]

checkfiles[i] <- local_text == network_text

# n <- max(length(local_text), length(network_text))
# 
# for (i in seq_len(n)) {
#   l1 <- if (i <= length(local_text)) local_text[i] else ""
#   l2 <- if (i <= length(network_text)) network_text[i] else ""
#   
#   if (!identical(l1, l2)) {
#     cat(sprintf("Line %d:\n  file1: %s\n  file2: %s\n\n", i, l1, l2))
#   }
# }
# Reduce(setdiff, strsplit(c(local_text, network_text), split = ""))

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
