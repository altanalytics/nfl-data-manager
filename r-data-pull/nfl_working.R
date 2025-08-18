###########################
# File: nfl_working.R
# Description: This file is used to handle working manually
# Date: 8/17/2025
# Author: Anthony Trevisan
# Notes:
###########################


### DCA ??

profile = 'nfl'
setwd('~/Coding/git_repos/nfl-data-manager/r-data-pull')
Sys.setenv(AWS_PROFILE = profile, AWS_REGION = 'us-east-1')
# source('nfl_helpers.R')


library(tidyverse)
library(jsonlite)

# Read the most recent cache file
cache_dir <- "~/.aws/cli/cache/"
system('rm -rf ~/.aws/cli/cache/')
system(paste0('aws s3 ls --profile ',profile)) # forces new cli file
cache_files <- list.files(cache_dir, full.names = TRUE)
latest_cache <- cache_files[which.max(file.info(cache_files)$mtime)]
cache_data <- fromJSON(latest_cache)

# Set environment variables
Sys.setenv(
  AWS_ACCESS_KEY_ID = cache_data$Credentials$AccessKeyId,
  AWS_SECRET_ACCESS_KEY = cache_data$Credentials$SecretAccessKey,
  AWS_SESSION_TOKEN = cache_data$Credentials$SessionToken
)


