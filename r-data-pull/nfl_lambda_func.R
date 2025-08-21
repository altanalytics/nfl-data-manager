###########################
# File: lambda.R
# Description: Lambda Manager for NFL App
# Date: 8/16/2025
# Author: Anthony Trevisan
# Notes: 
###########################


lambda_manager <- function(lambda_input = list(msg='default')){
  
  print("Hello from ARM64 R! The current time is: ")
  print(Sys.time())
  print('My instructions are: ')
  print(lambda_input)
  
  
  if(!('msg' %in% names(lambda_input))){
    
    print('"msg" value not passed to Lambda - exiting')
    return('No Message Passed')
    
  }
  
  
  ### Change wd for running locally
  # setwd('~/Coding/git_repos/nfl-data-manager/r-data-pulls')
  print('Time to load some helper functions!')
  source('nfl_helpers.R')
  print('helpers loaded')
  print('NY Datetime:')
  print(ny_datetime)
  
  ### Get ESPN Data
  if(lambda_input$msg == 'get_espn'){
    
    print('Fetching Daily ESPN grab')
    source('get_espn_data.R')
    exec_func(get_espn_data)
    return('ESPN Daily Completed')
    
  }
  
  ### Get Fantasy Data
  if(lambda_input$msg == 'get_fantasy'){
    
    print('Fetching Real-Time Fantasy Data')
    week_number = s3readRDS(bucket = s3_bucket, object = 'admin/current_week.rds')
    espn_fantasy_loop(week_number)
    return('In Game Fantasy Completed')
    
  }

  
}


lambdr::start_lambda()
