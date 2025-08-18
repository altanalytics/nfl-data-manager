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

  library(jsonlite)

  
}


lambdr::start_lambda()
