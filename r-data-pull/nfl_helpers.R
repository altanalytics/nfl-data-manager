###########################
# File: helpers.R
# Description: Used to load functions across the application
# Date: 8/18/2025
# Author: Anthony Trevisan
# Notes: List of functions
# To do:
###########################

library(jsonlite)
library(base64enc)
library(lubridate)
library(tableHTML)
library(httr)
library(readr)
library(aws.s3)
library(dplyr)
library(tidyr)
library(rvest)
options(dplyr.width = Inf)

s3_bucket = 'alt-nfl-bucket'
ny_datetime = as_datetime(Sys.time(), tz = "America/New_York")

### Primary Execution Function Handler
exec_func = function(func_name, ...){

  ny_time = as_datetime(Sys.time(),tz='America/New_York')
  func_text = deparse(substitute(func_name))
  print(func_text)
  print(ny_time)
  # hour 25 force runs the app
  tml = Sys.time()
  out <- tryCatch(
    {
      print('running function')
      result <- do.call(func_name, list(...))
      list(res = 'success',mes = result)


    },
    error=function(cond) {
      ### If fails, explain why
      print(cond)
      print('run failed')
      send_r_email(paste0('Data Job Failed: ',func_text), 'Check Logs')
      print('Fail email sent')
      return(list(res = 'Failure', mes = cond$message))
    }
  )

  print('Function ran')
  time_diff <- difftime(Sys.time(), tml, units = "mins")
  fun_res = tibble(function_name = func_text, result = out$res, status = out$mes,
                   hour = hour(ny_time), dt = Sys.time(), date = Sys.Date(), time_len = time_diff)
  print(fun_res)
  job_status = s3read_using(FUN = read_csv, bucket = s3_bucket, object = 'admin/lambda_job_tracker.csv') %>%
    # filter(!(function_name == 'execute_trades')) %>% ### Filter our tests
    filter(date >= Sys.Date()-75)

  job_status = rbind(job_status,fun_res)
  # Don't write if running locally
  s3write_using(job_status, FUN = write_csv, bucket = s3_bucket, object = 'admin/lambda_job_tracker.csv')
  return(fun_res)

}


### Save S3 File
s3_csv_save = function(sv_df, path, bucket = 'alt-nfl-bucket', delim =','){
  if(!is.null(sv_df)){
    if(nrow(sv_df)>0){
      colnames(sv_df) = tolower(colnames(sv_df))
      s3write_using(sv_df, FUN = write_delim,
                    bucket = bucket,
                    object = path,
                    delim = delim)
    }
    return(paste0('File Saved: ',bucket,'/',path))
  } else {
    return('DF is NULL')
  }
}


# Tool to send R emails through SES
send_r_email = function(subject, embody, sendto=c("tony@altanalyticsllc.com")){
  
  
  frm = 'NFL App <tony@altanalyticsllc.com>'
  
  sendto = paste0(sendto,collapse = ' ')
  # bcc = paste0(bcc,collapse = ' ')
  em_api = paste0('aws ses send-email ',
                  '--from ',frm,' ',
                  '--to ',sendto,' ',
                  # '--bcc ',bcc,' ',
                  '--subject "',subject,'" ',
                  "--html '",embody,"'")
  
  
  system(em_api)
  
}
