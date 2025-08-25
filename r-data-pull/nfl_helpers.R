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
s3_db = 'alt-nfl-database'
ny_datetime = as_datetime(Sys.time(), tz = "America/New_York")

team_df = aws.s3::s3read_using(FUN=read_csv, bucket = s3_bucket, object = 'fantasy_data/bbr_teams.csv') %>%
  filter(!is.na(team_number))
espn_login = aws.s3::s3readRDS(bucket = s3_bucket, object = 'fantasy_data/espn_login.rds')
sched = aws.s3::s3read_using(FUN=read_csv, bucket = s3_bucket, object = 'fantasy_data/bbr_schedule.csv')
schedule_df <- s3read_using(FUN=read_csv, bucket = s3_bucket, object = 'fantasy_data/bbr_schedule.csv') %>%
  left_join(select(team_df,home=team_number,home_team=team_name)) %>%
  left_join(select(team_df,away=team_number,away_team=team_name)) %>%
  mutate(winner = ifelse(is.na(winner),'TBD',winner),
         loser = ifelse(is.na(loser),'TBD',loser))

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



# Function to get fantasy data for a single league for a single week
espn_list_func = function(espn_league,espn_period,fant_yr){
  
  url = paste0('https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/',fant_yr,'/segments/0/leagues/',
               espn_league,'?scoringPeriodId=',espn_period,'&view=mRoster&view=mTeam&view=mBoxscore')
  results = GET(url,add_headers('cookie'=paste0(espn_login$espn_2,espn_login$swid)))
  espn_data = content(results)
  
  
  # Get ESPN Members
  memb_df = NULL
  for(m in espn_data$members){
    
    
    temp = tibble(espn_id = gsub("[[:punct:]]", "", m$id),
                  team_name = m$displayName,
                  first_name = m$firstName,
                  last_name = m$lastName)
    memb_df = rbind(memb_df,temp)
    
  }
  
  # Get Team Dataframe
  espn_team_df = NULL
  for(t in espn_data$teams){
    
    
    temp = tibble(abbrev = t$abbrev,
                  divisionId = t$divisionId,
                  id = t$id,
                  name = t$name,
                  primaryOwner = t$primaryOwner,
                  waiverRank = t$waiverRank,
                  espn_id = gsub("[[:punct:]]", "", t$owners[[1]]))
    espn_team_df = rbind(espn_team_df,temp)
    
  }
  
  # Merge team and member data
  league_df = espn_team_df %>%
    mutate(espn_league = espn_league) %>%
    left_join(memb_df)
  
  # Get ESPN Schedule
  sched_df = NULL
  for(s in espn_data$schedule){
    s$matchupPeriodId
    
    temp = tibble(week = s$matchupPeriodId,
                  game_id = s$id,
                  home_team = s$home$teamId,
                  home_score = s$home$totalPoints,
                  away_team = s$away$teamId,
                  away_score = s$away$totalPoints)
    sched_df = rbind(sched_df,temp)
    
  }
  
  # Add League ID
  sched_df$espn_league = espn_league
  
  # Extract Roster details for each team
  roster_details = NULL
  for(t in espn_data$teams){
    team_abbrev = t$abbrev
    team_id = t$id
    print(team_id)
    for(p in t$roster$entries){
      
      player_name = p$playerPoolEntry$player$fullName
      player_slot = p$lineupSlotId
      player_injury_status = p$playerPoolEntry$player$injured
      player_default_pos_id = p$playerPoolEntry$player$defaultPositionId
      player_status = p$status
      player_team = p$playerPoolEntry$player$proTeamId
      player_id = p$playerId
      for(st in p$playerPoolEntry$player$stats){
        st$appliedStats = NULL
        st$stats = NULL
        temp_df = as.data.frame(st)
        temp_df = mutate(temp_df,
                         team_abbrev = team_abbrev,
                         team_id = team_id,
                         player_name = player_name,
                         player_slot = player_slot,
                         player_injury_status = player_injury_status,
                         player_default_pos_id = player_default_pos_id,
                         player_status = player_status,
                         player_team = player_team,
                         player_id = player_id,
                         search_period = espn_period,
        )
        roster_details = bind_rows(roster_details,temp_df) %>% as_tibble()
      }
      
    }
    
  }
  
  
  # Merge the data together
  roster_details %>%
    filter(search_period == scoringPeriodId, seasonId == fant_yr) %>%
    mutate(score_type = ifelse(nchar(externalId)<8,'projected','actual'),
           espn_league = as.numeric(espn_league),
           upd_player_slot = ifelse(player_slot==23,7,player_slot)) %>%
    left_join(select(team_df,espn_league,team_id=espn_league_team_id,bbr_team_id = team_number)) %>%
    select(espn_league,team_abbrev:search_period,bbr_team_id,scoringPeriodId,seasonId,upd_player_slot,score_type,appliedTotal)%>%
    pivot_wider(names_from = score_type, values_from = appliedTotal) %>%
    arrange(team_id,upd_player_slot) %>%
    group_by(bbr_team_id)-> roster_df
  
  
  # Fof future weeks, add in value of 0
  if(!('actual' %in% colnames(roster_df))){
    roster_df$actual = 0
  }
  roster_df %>%
    mutate(unique_slot = row_number(),
           projected_total = ifelse(upd_player_slot<20,projected,0),
           actual_total = ifelse(upd_player_slot<20,actual,0)) %>%
    ungroup() -> roster_df
  
  
  
  # Return list and members
  return(list(roster_df = roster_df,
              sched_df = sched_df,
              memb_df = memb_df,
              league_df = league_df))
  
}



# Do full loop on Fantasy periods
espn_fantasy_loop = function(periods = c(1:12)){
  

    for(espn_period in periods){
      print(espn_period)
      tm_lst = unique(team_df$espn_league)
      lg1 = espn_list_func(tm_lst[1],espn_period,year(Sys.Date()))
      Sys.sleep(3)
      lg2 = espn_list_func(tm_lst[2],espn_period,year(Sys.Date()))
      Sys.sleep(3)
      
      sched_all = rbind(mutate(lg1$sched_df,
                               home_team = ifelse(home_team>12,home_team-7,home_team),
                               away_team = ifelse(away_team>12,away_team-7,away_team)),
                        mutate(lg2$sched_df,game_id = game_id+100,
                               home_team = ifelse(home_team>12,home_team-3,home_team),
                               away_team = ifelse(away_team>12,away_team-3,away_team)))
      roster_all = rbind(lg1$roster_df,lg2$roster_df)
      
      
      sched_all %>%
        filter(week == espn_period) %>%
        left_join(tibble(week = espn_period, unique_slot = c(1:50))) %>%
        left_join(select(roster_all, espn_league, unique_slot, home_player_slot = player_slot, home_team = team_id, home_player = player_name, home_bbr_team_id = bbr_team_id,
                         home_projected=projected,home_actual=actual, home_projected_total = projected_total, home_actual_total = actual_total)) %>%
        left_join(select(roster_all, espn_league, unique_slot, away_player_slot = player_slot, away_team = team_id, away_player = player_name, away_bbr_team_id = bbr_team_id,
                         away_projected=projected,away_actual=actual, away_projected_total = projected_total, away_actual_total = actual_total)) %>%
        filter(!is.na(home_player) | !is.na(away_player)) %>%
        group_by(week,game_id,espn_league) %>%
        mutate(game_type = 'espn') %>%
        ungroup() -> pre_internal
      
      pre_internal %>%
        mutate(home_player = 'TOTAL',away_player = 'TOTAL',unique_slot = 0) %>%
        group_by(game_type,espn_league,week,game_id,unique_slot,home_bbr_team_id,home_player,away_bbr_team_id,away_player) %>%
        summarise(home_projected = sum(home_projected_total,na.rm = T), home_actual = sum(home_actual_total,na.rm = T), 
                  away_projected = sum(away_projected_total,na.rm = T),away_actual = sum(away_actual_total,na.rm = T)) %>%
        filter(!is.na(home_bbr_team_id),!is.na(away_bbr_team_id)) -> pre_totals
      
      internals = pre_internal %>% 
        select(colnames(pre_totals)) %>%
        rbind(pre_totals) %>%
        arrange(week,game_id,unique_slot)
      
      schedule_df %>%
        mutate(game_id = row_number()+200,
               espn_league = '10101010') %>%
        select(week,game_id,home_team = home,away_team = away,espn_league) %>%
        filter(week == espn_period) %>%
        left_join(tibble(week = espn_period, unique_slot = c(1:50))) %>%
        left_join(select(roster_all, unique_slot, home_player_slot = player_slot, home_team = bbr_team_id, home_player = player_name, home_bbr_team_id = bbr_team_id,
                         home_projected=projected,home_actual=actual, home_projected_total = projected_total, home_actual_total = actual_total)) %>%
        left_join(select(roster_all, unique_slot, away_player_slot = player_slot, away_team = bbr_team_id, away_player = player_name, away_bbr_team_id = bbr_team_id,
                         away_projected=projected,away_actual=actual, away_projected_total = projected_total, away_actual_total = actual_total)) %>%
        filter(!is.na(home_player) | !is.na(away_player)) %>%
        group_by(week,game_id,espn_league) %>%
        mutate(game_type = 'blacktop') %>%
        ungroup() -> pre_external
      
      pre_external %>%
        mutate(home_player = 'TOTAL',away_player = 'TOTAL',unique_slot = 0) %>%
        group_by(game_type,espn_league,week,game_id,unique_slot,home_bbr_team_id,home_player,away_bbr_team_id,away_player) %>%
        summarise(home_projected = sum(home_projected_total,na.rm = T), home_actual = sum(home_actual_total,na.rm = T), 
                  away_projected = sum(away_projected_total,na.rm = T),away_actual = sum(away_actual_total,na.rm = T)) %>%
        filter(!is.na(home_bbr_team_id),!is.na(away_bbr_team_id)) -> pre_totals
      
      externals = pre_external %>% 
        select(colnames(pre_totals)) %>%
        rbind(pre_totals) %>%
        arrange(week,game_id,unique_slot)
      
      rbind(internals,externals) %>%
        arrange(game_type,espn_league,week,game_id,unique_slot) %>%
        left_join(select(team_df,home_bbr_team_id=team_number,home_team=team_name)) %>%
        left_join(select(team_df,away_bbr_team_id=team_number,away_team=team_name)) -> weekly_data
      
      
      s3write_using(weekly_data, write_csv, 
                    bucket = s3_bucket, object = paste0('fantasy_data/week_',espn_period,'.csv'))
      
    }
  
  return(rbind(lg1$league_df,lg2$league_df))
  
}



### Pull Recap Manually
get_espn_recap <- function(game_id) {
  url <- paste0("https://www.espn.com/nfl/recap?gameId=", game_id)
  #url = game_id
  
  page <- httr::GET(url, httr::user_agent("Mozilla/5.0"))
  html <- read_html(page)
  
  # Extract the title from <title> tag in <head>
  title_text <- html %>%
    html_element("title") %>%
    html_text2() %>%
    sub(" - ESPN.*", "", .)  # Remove trailing " - ESPN"
  
  # Try <article> first
  article_node <- html_element(html, "article")
  
  if (!is.na(article_node)) {
    # If <article> exists, get <p> inside
    recap_paragraphs <- article_node %>%
      html_elements("p") %>%
      html_text2()
  } else {
    # Fallback: Try all <p> tags and filter
    all_paragraphs <- html %>% html_elements("p") %>% html_text2()
    recap_paragraphs <- all_paragraphs[nchar(all_paragraphs) > 50]
  }
  
  if (length(recap_paragraphs) == 0) {
    return("⚠️ No recap text found.")
  }
  
  # Combine title and recap body
  full_text <- paste0("**", title_text, "**\n\n", paste(recap_paragraphs, collapse = "\n\n"))
  return(full_text)
}


