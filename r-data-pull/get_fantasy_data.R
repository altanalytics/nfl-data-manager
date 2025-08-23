###########################
# File: get_fantasy_data.R
# Description: This file is used to get the Fantasy data for the two specific leagues
# Date: 8/17/2025
# Author: Anthony Trevisan
# Notes: Refresh happens every 10 minutes during standard games
###########################


get_fantasy_data = function(){
  
  # Get Login Creds for Fantasy Data
  espn_login = aws.s3::s3readRDS(bucket = s3_bucket, object = 'fantasy_data/espn_login.rds')
  bbr_teams = aws.s3::s3read_using(FUN=read_csv, bucket = s3_bucket, object = 'fantasy_data/bbr_teams.csv') %>%
    filter(!is.na(team_number))
  sched = aws.s3::s3read_using(FUN=read_csv, bucket = s3_bucket, object = 'fantasy_data/bbr_schedule.csv')

  
  #############################
  ########### Get League Data
  #############################
  
  fant_yr = year(Sys.Date())
  espn_period = 1 # MAKE DYNAMIC
  schedule_df <- sched  %>%
    left_join(select(bbr_teams,home=team_number,home_team=team_name)) %>%
    left_join(select(bbr_teams,away=team_number,away_team=team_name)) %>%
    mutate(winner = ifelse(is.na(winner),'TBD',winner),
           loser = ifelse(is.na(loser),'TBD',loser))
  
  
  
  league_ids = unique(bbr_teams$espn_league)
  all_leagues = NULL
  for(lgid in league_ids){
    
    url = paste0('https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/',fant_yr,'/segments/0/leagues/',
                 lgid,'?scoringPeriodId=',lgid,'&view=mRoster&view=mTeam&view=mBoxscore')
    results = GET(url,add_headers('cookie'=paste0(espn_login$espn_2,espn_login$swid)))
    espn_data = content(results)
    
    
    memb_df = NULL
    team_df = NULL
    for(m in espn_data$members){
      
      
      temp = tibble(espn_id = gsub("[[:punct:]]", "", m$id),
                    team_name = m$displayName,
                    first_name = m$firstName,
                    last_name = m$lastName)
      memb_df = rbind(memb_df,temp)
      
    }
    
    for(t in espn_data$teams){
      
      
      temp = tibble(abbrev = t$abbrev,
                    divisionId = t$divisionId,
                    id = t$id,
                    name = t$name,
                    primaryOwner = t$primaryOwner,
                    waiverRank = t$waiverRank,
                    espn_id = gsub("[[:punct:]]", "", t$owners[[1]]))
      team_df = rbind(team_df,temp)
      
    }
    
    
    lg_temp = team_df %>%
      mutate(espn_league = lgid) %>%
      left_join(memb_df)
    all_leagues = rbind(all_leagues,lg_temp)
    
  }
  
  
  

  all_leagues %>%
    print(n=50)
  
  
  
  
  
}

