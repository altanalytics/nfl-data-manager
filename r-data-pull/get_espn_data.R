###########################
# File: get_espn_data.R
# Description: This file is used to get the PBP and ESPN data for each game
# Date: 8/17/2025
# Author: Anthony Trevisan
# Notes: Links found here: https://gist.github.com/nntrn/ee26cb2a0716de0947a0a4e9a157bc1c
###########################


get_espn_data = function(){
  
  # Get Existing ESPN Game details
  espn_game_details = s3readRDS(bucket = s3_bucket, object = 'admin/espn_api_game_detail.rds')
  
  
  #############################
  ########### Get Full Schedule
  #############################
  
  
  # Set API Call for entire latest season
  dts = year(Sys.Date()-90)
  strt = paste0(dts,'0601')
  end = paste0(dts+1,'0501')
  gms = GET(paste0('https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates=',strt,'-',end,'&limit=1000'))
  gms = content(gms)
  
  # Create Game List
  print('Parse NFL Schedule Data')
  all_games=NULL
  for(ev in gms$events){
     
      # Filter for completed games only
      print(ev$shortName)
      if(ev$status$type$completed!=FALSE){
      # Extract key fields from json file
      temp_game = tibble(
        season = ev$season$year,
        season_type = ev$season$type,
        season_name = ev$season$slug,
        season_week = ev$week$number,
        game_week = ev$week$number,
        espn_id = ev$id,
        unique_id = 'temp',
        date_time = ev$date,
        date = ev$date,
        game_short_name = ev$shortName,
        game_long_name = ev$name,
        game_description = ifelse(length(ev$competitions[[1]]$notes)>0,
                                  ev$competitions[[1]]$notes[[1]]$headline,
                                  ev$name),
        venue = ev$competitions[[1]]$venue$fullName,
        location = paste0(ev$competitions[[1]]$venue$address, collapse = ' '),
        game_status = ev$status$name,
        game_state = ev$status$state,
        game_winner = ifelse(ev$competitions[[1]]$competitors[[1]]$winner,'team_1','team_2'),
        team_1_id = ev$competitions[[1]]$competitors[[1]]$id,
        team_1_home_away = ev$competitions[[1]]$competitors[[1]]$homeAway,
        team_1_location = ev$competitions[[1]]$competitors[[1]]$team$location,
        team_1_name = ev$competitions[[1]]$competitors[[1]]$team$name,
        team_1_abbreviation = ev$competitions[[1]]$competitors[[1]]$team$abbreviation,
        team_1_full_name = ev$competitions[[1]]$competitors[[1]]$team$displayName,
        team_1_mascot = ev$competitions[[1]]$competitors[[1]]$team$shortDisplayName,
        team_1_q1_score = as.numeric(ev$competitions[[1]]$competitors[[1]]$linescores[[1]]$value),
        team_1_q2_score = as.numeric(ev$competitions[[1]]$competitors[[1]]$linescores[[2]]$value),
        team_1_q3_score = as.numeric(ev$competitions[[1]]$competitors[[1]]$linescores[[3]]$value),
        team_1_q4_score = as.numeric(ev$competitions[[1]]$competitors[[1]]$linescores[[4]]$value),
        team_1_final_score = as.numeric(ev$competitions[[1]]$competitors[[1]]$score),
        team_2_id = ev$competitions[[1]]$competitors[[2]]$id,
        team_2_home_away = ev$competitions[[1]]$competitors[[2]]$homeAway,
        team_2_location = ev$competitions[[1]]$competitors[[2]]$team$location,
        team_2_name = ev$competitions[[1]]$competitors[[2]]$team$name,
        team_2_abbreviation = ev$competitions[[1]]$competitors[[2]]$team$abbreviation,
        team_2_full_name = ev$competitions[[1]]$competitors[[2]]$team$displayName,
        team_2_mascot = ev$competitions[[1]]$competitors[[2]]$team$shortDisplayName,
        team_2_q1_score = as.numeric(ev$competitions[[1]]$competitors[[2]]$linescores[[1]]$value),
        team_2_q2_score = as.numeric(ev$competitions[[1]]$competitors[[2]]$linescores[[2]]$value),
        team_2_q3_score = as.numeric(ev$competitions[[1]]$competitors[[2]]$linescores[[3]]$value),
        team_2_q4_score = as.numeric(ev$competitions[[1]]$competitors[[2]]$linescores[[4]]$value),
        team_2_final_score = as.numeric(ev$competitions[[1]]$competitors[[2]]$score),
      ) %>% mutate(date_time = as.POSIXct(date_time, format = "%Y-%m-%dT%H:%MZ", tz = "UTC"),
                   date_time = as_datetime(date_time, tz = "America/New_York"),
                   date = as.Date(date_time),
                   game_week = ifelse(season_type == 1,game_week-6,ifelse(season_type==3,game_week+20,game_week)),
                   unique_id = paste(season,season_type,ifelse(season_week<10,paste0(0,season_week),season_week),
                                     team_1_abbreviation,team_2_abbreviation,sep='_'))
      
      all_games = bind_rows(temp_game,all_games)
      }
  }
    
  
  print('Schedule Parsed')
  
  # Merge new schedule with existing schedule
  new_game_details = espn_game_details %>%
    filter(!(espn_id %in% all_games$espn_id))
  new_game_details = rbind(new_game_details,all_games) %>%
    distinct(unique_id,.keep_all = T) %>% 
    arrange(date_time)
  aws.s3::s3saveRDS(new_game_details,bucket = s3_bucket, object = 'admin/espn_api_game_detail.rds')
  

  # Create a clean schedule with consolidated team names
  schedule = new_game_details %>% 
    mutate(home_team = ifelse(team_1_home_away=='home',team_1_abbreviation,team_2_abbreviation),
           home_team = case_when(home_team == 'LA'~'LAR',
                                 home_team == 'SD'~'LAC',
                                 home_team == 'STL'~'LAR',
                                 home_team == 'WAS'~'WSH',
                                 home_team == 'OAK'~'LV',
                                 TRUE ~ home_team),
           away_team = ifelse(team_2_home_away=='home',team_1_abbreviation,team_2_abbreviation),
           away_team = case_when(away_team == 'LA'~'LAR',
                                 away_team == 'SD'~'LAC',
                                 away_team == 'STL'~'LAR',
                                 away_team == 'WAS'~'WSH',
                                 away_team == 'OAK'~'LV',
                                 TRUE ~ away_team),
           winning_team = ifelse(team_1_final_score==team_2_final_score,'TIE',
                                 ifelse(team_1_final_score>team_2_final_score,home_team,away_team)),
           matchup = paste0(away_team,'_',home_team)) %>%
    select(season:game_short_name,matchup,home_team,home_score = team_1_final_score,
           away_team,away_score=team_2_final_score,winning_team) %>%
    filter(season_name != 'preseason') %>%
    arrange(date_time)
  
  s3_csv_save(schedule,path='admin/clean_schedule.csv')

  
  
 
  
  #############################
  ########### Get Game Details
  #############################
  
  filt_df = new_game_details %>%
    filter(date > Sys.Date()-3)
  
  
  
  for(rn in 1:nrow(filt_df)){
    Sys.sleep(1)
    print(paste0('Row Number: ',rn))
    
    # Get current game
    game_summary = as.list(filt_df[rn,])
    
    # Create key variables
    gm_unq_id = game_summary$unique_id
    gm_espn_id = game_summary$espn_id
    print(gm_unq_id)
    week_text = espn_game_details$season_week[rn]
    week_text = ifelse(week_text<10,paste0('0',week_text),week_text)
    
    # Make API Call and create base save path for this game
    gm_summary = GET(paste0('https://site.web.api.espn.com/apis/site/v2/sports/football/nfl/summary?event=',gm_espn_id))
    gm_summary = content(gm_summary)  
    path = paste('nfl_espn_data',paste0('season_',espn_game_details$season[rn]),espn_game_details$season_name[rn],
                 paste0('week_',week_text),gm_unq_id,sep='/')
    
    # Save current game as a json 
    s3write_using(
      FUN = function(obj, file) {
        write_json(obj, path = file, pretty = TRUE, auto_unbox = TRUE)
      },
      x = game_summary,
      bucket = s3_bucket,
      object = paste0(path,'/inputs/',gm_unq_id,'_game_summary.json')
    )
    
    #######################################
    ## 1. Get Team Statistics from the game
    
    all_tm_stats = NULL
    print('Get summary stats for each team')
    for(tm in gm_summary$boxscore$teams){
      
      tm_stats = bind_rows(lapply(tm$statistics , function(x)
        tibble(espn_id = gm_espn_id,
               unique_id = gm_unq_id,
               team_abbreviation = tm$team$abbreviation,
               team_id = tm$team$id,
               label=x$label,
               name=x$name,
               value=as.character(x$value),
               display=x$displayValue
        )))
      all_tm_stats = rbind(all_tm_stats,tm_stats)
      
    }
    
    nrow(all_tm_stats)
    s3_csv_save(all_tm_stats,path=paste0(path,'/inputs/',gm_unq_id,'_game_stats.csv'))
    
    #########################################
    ## 1. Get player Statistics from the game
    
    print('Get Player Stats')
    all_play_stats = NULL
    for(tm in gm_summary$boxscore$players){
      team = tm$team$abbreviation
      team_id = tm$team$id
      all_play_team_stats = NULL
      for(st in tm$statistics){
        
        st_lbl =  unlist(st$labels)
        st_desc = unlist(st$descriptions)
        plyr_stats = bind_rows(lapply(st$athletes,function(x)
          tmp_stats = tibble(espn_id = gm_espn_id,
                             unique_id = gm_unq_id,
                             team_abbreviation = team,
                             team_id = team_id,
                             athlete_id = x$athlete$id,
                             athlete_name = x$athlete$displayName,
                             athlete_first = x$athlete$firstName,
                             athlete_last = x$athlete$lastName,
                             athlete_jersey = x$athlete$jersey,
                             stat_type = st$name,
                             stat_label = st_lbl,
                             stat_description = st_desc,
                             stat_value = unlist(x$stats)))
        )
        all_play_team_stats = bind_rows(all_play_team_stats,plyr_stats)
      }
      all_play_stats = rbind(all_play_stats,all_play_team_stats)
      
    }
    
    nrow(all_play_stats)
    s3_csv_save(all_play_stats,path=paste0(path,'/inputs/',gm_unq_id,'_player_stats.csv'))
    
    #######################################
    ## 3. Get Drive and Play by Play Data
    
    all_drives = NULL
    pbp_all= NULL
    print('Getting drive info')
    for(drv in gm_summary$drives$previous){
      
      drv_tmp = tibble(
        espn_id = gm_espn_id,
        unique_id = gm_unq_id,
        team_abbreviation = drv$team$abbreviation,
        team_id = drv$team$id,
        description = drv$description,
        start_quarter = drv$start$period$number,
        start_time = drv$start$clock$displayValue,
        start_home_score = drv$ply[[1]]$homeScore,
        start_away_score = drv$ply[[1]]$awayScore,
        yards = drv$yards,
        scoring = drv$isScore,
        offensive_plays = drv$offensivePlays,
        result = drv$result,
        display_result = drv$displayResult,
        start_text = drv$start$text,
        start_yard = drv$start$yardLine,
        end_text = drv$end$text,
        end_yard = drv$end$yardLine,
        time_length = drv$timeElapsed$displayValue
      )
      
      all_drives = bind_rows(all_drives,drv_tmp)
      
      for(ply in drv$plays){
        
        
        ply_tmp = tibble(
          espn_id = gm_espn_id,
          unique_id = gm_unq_id,
          drive_id = drv$id,
          play_id = ply$id,
          sequence = ply$sequenceNumber,
          yardage = ply$statYardage,
          quarter = ply$period$number,
          time_remaining = ply$clock$displayValue,
          home_score = ply$homeScore,
          away_score = ply$awayScore,
          down = ply$start$down,
          distance = ply$start$distance,
          yardline = ply$start$yardLine,
          yards_to_endzone = ply$start$yardsToEndzone,
          possession = ply$start$possessionText,
          scoring_play = ply$scoringPlay,
          scoring_value = ply$scoreValue,
          play_text = ply$text
        )
        pbp_all = bind_rows(pbp_all,ply_tmp)
      }
      
      
    }
    
    nrow(all_drives)
    s3_csv_save(all_drives,path=paste0(path,'/inputs/',gm_unq_id,'_drive_report.csv'))
    s3_csv_save(pbp_all,path=paste0(path,'/inputs/',gm_unq_id,'_play_by_play.csv'))

    
    #######################################
    ## 4. Get game outputs (recap and headling)
    
    ### OUTPUTS
    print('Get End of Game Recap')
    if(!is.null(gm_summary$article$story)){
      game_recap = list(espn_id = gm_espn_id,
                        unique_id = gm_unq_id,
                        headline = gm_summary$article$headline,
                        description = gm_summary$article$description,
                        article = read_html(gm_summary$article$story) %>% html_text())
      s3write_using(
        FUN = function(obj, file) {
          write_json(obj, path = file, pretty = TRUE, auto_unbox = TRUE)
        },
        x = game_recap,
        bucket = s3_bucket,
        object = paste0(path,'/outputs/',gm_unq_id,'_game_recap.json')
      )
      
    }
  }
  
  
  
  
}

