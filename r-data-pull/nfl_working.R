###########################
# File: nfl_working.R
# Description: This file is used to handle working manually
# Date: 8/17/2025
# Author: Anthony Trevisan
# Notes:
# ToDo: validate data saves, determine winner/loser, cleanup/comment, set live updates, PBP pass/Rush/rec (database and then tool to query)
###########################


profile = 'nfl'
setwd('~/Coding/git_repos/nfl-data-manager/r-data-pull')
Sys.setenv(AWS_PROFILE = profile, AWS_REGION = 'us-east-1')
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

source('nfl_helpers.R')


###### Get token saved
# from MM - JT
espn_har = read_json('/Users/tonytrevisan/Downloads/fantasy.espn.com.har')
for(req in espn_har$log$entries){
  
  if(grepl('131217621',req$request$url)){
    m_req = req
  }
  
}
for(hd in m_req$request$headers){
  
  if(hd$name == 'Cookie'){
    e_token = hd$value
  }
  
}



pbp <- nflfastR::load_pbp(2024)
pbp %>%
  filter(!is.na(passer_id))
nflfastR::fast_scraper_roster(2024)



vec = strsplit(e_token,';')[[1]]

espn_2 = vec[grepl('espn_s2',vec)]
swid = vec[grepl('SWID',vec)]
espn_login = list(espn_2 = espn_2, swid = swid)
aws.s3::s3saveRDS(espn_login, bucket = s3_bucket, object = 'fantasy_data/espn_login.rds')


#### Done saving token

s3 = paws::s3()

get_s3_listing <- function(bucket, prefix) {
  token <- NULL
  rows  <- list()
  
  repeat {
    resp <- s3$list_objects_v2(Bucket = bucket, Prefix = prefix,
                                ContinuationToken = token)
    
    if (!is.null(resp$KeyCount)) message("Fetched: ", resp$KeyCount, " keys")
    
    if (!is.null(resp$Contents)) {
      page <- lapply(resp$Contents, function(obj) {
        vec <- strsplit(obj$Key, "/", fixed = TRUE)[[1]]
        tibble(
          top    = vec[1],
          season = vec[2],
          stype  = vec[3],
          week   = vec[4],
          game   = vec[5],
          inout  = vec[6],
          file   = vec[7],
          path = obj$Key,
        )
      })
      rows <- c(rows, page)
    }
    
    # Stop if no more pages
    if (!isTRUE(resp$IsTruncated)) break
    token <- resp$NextContinuationToken
  }
  
  bind_rows(rows)
}

lst <- get_s3_listing(s3_bucket, "nfl_espn_data/")

db_path = paste('nfl_espn_database','PLACEHOLDER',
                paste0('nfl_season=',filt_df$season[rn]),
                paste0('nfl_season_type=',filt_df$season_name[rn]),
                paste0('nfl_week=',week_text),gm_unq_id,sep='/')
s3_csv_save(all_tm_stats,path= paste0(gsub('PLACEHOLDER','team_stats',db_path),'_team_stats.csv'))

lst$database = lst$path

lst %>%
  mutate(top_folder = case_when(grepl('drive_report',file) ~ 'drive_report',
                                grepl('game_stats',file) ~ 'team_stats',
                                grepl('play_by_play',file) ~ 'play_by_play',
                                grepl('player_stats',file) ~ 'player_stats',
                                TRUE ~ 'OTHER'),
         database = paste('nfl_espn_database',top_folder,gsub('season_','nfl_season=',season),
                          paste0('nfl_season_type=',stype),paste0('nfl_week=',week),file,sep='/')) %>%
  filter(top_folder != 'OTHER') -> copy_over

for(co in 9895:nrow(copy_over)){
  
 print(co)


aws.s3::copy_object(from_object = copy_over$path[co],to_object = copy_over$database[co],
                    from_bucket = s3_bucket, to_bucket = s3_bucket)

}

distinct(lst,game)
lst %>%
  rename(file_nm = file) %>%
  mutate(
    yrs = gsub("season_", "", season),
    chk = mapply(function(y, f) grepl(y, f), yrs, file_nm)
  ) %>% filter(chk) -> upd


upd %>% 
  filter(season>'season_2011') %>%
  group_by(season,game) %>%
  mutate(ct=n()) %>% filter(ct==5) -> missing_recaps


espn_game_details %>%
  filter((unique_id %in% missing_recaps$game)) -> miss_recap


for(g in c(1:nrow(miss_recap))){
  Sys.sleep(.5)
  print('Start')
  print(g)
  gm_espn_id = miss_recap$espn_id[g]
  gm_unq_id = miss_recap$unique_id[g]
  
text_recap = get_espn_recap(gm_espn_id)

if(!grepl('No recap text found',text_recap)){
  print('success')
  print(g)
  
  game_recap = list(espn_id = gm_espn_id,
                    unique_id = gm_unq_id,
                    article = text_recap)
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




lst %>% group_by(season) %>% summarise(ct = n())
espn_game_details %>%
  filter(!(unique_id %in% upd$game))

library(aws.s3)
library(dplyr)

# List objects
objs <- get_bucket(bucket = s3_bucket, prefix = "nfl_espn_data")
length(objs)
# Convert to data frame
df <- do.call(rbind, lapply(objs, as.data.frame))

# Clean up columns
df <- df %>%
  select(Key, LastModified, Size, StorageClass)

#### Resave ESPN Login details
espn_2 = 'espn_s2=ABCD;'
swid = 'SWID={12345};'
espn_login = list(espn_2 = espn_2, swid = swid)
aws.s3::s3saveRDS(espn_login, bucket = s3_bucket, object = 'fantasy_data/espn_login.rds')


rvest::read_html('https://www.espn.com/nfl/boxscore/_/gameId/401773017')

url = 'https://www.espn.com/nfl/boxscore/_/gameId/401773017'
page <- httr::GET(url, httr::user_agent("Mozilla/5.0"))
html <- read_html(page)

  spot_table <- read_html(url)
  spot_table %>%
    html_nodes("table") %>%
    .[[1]] %>%
    html_table()
  spot_table %>%
    html_nodes("table") %>%
    .[[8]] %>%
    html_table()

  gms = GET(paste0('https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates=20240501-2025&limit=1000'))
  all_games = NULL
  dts=2023
  for(dts in 2008:2025){
    print(dts)
    strt = paste0(dts,'0601')
    end = paste0(dts+1,'0501')
    gms = GET(paste0('https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates=',strt,'-',end,'&limit=1000'))
    gms = content(gms)
    


    for(ev in gms$events){
      print(ev$shortName)
      

      if(!is.null(ev$competitions[[1]]$competitors[[1]]$winner)&ev$shortName!="HOU VS CHI"){
      
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
    
  }

  upd_df = all_games %>% distinct() %>% arrange(date_time)
  aws.s3::s3saveRDS(upd_df,bucket = s3_bucket, object = 'admin/espn_api_game_detail.rds')

  
  espn_game_details %>% filter(team_1_abbreviation=='WSH',season == 2019)
  espn_game_details = s3readRDS(bucket = s3_bucket, object = 'admin/espn_api_game_detail.rds')
  rn=1389
  for(rn in 5339:nrow(espn_game_details)){
    Sys.sleep(1)
    print(paste0('Row Number: ',rn))
    
    game_summary = as.list(espn_game_details[rn,])

    
    gm_unq_id = game_summary$unique_id
    gm_espn_id = game_summary$espn_id
    print(gm_unq_id)
    week_text = espn_game_details$season_week[rn]
    week_text = ifelse(week_text<10,paste0('0',week_text),week_text)
    
    gm_summary = GET(paste0('https://site.web.api.espn.com/apis/site/v2/sports/football/nfl/summary?event=',gm_espn_id))
    gm_summary = content(gm_summary)  
    path = paste('nfl_espn_data',paste0('season_',espn_game_details$season[rn]),espn_game_details$season_name[rn],
                 paste0('week_',week_text),gm_unq_id,sep='/')
    
    s3write_using(
      FUN = function(obj, file) {
        write_json(obj, path = file, pretty = TRUE, auto_unbox = TRUE)
      },
      x = game_summary,
      bucket = s3_bucket,
      object = paste0(path,'/inputs/',gm_unq_id,'_game_summary.json')
    )
    
    ## Get Team Statistics from the game
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
    
    # all_injuries = NULL
    # print('Checking Injuries')
    # for(inj in gm_summary$injuries){
    #   
    #   team = inj$team$abbreviation
    #   team_id = inj$team$id
    #   tm_injury = bind_rows(lapply(inj$injuries,function(x)
    #     tibble(espn_id = gm_espn_id,
    #            unique_id = gm_unq_id,
    #            team_abbreviation = team,
    #            team_id = team_id,
    #            status = x$status,
    #            athlete_name = x$athlete$displayName,
    #            athlete_first = x$athlete$firstName,
    #            athlete_last = x$athlete$lastName,
    #            athlete_short = x$athlete$shortName,
    #            injury_id = x$type$id,
    #            injury_name = x$type$name,
    #            injury_description = x$type$description,
    #            injury_abbreviation = x$type$abbreviation,
    #            
    #     )
    #   )
    #   )
    #   all_injuries = bind_rows(all_injuries,tm_injury)
    # }
    # 
    # nrow(all_injuries)
    # s3_csv_save(all_injuries,path=paste0(path,'/inputs/',gm_unq_id,'_injury_report.csv'))
    
    
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
    # print('Get play by play data')
    # pbp = GET(paste0('https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/',gm_espn_id,
    #                  '/competitions/',gm_espn_id,'/plays?limit=500'))
    # pbp = content(pbp)
    
    
    
    
    # nrow(pbp_all)
    
    
    
    
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






library(rvest)






pbp = GET('https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401773017/competitions/401773017/plays?limit=500')
pbp = content(pbp)

pbp_all= NULL
for(ply in pbp$items){
  
  
  ply_tmp = tibble(
    espn_id = gm_espn_id,
    unique_id = gm_unq_id,
    yardage = ply$statYardage,
    quarter = ply$period$number,
    time_remaining = ply$clock$displayValue,
    down = ply$start$down,
    distance = ply$start$distance,
    yardline = ply$start$yardLine,
    yards_to_endzone = ply$start$yardsToEndzone,
    possession = ply$start$possessionText,
    play_text = ply$text
  )
  pbp_all = bind_rows(pbp_all,ply_tmp)
}


df <- gm_summary$boxscore$teams[[1]]$statistics |>
  map(~as.data.frame(.x)) |>
  mutate(across(any_of("value"), as.character)) |>
  bind_rows(.id = "stat_idx")


pbp_summary = GET('https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/401249063/competitions/401249063/plays?limit=500')
pbp_summary = content(pbp_summary)  


