###########################
# File: migrate_historical_data.R
# Description: Migrate historical NFL data from legacy format to database format
# Date: 8/20/2025
# Author: Anthony Trevisan
# Notes: Run this to convert existing CSV/JSON files to partitioned Parquet format
###########################

migrate_historical_data <- function(s3_bucket, dry_run = TRUE) {
  
  library(aws.s3)
  library(arrow)
  library(dplyr)
  library(jsonlite)
  
  # Get list of all objects in the legacy nfl_espn_data folder
  legacy_objects <- get_bucket_df(bucket = s3_bucket, prefix = "nfl_espn_data/", max = Inf)
  
  if(nrow(legacy_objects) == 0) {
    print("No legacy data found")
    return()
  }
  
  # Parse the legacy file structure
  legacy_files <- legacy_objects %>%
    filter(grepl("\\.(csv|json)$", Key)) %>%
    mutate(
      # Extract path components
      path_parts = strsplit(Key, "/"),
      season = sapply(path_parts, function(x) gsub("season_", "", x[2])),
      season_type = sapply(path_parts, function(x) x[3]),
      week = sapply(path_parts, function(x) gsub("week_", "", x[4])),
      game_id = sapply(path_parts, function(x) x[5]),
      file_type = sapply(path_parts, function(x) {
        filename <- x[length(x)]
        if(grepl("_game_stats\\.csv$", filename)) return("team_stats")
        if(grepl("_player_stats\\.csv$", filename)) return("player_stats")
        if(grepl("_drive_report\\.csv$", filename)) return("drive_report")
        if(grepl("_play_by_play\\.csv$", filename)) return("play_by_play")
        if(grepl("_game_recap\\.json$", filename)) return("game_recap")
        if(grepl("_game_summary\\.json$", filename)) return("game_summary")
        return("other")
      })
    ) %>%
    filter(file_type %in% c("team_stats", "player_stats", "drive_report", "play_by_play", "game_recap")) %>%
    arrange(season, season_type, week, game_id, file_type)
  
  print(paste("Found", nrow(legacy_files), "files to migrate"))
  print(table(legacy_files$file_type))
  
  if(dry_run) {
    print("DRY RUN - No files will be migrated. Set dry_run = FALSE to execute.")
    return(legacy_files)
  }
  
  # Process each file
  migration_log <- data.frame()
  
  for(i in 1:nrow(legacy_files)) {
    
    file_info <- legacy_files[i, ]
    
    tryCatch({
      
      print(paste("Processing", i, "of", nrow(legacy_files), ":", file_info$Key))
      
      # Create new database path
      new_path <- paste0(
        "nfl_espn_database/", file_info$file_type, "/",
        "nfl_season=", file_info$season, "/",
        "nfl_season_type=", file_info$season_type, "/", 
        "nfl_week=", file_info$week, "/",
        file_info$game_id, "_", file_info$file_type, ".parquet"
      )
      
      # Read the legacy file
      if(file_info$file_type == "game_recap") {
        # Handle JSON files
        json_content <- s3read_using(
          FUN = function(x) fromJSON(x, flatten = TRUE),
          bucket = s3_bucket,
          object = file_info$Key
        )
        data_to_save <- as.data.frame(json_content)
      } else {
        # Handle CSV files
        data_to_save <- s3read_using(
          FUN = read.csv,
          bucket = s3_bucket,
          object = file_info$Key
        )
      }
      
      # Save as parquet in new location
      s3write_using(
        x = data_to_save,
        FUN = function(x, file) write_parquet(x, file, compression = "snappy"),
        object = new_path,
        bucket = s3_bucket
      )
      
      # Log successful migration
      migration_log <- rbind(migration_log, data.frame(
        original_path = file_info$Key,
        new_path = new_path,
        status = "SUCCESS",
        rows = nrow(data_to_save),
        timestamp = Sys.time()
      ))
      
      print(paste("✓ Migrated:", file_info$Key, "->", new_path))
      
    }, error = function(e) {
      
      # Log failed migration
      migration_log <<- rbind(migration_log, data.frame(
        original_path = file_info$Key,
        new_path = NA,
        status = paste("ERROR:", e$message),
        rows = NA,
        timestamp = Sys.time()
      ))
      
      print(paste("✗ Failed:", file_info$Key, "-", e$message))
    })
    
    # Small delay to avoid rate limiting
    Sys.sleep(0.1)
  }
  
  # Save migration log
  s3write_using(
    x = migration_log,
    FUN = function(x, file) write.csv(x, file, row.names = FALSE),
    object = paste0("admin/migration_log_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv"),
    bucket = s3_bucket
  )
  
  print("Migration completed!")
  print(paste("Successfully migrated:", sum(migration_log$status == "SUCCESS"), "files"))
  print(paste("Failed migrations:", sum(grepl("ERROR", migration_log$status)), "files"))
  
  return(migration_log)
}

# Example usage:
# First run with dry_run = TRUE to see what would be migrated
# migration_preview <- migrate_historical_data(s3_bucket = "your-bucket-name", dry_run = TRUE)

# Then run the actual migration
# migration_results <- migrate_historical_data(s3_bucket = "your-bucket-name", dry_run = FALSE)
