import pandas as pd
import boto3
from io import StringIO
from typing import Dict, List
from datetime import datetime


def get_game_context(unique_game_id: str, context: int, include_preseason: bool = False) -> Dict[str, pd.DataFrame]:
    """
    Get context for a specific game by returning previous games for each team and their head-to-head history
    
    Args:
        unique_game_id: Unique game identifier (e.g., "2024_2_18_DAL_WSH")
        context: Number of previous games to return for each category
        include_preseason: Whether to include preseason games (default: False)
        
    Returns:
        Dictionary with three DataFrames:
        - 'team1_previous': Previous games for the first team
        - 'team2_previous': Previous games for the second team  
        - 'head_to_head': Previous head-to-head matchups
    """
    # Initialize AWS session with nfl profile
    session = boto3.Session(profile_name='nfl')
    s3_client = session.client('s3')
    s3_bucket = "alt-nfl-bucket"
    
    # Load schedule data from S3
    response = s3_client.get_object(Bucket=s3_bucket, Key="admin/clean_schedule.csv")
    schedule_df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
    schedule_df['date_time'] = pd.to_datetime(schedule_df['date_time'])
    schedule_df = schedule_df.sort_values('date_time', ascending=True)
    
    # Find the target game
    target_game = schedule_df[schedule_df['unique_id'] == unique_game_id]
    if target_game.empty:
        return {
            'team1_previous': pd.DataFrame(),
            'team2_previous': pd.DataFrame(),
            'head_to_head': pd.DataFrame()
        }
    
    target_game = target_game.iloc[0]
    target_date = target_game['date_time']
    team1 = target_game['away_team']  # Away team from matchup
    team2 = target_game['home_team']  # Home team from matchup
    
    # Filter out preseason if not included
    if not include_preseason:
        filtered_df = schedule_df[schedule_df['season_type'] != 1]
    else:
        filtered_df = schedule_df.copy()
    
    # Get games before the target date
    games_before = filtered_df[filtered_df['date_time'] < target_date].sort_values('date_time', ascending=False)
    
    # Get previous games for team1 (away team)
    team1_games = games_before[
        (games_before['home_team'] == team1) | (games_before['away_team'] == team1)
    ].head(context)
    
    # Get previous games for team2 (home team)
    team2_games = games_before[
        (games_before['home_team'] == team2) | (games_before['away_team'] == team2)
    ].head(context)
    
    # Get head-to-head games (excluding games already in team1_games and team2_games)
    h2h_games = games_before[
        ((games_before['home_team'] == team1) & (games_before['away_team'] == team2)) |
        ((games_before['home_team'] == team2) & (games_before['away_team'] == team1))
    ]
    
    # Remove any h2h games that are already in the team-specific results
    team_game_ids = set(team1_games['unique_id'].tolist() + team2_games['unique_id'].tolist())
    h2h_games = h2h_games[~h2h_games['unique_id'].isin(team_game_ids)].head(context)
    
    # Return only requested columns
    columns = ['unique_id', 'espn_id', 'matchup', 'date', 'season', 'season_name', 
              'home_score', 'away_score', 'date_time']
    
    return {
        'team1_previous': team1_games[columns] if not team1_games.empty else pd.DataFrame(),
        'team2_previous': team2_games[columns] if not team2_games.empty else pd.DataFrame(),
        'head_to_head': h2h_games[columns] if not h2h_games.empty else pd.DataFrame()
    }


def main():
    """Example usage"""
    # Test with the example game
    game_id = "2024_2_18_DAL_WSH"
    context_results = get_game_context(game_id, context=2)
    
    print(f"=== CONTEXT FOR GAME: {game_id} ===\n")
    
    print("AWAY TEAM (DAL) PREVIOUS GAMES:")
    print(f"Found {len(context_results['team1_previous'])} games")
    if not context_results['team1_previous'].empty:
        print(context_results['team1_previous'][['date', 'matchup', 'home_score', 'away_score']])
    print()
    
    print("HOME TEAM (WSH) PREVIOUS GAMES:")
    print(f"Found {len(context_results['team2_previous'])} games")
    if not context_results['team2_previous'].empty:
        print(context_results['team2_previous'][['date', 'matchup', 'home_score', 'away_score']])
    print()
    
    print("HEAD-TO-HEAD PREVIOUS GAMES:")
    print(f"Found {len(context_results['head_to_head'])} games")
    if not context_results['head_to_head'].empty:
        print(context_results['head_to_head'][['date', 'matchup', 'home_score', 'away_score']])
    print()
    
    # Test with preseason included
    print("=== WITH PRESEASON INCLUDED ===")
    context_with_preseason = get_game_context(game_id, context=2, include_preseason=True)
    print(f"Away team games: {len(context_with_preseason['team1_previous'])}")
    print(f"Home team games: {len(context_with_preseason['team2_previous'])}")
    print(f"Head-to-head games: {len(context_with_preseason['head_to_head'])}")


if __name__ == "__main__":
    main()
