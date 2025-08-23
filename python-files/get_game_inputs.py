import pandas as pd
import boto3
import json
from typing import Dict, Any


def get_game_inputs(unique_game_id: str) -> Dict[str, Any]:
    """
    Pull all input files for a specific game from S3 and return as JSON objects
    
    Args:
        unique_game_id: Unique game identifier (e.g., "2024_2_18_DAL_WSH")
        
    Returns:
        Dictionary where keys are filenames and values are the file contents as JSON/data
    """
    # Initialize AWS session with nfl profile
    session = boto3.Session(profile_name='nfl')
    s3_client = session.client('s3')
    s3_bucket = "alt-nfl-bucket"
    
    # Parse the unique_game_id to construct S3 path
    # Format: YYYY_T_WW_TEAM1_TEAM2
    parts = unique_game_id.split('_')
    if len(parts) < 4:
        return {"error": f"Invalid unique_game_id format: {unique_game_id}"}
    
    season = parts[0]  # e.g., "2024"
    season_type_code = parts[1]  # e.g., "2"
    week = parts[2]  # e.g., "18"
    
    # Map season type code to folder name
    season_type_map = {
        "1": "preseason",
        "2": "regular-season", 
        "3": "post-season"
    }
    
    season_type = season_type_map.get(season_type_code)
    if not season_type:
        return {"error": f"Invalid season type code: {season_type_code}"}
    
    # Format week with leading zero if needed
    week_formatted = f"week_{week.zfill(2)}"
    
    # Construct S3 path
    s3_path = f"nfl_espn_data/season_{season}/{season_type}/{week_formatted}/{unique_game_id}/"
    
    try:
        # List all objects in the folder
        response = s3_client.list_objects_v2(
            Bucket=s3_bucket,
            Prefix=s3_path
        )
        
        if 'Contents' not in response:
            return {"error": f"No files found in path: s3://{s3_bucket}/{s3_path}"}
        
        # Dictionary to store all file contents
        game_inputs = {}
        
        # Process each file
        for obj in response['Contents']:
            file_key = obj['Key']
            filename = file_key.split('/')[-1]  # Get just the filename
            
            # Skip if it's just the folder itself
            if filename == '':
                continue
            
            try:
                # Get file content
                file_response = s3_client.get_object(Bucket=s3_bucket, Key=file_key)
                file_content = file_response['Body'].read().decode('utf-8')
                
                # Try to parse as JSON first
                try:
                    game_inputs[filename] = json.loads(file_content)
                except json.JSONDecodeError:
                    # If not JSON, store as string
                    game_inputs[filename] = file_content
                    
            except Exception as e:
                game_inputs[filename] = {"error": f"Failed to read file: {str(e)}"}
        
        # Add metadata about the path
        game_inputs["_metadata"] = {
            "s3_path": f"s3://{s3_bucket}/{s3_path}",
            "unique_game_id": unique_game_id,
            "season": season,
            "season_type": season_type,
            "week": week,
            "files_found": len([k for k in game_inputs.keys() if k != "_metadata"])
        }
        
        return game_inputs
        
    except Exception as e:
        return {"error": f"Failed to access S3 path {s3_path}: {str(e)}"}


def main():
    """Example usage"""
    # Test with the example game
    game_id = "2024_2_18_DAL_WSH"
    
    print(f"=== GETTING INPUT FILES FOR GAME: {game_id} ===\n")
    
    inputs = get_game_inputs(game_id)
    inputs['2024_2_18_DAL_WSH_game_stats.csv']
    if "error" in inputs:
        print(f"Error: {inputs['error']}")
        return
    
    # Print metadata
    if "_metadata" in inputs:
        metadata = inputs["_metadata"]
        print("METADATA:")
        print(f"  S3 Path: {metadata['s3_path']}")
        print(f"  Season: {metadata['season']}")
        print(f"  Season Type: {metadata['season_type']}")
        print(f"  Week: {metadata['week']}")
        print(f"  Files Found: {metadata['files_found']}")
        print()
    
    # List all files found
    files = [k for k in inputs.keys() if k != "_metadata"]
    print("FILES FOUND:")
    for filename in files:
        file_data = inputs[filename]
        if isinstance(file_data, dict) and "error" not in file_data:
            print(f"  {filename}: JSON object with {len(file_data)} keys")
        elif isinstance(file_data, str):
            print(f"  {filename}: Text file ({len(file_data)} characters)")
        else:
            print(f"  {filename}: {type(file_data)}")
    
    # Show sample of first file if available
    if files:
        first_file = files[0]
        print(f"\nSAMPLE FROM {first_file}:")
        sample_data = inputs[first_file]
        if isinstance(sample_data, dict):
            # Show first few keys if it's a dict
            sample_keys = list(sample_data.keys())[:5]
            for key in sample_keys:
                print(f"  {key}: {type(sample_data[key])}")
            if len(sample_data) > 5:
                print(f"  ... and {len(sample_data) - 5} more keys")
        elif isinstance(sample_data, str):
            # Show first 200 characters if it's a string
            print(f"  {sample_data[:200]}...")


if __name__ == "__main__":
    main()
