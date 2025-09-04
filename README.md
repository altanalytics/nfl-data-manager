# NFL Data Manager

A comprehensive data pipeline for collecting, processing, and storing NFL fantasy and statistical data. This system provides the data infrastructure that powers the NFL Super Fantasy League and GenAI applications.

## Overview

This project handles all aspects of NFL data management, from real-time fantasy scoring during games to comprehensive statistical analysis. It pulls data from multiple sources, normalizes it, and stores it in a queryable format for downstream applications.

## Features

- **Real-time Data Collection**: Pulls ESPN fantasy data every 5 minutes during games
- **Batch Processing**: Nightly jobs for comprehensive NFL statistics
- **Multi-source Integration**: ESPN fantasy data, NFL APIs, and custom calculations
- **Scalable Storage**: S3-based data lake with Athena querying capabilities
- **Containerized Deployment**: Docker containers on AWS Lambda for reliable execution

## Tech Stack

- **Language**: R for data processing and API integration
- **Compute**: AWS Lambda with Docker containers (ECR)
- **Scheduling**: EventBridge for automated job execution
- **Storage**: S3 for data lake storage
- **Processing**: AWS Glue for data transformation
- **Querying**: Amazon Athena for SQL-based analysis

## Data Sources

1. **ESPN Fantasy APIs**: Real-time scoring, roster data, matchup information
2. **NFL Statistical APIs**: Player stats, team performance, game results
3. **Custom Calculations**: Super league scoring, cross-league matchups

## Architecture

### Real-time Pipeline
```
ESPN APIs → Lambda (R Scripts) → S3 → Super Fantasy App
```

### Batch Pipeline  
```
NFL APIs → Lambda (R Scripts) → S3 → Glue → Athena → GenAI Tool
```

## Data Processing

### Real-time (Every 5 minutes during games)
- Fantasy roster updates
- Live scoring calculations
- Matchup status tracking
- Super league standings

### Batch (Nightly)
- Player statistics aggregation
- Team performance metrics
- Historical data updates
- Data quality validation

## Storage Structure

Data is organized in S3 with the following structure:
- `/fantasy/live/` - Real-time fantasy data
- `/fantasy/historical/` - Season and historical fantasy data
- `/nfl/stats/` - Comprehensive NFL statistics
- `/processed/` - Cleaned and normalized datasets

## Deployment

The system uses AWS CDK (TypeScript) for infrastructure as code:

### Prerequisites
- AWS Account with appropriate permissions
- Docker installed for container builds
- R environment for local testing

### Commands

```bash
# Build and deploy infrastructure
npm run build
npx cdk deploy

# Test locally
npm run test

# Monitor deployments
npx cdk diff
```

## Monitoring

- **CloudWatch Logs**: All Lambda executions logged
- **EventBridge Rules**: Scheduled job monitoring  
- **S3 Metrics**: Data volume and access patterns
- **Athena Query Performance**: Query execution monitoring

## Data Quality

The system includes several data quality measures:
- **Validation Rules**: Check data completeness and accuracy
- **Error Handling**: Retry logic for failed API calls
- **Data Lineage**: Track data source and processing steps
- **Alerting**: Notifications for processing failures

This data pipeline provides reliable, scalable NFL data infrastructure that supports both real-time fantasy applications and comprehensive analytical tools.
