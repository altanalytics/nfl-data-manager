import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as ecr_assets from 'aws-cdk-lib/aws-ecr-assets';

export class NflDataManagerStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

     // IAM policy for Lambda function
    const lambdaPolicy = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: ['*'],
      actions: [
        "s3:*", 
        "lambda:*"
      ],
    });

    // R-based Lambda function using Docker
    const nflRLambda = new lambda.DockerImageFunction(this, 'NFL_R_DataGrab', {
      functionName: 'NFL_R_DataGrab',
      initialPolicy: [lambdaPolicy],
      retryAttempts: 0,
      architecture: lambda.Architecture.ARM_64,
      code: lambda.DockerImageCode.fromImageAsset('./r-data-pull', {
        platform: ecr_assets.Platform.LINUX_ARM64,
      }),
      memorySize: 2048,
      timeout: cdk.Duration.seconds(590),
      environment: {
        // Add environment variables as needed
      },
    });

    // Function to create event rules
    const createRule = (ruleName: string, schedule: events.Schedule, message: any) => {
      const rule = new events.Rule(this, ruleName, { 
        schedule,
        description: `EventBridge rule for ${ruleName}`
      });
      rule.addTarget(new targets.LambdaFunction(nflRLambda, {
        event: events.RuleTargetInput.fromObject(message),
      }));
    };

    // Define schedules and messages for rules
    const schedules = [
      { 
        name: 'daily_data', 
        cron: events.Schedule.cron({ 
          minute: '30', 
          hour: '12', 
          month: '1,2,8,9,10,11,12', 
          weekDay: '*', 
          year: '*' 
        }), 
        msg: { lambda_input: { msg: 'get_espn' } } 
      },
      // Fantasy Ends by December
      { 
        name: 'fantasy_data_night', 
        cron: events.Schedule.cron({ 
          minute: '0/10', 
          hour: '0-5', 
          month: '9-12', 
          weekDay: 'MON,THUR,SUN', 
          year: '*' 
        }), 
        msg: { lambda_input: { msg: 'get_fantasy_night' } } 
      },
      { 
        name: 'fantasy_data_day', 
        cron: events.Schedule.cron({ 
          minute: '0/10', 
          hour: '17-23', 
          month: '9-12', 
          weekDay: 'SUN', 
          year: '*' 
        }), 
        msg: { lambda_input: { msg: 'get_fantasy_day' } } 
      },
    ];

    // Create rules
    schedules.forEach(({ name, cron, msg }) => createRule(name, cron, msg));

    // Output the Lambda function ARN
    new cdk.CfnOutput(this, 'NFLRLambdaArn', {
      value: nflRLambda.functionArn,
      description: 'ARN of the NFL R Lambda function',
    });
  }
}
