# ccaas-removeInactiveUsers

This AWS Lambda function identifies and logs inactive users in the Amazon Connect contact center environment.  
Deletion functionality is currently disabled — it only performs logging to gather real-world data.

## Usage

- **Runtime**: Python 3.x
- **Trigger**: Scheduled CloudWatch Event (for simulation only)
- **Output**: CVS logs of deletion user list save to S3, Logs to CloudWatch

## Notes

- Deletion logic is disabled temporarily.
- Deletion Condition:
1. Mark Amazon Connect users for deletion if not in Cognito and account is older than 6 months
2. Mark Connect users with NHT profile as training accounts for deletion if inactive for 1+ month and account is 2+ months old
3. Retrieves a dictionary of Amazon Connect users who have not logged into Cognito for the past 6 months (180 days). Only includes users who exist in both lists.
