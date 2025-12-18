import json, boto3, os, re, base64, time
from boto3.dynamodb.conditions import Key
import boto3.dynamodb.conditions

def lambda_handler(event, context):
    print(type(event))
    try:
        def describeSnapshotJob(awsAccountId, dashboardId,snapshotJobId):
            #Create QuickSight client
            
            dashboardRegion = os.environ['DashboardRegion']
            quickSight = boto3.client('quicksight', region_name=dashboardRegion);
           
            
            response = quickSight.describe_dashboard_snapshot_job(
                     AwsAccountId = awsAccountId,
                     DashboardId = dashboardId,
                     SnapshotJobId = snapshotJobId
                     
                 )
                
            return response
            
        def describeSnapshotJobResult(awsAccountId, dashboardId,snapshotJobId):
            #Create QuickSight client
            
            dashboardRegion = os.environ['DashboardRegion']
            quickSight = boto3.client('quicksight', region_name=dashboardRegion);
            response = quickSight.describe_dashboard_snapshot_job_result(
                     AwsAccountId = awsAccountId,
                     DashboardId = dashboardId,
                     SnapshotJobId = snapshotJobId
                     
                 )
                
            return response
            
        def get_running_jobs_count():
            dyn_resource = boto3.resource('dynamodb')
            table = dyn_resource.Table(os.environ['TABLE_NAME'])
            response = table.scan(FilterExpression=boto3.dynamodb.conditions.Attr('jobStatus').eq('RUNNING'))
            return response['Count']
        
        def start_queued_job(job_item, awsAccountId):
            dashboardRegion = os.environ['DashboardRegion']
            quickSight = boto3.client('quicksight', region_name=dashboardRegion)
            
            # Build snapshot configuration
            outputFormat = job_item['outputFormat']
            sheetId = job_item['sheetId']
            visualId = job_item.get('visualId')
            outputDestination = job_item['outputDestination']
            
            snapShortConfigurationTemplate_PDF = {
                "FileGroups": [{
                    "Files": [{
                        "SheetSelections": [{
                            "SheetId": sheetId,
                            "SelectionScope": "ALL_VISUALS" if outputFormat == 'PDF' else "SELECTED_VISUALS"
                        }],
                        "FormatType": outputFormat
                    }]
                }],
                "DestinationConfiguration": {
                    "S3Destinations": [{"BucketConfiguration": outputDestination}]
                }
            }
            
            snapShortConfigurationTemplate_CSV = {
                "FileGroups": [{
                    "Files": [{
                        "SheetSelections": [{
                            "SheetId": sheetId,
                            "SelectionScope": "ALL_VISUALS" if outputFormat == 'PDF' else "SELECTED_VISUALS",
                            "VisualIds": visualId
                        }],
                        "FormatType": outputFormat
                    }]
                }],
                "DestinationConfiguration": {
                    "S3Destinations": [{"BucketConfiguration": outputDestination}]
                }
            }
            
            snapShortConfigurationTemplate = snapShortConfigurationTemplate_PDF if outputFormat == 'PDF' else snapShortConfigurationTemplate_CSV
            
            if job_item.get('parameters'):
                snapShortConfigurationTemplate.update({"Parameters": job_item['parameters']})
            
            # Start the job
            response = quickSight.start_dashboard_snapshot_job(
                AwsAccountId=awsAccountId,
                DashboardId=job_item['dashboardId'],
                SnapshotJobId=job_item['jobId'],
                SnapshotConfiguration=snapShortConfigurationTemplate,
                UserConfiguration={
                    "AnonymousUsers": [{
                        "RowLevelPermissionTags": [{"Key": "Tag1", "Value": "123"}]
                    }]
                }
            )
            
            # Update job status to SUBMITTED
            dyn_resource = boto3.resource('dynamodb')
            table = dyn_resource.Table(os.environ['TABLE_NAME'])
            table.update_item(
                Key={'dashboardId': job_item['dashboardId'], 'jobId': job_item['jobId']},
                UpdateExpression="set jobStatus=:r",
                ExpressionAttributeValues={':r': 'SUBMITTED'}
            )
            
            return response
        
        def process_queued_jobs():
            awsAccountId = context.invoked_function_arn.split(':')[4]
            max_concurrent_jobs = int(os.environ.get('MAX_CONCURRENT_JOBS', '5'))
            
            while get_running_jobs_count() < max_concurrent_jobs:
                # Get oldest queued job
                dyn_resource = boto3.resource('dynamodb')
                table = dyn_resource.Table(os.environ['TABLE_NAME'])
                response = table.scan(
                    FilterExpression=boto3.dynamodb.conditions.Attr('jobStatus').eq('QUEUED'),
                    Limit=1
                )
                
                if not response['Items']:
                    break  # No queued jobs
                
                queued_job = response['Items'][0]
                try:
                    start_queued_job(queued_job, awsAccountId)
                    print(f"Started queued job: {queued_job['jobId']}")
                except Exception as e:
                    print(f"Failed to start queued job {queued_job['jobId']}: {e}")
                    # Update job status to FAILED
                    table.update_item(
                        Key={'dashboardId': queued_job['dashboardId'], 'jobId': queued_job['jobId']},
                        UpdateExpression="set jobStatus=:r",
                        ExpressionAttributeValues={':r': 'FAILED'}
                    )
        
        def listSnapshots():
            print("inside listSnapshots")
            awsAccountId = context.invoked_function_arn.split(':')[4]
            dyn_resource = boto3.resource('dynamodb')
            table = dyn_resource.Table(os.environ['TABLE_NAME'])
            
            # Process queued jobs first
            process_queued_jobs()
            
            # Update running jobs status
            response = table.scan()
            for job in response["Items"]:
                if job["jobStatus"] in ['SUBMITTED', 'RUNNING']:
                    r = describeSnapshotJob(awsAccountId, job["dashboardId"], job["jobId"])
                    print(r)
                    url = ""
                    completedTime = ""
                    if r["JobStatus"] == 'COMPLETED':
                        r_result = describeSnapshotJobResult(awsAccountId, job["dashboardId"], job["jobId"])
                        print(r_result)
                        url = r_result["Result"]["AnonymousUsers"][0]["FileGroups"][0]["S3Results"][0]["S3Uri"]
                        completedTime = r_result["LastUpdatedTime"]
                    
                    table.update_item(
                        Key={'dashboardId': job["dashboardId"], 'jobId': job["jobId"]},
                        UpdateExpression="set jobStatus=:r, outputLocation= :u, jobParameters = :p, createdTime = :c, completedTime = :f",
                        ExpressionAttributeValues={
                            ':r': r["JobStatus"], 
                            ':u': url, 
                            ':p': json.dumps(r["SnapshotConfiguration"]["Parameters"], indent=4, sort_keys=True, default=str), 
                            ':c': json.dumps(r["CreatedTime"], indent=4, sort_keys=True, default=str), 
                            ':f': json.dumps(completedTime, indent=4, sort_keys=True, default=str)
                        },
                        ReturnValues="UPDATED_NEW"
                    )
            return True

        response = listSnapshots()
        
        
          
        
        
        
        
        return response

    except Exception as e: #catch all
        print(e)
        return {'statusCode':400,
                'headers': {"Access-Control-Allow-Origin": "*",
                            "Content-Type":"text/plain"},
                'body':json.dumps('Error: ' + str(e))
                }     