import json, boto3, os, re, base64, time
from boto3.dynamodb.conditions import Key

def lambda_handler(event, context):
    print(type(event))
    try:
        def get_running_jobs_count():
            dyn_resource = boto3.resource('dynamodb')
            table = dyn_resource.Table(os.environ['TABLE_NAME'])
            response = table.scan(FilterExpression=boto3.dynamodb.conditions.Attr('jobStatus').eq('RUNNING'))
            return response['Count']
        
        def createSnapshot(awsAccountId, dashboardId,sheetId,visualId,parameters, outputFormat,outputDestination, from_queue=False):
            #Create QuickSight client
            print("preparing for createSnapshot")
            ts = str(time.time()).replace(".","_")
            snapshotJobId = outputFormat + "_" + ts 
            dashboardRegion = os.environ['DashboardRegion']
            quickSight = boto3.client('quicksight', region_name=dashboardRegion);
            
            # Check queue limit if not from polling
            if not from_queue:
                max_concurrent_jobs = int(os.environ.get('MAX_CONCURRENT_JOBS', '5'))
                running_count = get_running_jobs_count()
                
                if running_count >= max_concurrent_jobs:
                    # Queue the job
                    dyn_resource = boto3.resource('dynamodb')
                    table = dyn_resource.Table(os.environ['TABLE_NAME'])
                    table.put_item(Item={
                        'dashboardId': dashboardId,
                        'jobId': snapshotJobId,
                        'jobStatus': 'QUEUED',
                        'sheetId': sheetId,
                        'visualId': visualId,
                        'parameters': parameters,
                        'outputFormat': outputFormat,
                        'outputDestination': outputDestination,
                        'timestamp': int(time.time())
                    })
                    return {'SnapshotJobId': snapshotJobId, 'Status': 'QUEUED'}
            
            print("Inside snapshot creation")
            snapShortConfigurationTemplate_PDF = {
                                                "FileGroups": [
                                                  {
                                                    "Files": [
                                                      {
                                                        "SheetSelections": [
                                                          {
                                                            "SheetId": sheetId,
                                                            "SelectionScope": "ALL_VISUALS" if outputFormat =='PDF' else "SELECTED_VISUALS"  ,
                                                            
                                                          }
                                                        ],
                                                        "FormatType": outputFormat
                                                      }
                                                    ]
                                                  }
                                                ],
                                                "DestinationConfiguration": {
                                                  "S3Destinations": [
                                                    {
                                                      "BucketConfiguration": outputDestination
                                                    }
                                                  ]
                                                },
                                               
                                            }
            snapShortConfigurationTemplate_CSV = {
                                                "FileGroups": [
                                                  {
                                                    "Files": [
                                                      {
                                                        "SheetSelections": [
                                                          {
                                                            "SheetId": sheetId,
                                                            "SelectionScope": "ALL_VISUALS" if outputFormat =='PDF' else "SELECTED_VISUALS"  ,
                                                            "VisualIds": visualId
                                                          }
                                                        ],
                                                        "FormatType": outputFormat
                                                      }
                                                    ]
                                                  }
                                                ],
                                                "DestinationConfiguration": {
                                                  "S3Destinations": [
                                                    {
                                                      "BucketConfiguration": outputDestination
                                                    }
                                                  ]
                                                }
                                            }
            snapShortConfigurationTemplate = snapShortConfigurationTemplate_PDF if outputFormat =='PDF' else snapShortConfigurationTemplate_CSV
            print("cheking parameters")
            if not event["body"]["parameters"] is None:
              snapShortConfigurationTemplate.update({"Parameters": event["body"]["parameters"]})
            print("Updated snapShotConfiguration")
            print(snapShortConfigurationTemplate)
            response = quickSight.start_dashboard_snapshot_job(
                     AwsAccountId = awsAccountId,
                     DashboardId = dashboardId,
                     SnapshotJobId = snapshotJobId,
                     SnapshotConfiguration = snapShortConfigurationTemplate,
                     UserConfiguration = {
                                            "AnonymousUsers": [
                                              {
                                                "RowLevelPermissionTags": [
                                                  {
                                                    "Key": "Tag1",
                                                    "Value": "123"
                                                  }
                                                ]
                                              }
                                            ]
                                          }
                 )
            dyn_resource = boto3.resource('dynamodb')
            table = dyn_resource.Table(os.environ['TABLE_NAME'])
            table.put_item(Item={
                'dashboardId': dashboardId,
                'jobId': response["SnapshotJobId"],
                'jobStatus': "RUNNING",
                'timestamp': int(time.time()),
                'sheetId': sheetId,
                'visualId': visualId,
                'outputFormat': outputFormat,
                'parameters': parameters,
                'outputDestination': outputDestination
            })
            return response
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
          
            
        def listSnapshots ():
          print("inside listSnapshots")
          dyn_resource = boto3.resource('dynamodb')
          table = dyn_resource.Table(os.environ['TABLE_NAME'])
          query_params = event.get("queryStringParameters") or {}
          dashboardId = query_params.get("dashboardId")
          next_token = query_params.get("nextToken")
          
          if dashboardId:
              # Query by dashboardId
              scan_kwargs = {'KeyConditionExpression': Key('dashboardId').eq(dashboardId)}
              if next_token:
                  scan_kwargs['ExclusiveStartKey'] = json.loads(base64.b64decode(next_token).decode())
              response = table.query(**scan_kwargs)
          else:
              # Scan all items
              scan_kwargs = {}
              if next_token:
                  scan_kwargs['ExclusiveStartKey'] = json.loads(base64.b64decode(next_token).decode())
              response = table.scan(**scan_kwargs)
          
          # Add nextToken to response if more items exist
          if 'LastEvaluatedKey' in response:
              response['NextToken'] = base64.b64encode(json.dumps(response['LastEvaluatedKey']).encode()).decode()
              del response['LastEvaluatedKey']
          
          return response
        
        def create_presigned_url(bucket_name, object_name, expiration=3600):
          """Generate a presigned URL to share an S3 object
      
          :param bucket_name: string
          :param object_name: string
          :param expiration: Time in seconds for the presigned URL to remain valid
          :return: Presigned URL as string. If error, returns None.
          """
      
          # Generate a presigned URL for the S3 object
          s3_client = boto3.client('s3')
          try:
              response = s3_client.generate_presigned_url('get_object',
                                                          Params={'Bucket': bucket_name,
                                                                  'Key': object_name},
                                                          ExpiresIn=expiration)
          except ClientError as e:
              logging.error(e)
              return None
      
          # The response contains the presigned URL
          return response
        
        def downloadOutput():
          awsAccountId = context.invoked_function_arn.split(':')[4]
          dashboardId = event['pathParameters']['dashboardId']
          jobId = event['pathParameters']['jobId']
          
          dyn_resource = boto3.resource('dynamodb')
          table = dyn_resource.Table(os.environ['TABLE_NAME'])
          key_condition_expression = \
          Key('dashboardId').eq(dashboardId)  & Key("jobId").eq(jobId)
  
          response=table.query(KeyConditionExpression=key_condition_expression)
          
          if not response["Items"]:
              return {'error': 'Job not found'}
              
          outputLocation = response["Items"][0].get("outputLocation")
          if not outputLocation:
              return {'error': 'Output location not available'}
              
          print(f"Output location: {outputLocation}")
          
          # Parse S3 URI: s3://bucket-name/path/to/file
          if not outputLocation.startswith('s3://'):
              return {'error': 'Invalid S3 URI format'}
              
          s3_parts = outputLocation[5:].split('/', 1)  # Remove 's3://' and split
          bucket_name = s3_parts[0]
          object_name = s3_parts[1] if len(s3_parts) > 1 else ''
          
          print(f"Bucket: {bucket_name}, Object: {object_name}")
          
          r_result = {}
          r_result["presignedUrl"] = create_presigned_url(bucket_name, object_name)
          r_result["Result"] = None
          r_result["Arn"] = None
          
          return r_result
          
        def deleteSnapshot():
          dashboardId = event['pathParameters']['dashboardId']
          jobId = event['pathParameters']['jobId']
          
          dyn_resource = boto3.resource('dynamodb')
          table = dyn_resource.Table(os.environ['TABLE_NAME'])
          
          # Check current status
          key_condition_expression = Key('dashboardId').eq(dashboardId) & Key('jobId').eq(jobId)
          response = table.query(KeyConditionExpression=key_condition_expression)
          
          if not response['Items']:
              return {'error': 'Snapshot job not found'}
              
          job_status = response['Items'][0]['jobStatus']
          if job_status not in ['FAILED', 'COMPLETED', 'QUEUED']:
              return {'error': f'Cannot delete job with status: {job_status}. Only FAILED, COMPLETED, or QUEUED jobs can be deleted.'}
          
          # Delete the record
          table.delete_item(Key={'dashboardId': dashboardId, 'jobId': jobId})
          return {'message': 'Snapshot job deleted successfully'}
          
        #Get AWS Account Id
        awsAccountId = context.invoked_function_arn.split(':')[4]
        
        # Determine operation based on HTTP method and path
        http_method = event['httpMethod']
        path = event['resource']
        
        print(f"HTTP Method: {http_method}, Path: {path}")
        
        if http_method == 'POST' and path == '/qs-anonymous-snapshot':
            # Create snapshot
            event["body"] = json.loads(event["body"])
            dashboardId = event["body"]["dashboardId"]
            sheetId = event["body"]["sheetId"]
            visualId = event["body"].get("visualId")
            outputFormat = event["body"]["format"]
            parameters = event["body"]["parameters"]
            outputDestination = event["body"]["bucketConfiguration"]
            outputDestination["BucketName"] = os.environ['BUCKET_NAME']
            outputDestination["BucketRegion"] = os.environ['BUCKET_REGION']
            response = createSnapshot(awsAccountId, dashboardId, sheetId, visualId, parameters, outputFormat, outputDestination)
            
        elif http_method == 'GET' and path == '/qs-anonymous-snapshot':
            # List snapshots
            response = listSnapshots()
            
        elif http_method == 'GET' and path == '/qs-anonymous-snapshot/describe_job/{dashboardId}/{jobId}':
            # Describe snapshot job
            dashboardId = event['pathParameters']['dashboardId']
            jobId = event['pathParameters']['jobId']
            response = describeSnapshotJob(awsAccountId, dashboardId, jobId)
            
        elif http_method == 'GET' and path == '/qs-anonymous-snapshot/describe_job_result/{dashboardId}/{jobId}':
            # Describe snapshot job result
            dashboardId = event['pathParameters']['dashboardId']
            jobId = event['pathParameters']['jobId']
            response = describeSnapshotJobResult(awsAccountId, dashboardId, jobId)
            
        elif http_method == 'GET' and path == '/qs-anonymous-snapshot/download_output/{dashboardId}/{jobId}':
            # Download output
            response = downloadOutput()
            
        elif http_method == 'DELETE' and path == '/qs-anonymous-snapshot/{dashboardId}/{jobId}':
            # Delete snapshot
            response = deleteSnapshot()
            
        else:
            response = {'error': 'Unsupported operation'}

        return {'statusCode':200,
                'headers': {"Access-Control-Allow-Origin": "*",
                            "Content-Type":"text/plain"},
                'body':json.dumps(response,indent=4, sort_keys=True, default=str)
                } 
    except Exception as e: #catch all
        print(e)
        return {'statusCode':400,
                'headers': {"Access-Control-Allow-Origin": "*",
                            "Content-Type":"text/plain"},
                'body':json.dumps('Error: ' + str(e))
                }     