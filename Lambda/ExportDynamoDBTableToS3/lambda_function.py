import boto3
import json
import csv
import io

def lambda_handler(event, context):
    dynamodb_table = 'CustomerReviewsAnalysis'
    s3_bucket = 'ecommerce-customer-reviews-rawdata'
    json_key = 'dynamodb-export/CustomerReviewsAnalysis.json'
    csv_key = 'dynamodb-export/CustomerReviewsAnalysis.csv'

    # Initializing AWS clients
    dynamodb = boto3.resource('dynamodb')
    s3 = boto3.client('s3')
    table = dynamodb.Table(dynamodb_table)

    # Exporting from DynamoDB to JSON 
    response = table.scan()
    data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    json_body = json.dumps(data)
    s3.put_object(Bucket=s3_bucket, Key=json_key, Body=json_body, ContentType='application/json')
    print(f"Exported {len(data)} items to s3://{s3_bucket}/{json_key}")

    # Converting JSON to CSV in the S3 Bucket
    fieldnames = sorted({k for item in data for k in item.keys()})
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(data)

    s3.put_object(Bucket=s3_bucket, Key=csv_key, Body=csv_buffer.getvalue(), ContentType='text/csv')
    print(f"Uploaded CSV to s3://{s3_bucket}/{csv_key}")

    return {
        'statusCode': 200,
        'body': json.dumps(f"Exported to JSON and CSV in S3 successfully.")
    }
