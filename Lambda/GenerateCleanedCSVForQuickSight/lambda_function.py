import boto3
import csv
import io
import json

def lambda_handler(event, context):
    # Initialize the S3 client to interact with the S3 bucket
    s3 = boto3.client('s3')

    # Define the S3 bucket name and input/output file paths
    bucket = 'ecommerce-customer-reviews-rawdata'
    input_key = 'dynamodb-export/CustomerReviewsAnalysis.csv'
    output_csv_key = 'dynamodb-export/customer-quicksight/CustomerReviewsAnalysis_Cleaned.csv'
    manifest_key = 'dynamodb-export/customer-quicksight/manifest.json'

    # Fetch the original CSV file from S3 and read its content line by line
    response = s3.get_object(Bucket=bucket, Key=input_key)
    csv_lines = response['Body'].read().decode('utf-8').splitlines()
    reader = csv.DictReader(csv_lines)

    # Specify the columns we want to retain for QuickSight analysis
    columns_to_keep = [
        'HelpfulYes', 'ProductID', 'Rating', 'ReviewID', 'ReviewText',
        'ReviewTime', 'ReviewerName', 'Sentiment',
        'SentimentScore_Mixed', 'SentimentScore_Negative', 'SentimentScore_Neutral',
        'SentimentScore_Positive', 'Summary', 'TotalVote', 'UnixReviewTime'
    ]

    # Function to clean unwanted characters from text fields like quotes and commas
    def clean_text(text):
        if not text:
            return ""
        text = text.replace('"', '').replace("'", '')  # remove both double and single quotes
        text = text.replace(", ", '')                  # remove comma followed by space
        text = text.replace(",", " ")                  # replace commas with space to avoid breaking CSV format
        return text

    # Prepare an in-memory CSV buffer to store cleaned data
    cleaned_csv_buffer = io.StringIO()
    writer = csv.DictWriter(cleaned_csv_buffer, fieldnames=columns_to_keep, quoting=csv.QUOTE_NONNUMERIC)
    writer.writeheader()

    # Go through each row and clean only the necessary text fields
    for row in reader:
        cleaned_row = {
            col: clean_text(row.get(col, '')) if col in ['ReviewText', 'ReviewerName', 'Summary'] else row.get(col, '')
            for col in columns_to_keep
        }
        writer.writerow(cleaned_row)

    # Upload the cleaned CSV file to the specified S3 path
    s3.put_object(
        Bucket=bucket,
        Key=output_csv_key,
        Body=cleaned_csv_buffer.getvalue(),
        ContentType='text/csv'
    )

    # Build the manifest.json required for QuickSight to locate and interpret the CSV file
    manifest = {
        "fileLocations": [
            {
                "URIs": [f"s3://{bucket}/{output_csv_key}"]
            }
        ],
        "globalUploadSettings": {
            "format": "CSV",
            "delimiter": ",",
            "textqualifier": "'",
            "containsHeader": "true"
        }
    }

    # Upload the manifest file to the same folder in S3
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest),
        ContentType='application/json'
    )

    # Final message to indicate success when testing locally or via CloudWatch logs
    print("Cleaned CSV and manifest.json successfully uploaded.")

    # Return success response
    return {
        'statusCode': 200,
        'body': json.dumps('CSV and manifest successfully created for QuickSight')
    }
