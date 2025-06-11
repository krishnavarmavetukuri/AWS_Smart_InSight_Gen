import boto3
import csv
import io
import json
import ast

# Initialize the S3 client to read and write files from S3 bucket
s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Set up bucket and file paths for reading input and writing output
    bucket = 'ecommerce-customer-reviews-rawdata'
    input_key = 'dynamodb-export/CustomerReviewsAnalysis.csv'
    output_key = 'dynamodb-export/customer-keyphrases/CustomerReviewAnalysis_KeyPhrases.csv'
    manifest_key = 'dynamodb-export/customer-keyphrases/keyPhrasesManifest.json'

    # Fetch the original CSV file from S3 and decode it line by line
    response = s3.get_object(Bucket=bucket, Key=input_key)
    csv_lines = response['Body'].read().decode('utf-8').splitlines()
    reader = csv.DictReader(csv_lines)

    # Create a StringIO buffer to prepare the cleaned output CSV content
    output_buffer = io.StringIO()
    writer = csv.DictWriter(output_buffer, fieldnames=['ReviewID', 'Sentiment', 'KeyPhrases'], quoting=csv.QUOTE_NONNUMERIC)
    writer.writeheader()

    # Loop through each row and extract individual key phrases
    for row in reader:
        key_phrases_raw = row.get('KeyPhrases', '')
        sentiment = row.get('Sentiment', '')
        review_id = row.get('ReviewID', '')

        # Skip rows that are missing KeyPhrases or Sentiment values
        if not key_phrases_raw or not sentiment:
            continue

        # Attempt to convert the KeyPhrases string into a proper list
        try:
            parsed_list = ast.literal_eval(key_phrases_raw)
            if not isinstance(parsed_list, list):
                continue
        except:
            continue  # Ignore parsing errors and move to next row

        # Process each phrase and split it if it contains commas
        for phrase in parsed_list:
            split_phrases = [p.strip().lower().replace("'", "") for p in phrase.split(',')]
            for p in split_phrases:
                if p:  # Avoid writing empty strings
                    writer.writerow({
                        'ReviewID': review_id,
                        'Sentiment': sentiment,
                        'KeyPhrases': p
                    })

    # Save the cleaned key phrases CSV back to S3
    s3.put_object(
        Bucket=bucket,
        Key=output_key,
        Body=output_buffer.getvalue(),
        ContentType='text/csv'
    )

    # Build the manifest file required for loading data in QuickSight
    manifest = {
        "fileLocations": [
            {
                "URIs": [f"s3://{bucket}/{output_key}"]
            }
        ],
        "globalUploadSettings": {
            "format": "CSV",
            "delimiter": ",",
            "textqualifier": "'",
            "containsHeader": "true"
        }
    }

    # Upload the manifest.json to the same S3 folder
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest),
        ContentType='application/json'
    )

    # Final confirmation message for logs or debugging
    print("KeyPhrases CSV and manifest uploaded successfully.")

    return {
        'statusCode': 200,
        'body': json.dumps('KeyPhrases extraction complete.')
    }
