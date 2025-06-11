import boto3
import csv
import os
import urllib.parse
import math

# Initializing AWS services: DynamoDB for storing structured analysis results, Comprehend for NLP
dynamodb = boto3.resource('dynamodb')
comprehend = boto3.client('comprehend')
table = dynamodb.Table('CustomerReviewsAnalysis')

# Utility function to batch items (used for batch NLP API calls to Comprehend)
def batch(items, size=25):
    for i in range(0, len(items), size):
        yield items[i:i+size]

def lambda_handler(event, context):
    # Data Ingestion
    # Triggered by S3 file upload: reads customer review data from uploaded CSV
    s3 = boto3.client('s3')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])

    response = s3.get_object(Bucket=bucket, Key=key)
    lines = response['Body'].read().decode('utf-8').splitlines()
    reader = list(csv.DictReader(lines))

    # NLP Review Analysis
    # Extracting review texts for NLP tasks
    texts = [row.get('ReviewText', '') or " " for row in reader]

    # Performing batch sentiment analysis using Amazon Comprehend
    sentiment_results = []
    for batch_texts in batch(texts):
        response = comprehend.batch_detect_sentiment(TextList=batch_texts, LanguageCode='en')
        sentiment_results.extend(response['ResultList'])

    # Performing batch key phrase extraction using Amazon Comprehend
    key_phrases_results = []
    for batch_texts in batch(texts):
        response = comprehend.batch_detect_key_phrases(TextList=batch_texts, LanguageCode='en')
        key_phrases_results.extend(response['ResultList'])

    # Performing batch named entity recognition using Amazon Comprehend
    entities_results = []
    for batch_texts in batch(texts):
        response = comprehend.batch_detect_entities(TextList=batch_texts, LanguageCode='en')
        entities_results.extend(response['ResultList'])

    # Structured Storage
    # Stored review data (sentiment, phrases, entities) in DynamoDB
    for idx, row in enumerate(reader):
        sentiment_data = sentiment_results[idx]
        key_phrases_data = key_phrases_results[idx]
        entity_data = entities_results[idx]

        table.put_item(
            Item={
                'ReviewID': row.get('ReviewID', ''),
                'ProductID': row.get('ProductID', ''),
                'ReviewerName': row.get('reviewerName', ''),
                'Helpful': row.get('helpful', ''),
                'ReviewText': texts[idx],
                'Rating': row.get('Rating', ''),
                'Summary': row.get('summary', ''),
                'ReviewTime': row.get('reviewTime', ''),
                'UnixReviewTime': row.get('unixReviewTime', ''),
                'DayDiff': row.get('day_diff', ''),
                'HelpfulYes': row.get('helpful_yes', ''),
                'TotalVote': row.get('total_vote', ''),
                'Sentiment': sentiment_data['Sentiment'],
                'SentimentScore_Positive': str(sentiment_data['SentimentScore'].get('Positive', '')),
                'SentimentScore_Negative': str(sentiment_data['SentimentScore'].get('Negative', '')),
                'SentimentScore_Neutral': str(sentiment_data['SentimentScore'].get('Neutral', '')),
                'SentimentScore_Mixed': str(sentiment_data['SentimentScore'].get('Mixed', '')),
                'KeyPhrases': [phrase['Text'] for phrase in key_phrases_data.get('KeyPhrases', [])],
                'Entities': [entity['Text'] for entity in entity_data.get('Entities', [])]
                
            }
        )

    return {
        'statusCode': 200,
        'body': f'Processed file {key} from bucket {bucket} using batch processing.'
    }
