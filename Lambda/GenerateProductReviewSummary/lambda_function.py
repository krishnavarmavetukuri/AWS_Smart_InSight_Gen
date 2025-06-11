import boto3
import json
from boto3.dynamodb.conditions import Key
from datetime import datetime

# Using Amazon DynamoDB to store both detailed analysis and summary output
dynamodb = boto3.resource('dynamodb')

# Stores raw review analysis from Comprehend
detailed_table = dynamodb.Table('CustomerReviewsAnalysis')

# Stores generated product-level summary
summary_table = dynamodb.Table('ProductReviewSummaries')

def lambda_handler(event, context):
    # Summary generation process - Insight Generation stage
    product_ids = get_unique_product_ids()
    for product_id in product_ids:
        reviews = get_reviews_by_product(product_id)
        if not reviews:
            continue
        
        # Sentiment aggregation - part of summarizing sentiment trends
        sentiment_counts = {"POSITIVE": 0, "NEGATIVE": 0, "NEUTRAL": 0, "MIXED": 0}
        combined_phrases = []
        for review in reviews:
            sentiment = review.get('Sentiment', 'NEUTRAL')
            sentiment_counts[sentiment] += 1
            
            # Aggregating key phrases from Comprehend - Insight generation step
            key_phrases = review.get('KeyPhrases', [])
            combined_phrases.extend(key_phrases)
        
        # Creating simple text summary based on aggregated key phrases - Summary generation logic
        summary_text = f"Top phrases: {', '.join(set(combined_phrases[:10]))}"
        sentiment_summary = f"Positive: {sentiment_counts['POSITIVE']}, Negative: {sentiment_counts['NEGATIVE']}, Neutral: {sentiment_counts['NEUTRAL']}"
        total_reviews = len(reviews)
        
        # Storing summarized insights into DynamoDB - Final insight storage in structured form
        summary_table.put_item(
            Item={
                'ProductID': product_id,
                'SummaryDate': datetime.now().strftime('%Y-%m-%d'),
                'SummaryText': summary_text,
                'SentimentSummary': sentiment_summary,
                'TotalReviews': total_reviews
            }
        )
    return {
        'statusCode': 200,
        'body': json.dumps('Summaries generated and saved.')
    }

def get_unique_product_ids():
    # Fetching distinct ProductIDs from detailed analysis table - supports grouping for summarization
    response = detailed_table.scan(ProjectionExpression='ProductID')
    items = response.get('Items', [])
    product_ids = set(item['ProductID'] for item in items if 'ProductID' in item)
    
    # Handling pagination of DynamoDB scan - scalable for large datasets
    while 'LastEvaluatedKey' in response:
        response = detailed_table.scan(
            ProjectionExpression='ProductID',
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items = response.get('Items', [])
        product_ids.update(item['ProductID'] for item in items if 'ProductID' in item)
    return list(product_ids)

def get_reviews_by_product(product_id):
    response = detailed_table.scan(
        FilterExpression=Key('ProductID').eq(product_id)
    )
    return response.get('Items', [])
